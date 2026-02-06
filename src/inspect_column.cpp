#include "inspect_column.hpp"
#include "util.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// inspect_column() - Show per-segment storage info for a specific column
//===--------------------------------------------------------------------===//

// Filtered segment entry with calculated compressed size
struct FilteredSegmentEntry {
	idx_t row_group_index;
	string column_name;
	LogicalType column_type;
	string compression_type;
	idx_t compressed_size;
	idx_t additional_blocks_size; // Size from additional_blocks (for large segments)
	idx_t row_count;
};

struct InspectColumnBindData : public TableFunctionData {
	InspectColumnBindData(TableCatalogEntry &table_entry, string column_name, LogicalType column_type)
	    : table_entry(table_entry), column_name(std::move(column_name)), column_type(std::move(column_type)) {
	}

	TableCatalogEntry &table_entry;
	string column_name;
	LogicalType column_type;
	vector<FilteredSegmentEntry> filtered_segments;
};

struct InspectColumnState : public GlobalTableFunctionState {
	InspectColumnState() : offset(0) {
	}

	idx_t offset;
};

// Check if segment is a target column's main data segment (not validity bitmap)
bool IsTargetMainDataSegment(const ColumnSegmentInfo &seg, idx_t target_column_id) {
	if (seg.column_id != target_column_id) {
		return false;
	}
	// Skip validity bitmap segments (e.g., "[0, 0]") - only include main data segments ("[column_id]")
	const string expected_path = "[" + std::to_string(seg.column_id) + "]";
	if (seg.column_path != expected_path) {
		return false;
	}
	// Skip non-persistent segments
	if (!seg.persistent || seg.block_id == INVALID_BLOCK) {
		return false;
	}
	return true;
}

// Groups ALL segments by block_id to calculate sizes based on offset differences
vector<FilteredSegmentEntry> FilterAndCalculateSegments(const vector<ColumnSegmentInfo> &all_segments,
                                                        idx_t target_column_id, const string &column_name,
                                                        const LogicalType &column_type, idx_t block_alloc_size) {
	// Build a map of block_id -> all segment offsets (sorted)
	// This includes segments from ALL columns, so we can calculate sizes
	unordered_map<block_id_t, vector<idx_t>> block_to_all_offsets;

	for (const auto &seg : all_segments) {
		if (!seg.persistent || seg.block_id == INVALID_BLOCK) {
			continue;
		}
		block_to_all_offsets[seg.block_id].push_back(seg.block_offset);
	}

	// Sort offsets within each block
	for (auto &block_entry : block_to_all_offsets) {
		std::sort(block_entry.second.begin(), block_entry.second.end());
	}

	vector<FilteredSegmentEntry> entries;
	for (const auto &seg : all_segments) {
		if (!IsTargetMainDataSegment(seg, target_column_id)) {
			continue;
		}

		// Calculate compressed size by looking at the next offset in the same block
		auto &offsets = block_to_all_offsets[seg.block_id];
		auto it = std::find(offsets.begin(), offsets.end(), seg.block_offset);
		D_ASSERT(it != offsets.end());

		idx_t compressed_size;
		if (it + 1 != offsets.end()) {
			// There's a next segment in this block - exact size
			compressed_size = *(it + 1) - seg.block_offset;
		} else {
			// Last segment in block: upper bound
			compressed_size = block_alloc_size - seg.block_offset;
		}

		// Handle additional_blocks for large segments that span multiple blocks
		const idx_t additional_size = seg.additional_blocks.size() * block_alloc_size;

		FilteredSegmentEntry entry;
		entry.row_group_index = seg.row_group_index;
		entry.column_name = column_name;
		entry.column_type = column_type;
		entry.compression_type = seg.compression_type;
		entry.compressed_size = compressed_size;
		entry.additional_blocks_size = additional_size;
		entry.row_count = seg.segment_count;

		entries.push_back(std::move(entry));
	}

	return entries;
}

// Calculate estimated decompressed size
// For fixed-size types: type_size * row_count
// For variable-length types: return 0 (will display as "N/A")
idx_t CalculateEstimatedDecompressedSize(const LogicalType &type, idx_t row_count) {
	const PhysicalType physical_type = type.InternalType();
	if (!TypeIsConstantSize(physical_type)) {
		return 0; // Variable-length type, cannot estimate
	}
	const idx_t type_size = GetTypeIdSize(physical_type);
	return type_size * row_count;
}

unique_ptr<FunctionData> InspectColumnBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(names.empty());
	D_ASSERT(return_types.empty());

	// Define output columns
	names.reserve(7);
	return_types.reserve(7);
	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("column_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("column_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("compressed_size");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("estimated_decompressed_size");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("row_count");
	return_types.emplace_back(LogicalType::BIGINT);

	// Parse input parameters
	const auto database_name = input.inputs[0].GetValue<string>();
	const auto table_name_str = input.inputs[1].GetValue<string>();
	const auto column_name = input.inputs[2].GetValue<string>();

	// Parse table name (handles schema.table format)
	auto qname = QualifiedName::Parse(table_name_str);
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);

	// Use database_name parameter to get the catalog
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, database_name, qname.schema, qname.name);

	// Find the target column
	auto &columns = table_entry.GetColumns();
	if (!columns.ColumnExists(column_name)) {
		throw InvalidInputException("Column '%s' not found in table '%s'", column_name, qname.name);
	}
	const auto &col = columns.GetColumn(column_name);
	const idx_t target_column_id = col.Physical().index;
	const LogicalType column_type = col.Type();

	auto result = make_uniq<InspectColumnBindData>(table_entry, column_name, column_type);

	// Get block allocation size for compressed size calculation
	auto &storage_manager = table_entry.ParentCatalog().GetAttached().GetStorageManager();
	const idx_t block_alloc_size = storage_manager.GetBlockManager().GetBlockAllocSize();

	const auto all_segments = table_entry.GetColumnSegmentInfo();
	result->filtered_segments =
	    FilterAndCalculateSegments(all_segments, target_column_id, column_name, column_type, block_alloc_size);

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InspectColumnInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<InspectColumnState>();
}

void InspectColumnExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<InspectColumnBindData>();
	auto &state = data.global_state->Cast<InspectColumnState>();

	idx_t count = 0;

	constexpr idx_t ROW_GROUP_ID_IDX = 0;
	constexpr idx_t COLUMN_NAME_IDX = 1;
	constexpr idx_t COLUMN_TYPE_IDX = 2;
	constexpr idx_t COMPRESSION_IDX = 3;
	constexpr idx_t COMPRESSED_SIZE_IDX = 4;
	constexpr idx_t ESTIMATED_DECOMPRESSED_SIZE_IDX = 5;
	constexpr idx_t ROW_COUNT_IDX = 6;

	while (state.offset < bind_data.filtered_segments.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.filtered_segments[state.offset];

		output.SetValue(ROW_GROUP_ID_IDX, count, Value::BIGINT(NumericCast<int64_t>(entry.row_group_index)));
		output.SetValue(COLUMN_NAME_IDX, count, Value(entry.column_name));
		output.SetValue(COLUMN_TYPE_IDX, count, Value(entry.column_type.ToString()));
		output.SetValue(COMPRESSION_IDX, count, Value(entry.compression_type));

		// Total compressed size = main block portion + additional blocks
		const idx_t total_compressed_size = entry.compressed_size + entry.additional_blocks_size;
		output.SetValue(COMPRESSED_SIZE_IDX, count, Value(FormatSize(total_compressed_size)));

		const idx_t estimated_size = CalculateEstimatedDecompressedSize(entry.column_type, entry.row_count);
		if (estimated_size > 0) {
			output.SetValue(ESTIMATED_DECOMPRESSED_SIZE_IDX, count, Value(FormatSize(estimated_size)));
		} else {
			output.SetValue(ESTIMATED_DECOMPRESSED_SIZE_IDX, count, Value("N/A"));
		}

		output.SetValue(ROW_COUNT_IDX, count, Value::BIGINT(NumericCast<int64_t>(entry.row_count)));

		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

} // namespace

void RegisterInspectColumnFunction(ExtensionLoader &loader) {
	// Register inspect_column(database_name, table_name, column_name) table function
	TableFunction inspect_column_func("inspect_column",
	                                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                  InspectColumnExecute, InspectColumnBind, InspectColumnInit);
	loader.RegisterFunction(inspect_column_func);
}

} // namespace duckdb
