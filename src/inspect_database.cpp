#include "inspect_database.hpp"

#include <algorithm>
#include <set>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Helper Functions- Size Formatting
//===--------------------------------------------------------------------===//

static string FormatSize(idx_t bytes) {
	const char *units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
	int32_t unit_idx = 0;
	double size = static_cast<double>(bytes);

	while (size >= 1024.0 && unit_idx < 4) {
		size /= 1024.0;
		unit_idx++;
	}

	char buffer[32];
	if (unit_idx == 0) {
		snprintf(buffer, sizeof(buffer), "%.0f %s", size, units[unit_idx]);
	} else {
		snprintf(buffer, sizeof(buffer), "%.1f %s", size, units[unit_idx]);
	}
	return string(buffer);
}

//===--------------------------------------------------------------------===//
// Table Data Size Calculation - Block Counting Method
//===--------------------------------------------------------------------===//

// Calculates table data size by counting unique blocks used by the table.
// Only counts persistent segments (data checkpointed to disk).
// - Collects all unique block IDs used by table segments.
// - Each block contributes DEFAULT_BLOCK_ALLOC_SIZE  bytes to total size.

static idx_t CalculateTableDataSize(const vector<ColumnSegmentInfo> &segment_info) {
	if (segment_info.empty()) {
		return 0;
	}

	// Collect unique block IDs from all segments
	std::set<block_id_t> unique_blocks;

	for (const auto &seg : segment_info) {
		// Only count persistent segments on real blocks (skip constant segments with block_id = -1)
		if (seg.persistent && seg.block_id >= 0) {
			unique_blocks.insert(seg.block_id);
			// Additional blocks are full blocks
			for (const auto &block_id : seg.additional_blocks) {
				D_ASSERT(block_id >= 0);
				unique_blocks.insert(block_id);
			}
		}
	}

	return unique_blocks.size() * DEFAULT_BLOCK_ALLOC_SIZE;
}

//===--------------------------------------------------------------------===//
// Index Size Calculation
//===--------------------------------------------------------------------===//

static idx_t CalculateIndexSizeFromStorageInfo(const IndexStorageInfo &index_info) {
	// Collect unique block IDs from all allocator infos
	// Use set to avoid counting the same block multiple times
	std::set<block_id_t> unique_blocks;
	for (const auto &alloc_info : index_info.allocator_infos) {
		for (const auto &block_ptr : alloc_info.block_pointers) {
			if (block_ptr.block_id >= 0) {
				unique_blocks.insert(block_ptr.block_id);
			}
		}
	}
	return unique_blocks.size() * DEFAULT_BLOCK_ALLOC_SIZE;
}

//===--------------------------------------------------------------------===//
// Get Total Index Size for a Table
//===--------------------------------------------------------------------===//

// Gets total index size for persistent databases.
//
// This function relies on index storage info loaded from checkpoint metadata.
// Indexes must be in UnboundIndex state (which occurs after attach/reopen)
// for accurate size measurement.

static idx_t GetTableIndexSize(TableCatalogEntry &table) {
	if (!table.IsDuckTable()) {
		return 0;
	}

	auto &duck_table = table.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();
	auto &info = storage.GetDataTableInfo();

	idx_t total_index_bytes = 0;

	// Only consider UnboundIndex - loaded from checkpoint with reliable storage info.
	info->GetIndexes().Scan([&](Index &index) {
		if (!index.IsBound()) {
			auto &unbound = index.Cast<UnboundIndex>();
			total_index_bytes += CalculateIndexSizeFromStorageInfo(unbound.GetStorageInfo());
		}
		return false;
	});

	return total_index_bytes;
}

//===--------------------------------------------------------------------===//
// inspect_database() - List all tables with storage info
//===--------------------------------------------------------------------===//

struct InspectDatabaseData : public GlobalTableFunctionState {
	InspectDatabaseData() : offset(0) {
	}

	struct TableEntry {
		string database_name;
		string schema_name;
		string table_name;
		idx_t row_count;
		idx_t column_count;
		idx_t size_bytes;
	};

	vector<TableEntry> entries;
	idx_t offset;
};

unique_ptr<FunctionData> InspectDatabaseBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(names.empty());
	D_ASSERT(return_types.empty());

	// Define output columns
	names.reserve(7);
	return_types.reserve(7);
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("row_count");
	return_types.emplace_back(LogicalType {LogicalTypeId::BIGINT});
	names.emplace_back("column_count");
	return_types.emplace_back(LogicalType {LogicalTypeId::BIGINT});
	names.emplace_back("size_bytes");
	return_types.emplace_back(LogicalType {LogicalTypeId::BIGINT});
	names.emplace_back("size_format");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> InspectDatabaseInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<InspectDatabaseData>();

	// INVALID_CATALOG retrieves the currently active catalog
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);

	// Check if database is persistent
	// inspect_database() requires a persistent database file because it measures
	// on-disk storage size.
	if (catalog.InMemory()) {
		throw InvalidInputException("inspect_database() requires a persistent database file.\n"
		                            "This tool is designed to analyze the storage size of existing .duckdb files.\n\n"
		                            "Correct usage:\n"
		                            "  1. Open a database file directly:\n"
		                            "     $ duckdb mydata.duckdb\n"
		                            "     D SELECT * FROM inspect_database();\n\n"
		                            "  2. Or attach a database file:\n"
		                            "     D ATTACH 'mydata.duckdb' AS mydb;\n"
		                            "     D USE mydb;\n"
		                            "     D SELECT * FROM inspect_database();\n\n");
	}

	auto schemas = catalog.GetSchemas(context);

	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();

		// Skip internal schemas
		if (DefaultSchemaGenerator::IsDefaultSchema(schema.name)) {
			continue;
		}

		// Scan all tables in this schema
		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			auto &table = entry.Cast<TableCatalogEntry>();

			// Calculate table data size using unique data blocks.
			auto segment_info = table.GetColumnSegmentInfo();
			idx_t data_bytes = CalculateTableDataSize(segment_info);

			// Get index size.
			idx_t index_bytes = GetTableIndexSize(table);

			// Total size = data + indexes.
			idx_t total_bytes = data_bytes + index_bytes;

			// Get row count and column count
			auto storage_info = table.GetStorageInfo(context);
			idx_t row_count = storage_info.cardinality.IsValid() ? storage_info.cardinality.GetIndex() : 0;
			idx_t column_count = table.GetColumns().LogicalColumnCount();

			InspectDatabaseData::TableEntry table_entry;
			table_entry.database_name = table.catalog.GetName();
			table_entry.schema_name = schema.name;
			table_entry.table_name = table.name;
			table_entry.row_count = row_count;
			table_entry.column_count = column_count;
			table_entry.size_bytes = total_bytes;

			result->entries.push_back(std::move(table_entry));
		});
	}

	// Sort tables by size in descending order.
	std::sort(result->entries.begin(), result->entries.end(),
	          [](const InspectDatabaseData::TableEntry &a, const InspectDatabaseData::TableEntry &b) {
		          return a.size_bytes > b.size_bytes;
	          });

	return std::move(result);
}

void InspectDatabaseExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<InspectDatabaseData>();

	idx_t count = 0;

	constexpr idx_t DATABASE_NAME_IDX = 0;
	constexpr idx_t SCHEMA_NAME_IDX = 1;
	constexpr idx_t TABLE_NAME_IDX = 2;
	constexpr idx_t ROW_COUNT_IDX = 3;
	constexpr idx_t COLUMN_COUNT_IDX = 4;
	constexpr idx_t SIZE_BYTES_IDX = 5;
	constexpr idx_t SIZE_FORMAT_IDX = 6;

	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset];

		output.SetValue(DATABASE_NAME_IDX, count, Value(entry.database_name));
		output.SetValue(SCHEMA_NAME_IDX, count, Value(entry.schema_name));
		output.SetValue(TABLE_NAME_IDX, count, Value(entry.table_name));
		output.SetValue(ROW_COUNT_IDX, count, Value::BIGINT(static_cast<int64_t>(entry.row_count)));
		output.SetValue(COLUMN_COUNT_IDX, count, Value::BIGINT(static_cast<int64_t>(entry.column_count)));
		output.SetValue(SIZE_BYTES_IDX, count, Value::BIGINT(static_cast<int64_t>(entry.size_bytes)));
		output.SetValue(SIZE_FORMAT_IDX, count, Value(FormatSize(entry.size_bytes)));

		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

} // namespace

void RegisterInspectDatabaseFunction(ExtensionLoader &loader) {
	// Register inspect_database() table function
	TableFunction inspect_database_func("inspect_database", {}, InspectDatabaseExecute, InspectDatabaseBind,
	                                    InspectDatabaseInit);
	loader.RegisterFunction(inspect_database_func);
}

} // namespace duckdb
