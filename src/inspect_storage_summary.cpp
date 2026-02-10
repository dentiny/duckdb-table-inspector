#include "inspect_storage_summary.hpp"
#include "util.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// inspect_storage_summary() - File-level storage breakdown
//===--------------------------------------------------------------------===//

// Breaks down a .duckdb file into 4 non-overlapping components:
// table_data, index, metadata, free_blocks.

struct StorageSummaryEntry {
	string component;
	idx_t block_count;
	idx_t size_bytes;
	string percentage;
};

struct InspectStorageSummaryBindData : public TableFunctionData {
	explicit InspectStorageSummaryBindData(string database_name_p) : database_name(std::move(database_name_p)) {
	}

	string database_name;
};

struct InspectStorageSummaryState : public GlobalTableFunctionState {
	InspectStorageSummaryState() : offset(0) {
	}

	vector<StorageSummaryEntry> entries;
	idx_t offset;
};

// Collect all unique block IDs used by table data across all tables
idx_t CollectTableDataBlocks(ClientContext &context, Catalog &catalog) {
	unordered_set<block_id_t> unique_blocks;

	auto schemas = catalog.GetSchemas(context);
	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();

		// Skip internal schemas
		if (DefaultSchemaGenerator::IsDefaultSchema(schema.name)) {
			continue;
		}

		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			auto &table = entry.Cast<TableCatalogEntry>();
			const auto segment_info = table.GetColumnSegmentInfo();

			for (const auto &seg : segment_info) {
				if (seg.persistent && seg.block_id != INVALID_BLOCK) {
					unique_blocks.insert(seg.block_id);
					for (const auto &block_id : seg.additional_blocks) {
						D_ASSERT(block_id != INVALID_BLOCK);
						unique_blocks.insert(block_id);
					}
				}
			}
		});
	}

	return unique_blocks.size();
}

// Count physical metadata blocks
// Each MetadataBlockInfo entry represents one physical block.
idx_t CountMetadataBlocks(const vector<MetadataBlockInfo> &metadata_info) {
	return metadata_info.size();
}

// Shared bind logic for all inspect_storage_summary overloads
unique_ptr<FunctionData> InspectStorageSummaryBindInternal(ClientContext &context, const string &database_name,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(names.empty());
	D_ASSERT(return_types.empty());

	// Define output columns
	names.reserve(4);
	return_types.reserve(4);
	names.emplace_back("component");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("size");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("percentage");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("block_count");
	return_types.emplace_back(LogicalType {LogicalTypeId::BIGINT});

	return make_uniq<InspectStorageSummaryBindData>(database_name);
}

// inspect_storage_summary(database_name)
unique_ptr<FunctionData> InspectStorageSummaryBindWithDatabase(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	const auto database_name = input.inputs[0].GetValue<string>();
	return InspectStorageSummaryBindInternal(context, database_name, return_types, names);
}

// inspect_storage_summary() — uses current database
unique_ptr<FunctionData> InspectStorageSummaryBindCurrentDB(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	return InspectStorageSummaryBindInternal(context, INVALID_CATALOG, return_types, names);
}

unique_ptr<GlobalTableFunctionState> InspectStorageSummaryInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<InspectStorageSummaryState>();

	auto &bind_data = input.bind_data->Cast<InspectStorageSummaryBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.database_name);

	// Require persistent database
	if (catalog.InMemory()) {
		throw InvalidInputException(
		    "inspect_storage_summary() requires a persistent database file.\n"
		    "This tool is designed to analyze the storage breakdown of existing .duckdb files.\n\n"
		    "Correct usage:\n"
		    "  1. Open a database file directly:\n"
		    "     $ duckdb mydata.duckdb\n"
		    "     D SELECT * FROM inspect_storage_summary();\n\n"
		    "  2. Or attach a database file:\n"
		    "     D ATTACH 'mydata.duckdb' AS mydb;\n"
		    "     D SELECT * FROM inspect_storage_summary('mydb');\n\n");
	}

	// Get database size info
	const auto ds = catalog.GetDatabaseSize(context);
	const idx_t total_blocks = ds.total_blocks;
	const idx_t free_blocks = ds.free_blocks;
	const idx_t block_alloc_size = ds.block_size;

	// Count metadata blocks
	const auto metadata_info = catalog.GetMetadataInfo(context);
	const idx_t metadata_blocks = CountMetadataBlocks(metadata_info);

	// Count table data blocks (unique block IDs across all tables)
	const idx_t table_data_blocks = CollectTableDataBlocks(context, catalog);

	// Index blocks = total - table_data - metadata - free_blocks
	// TODO: count index blocks directly once IndexStorageInfo updates correctly after checkpoint.
	const idx_t index_blocks = total_blocks - table_data_blocks - metadata_blocks - free_blocks;

	// Build entries
	result->entries.reserve(5);

	StorageSummaryEntry table_data_entry;
	table_data_entry.component = "table_data";
	table_data_entry.block_count = table_data_blocks;
	table_data_entry.size_bytes = table_data_blocks * block_alloc_size;
	table_data_entry.percentage = FormatPercentage(table_data_blocks, total_blocks);
	result->entries.push_back(std::move(table_data_entry));

	StorageSummaryEntry index_entry;
	index_entry.component = "index";
	index_entry.block_count = index_blocks;
	index_entry.size_bytes = index_blocks * block_alloc_size;
	index_entry.percentage = FormatPercentage(index_blocks, total_blocks);
	result->entries.push_back(std::move(index_entry));

	StorageSummaryEntry metadata_entry;
	metadata_entry.component = "metadata";
	metadata_entry.block_count = metadata_blocks;
	metadata_entry.size_bytes = metadata_blocks * block_alloc_size;
	metadata_entry.percentage = FormatPercentage(metadata_blocks, total_blocks);
	result->entries.push_back(std::move(metadata_entry));

	StorageSummaryEntry free_blocks_entry;
	free_blocks_entry.component = "free_blocks";
	free_blocks_entry.block_count = free_blocks;
	free_blocks_entry.size_bytes = free_blocks * block_alloc_size;
	free_blocks_entry.percentage = FormatPercentage(free_blocks, total_blocks);
	result->entries.push_back(std::move(free_blocks_entry));

	StorageSummaryEntry total_entry;
	total_entry.component = "total";
	total_entry.block_count = total_blocks;
	total_entry.size_bytes = total_blocks * block_alloc_size;
	total_entry.percentage = FormatPercentage(total_blocks, total_blocks);
	result->entries.push_back(std::move(total_entry));

	return std::move(result);
}

void InspectStorageSummaryExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<InspectStorageSummaryState>();

	idx_t count = 0;

	constexpr idx_t COMPONENT_IDX = 0;
	constexpr idx_t SIZE_IDX = 1;
	constexpr idx_t PERCENTAGE_IDX = 2;
	constexpr idx_t BLOCK_COUNT_IDX = 3;

	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset];

		output.SetValue(COMPONENT_IDX, count, Value(entry.component));
		output.SetValue(SIZE_IDX, count, Value(FormatSize(entry.size_bytes)));
		output.SetValue(PERCENTAGE_IDX, count, Value(entry.percentage));
		output.SetValue(BLOCK_COUNT_IDX, count, Value::BIGINT(NumericCast<int64_t>(entry.block_count)));

		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

} // namespace

void RegisterInspectStorageSummaryFunction(ExtensionLoader &loader) {
	// inspect_storage_summary(database_name)
	TableFunction inspect_storage_summary_with_db("inspect_storage_summary", {LogicalType {LogicalTypeId::VARCHAR}},
	                                              InspectStorageSummaryExecute, InspectStorageSummaryBindWithDatabase,
	                                              InspectStorageSummaryInit);
	loader.RegisterFunction(std::move(inspect_storage_summary_with_db));

	// inspect_storage_summary() — uses current database
	TableFunction inspect_storage_summary_current_db("inspect_storage_summary", {}, InspectStorageSummaryExecute,
	                                                 InspectStorageSummaryBindCurrentDB, InspectStorageSummaryInit);
	loader.RegisterFunction(std::move(inspect_storage_summary_current_db));
}

} // namespace duckdb
