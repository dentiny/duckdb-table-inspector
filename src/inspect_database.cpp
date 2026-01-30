#include "inspect_database.hpp"
#include "util.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Table Data Size Calculation - Block Counting Method
//===--------------------------------------------------------------------===//

// Calculates table data size by counting unique blocks used by the table.
// Only counts persistent segments (data checkpointed to disk).
// - Collects all unique block IDs used by table segments.
// - Each block contributes the database's configured block allocation size.

idx_t CalculateTableDataSize(const vector<ColumnSegmentInfo> &segment_info, TableCatalogEntry &table) {
	if (segment_info.empty()) {
		return 0;
	}

	// Collect unique block IDs from all segments
	unordered_set<block_id_t> unique_blocks;

	for (const auto &seg : segment_info) {
		// Only count persistent segments on real blocks (skip constant segments)
		if (seg.persistent && seg.block_id != INVALID_BLOCK) {
			unique_blocks.insert(seg.block_id);
			// Additional blocks are full blocks
			for (const auto &block_id : seg.additional_blocks) {
				D_ASSERT(block_id != INVALID_BLOCK);
				unique_blocks.insert(block_id);
			}
		}
	}

	// Get actual block allocation size from storage manager
	auto &storage_manager = table.ParentCatalog().GetAttached().GetStorageManager();
	const idx_t block_alloc_size = storage_manager.GetBlockManager().GetBlockAllocSize();
	return unique_blocks.size() * block_alloc_size;
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
		idx_t persisted_data_size_bytes = 0;
	};

	vector<TableEntry> entries;
	idx_t offset;
};

unique_ptr<FunctionData> InspectDatabaseBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(names.empty());
	D_ASSERT(return_types.empty());

	// Define output columns
	names.reserve(4);
	return_types.reserve(4);
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("persisted_data_size");
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
			const auto segment_info = table.GetColumnSegmentInfo();
			const idx_t data_bytes = CalculateTableDataSize(segment_info, table);

			InspectDatabaseData::TableEntry table_entry;
			table_entry.database_name = table.ParentCatalog().GetName();
			table_entry.schema_name = schema.name;
			table_entry.table_name = table.name;
			table_entry.persisted_data_size_bytes = data_bytes;

			result->entries.push_back(std::move(table_entry));
		});
	}

	return std::move(result);
}

void InspectDatabaseExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<InspectDatabaseData>();

	idx_t count = 0;

	constexpr idx_t DATABASE_NAME_IDX = 0;
	constexpr idx_t SCHEMA_NAME_IDX = 1;
	constexpr idx_t TABLE_NAME_IDX = 2;
	constexpr idx_t DATA_SIZE_IDX = 3;

	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset];

		output.SetValue(DATABASE_NAME_IDX, count, Value(entry.database_name));
		output.SetValue(SCHEMA_NAME_IDX, count, Value(entry.schema_name));
		output.SetValue(TABLE_NAME_IDX, count, Value(entry.table_name));
		output.SetValue(DATA_SIZE_IDX, count, Value(FormatSize(entry.persisted_data_size_bytes)));

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
