#include "duckdb/common/assert.hpp"
#define DUCKDB_EXTENSION_MAIN

#include "table_inspector_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// inspect_database() - List all tables
//===--------------------------------------------------------------------===//

struct InspectDatabaseData : public GlobalTableFunctionState {
	InspectDatabaseData() : offset(0) {
	}

	struct TableEntry {
		string database_name;
		string schema_name;
		string table_name;
	};

	vector<TableEntry> entries;
	idx_t offset;
};

unique_ptr<FunctionData> InspectDatabaseBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {

	D_ASSERT(names.empty());
	D_ASSERT(return_types.empty());

	//Define output columns
	names.reserve(7);
	return_types.reserve(7);
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("row_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("column_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("size_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("size_format");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> InspectDatabaseInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<InspectDatabaseData>();

	// INVALID_CATALOG is an empty string ("") that tells GetCatalog to retrieve schemas from all accessible catalogs
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto schemas = catalog.GetSchemas(context);

	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();

		// Skip internal schemas (pg_catalog, information_schema)
		if (DefaultSchemaGenerator::IsDefaultSchema(schema.name)) {
			continue;
		}

		// Scan all tables in this schema
		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			auto &table = entry.Cast<TableCatalogEntry>();

			// Add entry
			InspectDatabaseData::TableEntry table_entry;
			table_entry.database_name = table.catalog.GetName();
			table_entry.schema_name = schema.name;
			table_entry.table_name = table.name;

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
	constexpr idx_t ROW_COUNT_IDX = 3;
	constexpr idx_t COLUMN_COUNT_IDX = 4;
	constexpr idx_t SIZE_BYTES_IDX = 5;
	constexpr idx_t SIZE_FORMAT_IDX = 6;

	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset];

		output.SetValue(DATABASE_NAME_IDX, count, Value(entry.database_name));
		output.SetValue(SCHEMA_NAME_IDX, count, Value(entry.schema_name));
		output.SetValue(TABLE_NAME_IDX, count, Value(entry.table_name));
		output.SetValue(ROW_COUNT_IDX, count, Value::BIGINT(0));
		output.SetValue(COLUMN_COUNT_IDX, count, Value::BIGINT(0));
		output.SetValue(SIZE_BYTES_IDX, count, Value::BIGINT(0));
		output.SetValue(SIZE_FORMAT_IDX, count, Value("0 B"));

		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

void LoadInternal(ExtensionLoader &loader) {
	// Register inspect_database() table function
	TableFunction inspect_database_func("inspect_database", {}, InspectDatabaseExecute, InspectDatabaseBind,
	                                    InspectDatabaseInit);
	loader.RegisterFunction(inspect_database_func);
}

} // namespace

void TableInspectorExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string TableInspectorExtension::Name() {
	return "table_inspector";
}

std::string TableInspectorExtension::Version() const {
#ifdef EXT_VERSION_TABLE_INSPECTOR
	return EXT_VERSION_TABLE_INSPECTOR;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(table_inspector, loader) {
	duckdb::LoadInternal(loader);
}
}
