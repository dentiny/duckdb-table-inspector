#define DUCKDB_EXTENSION_MAIN

#include "table_inspector_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//--------------------------------------------------------------------------------------------------//
// inspect_database() - List all tables
//--------------------------------------------------------------------------------------------------//
//

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

static unique_ptr<FunctionData> InspectDatabaseBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// Define output columns
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

static unique_ptr<GlobalTableFunctionState> InspectDatabaseInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<InspectDatabaseData>();

	// Get all schemas from all databases
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto schemas = catalog.GetSchemas(context);

	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();

		// Skip internal schemas (as defined in duckdb/src/catalog/default/default_schemas.cpp)
		// DuckDB only has two internal schemas: pg_catalog and information_schema
		if (schema.name == "pg_catalog" || schema.name == "information_schema") {
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

static void InspectDatabaseExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<InspectDatabaseData>();

	idx_t count = 0;

	constexpr idx_t database_name_idx = 0;
	constexpr idx_t schema_name_idx = 1;
	constexpr idx_t table_name_idx = 2;
	constexpr idx_t row_count_idx = 3;
	constexpr idx_t column_count_idx = 4;
	constexpr idx_t size_bytes_idx = 5;
	constexpr idx_t size_format_idx = 6;

	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset];

		output.SetValue(database_name_idx, count, Value(entry.database_name));
		output.SetValue(schema_name_idx, count, Value(entry.schema_name));
		output.SetValue(table_name_idx, count, Value(entry.table_name));
		output.SetValue(row_count_idx, count, Value::BIGINT(0));
		output.SetValue(column_count_idx, count, Value::BIGINT(0));
		output.SetValue(size_bytes_idx, count, Value::BIGINT(0));
		output.SetValue(size_format_idx, count, Value("0 B"));

		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register inspect_database() table function
	TableFunction inspect_database_func("inspect_database", {}, InspectDatabaseExecute, InspectDatabaseBind,
	                                    InspectDatabaseInit);
	loader.RegisterFunction(inspect_database_func);
}

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
