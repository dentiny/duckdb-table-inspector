#include "inspect_storage.hpp"
#include "util.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// inspect_storage() - List all attached databases with file sizes
//===--------------------------------------------------------------------===//

// Lists all persistent (non-system, non-temporary, non-in-memory) attached
// databases and reports their .duckdb file size and WAL file size.
// Both sizes are retrieved from Catalog::GetDatabaseSize().

struct InspectStorageData : public GlobalTableFunctionState {
	InspectStorageData() : offset(0) {
	}

	struct DatabaseEntry {
		string database_name;
		idx_t database_file_size_bytes = 0;
		idx_t wal_file_size_bytes = 0;
	};

	vector<DatabaseEntry> entries;
	idx_t offset;
};

unique_ptr<FunctionData> InspectStorageBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(names.empty());
	D_ASSERT(return_types.empty());

	names.reserve(3);
	return_types.reserve(3);
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("database_file_size");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("wal_file_size");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> InspectStorageInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<InspectStorageData>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		if (db->IsSystem() || db->IsTemporary() || db->GetCatalog().InMemory()) {
			continue;
		}

		auto ds = db->GetCatalog().GetDatabaseSize(context);

		InspectStorageData::DatabaseEntry entry;
		entry.database_name = db->GetName();
		entry.database_file_size_bytes = static_cast<idx_t>(ds.bytes);
		entry.wal_file_size_bytes = static_cast<idx_t>(ds.wal_size);
		result->entries.push_back(std::move(entry));
	}

	return std::move(result);
}

void InspectStorageExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<InspectStorageData>();

	idx_t count = 0;

	constexpr idx_t DATABASE_NAME_IDX = 0;
	constexpr idx_t DATABASE_FILE_SIZE_IDX = 1;
	constexpr idx_t WAL_FILE_SIZE_IDX = 2;

	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset];

		output.SetValue(DATABASE_NAME_IDX, count, Value(entry.database_name));
		output.SetValue(DATABASE_FILE_SIZE_IDX, count, Value(FormatSize(entry.database_file_size_bytes)));
		output.SetValue(WAL_FILE_SIZE_IDX, count, Value(FormatSize(entry.wal_file_size_bytes)));

		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

} // namespace

void RegisterInspectStorageFunction(ExtensionLoader &loader) {
	// Register inspect_storage() table function
	TableFunction inspect_storage_func("inspect_storage", {}, InspectStorageExecute, InspectStorageBind,
	                                   InspectStorageInit);
	loader.RegisterFunction(inspect_storage_func);
}

} // namespace duckdb
