#define DUCKDB_EXTENSION_MAIN

#include "table_inspector_extension.hpp"

#include "inspect_column.hpp"
#include "inspect_database.hpp"
#include "inspect_storage.hpp"
#include "inspect_block_usage.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	RegisterInspectColumnFunction(loader);
	RegisterInspectDatabaseFunction(loader);
	RegisterInspectStorageFunction(loader);
	RegisterInspectBlockUsageFunction(loader);
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
