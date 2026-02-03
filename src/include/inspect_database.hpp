#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterInspectDatabaseFunction(ExtensionLoader &loader);

} // namespace duckdb
