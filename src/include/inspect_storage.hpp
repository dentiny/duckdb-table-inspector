#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterInspectStorageFunction(ExtensionLoader &loader);

} // namespace duckdb
