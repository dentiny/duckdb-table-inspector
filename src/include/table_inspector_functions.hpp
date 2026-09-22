#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterTableInspectorFunctions(ExtensionLoader &loader);

} // namespace duckdb
