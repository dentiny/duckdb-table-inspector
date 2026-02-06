#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterInspectColumnFunction(ExtensionLoader &loader);

} // namespace duckdb
