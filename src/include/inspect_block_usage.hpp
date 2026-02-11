#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterInspectBlockUsageFunction(ExtensionLoader &loader);

} // namespace duckdb
