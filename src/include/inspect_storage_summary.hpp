#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterInspectStorageSummaryFunction(ExtensionLoader &loader);

} // namespace duckdb
