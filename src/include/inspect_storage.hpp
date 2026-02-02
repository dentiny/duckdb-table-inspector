#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterInspectStorageFunction(ExtensionLoader &loader);

} // namespace duckdb
