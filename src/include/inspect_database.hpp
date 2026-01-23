#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterInspectDatabaseFunction(ExtensionLoader &loader);

} // namespace duckdb
