#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Formats a byte count into a human-readable string.
// Output format: B, KiB, MiB, GiB, TiB (1024-based).
// Precision: bytes are integers (e.g., "512 B"), larger units have one decimal place (e.g., "1.5 MiB").
string FormatSize(idx_t bytes);

} // namespace duckdb
