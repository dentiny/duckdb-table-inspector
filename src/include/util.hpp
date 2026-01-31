#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Formats a byte count into a human-readable string.
// Uses the largest 1024-based (IEC) unit (B, KiB, MiB, GiB, TiB) where the value is >= 1.0.
// Precision: bytes are integers (e.g., "512 B"), larger units have one decimal place (e.g., "1.5 MiB").
string FormatSize(idx_t bytes);

} // namespace duckdb
