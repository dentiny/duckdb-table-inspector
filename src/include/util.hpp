#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

// Formats a byte count into a human-readable string.
// Uses the largest 1024-based (IEC) unit (B, KiB, MiB, GiB, TiB) where the value is >= 1.0.
// Precision: bytes are integers (e.g., "512 B"), larger units have one decimal place (e.g., "1.5 MiB").
string FormatSize(idx_t bytes);

// Formats a block count as a percentage of total blocks (e.g., "50.0%").
// Guards against division by zero: returns "0.0%" when total_blocks is zero.
string FormatPercentage(idx_t blocks, idx_t total_blocks);

// Counts unique persistent block IDs from column segment info.
idx_t CountUniqueBlocks(const vector<ColumnSegmentInfo> &segment_info);

} // namespace duckdb
