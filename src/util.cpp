#include "util.hpp"

#include "duckdb/common/array.hpp"

namespace duckdb {

string FormatSize(idx_t bytes) {
	constexpr array<const char *, 5> units = {"B", "KiB", "MiB", "GiB", "TiB"};
	int32_t unit_idx = 0;
	double size = static_cast<double>(bytes);

	while (size >= 1024.0 && unit_idx < 4) {
		size /= 1024.0;
		unit_idx++;
	}

	array<char, 32> buffer;
	if (unit_idx == 0) {
		snprintf(buffer.data(), buffer.size(), "%.0f %s", size, units[unit_idx]);
	} else {
		snprintf(buffer.data(), buffer.size(), "%.1f %s", size, units[unit_idx]);
	}
	return string(buffer.data());
}

} // namespace duckdb
