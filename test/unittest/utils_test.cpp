
#include "catch/catch.hpp"

#include "util.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("FormatSize formats byte counts", "[util]") {
	// Zero bytes.
	REQUIRE(FormatSize(0) == "0 B");

	// Bytes (no decimal).
	REQUIRE(FormatSize(1) == "1 B");
	REQUIRE(FormatSize(512) == "512 B");
	REQUIRE(FormatSize(1023) == "1023 B");

	// KiB (one decimal place).
	REQUIRE(FormatSize(1024) == "1.0 KiB");
	REQUIRE(FormatSize(1536) == "1.5 KiB");

	// MiB.
	REQUIRE(FormatSize(1048576) == "1.0 MiB");

	// GiB.
	REQUIRE(FormatSize(1073741824) == "1.0 GiB");

	// TiB.
	REQUIRE(FormatSize(1099511627776) == "1.0 TiB");
}
