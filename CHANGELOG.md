# 0.1.0

## Added

- Add `inspect_database()` table function to list all tables with persisted data size ([#3], [#6])
- Add `inspect_column()` table function for per-segment storage details ([#16])
- Add `inspect_storage()` table function to list attached databases with file sizes ([#14])
- Add `inspect_block_usage()` table function for storage breakdown by component ([#19], [#20])
- Add explicit database name overload to `inspect_database()` ([#21])
- Add `index_bytes` column to `inspect_database()` to report per-table index size ([#30])
- Add extension description displayed in `duckdb_extensions()` output
- Add README with usage docs and examples ([#22])

[#3]: https://github.com/dentiny/duckdb-table-inspector/pull/3
[#6]: https://github.com/dentiny/duckdb-table-inspector/pull/6
[#14]: https://github.com/dentiny/duckdb-table-inspector/pull/14
[#16]: https://github.com/dentiny/duckdb-table-inspector/pull/16
[#19]: https://github.com/dentiny/duckdb-table-inspector/pull/19
[#20]: https://github.com/dentiny/duckdb-table-inspector/pull/20
[#21]: https://github.com/dentiny/duckdb-table-inspector/pull/21
[#22]: https://github.com/dentiny/duckdb-table-inspector/pull/22
[#30]: https://github.com/dentiny/duckdb-table-inspector/pull/30

## Fixed

- Reject in-memory databases in `inspect_database()` ([#5])
- Fix MSVC build by guarding GCC/Clang warning flags ([#23])

[#5]: https://github.com/dentiny/duckdb-table-inspector/pull/5
[#23]: https://github.com/dentiny/duckdb-table-inspector/pull/23

## Improved

- Bump duckdb and extension-ci-tools submodules to v1.5.0 ([#31])

## Changed

- Return raw bytes for all size columns instead of formatted strings ([#27])
- Replace `FormatSize` with DuckDB's built-in API ([#26])

[#26]: https://github.com/dentiny/duckdb-table-inspector/pull/26
[#27]: https://github.com/dentiny/duckdb-table-inspector/pull/27
[#31]: https://github.com/dentiny/duckdb-table-inspector/issues/31
