# Build and statically register the extension for both DuckDB and unit tests.
duckdb_extension_load(table_inspector SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
                      EXTENSION_VERSION 0.2.0 LOAD_TESTS)
duckdb_extension_statically_link(table_inspector)
