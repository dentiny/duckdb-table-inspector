include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

file(GLOB_RECURSE CATCHFS_UNITTEST_OBJECTS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(unittest_table_inspector ${CATCHFS_UNITTEST_OBJECTS})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS
   AND NOT PREBUILT_BINARY)
  target_link_libraries(unittest_table_inspector test_helpers duckdb
                        ${EXTENSION_NAME})
else()
  target_link_libraries(unittest_table_inspector test_helpers duckdb_static)
  link_extension_libraries(unittest_table_inspector "")
endif()
