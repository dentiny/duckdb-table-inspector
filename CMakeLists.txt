cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME table_inspector)

set(CMAKE_CXX_STANDARD 17)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/inspect_column.cpp src/inspect_database.cpp src/inspect_storage.cpp
    src/inspect_storage_summary.cpp src/table_inspector_extension.cpp
    src/util.cpp)

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wunused-variable -Wunused-function -Wunused-but-set-variable"
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(${BUILD_UNITTESTS})
  add_subdirectory(test/unittest)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
