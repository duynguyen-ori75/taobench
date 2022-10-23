cmake_minimum_required(VERSION 3.7)

project(taobench)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

option(WITH_CRDB OFF)
option(WITH_MYSQL OFF)
option(WITH_SPANNER OFF)
option(WITH_YUGABYTE OFF)
option(WITH_WIREDTIGER OFF)

include_directories(src)
file(GLOB SOURCES src/*.h src/*.cc)
add_executable(taobench ${SOURCES})

if(WITH_CRDB)
  include_directories(crdb)
  target_sources(taobench PRIVATE crdb/crdb_db.h crdb/crdb_db.cc)
  target_link_libraries(taobench -lpqxx)
  target_link_libraries(taobench -lpq)
endif()

if(WITH_MYSQL)
  include_directories(mysqldb)
  target_sources(taobench PRIVATE mysqldb/mysql_db.h mysqldb/mysql_db.cc)
  target_link_libraries(taobench -lmysqlclient)
endif()

if(WITH_SPANNER)
  include_directories(spannerdb)
  target_sources(taobench PRIVATE spannerdb/spanner_db.h
                                  spannerdb/spanner_db.cc)
  target_link_libraries(taobench google-cloud-cpp::spanner)
endif()

if(WITH_YUGABYTE)
  include_directories(yugabytedb)
  target_sources(taobench PRIVATE yugabytedb/yugabytedb.h
                                  yugabytedb/yugabytedb.cc)
  target_link_libraries(taobench -lpqxx)
  target_link_libraries(taobench -lpq)
endif()

if(WITH_WIREDTIGER)
  target_sources(taobench PRIVATE wiredtigerdb/wt_db.h wiredtigerdb/wt_db.cc)
  target_link_libraries(taobench wiredtiger)
endif()

set(UTILS_DIR "${CMAKE_SOURCE_DIR}/utils")
string(CONCAT BUILD_FORMAT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/src,"
              "${CMAKE_CURRENT_SOURCE_DIR}/wiredtigerdb,"
              "${CMAKE_CURRENT_SOURCE_DIR}/crdb,"
              "${CMAKE_CURRENT_SOURCE_DIR}/mysqldb,"
              "${CMAKE_CURRENT_SOURCE_DIR}/spannerdb,"
              "${CMAKE_CURRENT_SOURCE_DIR}/yugabytedb,")

# clang-format
find_program(CLANG_FORMAT_BIN NAMES clang-format clang-format-14)
if("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
  message(WARNING "Couldn't find clang-format.")
else()
  message(STATUS "Clang-format found at ${CLANG_FORMAT_BIN}")
endif()

# Runs clang format and updates files in place.
add_custom_target(format ${UTILS_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN}
                         --source_dirs ${BUILD_FORMAT_DIRS} --fix --quiet)

# Runs clang format and exits with a non-zero exit code if any files need to be
# reformatted
add_custom_target(
  check-format ${UTILS_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN}
               --source_dirs ${BUILD_FORMAT_DIRS} --quiet)
