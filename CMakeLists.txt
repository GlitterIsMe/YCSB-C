cmake_minimum_required(VERSION 3.14)
project(ycsb)

include(FindThreads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(TBB REQUIRED tbb)

add_subdirectory(redis)

#add_definitions(-DGENERATE_PREFIX_KEY)
option(LINK_FASTER "Support Faster" ON)

if (LINK_FASTER)
    add_subdirectory(FASTER/cc/src)
endif ()

add_executable(ycsb "")
target_sources(ycsb PUBLIC
        "core/client.h"
        "core/const_generator.h"
        "core/core_workload.cc"
        "core/core_workload.h"
        "core/counter_generator.h"
        "core/db.h"
        "core/discrete_generator.h"
        "core/generator.h"
        "core/properties.h"
        "core/scrambled_zipfian_generator.h"
        "core/skewed_latest_generator.h"
        "core/timer.h"
        "core/uniform_generator.h"
        "core/utils.h"
        "core/zipfian_generator.h"

        "db/basic_db.h"
        "db/db_factory.cc"
        "db/db_factory.h"
        "db/hashtable_db.cc"
        "db/hashtable_db.h"
        "db/lock_stl_db.h"
        "db/redis_db.cc"
        "db/redis_db.h"
        "db/tbb_rand_db.h"
        "db/tbb_scan_db.h"
        "db/faster_db.h"
        "db/faster_db.cc"

        "lib/lock_stl_hashtable.h"
        "lib/mem_alloc.h"
        "lib/stl_hashtable.h"
        "lib/string.h"
        "lib/string_hashtable.h"
        "lib/tbb_scan_hashtable.h"
        "lib/tbb_rand_hashtable.h"

        "ycsbc.cc"
        )

target_include_directories(ycsb PUBLIC .)
target_link_libraries(ycsb ${CMAKE_THREAD_LIBS_INIT} ${TBB_LIBRARIES} hiredis)

if (LINK_FASTER)
    target_include_directories(ycsb PUBLIC FASTER/cc)
    target_link_libraries(ycsb faster)
endif ()