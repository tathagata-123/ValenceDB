cmake_minimum_required(VERSION 3.20)
project(ValenceDB VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(VALENCEDB_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

add_library(valencedb STATIC
    src/common/version.cpp
    src/types/schema.cpp
    src/types/tuple.cpp
    src/storage/page_codec.cpp
    src/storage/disk_manager.cpp
    src/storage/slotted_page.cpp
    src/storage/heap_file.cpp
    src/storage/catalog.cpp
    src/storage/database.cpp
    src/buffer/buffer_pool.cpp
    src/buffer/page_guard.cpp
    src/buffer/scan_ring.cpp
    src/index/bplus_page.cpp
    src/index/bplus_tree.cpp
    src/index/bplus_iterator.cpp
    src/execution/work_budget.cpp
    src/execution/plan.cpp
    src/execution/executor.cpp
    src/execution/basic_executors.cpp
    src/execution/temp_relation.cpp
    src/execution/external_sort.cpp
    src/execution/nested_loop_join.cpp
    src/execution/block_nested_loop_join.cpp
    src/execution/temp_scan.cpp
    src/execution/grace_hash_join.cpp
    src/execution/aggregate.cpp
    src/optimizer/statistics.cpp
    src/optimizer/optimizer.cpp
)
target_include_directories(valencedb PUBLIC include)

add_executable(valencedb_tests tests/smoke_test.cpp)
target_link_libraries(valencedb_tests PRIVATE valencedb)

set(VALENCEDB_TEST_TARGETS
    valencedb_storage_primitives_tests
    valencedb_buffer_pool_tests
    valencedb_heap_catalog_tests
    valencedb_bplus_insert_tests
    valencedb_bplus_delete_tests
    valencedb_execution_tests
    valencedb_external_sort_tests
    valencedb_nested_loop_join_tests
    valencedb_grace_hash_join_tests
    valencedb_aggregation_tests
    valencedb_optimizer_tests
    valencedb_integration_tests
)

add_executable(valencedb_storage_primitives_tests tests/storage_primitives_test.cpp)
add_executable(valencedb_buffer_pool_tests tests/buffer_pool_test.cpp)
add_executable(valencedb_heap_catalog_tests tests/heap_catalog_test.cpp)
add_executable(valencedb_bplus_insert_tests tests/bplus_insert_test.cpp)
add_executable(valencedb_bplus_delete_tests tests/bplus_delete_test.cpp)
add_executable(valencedb_execution_tests tests/execution_test.cpp)
add_executable(valencedb_external_sort_tests tests/external_sort_test.cpp)
add_executable(valencedb_nested_loop_join_tests tests/nested_loop_join_test.cpp)
add_executable(valencedb_grace_hash_join_tests tests/grace_hash_join_test.cpp)
add_executable(valencedb_aggregation_tests tests/aggregation_test.cpp)
add_executable(valencedb_optimizer_tests tests/optimizer_test.cpp)
add_executable(valencedb_integration_tests tests/integration_test.cpp)

foreach(target IN LISTS VALENCEDB_TEST_TARGETS)
    target_link_libraries(${target} PRIVATE valencedb)
endforeach()

add_executable(valencedb_bench bench/valencedb_bench.cpp)
target_link_libraries(valencedb_bench PRIVATE valencedb)
target_compile_definitions(valencedb_bench PRIVATE
    VALENCEDB_BENCH_BUILD_TYPE="$<CONFIG>"
    VALENCEDB_BENCH_COMPILER="${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}"
    VALENCEDB_BENCH_PLATFORM="${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_PROCESSOR}"
)

set(VALENCEDB_ALL_TARGETS valencedb valencedb_tests valencedb_bench ${VALENCEDB_TEST_TARGETS})
foreach(target IN LISTS VALENCEDB_ALL_TARGETS)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4)
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    if(VALENCEDB_ENABLE_SANITIZERS)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
            target_link_options(${target} PRIVATE -fsanitize=address,undefined)
        else()
            message(FATAL_ERROR "Sanitizers require Clang or GCC")
        endif()
    endif()
endforeach()

enable_testing()
add_test(NAME valencedb_smoke COMMAND valencedb_tests)
add_test(NAME valencedb_storage_primitives COMMAND valencedb_storage_primitives_tests)
add_test(NAME valencedb_buffer_pool COMMAND valencedb_buffer_pool_tests)
add_test(NAME valencedb_heap_catalog COMMAND valencedb_heap_catalog_tests)
add_test(NAME valencedb_bplus_insert COMMAND valencedb_bplus_insert_tests)
add_test(NAME valencedb_bplus_delete COMMAND valencedb_bplus_delete_tests)
add_test(NAME valencedb_execution COMMAND valencedb_execution_tests)
add_test(NAME valencedb_external_sort COMMAND valencedb_external_sort_tests)
add_test(NAME valencedb_nested_loop_join COMMAND valencedb_nested_loop_join_tests)
add_test(NAME valencedb_grace_hash_join COMMAND valencedb_grace_hash_join_tests)
add_test(NAME valencedb_aggregation COMMAND valencedb_aggregation_tests)
add_test(NAME valencedb_optimizer COMMAND valencedb_optimizer_tests)
add_test(NAME valencedb_integration COMMAND valencedb_integration_tests)
