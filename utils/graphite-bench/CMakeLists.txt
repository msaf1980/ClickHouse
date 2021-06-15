add_executable(graphite-rollup-bench graphite-rollup-bench.cpp)
target_link_libraries(
    graphite-rollup-bench
    PRIVATE
    clickhouse_storages_system
    clickhouse_aggregate_functions
)
target_include_directories(
    graphite-rollup-bench
    PRIVATE
    ${ClickHouse_SOURCE_DIR}/src ${CMAKE_BINARY_DIR}/src
    ${ClickHouse_SOURCE_DIR}/base ${ClickHouse_SOURCE_DIR}/base/pcg-random
    ${CMAKE_BINARY_DIR}/src/Core/include
    ${POCO_INCLUDE_DIR}
    ${ClickHouse_SOURCE_DIR}/contrib/double-conversion ${ClickHouse_SOURCE_DIR}/contrib/dragonbox/include
    ${ClickHouse_SOURCE_DIR}/contrib/fmtlib/include
    ${ClickHouse_SOURCE_DIR}/contrib/cityhash102/include
    ${RE2_INCLUDE_DIR} ${CMAKE_BINARY_DIR}/contrib/re2_st
)
target_compile_options(graphite-rollup-bench PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-suggest-override
        -Wno-suggest-destructor-override
        -Wno-extra-semi-stmt
        -Wno-atomic-implicit-seq-cst
        -Wno-signed-enum-bitfield
        -Wno-non-virtual-dtor
        -Wno-unused-parameter
        -Wno-old-style-cast
    )

target_compile_definitions(graphite-rollup-bench PRIVATE RULES_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
