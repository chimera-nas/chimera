add_library(chimera_vfs_cairn STATIC
    cairn.c
)

target_link_libraries(chimera_vfs_cairn jansson rocksdb)

target_compile_definitions(chimera_vfs_cairn PRIVATE
    -DXXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:-DXXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:-DXXH_VECTOR=XXH_AVX2>
)