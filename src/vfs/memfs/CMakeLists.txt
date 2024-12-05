add_library(chimera_vfs_memfs STATIC
    memfs.c
)

target_compile_definitions(chimera_vfs_memfs PRIVATE
    -DXXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:-DXXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:-DXXH_VECTOR=XXH_AVX2>
)