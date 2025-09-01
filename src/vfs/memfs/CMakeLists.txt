# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_memfs SHARED
    memfs.c
)

target_compile_definitions(chimera_vfs_memfs PRIVATE
    -DXXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:-DXXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:-DXXH_VECTOR=XXH_AVX2>
)

install(TARGETS chimera_vfs_memfs DESTINATION lib)
