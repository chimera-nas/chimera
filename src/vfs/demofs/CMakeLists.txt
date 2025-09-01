# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs_demofs SHARED
    demofs.c
)

target_link_libraries(chimera_vfs_demofs jansson)

target_compile_definitions(chimera_vfs_demofs PRIVATE
    -DXXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:-DXXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:-DXXH_VECTOR=XXH_AVX2>
)

install(TARGETS chimera_vfs_demofs DESTINATION lib)