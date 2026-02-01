# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

include(CheckSymbolExists)

set(CMAKE_REQUIRED_LIBRARIES rocksdb)
check_symbol_exists(rocksdb_transaction_get_pinned "rocksdb/c.h" HAVE_ROCKSDB_TXN_GET_PINNED)

add_library(chimera_vfs_cairn SHARED
    cairn.c
)

target_link_libraries(chimera_vfs_cairn jansson rocksdb)

if (NOT HAVE_ROCKSDB_TXN_GET_PINNED)
    message(STATUS "RocksDB lacks rocksdb_transaction_get_pinned, using compat shim")
    target_compile_definitions(chimera_vfs_cairn PRIVATE ROCKSDB_LEGACY_COMPAT=1)
endif()

target_compile_definitions(chimera_vfs_cairn PRIVATE
    XXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:XXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:XXH_VECTOR=XXH_AVX2>
)

install(TARGETS chimera_vfs_cairn DESTINATION lib)
