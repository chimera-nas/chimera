# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

include(CheckSymbolExists)
include(CheckFunctionExists)

set(CMAKE_REQUIRED_LIBRARIES rocksdb)
check_symbol_exists(rocksdb_transaction_get_pinned "rocksdb/c.h" HAVE_ROCKSDB_TXN_GET_PINNED)
# rocksdb_flush_wal has been a librocksdb export since 5.8, but some distro
# headers (the same older ones missing transaction_get_pinned) fail to
# declare it.  Detect the declaration and the symbol separately so we can
# fall back to declaring it ourselves, or degrade gracefully if it's truly
# absent.
check_symbol_exists(rocksdb_flush_wal "rocksdb/c.h" HAVE_ROCKSDB_FLUSH_WAL_DECL)
check_function_exists(rocksdb_flush_wal HAVE_ROCKSDB_FLUSH_WAL_SYM)

add_library(chimera_vfs_cairn SHARED
    cairn.c
)

target_link_libraries(chimera_vfs_cairn jansson rocksdb)

if (NOT HAVE_ROCKSDB_TXN_GET_PINNED)
    message(STATUS "RocksDB lacks rocksdb_transaction_get_pinned, using compat shim")
    target_compile_definitions(chimera_vfs_cairn PRIVATE ROCKSDB_LEGACY_COMPAT=1)
endif()

if (HAVE_ROCKSDB_FLUSH_WAL_DECL)
    target_compile_definitions(chimera_vfs_cairn PRIVATE CHIMERA_HAVE_ROCKSDB_FLUSH_WAL=1)
elseif (HAVE_ROCKSDB_FLUSH_WAL_SYM)
    message(STATUS "rocksdb_flush_wal exported but not declared in header; declaring it locally")
    target_compile_definitions(chimera_vfs_cairn PRIVATE CHIMERA_HAVE_ROCKSDB_FLUSH_WAL=1 CHIMERA_DECLARE_FLUSH_WAL=1)
else ()
    message(STATUS "rocksdb_flush_wal unavailable; data writes will be synced for NFS COMMIT correctness")
endif()

target_compile_definitions(chimera_vfs_cairn PRIVATE
    XXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:XXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:XXH_VECTOR=XXH_AVX2>
)

install(TARGETS chimera_vfs_cairn DESTINATION lib)
