# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Give liburcu the inlined read-side primitives (rcu_read_lock/unlock,
# rcu_dereference): without _LGPL_SOURCE liburcu links the out-of-line
# cross-library functions, adding a PLT call + barrier per RCU op on the hot
# paths.  Applies to this directory and all subdirectories (the VFS core, the
# RCU caches, and the backend modules under it -- every urcu user lives here or
# under src/server).  Deliberately NOT set for src/fio (the GPL fio plugin,
# which does not use urcu directly).  chimera is LGPL-2.1.
# (URCU_API_MAP, which selects the unprefixed QSBR API, is set project-wide in
# the top-level CMakeLists.txt.)
add_compile_definitions(_LGPL_SOURCE)

add_library(chimera_vfs SHARED vfs.c vfs_lsan.c vfs_cred.c vfs_sdk_utils.c
            vfs_acl.c vfs_acl_serialize.c vfs_access.c vfs_idmap.c vfs_identity.c
            vfs_proc_mount.c vfs_proc_umount.c
            vfs_proc_mkfs.c vfs_proc_rmfs.c
            vfs_proc_mkdir_at.c vfs_proc_mknod_at.c vfs_proc_close.c vfs_proc_open_fh.c
            vfs_proc_readdir.c vfs_proc_getattr.c vfs_proc_lookup_at.c
            vfs_proc_lookup.c vfs_proc_read.c
            vfs_proc_remove_at.c vfs_proc_write.c vfs_proc_commit.c
            vfs_proc_open_at.c vfs_proc_symlink_at.c vfs_proc_link_at.c
            vfs_proc_readlink.c vfs_proc_rename_at.c vfs_proc_setattr.c
            vfs_proc_recall.c
            vfs_proc_gate.c
            vfs_proc_create.c vfs_proc_create_unlinked.c
            vfs_proc_open.c vfs_proc_mkdir.c vfs_proc_remove.c
            vfs_proc_rename.c vfs_proc_symlink.c vfs_proc_link.c
            vfs_proc_mknod.c
            vfs_proc_find.c vfs_proc_put_key.c vfs_proc_get_key.c
            vfs_proc_delete_key.c vfs_proc_search_keys.c
            vfs_proc_allocate.c vfs_proc_seek.c
            vfs_proc_copy_range.c vfs_proc_clone_range.c vfs_proc_move_range.c
            vfs_proc_read_plus.c vfs_proc_write_same.c
            vfs_proc_getparent.c vfs_notify.c vfs_dump.c
            vfs_claim.c vfs_claim_break.c vfs_claim_io.c vfs_claim_backend.c
            vfs_proc_claim_acquire.c vfs_proc_claim_release.c
            vfs_proc_get_xattr.c vfs_proc_set_xattr.c
            vfs_proc_list_xattrs.c vfs_proc_remove_xattr.c
            vfs_proc_open_stream.c vfs_proc_list_streams.c
            vfs_proc_remove_stream.c
            vfs_pnfs.c vfs_proc_get_layout.c)

target_compile_definitions(chimera_vfs PRIVATE
    XXH_INLINE_ALL
    XXH_VECTOR=${CHIMERA_XXH_VECTOR}
)

install(TARGETS chimera_vfs DESTINATION lib)

# Install the module SDK so an out-of-tree backend can build against the
# installed tree with -I<prefix>/include/chimera (resolving the same
# '#include "vfs/sdk/chimera_vfs_sdk.h"' form used in-tree) plus
# -I<prefix>/include for libevpl, which installs its own evpl/ headers.
# prometheus-c and oteltracing-c do not install theirs, so the three
# headers the SDK includes are co-installed here at the include forms the
# SDK uses; xxhash is a system dependency and is not shipped.
install(DIRECTORY sdk/ DESTINATION include/chimera/vfs/sdk
        FILES_MATCHING PATTERN "*.h")
install(FILES
        ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c/prometheus-c.h
        ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/prometheus-c/ext/stopwatch/stopwatch.h
        ${CMAKE_SOURCE_DIR}/ext/libevpl/ext/oteltracing-c/oteltracing.h
        DESTINATION include/chimera)

add_subdirectory(root)
add_subdirectory(memfs)
add_subdirectory(memkv)

# The linux passthrough backend is built on Linux-only interfaces (openat2,
# statx, per-thread setfsuid/setfsgid impersonation, sysmacros), so it is not
# available on other platforms.
if (CHIMERA_LINUX)
    add_subdirectory(linux)
endif()

add_subdirectory(nfs)
add_subdirectory(smb)

if (CHIMERA_VFS_IO_URING)
    add_subdirectory(io_uring)
endif()

add_subdirectory(diskfs)

if (CAIRN_ENABLED)
    add_subdirectory(cairn)
endif()

# sqlite KV backend: built when the sqlite3 development headers are available.
find_path(SQLITE3_INCLUDE_DIR sqlite3.h)
find_library(SQLITE3_LIBRARY sqlite3)
if (SQLITE3_INCLUDE_DIR AND SQLITE3_LIBRARY)
    set(SQLITE_ENABLED TRUE)
    message(STATUS "sqlite3 found (${SQLITE3_LIBRARY}); building chimera_vfs_sqlite")
    add_subdirectory(sqlite)
else ()
    set(SQLITE_ENABLED FALSE)
    message(STATUS "sqlite3 not found; skipping chimera_vfs_sqlite")
endif()

target_link_libraries(chimera_vfs
    chimera_vfs_root chimera_vfs_memfs chimera_vfs_memkv chimera_vfs_diskfs
    chimera_vfs_nfs chimera_vfs_smb
    chimera_common prometheus-c xxhash ${CHIMERA_DL_LIB} urcu-qsbr)

if (CHIMERA_LINUX)
    target_link_libraries(chimera_vfs chimera_vfs_linux)
endif()

if (CHIMERA_VFS_IO_URING)
    target_link_libraries(chimera_vfs chimera_vfs_io_uring)
endif()

if (SQLITE_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_sqlite)
endif()

if (DEMFS_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_diskfs)
endif()

if (CAIRN_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_cairn)
endif()

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
