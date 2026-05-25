# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

add_library(chimera_vfs SHARED vfs.c vfs_lsan.c
            vfs_acl.c vfs_acl_serialize.c vfs_access.c vfs_idmap.c vfs_identity.c
            vfs_proc_mount.c vfs_proc_umount.c
            vfs_proc_mkdir_at.c vfs_proc_mknod_at.c vfs_proc_close.c vfs_proc_open_fh.c
            vfs_proc_readdir.c vfs_proc_getattr.c vfs_proc_lookup_at.c
            vfs_proc_lookup.c vfs_proc_read.c
            vfs_proc_remove_at.c vfs_proc_write.c vfs_proc_commit.c
            vfs_proc_open_at.c vfs_proc_symlink_at.c vfs_proc_link_at.c
            vfs_proc_readlink.c vfs_proc_rename_at.c vfs_proc_setattr.c
            vfs_proc_gate.c
            vfs_proc_create.c vfs_proc_create_unlinked.c
            vfs_proc_open.c vfs_proc_mkdir.c vfs_proc_remove.c
            vfs_proc_rename.c vfs_proc_symlink.c vfs_proc_link.c
            vfs_proc_mknod.c
            vfs_proc_find.c vfs_proc_put_key.c vfs_proc_get_key.c
            vfs_proc_delete_key.c vfs_proc_search_keys.c
            vfs_proc_allocate.c vfs_proc_seek.c vfs_proc_lock.c
            vfs_proc_copy_range.c vfs_proc_clone_range.c vfs_proc_move_range.c
            vfs_proc_getparent.c vfs_notify.c vfs_state.c vfs_dump.c
            vfs_proc_get_xattr.c vfs_proc_set_xattr.c
            vfs_proc_list_xattrs.c vfs_proc_remove_xattr.c
            vfs_pnfs.c vfs_proc_get_layout.c)

target_compile_definitions(chimera_vfs PRIVATE
    XXH_INLINE_ALL
    $<$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>:XXH_VECTOR=XXH_NEON>
    $<$<NOT:$<STREQUAL:${CMAKE_SYSTEM_PROCESSOR},aarch64>>:XXH_VECTOR=XXH_AVX2>
)

install(TARGETS chimera_vfs DESTINATION lib)

add_subdirectory(root)
add_subdirectory(memfs)
add_subdirectory(linux)
add_subdirectory(nfs)

if (CHIMERA_VFS_IO_URING)
    add_subdirectory(io_uring)
endif()

add_subdirectory(diskfs)

if (CAIRN_ENABLED)
    add_subdirectory(cairn)
endif()

target_link_libraries(chimera_vfs
    chimera_vfs_root chimera_vfs_memfs chimera_vfs_linux chimera_vfs_diskfs
    chimera_vfs_nfs
    chimera_common prometheus-c xxhash dl urcu)

if (CHIMERA_VFS_IO_URING)
    target_link_libraries(chimera_vfs chimera_vfs_io_uring)
endif()

if (DEMFS_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_diskfs)
endif()

if (CAIRN_ENABLED)
    target_link_libraries(chimera_vfs chimera_vfs_cairn)
endif()

add_subdirectory(tests)
