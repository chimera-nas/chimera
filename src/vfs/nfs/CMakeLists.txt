# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(${NFS_COMMON_INCLUDE_DIR})

add_library(chimera_vfs_nfs SHARED
    nfs.c
    nfs3.c
    nfs4.c
    nfs3_close.c
    nfs3_commit.c
    nfs3_getattr.c
    nfs3_mount.c
    nfs3_umount.c
    nfs3_link_at.c
    nfs3_lookup_at.c
    nfs3_mkdir_at.c
    nfs3_mknod_at.c
    nfs3_open_at.c
    nfs3_open_fh.c
    nfs3_read.c
    nfs3_readdir.c
    nfs3_readlink.c
    nfs3_remove_at.c
    nfs3_rename_at.c
    nfs3_setattr.c
    nfs3_symlink_at.c
    nfs3_write.c
    nfs4_close.c
    nfs4_commit.c
    nfs4_getattr.c
    nfs4_mount.c
    nfs4_link_at.c
    nfs4_lookup_at.c
    nfs4_mkdir_at.c
    nfs4_mknod_at.c
    nfs4_open_at.c
    nfs4_open_fh.c
    nfs4_read.c
    nfs4_readdir.c
    nfs4_readlink.c
    nfs4_remove_at.c
    nfs4_rename_at.c
    nfs4_setattr.c
    nfs4_symlink_at.c
    nfs4_write.c
)

target_link_libraries(chimera_vfs_nfs chimera_nfs_common evpl evpl_rpc2)

install(TARGETS chimera_vfs_nfs DESTINATION lib)

