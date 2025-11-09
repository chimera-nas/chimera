# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(${NFS_COMMON_INCLUDE_DIR})

add_library(chimera_vfs_nfs SHARED
    nfs.c
    nfs3.c
    nfs4.c
    nfs3_close.c
    nfs3_commit.c
    nfs3_create_unlinked.c
    nfs3_getattr.c
    nfs3_mount.c
    nfs3_link.c
    nfs3_lookup.c
    nfs3_mkdir.c
    nfs3_open_at.c
    nfs3_open.c
    nfs3_read.c
    nfs3_readdir.c
    nfs3_readlink.c
    nfs3_remove.c
    nfs3_rename.c
    nfs3_setattr.c
    nfs3_symlink.c
    nfs3_write.c
    nfs4_close.c
    nfs4_commit.c
    nfs4_create_unlinked.c
    nfs4_getattr.c
    nfs4_mount.c
    nfs4_link.c
    nfs4_lookup.c
    nfs4_mkdir.c
    nfs4_open_at.c
    nfs4_open.c
    nfs4_read.c
    nfs4_readdir.c
    nfs4_readlink.c
    nfs4_remove.c
    nfs4_rename.c
    nfs4_setattr.c
    nfs4_symlink.c
    nfs4_write.c
)

target_link_libraries(chimera_vfs_nfs chimera_nfs_common evpl evpl_rpc2)

install(TARGETS chimera_vfs_nfs DESTINATION lib)

add_subdirectory(tests)

