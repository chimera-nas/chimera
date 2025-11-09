# SPDX-FileCopyrightText: 2025 Ben Jarvis
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(${NFS_COMMON_INCLUDE_DIR})

# Create libraries for NFSv3 and NFSv4
add_library(chimera_nfs SHARED nfs.c nfs4_session.c nfs_mount.c nfs_portmap.c nfs3_dump.c nfs4_dump.c

            nfs3_proc_null.c nfs3_proc_getattr.c nfs3_proc_setattr.c nfs3_proc_lookup.c 
            nfs3_proc_access.c nfs3_proc_readlink.c nfs3_proc_read.c nfs3_proc_write.c
            nfs3_proc_create.c nfs3_proc_mkdir.c nfs3_proc_remove.c nfs3_proc_mknod.c nfs3_proc_symlink.c
            nfs3_proc_rmdir.c nfs3_proc_rename.c nfs3_proc_readdir.c nfs3_proc_readdirplus.c
            nfs3_proc_link.c nfs3_proc_commit.c nfs3_proc_fsinfo.c nfs3_proc_pathconf.c nfs3_proc_fsstat.c


            nfs4_proc_getfh.c nfs4_proc_access.c nfs4_proc_putrootfh.c nfs4_proc_putfh.c
            nfs4_proc_getattr.c nfs4_proc_lookup.c nfs4_proc_compound.c nfs4_proc_null.c
            nfs4_proc_readdir.c nfs4_proc_close.c nfs4_proc_create.c  nfs4_proc_open.c
            nfs4_proc_setclientid.c nfs4_proc_setclientid_confirm.c nfs4_proc_remove.c
            nfs4_proc_read.c nfs4_proc_write.c nfs4_proc_commit.c nfs4_proc_readlink.c
            nfs4_proc_setattr.c nfs4_proc_link.c nfs4_proc_rename.c nfs4_proc_savefh.c
            nfs4_proc_exchange_id.c nfs4_proc_create_session.c nfs4_proc_destroy_session.c
            nfs4_proc_destroy_clientid.c nfs4_proc_sequence.c nfs4_proc_reclaim_complete.c
            nfs4_proc_secinfo_no_name.c)

target_link_libraries(chimera_nfs chimera_nfs_common chimera_common evpl evpl_rpc2 pthread uuid)

install(TARGETS chimera_nfs DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()

