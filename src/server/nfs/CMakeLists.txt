# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

include_directories(${NFS_COMMON_INCLUDE_DIR})

# Create libraries for NFSv3 and NFSv4
add_library(chimera_nfs SHARED nfs.c nfs4_session.c nfs4_state.c nfs4_lease.c nfs4_recovery.c nfs4_drc.c nfs3_drc.c nfs4_v40_drc.c nfs_drc_reply.c nfs4_callback.c nfs_mount.c nfs_portmap.c nfs3_dump.c nfs4_dump.c
            nfs_external_portmap.c nfs_nlm.c nfs_nlm_state.c nfs_nsm.c nfs_nsm_state.c
            nfs3_proc_null.c nfs3_proc_getattr.c nfs3_proc_setattr.c nfs3_proc_lookup.c 
            nfs3_proc_access.c nfs3_proc_readlink.c nfs3_proc_read.c nfs3_proc_write.c
            nfs3_proc_create.c nfs3_proc_mkdir.c nfs3_proc_remove.c nfs3_proc_mknod.c nfs3_proc_symlink.c
            nfs3_proc_rmdir.c nfs3_proc_rename.c nfs3_proc_readdir.c nfs3_proc_readdirplus.c
            nfs3_proc_link.c nfs3_proc_commit.c nfs3_proc_fsinfo.c nfs3_proc_pathconf.c nfs3_proc_fsstat.c


            nfs4_proc_getfh.c nfs4_proc_access.c nfs4_proc_putrootfh.c nfs4_proc_putfh.c
            nfs4_proc_getattr.c nfs4_proc_lookup.c nfs4_proc_lookupp.c nfs4_proc_compound.c nfs4_proc_null.c
            nfs4_proc_readdir.c nfs4_proc_close.c nfs4_proc_create.c  nfs4_proc_open.c nfs4_proc_delegreturn.c nfs4_proc_free_stateid.c nfs4_proc_backchannel_ctl.c
            nfs4_proc_setclientid.c nfs4_proc_setclientid_confirm.c nfs4_proc_remove.c
            nfs4_proc_read.c nfs4_proc_write.c nfs4_proc_copy.c nfs4_proc_commit.c nfs4_proc_readlink.c
            nfs4_proc_setattr.c nfs4_proc_link.c nfs4_proc_rename.c nfs4_proc_savefh.c nfs4_proc_restorefh.c
            nfs4_proc_exchange_id.c nfs4_proc_create_session.c nfs4_proc_destroy_session.c
            nfs4_proc_bind_conn_to_session.c
            nfs4_proc_destroy_clientid.c nfs4_proc_sequence.c nfs4_proc_reclaim_complete.c
            nfs4_proc_secinfo.c nfs4_proc_secinfo_no_name.c nfs4_proc_test_stateid.c
            nfs4_proc_set_ssv.c
            nfs4_proc_free_stateid.c nfs4_root.c
            nfs4_proc_allocate.c nfs4_proc_deallocate.c nfs4_proc_seek.c
            nfs4_proc_lock.c nfs4_proc_lockt.c nfs4_proc_locku.c
            nfs4_proc_verify.c
            nfs4_proc_putpubfh.c
            nfs4_proc_renew.c nfs4_proc_open_confirm.c
            nfs4_proc_open_downgrade.c nfs4_proc_release_lockowner.c
            nfs4_proc_getxattr.c nfs4_proc_setxattr.c
            nfs4_proc_listxattrs.c nfs4_proc_removexattr.c
            nfs4_pnfs.c nfs4_cb.c nfs4_layout_table.c
            nfs_gss.c)

target_compile_definitions(chimera_nfs PRIVATE XXH_INLINE_ALL)
target_link_libraries(chimera_nfs chimera_nfs_common chimera_common evpl evpl_rpc2 pthread uuid urcu-qsbr urcu-common gssapi_krb5)

install(TARGETS chimera_nfs DESTINATION lib)

if (NOT DISABLE_TESTS)
    add_subdirectory(tests)
endif()
