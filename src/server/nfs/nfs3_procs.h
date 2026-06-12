// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_common.h"
#include "vfs/vfs.h"

/* Bound on how many times an NFS3 op replays its transaction after a wait-die /
 * optimistic conflict before giving up. */
#define CHIMERA_NFS3_TXN_MAX_RETRIES 8

/* Shared NFS3 one-op-per-transaction driver (nfs3_txn.c).  See that file. */
void chimera_nfs3_txn_run(
    struct nfs_request       *req,
    const void               *fh,
    int                       fhlen,
    enum chimera_vfs_txn_mode mode,
    void (                   *start )(struct nfs_request *req),
    void (                   *reply )(struct nfs_request *req));

void chimera_nfs3_txn_finish(
    struct nfs_request    *req,
    enum chimera_vfs_error status);

void chimera_nfs3_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_getattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct GETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_setattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_lookup(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LOOKUP3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_access(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct ACCESS3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_readlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READLINK3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_read(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READ3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_write(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct WRITE3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_create(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CREATE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_mkdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct MKDIR3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_symlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SYMLINK3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_mknod(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct MKNOD3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_remove(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct REMOVE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_rmdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RMDIR3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_rename(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RENAME3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_link(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LINK3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_readdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_readdirplus(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READDIRPLUS3args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_fsstat(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct FSSTAT3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_fsinfo(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct FSINFO3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_pathconf(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct PATHCONF3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_commit(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct COMMIT3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs3_secinfo_no_name(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);
