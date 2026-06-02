// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_rename_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_rename.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        chimera_nfs3_set_wcc_data(&req->res_rename.resfail.fromdir_wcc, NULL, NULL);
        chimera_nfs3_set_wcc_data(&req->res_rename.resfail.todir_wcc, NULL, NULL);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_RENAME(thread->evpl, NULL,
                                                   &req->res_rename, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_rename_reply */

static void
chimera_nfs3_rename_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        req->res_rename.status = NFS3_OK;
        chimera_nfs3_set_wcc_data(&req->res_rename.resok.fromdir_wcc, fromdir_pre_attr, fromdir_post_attr);
        chimera_nfs3_set_wcc_data(&req->res_rename.resok.todir_wcc, todir_pre_attr, todir_post_attr);
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_rename_complete */

static void
chimera_nfs3_rename_start(struct nfs_request *req)
{
    struct RENAME3args *args = req->args_rename;

    chimera_vfs_rename_at(req->thread->vfs_thread,
                          &req->cred, req->txn,
                          args->from.dir.data.data,
                          args->from.dir.data.len,
                          args->from.name.str,
                          args->from.name.len,
                          args->to.dir.data.data,
                          args->to.dir.data.len,
                          args->to.name.str,
                          args->to.name.len,
                          NULL,
                          0,
                          CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                          CHIMERA_NFS3_ATTR_MASK,
                          chimera_nfs3_rename_complete,
                          req);
} /* chimera_nfs3_rename_start */

void
chimera_nfs3_rename(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RENAME3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_rename(req, args);

    req->args_rename = args;

    chimera_nfs3_txn_run(req, args->from.dir.data.data, args->from.dir.data.len,
                         CHIMERA_VFS_TXN_WRITE,
                         chimera_nfs3_rename_start, chimera_nfs3_rename_reply);
} /* chimera_nfs3_rename */
