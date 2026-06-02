// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_commit_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_commit.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        chimera_nfs3_set_wcc_data(&req->res_commit.resfail.file_wcc, NULL, NULL);
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_COMMIT(thread->evpl, NULL,
                                                   &req->res_commit, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_commit_reply */

static void
chimera_nfs3_commit_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_shared *shared = req->thread->shared;

    if (error_code == CHIMERA_VFS_OK) {
        req->res_commit.status = NFS3_OK;
        chimera_nfs3_set_wcc_data(&req->res_commit.resok.file_wcc, pre_attr, post_attr);
        memcpy(req->res_commit.resok.verf, &shared->nfs_verifier,
               sizeof(req->res_commit.resok.verf));
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_commit_complete */

static void
chimera_nfs3_commit_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct COMMIT3args *args = req->args_commit;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    chimera_vfs_commit(req->thread->vfs_thread, &req->cred, req->txn,
                       handle,
                       args->offset,
                       args->count,
                       CHIMERA_NFS3_ATTR_WCC_MASK,
                       CHIMERA_NFS3_ATTR_MASK,
                       chimera_nfs3_commit_complete,
                       req);
} /* chimera_nfs3_commit_open_callback */

static void
chimera_nfs3_commit_start(struct nfs_request *req)
{
    struct COMMIT3args *args = req->args_commit;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->file.data.data,
                        args->file.data.len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs3_commit_open_callback,
                        req);
} /* chimera_nfs3_commit_start */

void
chimera_nfs3_commit(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct COMMIT3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_commit(req, args);

    req->args_commit = args;

    chimera_nfs3_txn_run(req, args->file.data.data, args->file.data.len,
                         CHIMERA_VFS_TXN_WRITE,
                         chimera_nfs3_commit_start, chimera_nfs3_commit_reply);
} /* chimera_nfs3_commit */
