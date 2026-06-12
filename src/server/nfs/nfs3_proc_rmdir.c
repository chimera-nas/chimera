// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_rmdir_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_rmdir.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        chimera_nfs3_set_wcc_data(&req->res_rmdir.resfail.dir_wcc, NULL, NULL);
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_RMDIR(thread->evpl, NULL,
                                                  &req->res_rmdir, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_rmdir_reply */

static void
chimera_nfs3_rmdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        req->res_rmdir.status = NFS3_OK;
        chimera_nfs3_set_wcc_data(&req->res_rmdir.resok.dir_wcc, pre_attr, post_attr);
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_rmdir_complete */

static void
chimera_nfs3_rmdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct RMDIR3args  *args = req->args_rmdir;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    chimera_vfs_remove_at(req->thread->vfs_thread, &req->cred, req->txn,
                          handle,
                          args->object.name.str,
                          args->object.name.len,
                          NULL,
                          0,
                          CHIMERA_NFS3_ATTR_WCC_MASK,
                          CHIMERA_NFS3_ATTR_MASK,
                          chimera_nfs3_rmdir_complete,
                          req);
} /* chimera_nfs3_rmdir_open_callback */

static void
chimera_nfs3_rmdir_start(struct nfs_request *req)
{
    struct RMDIR3args *args = req->args_rmdir;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->object.dir.data.data,
                        args->object.dir.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_rmdir_open_callback,
                        req);
} /* chimera_nfs3_rmdir_start */

void
chimera_nfs3_rmdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RMDIR3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_rmdir(req, args);

    req->args_rmdir = args;

    chimera_nfs3_txn_run(req, args->object.dir.data.data, args->object.dir.data.len,
                         CHIMERA_VFS_TXN_WRITE,
                         chimera_nfs3_rmdir_start, chimera_nfs3_rmdir_reply);
} /* chimera_nfs3_rmdir */
