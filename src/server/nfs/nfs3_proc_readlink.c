// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_readlink_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_readlink.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status)
        ;
        req->res_readlink.resok.symlink_attributes.attributes_follow = 0;
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READLINK(thread->evpl, NULL,
                                                     &req->res_readlink, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_readlink_reply */

static void
chimera_nfs3_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request  *req = private_data;
    struct READLINK3res *res = &req->res_readlink;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS3_OK;
        chimera_nfs3_set_post_op_attr(&res->resok.symlink_attributes, attr);
        res->resok.data.len = targetlen;
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_readlink_complete */

static void
chimera_nfs3_readlink_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req = private_data;
    struct READLINK3res *res = &req->res_readlink;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    /* (Re)set the destination buffer length each attempt (allocated to 4096 in
     * the entry point and reused across replays). */
    res->resok.data.len = 4096;

    chimera_vfs_readlink(req->thread->vfs_thread, &req->cred, req->txn,
                         handle,
                         res->resok.data.str,
                         res->resok.data.len,
                         CHIMERA_NFS3_ATTR_MASK,
                         chimera_nfs3_readlink_complete,
                         req);
} /* chimera_nfs3_readlink_open_callback */

static void
chimera_nfs3_readlink_start(struct nfs_request *req)
{
    struct READLINK3args *args = req->args_readlink;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->symlink.data.data,
                        args->symlink.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs3_readlink_open_callback,
                        req);
} /* chimera_nfs3_readlink_start */

void
chimera_nfs3_readlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READLINK3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_readlink(req, args);

    req->res_readlink.resok.data.len = 4096;
    req->res_readlink.resok.data.str = xdr_dbuf_alloc_space(4096, encoding->dbuf);

    req->args_readlink = args;

    chimera_nfs3_txn_run(req, args->symlink.data.data, args->symlink.data.len,
                         CHIMERA_VFS_TXN_READ,
                         chimera_nfs3_readlink_start, chimera_nfs3_readlink_reply);
} /* chimera_nfs3_readlink */
