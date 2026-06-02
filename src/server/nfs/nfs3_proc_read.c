// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_read_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_read.status                                    = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        req->res_read.resfail.file_attributes.attributes_follow = 0;
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READ(thread->evpl, NULL,
                                                 &req->res_read, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_read_reply */

static void
chimera_nfs3_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct READ3res    *res = &req->res_read;

    if (error_code == CHIMERA_VFS_OK) {
        res->status            = NFS3_OK;
        res->resok.count       = count;
        res->resok.eof         = eof;
        res->resok.data.length = count;
        res->resok.data.iov    = iov;
        res->resok.data.niov   = niov;
        chimera_nfs3_set_post_op_attr(&res->resok.file_attributes, attr);
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_read_complete */

static void
chimera_nfs3_read_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct READ3args   *args = req->args_read;
    struct evpl_iovec  *iov;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    iov = xdr_dbuf_alloc_space(sizeof(*iov) * 256, req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    chimera_vfs_read(req->thread->vfs_thread, &req->cred, req->txn,
                     handle,
                     args->offset,
                     args->count,
                     iov,
                     256,
                     CHIMERA_NFS3_ATTR_MASK,
                     chimera_nfs3_read_complete,
                     req);
} /* chimera_nfs3_read_open_callback */

static void
chimera_nfs3_read_start(struct nfs_request *req)
{
    struct READ3args *args = req->args_read;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->file.data.data,
                        args->file.data.len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs3_read_open_callback,
                        req);
} /* chimera_nfs3_read_start */

void
chimera_nfs3_read(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READ3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_read(req, args);

    req->args_read = args;

    chimera_nfs3_txn_run(req, args->file.data.data, args->file.data.len,
                         CHIMERA_VFS_TXN_READ,
                         chimera_nfs3_read_start, chimera_nfs3_read_reply);
} /* chimera_nfs3_read */
