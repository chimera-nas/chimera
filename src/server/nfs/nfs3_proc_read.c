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
chimera_nfs3_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct READ3res                   res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.count       = count;
        res.resok.eof         = eof;
        res.resok.data.length = count;
        res.resok.data.iov    = iov;
        res.resok.data.niov   = niov;

        chimera_nfs3_set_post_op_attr(&res.resok.file_attributes, attr);
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.file_attributes, attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    chimera_vfs_release(thread->vfs_thread, req->handle);

    nfs_request_free(thread, req);
} /* chimera_nfs3_read_complete */

static void
chimera_nfs3_read_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct READ3args                 *args   = req->args_read;
    struct READ3res                   res;
    struct evpl_iovec                *iov;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        iov = xdr_dbuf_alloc_space(sizeof(*iov) * 64, req->encoding->dbuf);
        chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

        chimera_vfs_read(thread->vfs_thread, &req->cred,
                         handle,
                         args->offset,
                         args->count,
                         iov,
                         64,
                         CHIMERA_NFS3_ATTR_MASK,
                         chimera_nfs3_read_complete,
                         req);
    } else {
        res.status =
            chimera_vfs_error_to_nfsstat3(error_code);
        res.resfail.file_attributes.attributes_follow = 0;
        rc                                            = shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, NULL, &res,
                                                                                                req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_read_open_callback */

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

    chimera_vfs_open(thread->vfs_thread, &req->cred,
                     args->file.data.data,
                     args->file.data.len,
                     CHIMERA_VFS_OPEN_INFERRED,
                     chimera_nfs3_read_open_callback,
                     req);
} /* chimera_nfs3_read */
