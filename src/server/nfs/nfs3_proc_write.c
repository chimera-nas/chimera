// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct WRITE3args                *args   = req->args_write;
    struct WRITE3res                  res;
    int                               rc;

    /* Release write iovecs here on the server thread, not in VFS backend.
     * The iovecs were allocated on this thread and must be released here
     * to avoid cross-thread access to non-atomic refcounts.
     */
    evpl_iovecs_release(evpl, args->data.iov, args->data.niov);

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.count = length;

        if (sync) {
            res.resok.committed = FILE_SYNC;
        } else {
            res.resok.committed = UNSTABLE;
        }

        memcpy(res.resok.verf,
               &shared->nfs_verifier,
               sizeof(res.resok.verf));

        chimera_nfs3_set_wcc_data(&res.resok.file_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, pre_attr, post_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_write_complete */

static void
chimera_nfs3_write_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct WRITE3args                *args   = req->args_write;
    struct WRITE3res                  res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {

        req->handle = handle;

        chimera_vfs_write(thread->vfs_thread, &req->cred,
                          handle,
                          args->offset,
                          args->count,
                          (args->stable != UNSTABLE),
                          CHIMERA_NFS3_ATTR_WCC_MASK,
                          CHIMERA_NFS3_ATTR_MASK,
                          args->data.iov,
                          args->data.niov,
                          chimera_nfs3_write_complete,
                          req);
    } else {
        res.status =
            chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, NULL, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

        /* Iovecs were already taken from the message in chimera_nfs3_write,
         * so we need to release them here since VFS won't do it.
         */
        evpl_iovecs_release(evpl, args->data.iov, args->data.niov);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_write_open_callback */

void
chimera_nfs3_write(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct WRITE3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_write(req, args);

    req->args_write = args;

    /* Transfer ownership of write iovecs from the RPC2 message to prevent
     * msg_free from double-releasing (args->data.iov points to msg->read_chunk.iov
     * via XDR zerocopy). The iovecs will be released in the write completion
     * callback on this server thread, not in the VFS backend (which may run on
     * a different delegation thread). Must be done before any error paths.
     */
    evpl_rpc2_encoding_take_read_chunk(req->encoding, NULL, NULL);

    chimera_vfs_open(thread->vfs_thread, &req->cred,
                     args->file.data.data,
                     args->file.data.len,
                     CHIMERA_VFS_OPEN_INFERRED,
                     chimera_nfs3_write_open_callback,
                     req);
} /* chimera_nfs3_write */
