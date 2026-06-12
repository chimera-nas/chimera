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
chimera_nfs3_setattr_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_setattr.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        chimera_nfs3_set_wcc_data(&req->res_setattr.resfail.obj_wcc, NULL, NULL);
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_SETATTR(thread->evpl, NULL,
                                                    &req->res_setattr, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_setattr_reply */

static void
chimera_nfs3_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        req->res_setattr.status = NFS3_OK;
        chimera_nfs3_set_wcc_data(&req->res_setattr.resok.obj_wcc, pre_attr, post_attr);
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_setattr_complete */

static void
chimera_nfs3_setattr_do_setattr(struct nfs_request *req)
{
    struct SETATTR3args      *args = req->args_setattr;
    struct chimera_vfs_attrs *attr;

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    chimera_nfs3_sattr3_to_va(attr, &args->new_attributes);

    chimera_vfs_setattr(req->thread->vfs_thread, &req->cred, req->txn,
                        req->handle,
                        attr,
                        CHIMERA_NFS3_ATTR_WCC_MASK,
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_setattr_complete,
                        req);
} /* chimera_nfs3_setattr_do_setattr */

static void
chimera_nfs3_setattr_guard_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request  *req  = private_data;
    struct SETATTR3args *args = req->args_setattr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    if (attr->va_ctime.tv_sec  != args->guard.obj_ctime.seconds ||
        attr->va_ctime.tv_nsec != args->guard.obj_ctime.nseconds) {
        /* Guard mismatch: build NOT_SYNC now; finish OK so the (empty) txn just
         * releases and the reply preserves the status set here. */
        req->res_setattr.status = NFS3ERR_NOT_SYNC;
        chimera_nfs3_set_wcc_data(&req->res_setattr.resfail.obj_wcc, NULL, NULL);
        chimera_nfs3_txn_finish(req, CHIMERA_VFS_OK);
        return;
    }

    chimera_nfs3_setattr_do_setattr(req);
} /* chimera_nfs3_setattr_guard_callback */

static void
chimera_nfs3_setattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req  = private_data;
    struct SETATTR3args *args = req->args_setattr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    if (args->guard.check) {
        chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, req->txn,
                            handle,
                            CHIMERA_VFS_ATTR_CTIME,
                            chimera_nfs3_setattr_guard_callback,
                            req);
    } else {
        chimera_nfs3_setattr_do_setattr(req);
    }
} /* chimera_nfs3_setattr_open_callback */

static void
chimera_nfs3_setattr_start(struct nfs_request *req)
{
    struct SETATTR3args *args = req->args_setattr;
    unsigned int         open_flags;

    /*
     * Use a real file handle instead of OPEN_PATH when setting size, because
     * ftruncate() requires a real file descriptor, not an O_PATH handle.
     */
    if (args->new_attributes.size.set_it) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED;
    } else {
        open_flags = CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->object.data.data,
                        args->object.data.len,
                        open_flags,
                        chimera_nfs3_setattr_open_callback,
                        req);
} /* chimera_nfs3_setattr_start */

void
chimera_nfs3_setattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_setattr(req, args);

    req->args_setattr = args;

    chimera_nfs3_txn_run(req, args->object.data.data, args->object.data.len,
                         CHIMERA_VFS_TXN_WRITE,
                         chimera_nfs3_setattr_start, chimera_nfs3_setattr_reply);
} /* chimera_nfs3_setattr */
