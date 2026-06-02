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
chimera_nfs3_fsstat_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_fsstat.status = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        chimera_nfs3_set_post_op_attr(&req->res_fsstat.resfail.obj_attributes, NULL);
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_FSSTAT(thread->evpl, NULL,
                                                   &req->res_fsstat, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_fsstat_reply */

static void
chimera_nfs3_fsstat_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct FSSTAT3res  *res = &req->res_fsstat;

    if (error_code == CHIMERA_VFS_OK) {
        if ((attr->va_set_mask & CHIMERA_NFS3_FSSTAT_MASK) != CHIMERA_NFS3_FSSTAT_MASK) {
            /* Backend can't report fs stats: NFS3ERR_NOTSUPP.  Finish OK so the
            * (empty read) txn just releases and the reply keeps this status. */
            res->status = NFS3ERR_NOTSUPP;
            chimera_nfs3_set_post_op_attr(&res->resfail.obj_attributes, attr);
            chimera_nfs3_txn_finish(req, CHIMERA_VFS_OK);
            return;
        }

        res->status = NFS3_OK;
        chimera_nfs3_set_post_op_attr(&res->resok.obj_attributes, attr);

        res->resok.tbytes   = attr->va_fs_space_total;
        res->resok.fbytes   = attr->va_fs_space_free;
        res->resok.abytes   = attr->va_fs_space_avail;
        res->resok.tfiles   = attr->va_fs_files_total;
        res->resok.ffiles   = attr->va_fs_files_free;
        res->resok.afiles   = attr->va_fs_files_avail;
        res->resok.invarsec = 0;
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_fsstat_complete */

static void
chimera_nfs3_fsstat_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_txn_finish(req, error_code);
        return;
    }

    req->handle = handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, req->txn,
                        handle,
                        CHIMERA_NFS3_FSSTAT_MASK,
                        chimera_nfs3_fsstat_complete,
                        req);
} /* chimera_nfs3_fsstat_open_callback */

static void
chimera_nfs3_fsstat_start(struct nfs_request *req)
{
    struct FSSTAT3args *args = req->args_fsstat;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->fsroot.data.data,
                        args->fsroot.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_fsstat_open_callback,
                        req);
} /* chimera_nfs3_fsstat_start */

void
chimera_nfs3_fsstat(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct FSSTAT3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_fsstat(req, args);

    req->args_fsstat = args;

    chimera_nfs3_txn_run(req, args->fsroot.data.data, args->fsroot.data.len,
                         CHIMERA_VFS_TXN_READ,
                         chimera_nfs3_fsstat_start, chimera_nfs3_fsstat_reply);
} /* chimera_nfs3_fsstat */
