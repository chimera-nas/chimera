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
chimera_nfs3_fsinfo_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->txn_op_status != CHIMERA_VFS_OK) {
        req->res_fsinfo.status                                   = chimera_vfs_error_to_nfsstat3(req->txn_op_status);
        req->res_fsinfo.resfail.obj_attributes.attributes_follow = 0;
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_FSINFO(thread->evpl, NULL,
                                                   &req->res_fsinfo, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_fsinfo_reply */

static void
chimera_nfs3_fsinfo_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req      = private_data;
    struct FSINFO3res  *res      = &req->res_fsinfo;
    uint64_t            max_xfer = 1024 * 1024;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS3_OK;
        chimera_nfs3_set_post_op_attr(&res->resok.obj_attributes, attr);

        res->resok.maxfilesize         = UINT64_MAX;
        res->resok.time_delta.seconds  = 0;
        res->resok.time_delta.nseconds = 1;
        res->resok.rtmax               = max_xfer;
        res->resok.rtpref              = max_xfer;
        res->resok.rtmult              = 4096;
        res->resok.wtmax               = max_xfer;
        res->resok.wtpref              = max_xfer;
        res->resok.wtmult              = 4096;
        res->resok.dtpref              = 64 * 1024;
        res->resok.properties          = FSF3_LINK | FSF3_SYMLINK |
            FSF3_HOMOGENEOUS | FSF3_CANSETTIME;
    }

    chimera_nfs3_txn_finish(req, error_code);
} /* chimera_nfs3_fsinfo_complete */

static void
chimera_nfs3_fsinfo_open_callback(
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
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_fsinfo_complete,
                        req);
} /* chimera_nfs3_fsinfo_open_callback */

static void
chimera_nfs3_fsinfo_start(struct nfs_request *req)
{
    struct FSINFO3args *args = req->args_fsinfo;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->txn,
                        args->fsroot.data.data,
                        args->fsroot.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs3_fsinfo_open_callback,
                        req);
} /* chimera_nfs3_fsinfo_start */

void
chimera_nfs3_fsinfo(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct FSINFO3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_fsinfo(req, args);

    req->args_fsinfo = args;

    chimera_nfs3_txn_run(req, args->fsroot.data.data, args->fsroot.data.len,
                         CHIMERA_VFS_TXN_READ,
                         chimera_nfs3_fsinfo_start, chimera_nfs3_fsinfo_reply);
} /* chimera_nfs3_fsinfo */
