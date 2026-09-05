// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_remove_reply(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    if (req->compound_op_status != CHIMERA_VFS_OK) {
        req->res_remove.status = chimera_vfs_error_to_nfsstat3(req->compound_op_status);
        chimera_nfs3_set_wcc_data(&req->res_remove.resfail.dir_wcc, NULL, NULL);
    }

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_REMOVE(thread->evpl, NULL,
                                                   &req->res_remove, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_remove_reply */

static void
chimera_nfs3_remove_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        req->res_remove.status = NFS3_OK;
        chimera_nfs3_set_wcc_data(&req->res_remove.resok.dir_wcc, pre_attr, post_attr);
    }

    chimera_nfs3_compound_finish(req, error_code);
} /* chimera_nfs3_remove_complete */

/* Issue the unlink.  child_fh is left NULL: when a caching protocol is enabled
 * the VFS resolves the victim's FH itself and recalls any delegation/oplock/
 * lease on it before the unlink. */
static void
chimera_nfs3_remove_dispatch(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct REMOVE3args               *args   = req->args_remove;

    chimera_vfs_remove_at(thread->vfs_thread, &req->cred, req->compound,
                          req->handle,
                          args->object.name.str,
                          args->object.name.len,
                          NULL,
                          0,
                          CHIMERA_VFS_REMOVE_ISNOTDIR | CHIMERA_VFS_REMOVE_RECALL,
                          CHIMERA_NFS3_ATTR_WCC_MASK,
                          CHIMERA_NFS3_ATTR_MASK,
                          NULL,
                          chimera_nfs3_remove_complete,
                          req);
} /* chimera_nfs3_remove_dispatch */

static void
chimera_nfs3_remove_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs3_compound_finish(req, error_code);
        return;
    }

    req->handle = handle;

    chimera_nfs3_remove_dispatch(req);
} /* chimera_nfs3_remove_open_callback */

static void
chimera_nfs3_remove_start(struct nfs_request *req)
{
    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->compound,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_remove_open_callback,
                        req);
} /* chimera_nfs3_remove_start */

void
chimera_nfs3_remove(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct REMOVE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct REMOVE3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_remove(req, args);
    nfs3_trace_remove(req, args);

    req->args_remove = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.dir.data.data, args->object.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_REMOVE(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    chimera_nfs3_compound_run(req, req->fh, req->fhlen,
                              CHIMERA_VFS_COMPOUND_WRITE,
                              chimera_nfs3_remove_start, chimera_nfs3_remove_reply);
} /* chimera_nfs3_remove */
