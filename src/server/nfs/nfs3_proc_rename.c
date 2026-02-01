// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_rename_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct RENAME3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.fromdir_wcc, fromdir_pre_attr, fromdir_post_attr);
        chimera_nfs3_set_wcc_data(&res.resok.todir_wcc, todir_pre_attr, todir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.fromdir_wcc, fromdir_pre_attr, fromdir_post_attr);
        chimera_nfs3_set_wcc_data(&res.resfail.todir_wcc, todir_pre_attr, todir_post_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_RENAME(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */

void
chimera_nfs3_rename(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RENAME3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_rename(req, args);

    chimera_vfs_rename(thread->vfs_thread,
                       &req->cred,
                       args->from.dir.data.data,
                       args->from.dir.data.len,
                       args->from.name.str,
                       args->from.name.len,
                       args->to.dir.data.data,
                       args->to.dir.data.len,
                       args->to.name.str,
                       args->to.name.len,
                       NULL,
                       0,
                       CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                       CHIMERA_NFS3_ATTR_MASK,
                       chimera_nfs3_rename_complete,
                       req);
} /* chimera_nfs3_rename */
