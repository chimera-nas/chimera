// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_link_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct LINK3res                   res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        res.resok.file_attributes.attributes_follow    = 0;
        res.resok.linkdir_wcc.before.attributes_follow = 0;
        res.resok.linkdir_wcc.after.attributes_follow  = 0;
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_LINK(evpl, &res, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */

void
chimera_nfs3_link(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct LINK3args      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_link(req, args);

    chimera_vfs_link(thread->vfs_thread,
                     args->file.data.data,
                     args->file.data.len,
                     args->link.dir.data.data,
                     args->link.dir.data.len,
                     args->link.name.str,
                     args->link.name.len,
                     0,
                     0,
                     0,
                     0,
                     chimera_nfs3_link_complete,
                     req);
} /* chimera_nfs3_link */
