// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_rename_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct RENAME3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        res.resok.fromdir_wcc.before.attributes_follow = 0;
        res.resok.fromdir_wcc.after.attributes_follow  = 0;
        res.resok.todir_wcc.before.attributes_follow   = 0;
        res.resok.todir_wcc.after.attributes_follow    = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_RENAME(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */

void
chimera_nfs3_rename(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct RENAME3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_rename(req, args);

    chimera_vfs_rename(thread->vfs_thread,
                       args->from.dir.data.data,
                       args->from.dir.data.len,
                       args->from.name.str,
                       args->from.name.len,
                       args->to.dir.data.data,
                       args->to.dir.data.len,
                       args->to.name.str,
                       args->to.name.len,
                       chimera_nfs3_rename_complete,
                       req);
} /* chimera_nfs3_rename */
