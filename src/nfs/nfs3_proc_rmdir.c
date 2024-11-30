#include "nfs3_procs.h"
#include "nfs3_status.h"

#include "vfs/vfs_procs.h"

static void
chimera_nfs3_rmdir_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct RMDIR3res                  res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.dir_wcc.before.attributes_follow = 0;
        res.resok.dir_wcc.after.attributes_follow  = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_rmdir_complete */

void
chimera_nfs3_rmdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct RMDIR3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    chimera_vfs_remove(thread->vfs,
                       args->object.dir.data.data,
                       args->object.dir.data.len,
                       args->object.name.str,
                       args->object.name.len,
                       chimera_nfs3_rmdir_complete,
                       req);
} /* chimera_nfs3_rmdir */
