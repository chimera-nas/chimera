#include <fcntl.h>

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_mkdir_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct MKDIR3res                  res;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        res.resok.obj.handle_follows               = 0;
        res.resok.obj_attributes.attributes_follow = 0;
        res.resok.dir_wcc.before.attributes_follow = 0;
        res.resok.dir_wcc.after.attributes_follow  = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_MKDIR(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */

void
chimera_nfs3_mkdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct MKDIR3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    unsigned int                      mode;

    if (args->attributes.mode.set_it) {
        mode = args->attributes.mode.mode;
    } else {
        mode = S_IRWXU;
    }

    req = nfs_request_alloc(thread, conn, msg);

    chimera_vfs_mkdir(thread->vfs,
                      args->where.dir.data.data,
                      args->where.dir.data.len,
                      args->where.name.str,
                      args->where.name.len,
                      mode,
                      chimera_nfs3_mkdir_complete,
                      req);
} /* chimera_nfs3_mkdir */
