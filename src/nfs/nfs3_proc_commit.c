#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_commit_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct COMMIT3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.file_wcc.before.attributes_follow = 0;
        res.resok.file_wcc.after.attributes_follow  = 0;

        memcpy(res.resok.verf,
               &shared->nfs_verifier,
               sizeof(res.resok.verf));
    } else {
        res.resfail.file_wcc.before.attributes_follow = 0;
        res.resfail.file_wcc.after.attributes_follow  = 0;
    }

    chimera_vfs_release(thread->vfs, req->handle);

    shared->nfs_v3.send_reply_NFSPROC3_COMMIT(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_commit_complete */

static void
chimera_nfs3_commit_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct COMMIT3args               *args   = req->args_commit;
    struct COMMIT3res                 res;

    if (error_code == CHIMERA_VFS_OK) {

        req->handle = handle;

        chimera_vfs_commit(thread->vfs,
                           handle,
                           args->offset,
                           args->count,
                           chimera_nfs3_commit_complete,
                           req);
    } else {
        res.status =
            chimera_vfs_error_to_nfsstat3(error_code);
        res.resfail.file_wcc.before.attributes_follow = 0;
        res.resfail.file_wcc.after.attributes_follow  = 0;
        shared->nfs_v3.send_reply_NFSPROC3_COMMIT(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_commit_open_callback */

void
chimera_nfs3_commit(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct COMMIT3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    req->args_commit = args;

    chimera_vfs_open(thread->vfs,
                     args->file.data.data,
                     args->file.data.len,
                     CHIMERA_VFS_OPEN_RDWR,
                     chimera_nfs3_commit_open_callback,
                     req);

} /* chimera_nfs3_commit */
