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
    }

    shared->nfs_v3.send_reply_NFSPROC3_COMMIT(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_commit_complete */


void
chimera_nfs3_commit(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct COMMIT3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_open_handle   *handle;

    handle = nfs3_open_cache_lookup(&shared->nfs3_open_cache,
                                    args->file.data.data,
                                    args->file.data.len);

    chimera_nfs_abort_if(!handle,
                         "unhandled nfs3 op on unopened file");

    req = nfs_request_alloc(thread, conn, msg);

    chimera_vfs_commit(thread->vfs,
                       handle,
                       args->offset,
                       args->count,
                       chimera_nfs3_commit_complete,
                       req);
} /* chimera_nfs3_commit */
