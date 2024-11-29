#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_access_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct ACCESS3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.obj_attributes.attributes_follow = 0;
        res.resok.access                           = req->args_access->access;
    }

    shared->nfs_v3.send_reply_NFSPROC3_ACCESS(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_access_complete */

void
chimera_nfs3_access(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct ACCESS3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    uint32_t                          access_mask = 0;

    if (args->access & (ACCESS3_READ | ACCESS3_LOOKUP)) {
        access_mask |= CHIMERA_VFS_ACCESS_READ;
    }

    if (args->access & (ACCESS3_DELETE | ACCESS3_EXTEND | ACCESS3_MODIFY)) {
        access_mask |= CHIMERA_VFS_ACCESS_WRITE;
    }

    if (args->access & ACCESS3_EXECUTE) {
        access_mask |= CHIMERA_VFS_ACCESS_EXECUTE;
    }

    req = nfs_request_alloc(thread, conn, msg);

    req->args_access = args;

    chimera_vfs_access(thread->vfs,
                       args->object.data.data,
                       args->object.data.len,
                       access_mask,
                       chimera_nfs3_access_complete,
                       req);
} /* chimera_nfs3_access */
