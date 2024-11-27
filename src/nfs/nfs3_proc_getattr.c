#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct GETATTR3res                res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_marshall_attrs(attr, &res.resok.obj_attributes);
    }

    shared->nfs_v3.send_reply_NFSPROC3_GETATTR(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_getattr_complete */

void
chimera_nfs3_getattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct GETATTR3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req               = nfs_request_alloc(thread, conn, msg);
    req->args_getattr = args;

    chimera_vfs_getattr(thread->vfs,
                        args->object.data.data,
                        args->object.data.len,
                        CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_getattr_complete,
                        req);
} /* chimera_nfs3_getattr */
