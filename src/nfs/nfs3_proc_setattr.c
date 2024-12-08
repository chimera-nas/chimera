#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct SETATTR3res                res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.obj_wcc.before.attributes_follow = 0;

        if ((attr->va_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK)
        {
            res.resok.obj_wcc.after.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(attr,
                                        &res.resok.obj_wcc.after.attributes);
        } else {
            res.resok.obj_wcc.after.attributes_follow = 0;
        }
    } else {
        res.resfail.obj_wcc.before.attributes_follow = 0;
        res.resfail.obj_wcc.after.attributes_follow  = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_SETATTR(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_setattr_complete */

void
chimera_nfs3_setattr(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct SETATTR3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;
    struct chimera_vfs_attrs          attr;

    req = nfs_request_alloc(thread, conn, msg);

    chimera_nfs3_sattr3_to_va(&attr, &args->new_attributes);

    chimera_vfs_setattr(thread->vfs,
                        args->object.data.data,
                        args->object.data.len,
                        CHIMERA_NFS3_ATTR_MASK,
                        &attr,
                        chimera_nfs3_setattr_complete,
                        req);
} /* chimera_nfs3_setattr */
