#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
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
        if (pre_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) {
            res.resok.obj_wcc.before.attributes_follow = 1;
            chimera_nfs3_marshall_wcc_attrs(pre_attr,
                                            &res.resok.obj_wcc.before.attributes);
        } else {
            res.resok.obj_wcc.before.attributes_follow = 0;
        }

        if ((post_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK) {
            res.resok.obj_wcc.after.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(post_attr,
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
    struct chimera_vfs_attrs         *attr;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_setattr(req, args);

    attr = &req->setattr;

    chimera_nfs3_sattr3_to_va(attr, &args->new_attributes);

    chimera_vfs_setattr(thread->vfs_thread,
                        args->object.data.data,
                        args->object.data.len,
                        CHIMERA_NFS3_ATTR_MASK,
                        attr,
                        chimera_nfs3_setattr_complete,
                        req);
} /* chimera_nfs3_setattr */
