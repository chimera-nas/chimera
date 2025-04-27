#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct SETATTR4args              *args   = &req->args_compound->argarray[req->index].opsetattr;
    struct SETATTR4res               *res    = &req->res_compound.resarray[req->index].opsetattr;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS4_OK;

        xdr_dbuf_alloc_space(res->attrsset, sizeof(uint32_t) * 4, msg->dbuf);

        res->num_attrsset = chimera_nfs4_mask2attr(set_attr,
                                                   args->obj_attributes.num_attrmask,
                                                   args->obj_attributes.attrmask,
                                                   res->attrsset);
    } else {
        res->status       = chimera_nfs4_errno_to_nfsstat4(error_code);
        res->num_attrsset = 0;
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setattr_complete */

static void
chimera_nfs4_setattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request       *req  = private_data;
    struct SETATTR4args      *args = &req->args_compound->argarray[req->index].opsetattr;
    struct SETATTR4res       *res  = &req->res_compound.resarray[req->index].opsetattr;
    struct chimera_vfs_attrs *attr;
    int                       rc;

    xdr_dbuf_alloc_space(attr, sizeof(*attr), req->msg->dbuf);

    req->handle = handle;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    rc = chimera_nfs4_unmarshall_attrs(attr,
                                       args->obj_attributes.num_attrmask,
                                       args->obj_attributes.attrmask,
                                       args->obj_attributes.attr_vals.data,
                                       args->obj_attributes.attr_vals.len);

    if (rc != 0) {
        res->status = NFS4ERR_BADXDR;
        chimera_vfs_release(req->thread->vfs_thread, handle);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_setattr(req->thread->vfs_thread,
                        handle,
                        attr,
                        0,
                        0,
                        chimera_nfs4_setattr_complete,
                        req);
} /* chimera_nfs4_setattr_open_callback */

void
chimera_nfs4_setattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                     chimera_nfs4_setattr_open_callback,
                     req);
} /* chimera_nfs4_setattr */
