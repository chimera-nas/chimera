#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_open_cache.h"
static void
chimera_nfs4_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request  *req  = private_data;
    struct GETATTR4args *args = &req->args_compound->argarray[req->index].opgetattr;
    struct GETATTR4res  *res  = &req->res_compound.resarray[req->index].opgetattr;

    res->status = NFS4_OK;

    xdr_dbuf_reserve(&res->resok4.obj_attributes,
                     attrmask,
                     3,
                     req->msg->dbuf);

    xdr_dbuf_alloc_opaque(&res->resok4.obj_attributes.attr_vals,
                          4096,
                          req->msg->dbuf);

    chimera_nfs4_marshall_attrs(attr,
                                args->num_attr_request,
                                args->attr_request,
                                &res->resok4.obj_attributes.num_attrmask,
                                res->resok4.obj_attributes.attrmask,
                                3,
                                res->resok4.obj_attributes.attr_vals.data,
                                &res->resok4.obj_attributes.attr_vals.len);

    chimera_vfs_open_cache_release(req->thread->vfs->vfs_open_file_cache, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getattr_complete */

static void
chimera_nfs4_getattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req  = private_data;
    struct GETATTR4args *args = &req->args_compound->argarray[req->index].opgetattr;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        uint64_t attr_mask = chimera_nfs4_getattr2mask(args->attr_request,
                                                       args->num_attr_request);

        chimera_vfs_getattr(req->thread->vfs_thread,
                            handle,
                            attr_mask,
                            chimera_nfs4_getattr_complete,
                            req);
    } else {
        chimera_nfs4_compound_complete(req, chimera_nfs4_errno_to_nfsstat4(error_code));
    }
} /* chimera_nfs4_getattr_open_callback */

void
chimera_nfs4_getattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_RDWR,
                     chimera_nfs4_getattr_open_callback,
                     req);
} /* chimera_nfs4_getattr */