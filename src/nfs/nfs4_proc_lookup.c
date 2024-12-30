#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_open_cache.h"
static void
chimera_nfs4_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUP4res  *res    = &req->res_compound.resarray[req->index].oplookup;

    res->status = status;

    if (error_code == CHIMERA_VFS_OK) {
        chimera_nfs_abort_if(!(attr->va_mask & CHIMERA_VFS_ATTR_FH),
                             "NFS4 lookup: no file handle was returned");

        memcpy(req->fh, attr->va_fh, attr->va_fh_len);
        req->fhlen = attr->va_fh_len;
    }

    chimera_vfs_open_cache_release(req->thread->vfs->vfs_open_file_cache, req->handle);
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_lookup_complete */

static void
chimera_nfs4_lookup_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req    = private_data;
    struct LOOKUP4args *args   = &req->args_compound->argarray[req->index].oplookup;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUP4res  *res    = &req->res_compound.resarray[req->index].oplookup;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_lookup(req->thread->vfs_thread,
                           handle,
                           args->objname.data,
                           args->objname.len,
                           CHIMERA_VFS_ATTR_FH,
                           chimera_nfs4_lookup_complete,
                           req);
    } else {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
    }
} /* chimera_nfs4_lookup_open_callback */

void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_RDONLY,
                     chimera_nfs4_lookup_open_callback,
                     req);
} /* chimera_nfs4_lookup */