#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs4_lookup_complete(
    enum chimera_vfs_error    error_code,
    const void               *fh,
    int                       fhlen,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request *req    = private_data;
    nfsstat4            status = chimera_nfs4_errno_to_nfsstat4(error_code);
    struct LOOKUP4res  *res    = &req->res_compound.resarray[req->index].
        oplookup;

    res->status = status;

    if (error_code == CHIMERA_VFS_OK) {
        memcpy(req->fh, fh, fhlen);
        req->fhlen = fhlen;
    }

    chimera_nfs4_compound_complete(req, status);

} /* chimera_nfs4_lookup_complete */

void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOOKUP4args *args = &argop->oplookup;

    chimera_vfs_lookup(thread->vfs,
                       req->fh,
                       req->fhlen,
                       args->objname.data,
                       args->objname.len,
                       0,
                       chimera_nfs4_lookup_complete,
                       req);
} /* chimera_nfs4_lookup */