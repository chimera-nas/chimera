#include "nfs4_procs.h"
#include "vfs/vfs_procs.h"

void
chimera_nfs4_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTROOTFH4res *res = &resop->opputrootfh;

    chimera_vfs_getrootfh(thread->vfs_thread,
                          req->fh,
                          &req->fhlen);

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh */