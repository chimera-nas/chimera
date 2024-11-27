#include "nfs4_procs.h"

void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    #if 0
    CREATE4args *args = &argop->opcreate;

    chimera_vfs_open(thread->vfs,

                     args->objname.data,
                     args->objname.len,
                     args->dir_fh,
                     args->dir_fh_len,
                     chimera_nfs4_create_complete,
                     req);
                     #endif /* if 0 */
} /* chimera_nfs4_create */