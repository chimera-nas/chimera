#include "nfs4_procs.h"
#include "vfs/vfs_procs.h"
#include "nfs4_status.h"

void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CREATE4args *args = &argop->opcreate;
    struct CREATE4res  *res  = &resop->opcreate;

    switch (args->objtype.type) {
        case NF4REG:
            break;
        case NF4DIR:
            chimera_nfs_debug("NF4DIR objname %s", args->objname);
            break;
        case NF4BLK:
            break;
        case NF4CHR:
            break;
        case NF4LNK:
            break;
        case NF4SOCK:
            break;
        case NF4FIFO:
            break;
        case NF4ATTRDIR:
            break;
        case NF4NAMEDATTR:
            break;
        default:
            res->status = NFS4ERR_BADTYPE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
    } /* switch */

} /* chimera_nfs4_create */
