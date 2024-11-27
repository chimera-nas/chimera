#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs4_close_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;
    struct  CLOSE4res  *res = &req->res_compound.resarray[req->index].opclose;

    res->status = chimera_nfs4_errno_to_nfsstat4(error_code);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_close_complete */

void
chimera_nfs4_close(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CLOSE4args   *args    = &argop->opclose;
    struct nfs4_session *session = req->session;
    struct nfs4_state   *state;
    unsigned int         seqid = args->open_stateid.seqid;

    state = &session->nfs4_session_state[seqid];

    chimera_nfs_debug("close: seqid %u", seqid);

    chimera_vfs_close(thread->vfs,
                      &state->nfs4_state_handle,
                      chimera_nfs4_close_complete,
                      req);
} /* chimera_nfs4_close */