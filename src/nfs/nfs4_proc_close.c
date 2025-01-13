#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
void
chimera_nfs4_close(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CLOSE4args   *args    = &argop->opclose;
    struct CLOSE4res    *res     = &resop->opclose;
    struct nfs4_session *session = req->session;
    struct nfs4_state   *state;
    unsigned int         seqid = args->open_stateid.seqid;

    state = &session->nfs4_session_state[seqid];

    nfs4_session_free_slot(session, seqid);

    chimera_vfs_release(thread->vfs_thread, state->nfs4_state_handle);

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_close */