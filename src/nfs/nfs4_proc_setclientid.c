#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETCLIENTID4args          *args   = &argop->opsetclientid;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl_rpc2_conn            *conn   = req->conn;
    struct nfs4_session              *session;

    resop->opsetclientid.resok4.clientid = nfs4_client_register(
        &thread->shared->nfs4_shared_clients,
        args->client.id.data,
        args->client.id.len,
        *(uint64_t *) args->client.verifier,
        40,
        NULL, NULL);


    session = nfs4_create_session(
        &shared->nfs4_shared_clients,
        resop->opsetclientid.resok4.clientid,
        1,
        NULL,
        NULL);

    conn->private_data = session;
    req->session       = session;

    resop->opsetclientid.status = NFS4_OK;

    memcpy(&resop->opsetclientid.resok4.setclientid_confirm,
           session->nfs4_session_id,
           sizeof(session->nfs4_session_id));

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */