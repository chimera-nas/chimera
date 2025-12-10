// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_create_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl_rpc2_conn            *conn   = req->conn;
    struct CREATE_SESSION4args       *args   = &argop->opcreate_session;
    struct CREATE_SESSION4res        *res    = &resop->opcreate_session;
    struct nfs4_session              *session;
    uint32_t                          flags = 0;

    if (args->csa_flags & CREATE_SESSION4_FLAG_CONN_BACK_CHAN) {
        flags |= CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
    }

    session = nfs4_create_session(
        &shared->nfs4_shared_clients,
        args->csa_clientid,
        0,
        &args->csa_fore_chan_attrs,
        &args->csa_back_chan_attrs);

    if (!session) {
        res->csr_status = NFS4ERR_STALE_CLIENTID;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    conn->private_data = session;
    req->session       = session;

    res->csr_status = NFS4_OK;
    memcpy(res->csr_resok4.csr_sessionid, session->nfs4_session_id, sizeof(res->csr_resok4.csr_sessionid));
    res->csr_resok4.csr_sequence = 1;
    res->csr_resok4.csr_flags    = flags;

    res->csr_resok4.csr_fore_chan_attrs = session->nfs4_session_fore_attrs;
    res->csr_resok4.csr_back_chan_attrs = session->nfs4_session_back_attrs;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */