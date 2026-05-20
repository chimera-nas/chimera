// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"

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
    struct channel_attrs4             clamped_fore;
    uint32_t                          flags = 0;
    uint32_t                          replay_max_slots;
    uint32_t                          replay_maxresp_cached;

    if (args->csa_flags & CREATE_SESSION4_FLAG_CONN_BACK_CHAN) {
        flags |= CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
    }

    /* RFC 5661 18.36.4: the server may negotiate ca_maxrequests and
     * ca_maxresponsesize_cached downward and return the effective values
     * to the client. */
    clamped_fore = args->csa_fore_chan_attrs;

    replay_max_slots = clamped_fore.ca_maxrequests;
    if (replay_max_slots == 0 ||
        replay_max_slots > NFS4_MAX_REPLY_CACHE_SLOTS) {
        replay_max_slots = NFS4_MAX_REPLY_CACHE_SLOTS;
    }
    clamped_fore.ca_maxrequests = replay_max_slots;

    replay_maxresp_cached = clamped_fore.ca_maxresponsesize_cached;
    if (replay_maxresp_cached > NFS4_MAX_CACHED_RESPONSE_SIZE) {
        replay_maxresp_cached = NFS4_MAX_CACHED_RESPONSE_SIZE;
    }
    clamped_fore.ca_maxresponsesize_cached = replay_maxresp_cached;

    session = nfs4_create_session(
        &shared->nfs4_shared_clients,
        args->csa_clientid,
        0,
        replay_max_slots,
        replay_maxresp_cached,
        &clamped_fore,
        &args->csa_back_chan_attrs);

    if (!session) {
        res->csr_status = NFS4ERR_STALE_CLIENTID;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    nfs4_session_bind_conn(conn, session);
    req->session = session;

    res->csr_status = NFS4_OK;
    memcpy(res->csr_resok4.csr_sessionid, session->nfs4_session_id, sizeof(res->csr_resok4.csr_sessionid));
    res->csr_resok4.csr_sequence = 1;
    res->csr_resok4.csr_flags    = flags;

    res->csr_resok4.csr_fore_chan_attrs = session->nfs4_session_fore_attrs;
    res->csr_resok4.csr_back_chan_attrs = session->nfs4_session_back_attrs;

    /* Drop the +1 ref returned by nfs4_create_session; the conn now holds
     * its own ref via bind_conn, and the hash holds the other ref. */
    nfs4_session_put(session);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */