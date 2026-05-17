// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"

void
chimera_nfs4_sequence(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SEQUENCE4args *args = &argop->opsequence;
    struct SEQUENCE4res  *res  = &resop->opsequence;

    struct nfs4_session  *session = nfs4_session_lookup(
        &thread->shared->nfs4_shared_clients,
        args->sa_sessionid);

    if (!session) {
        res->sr_status = NFS4ERR_BADSESSION;
        chimera_nfs4_compound_complete(req, NFS4ERR_BADSESSION);
        return;
    }

    /* Bind this session to the conn (idempotent if already bound) and drop
     * the +1 ref returned by nfs4_session_lookup -- the conn's ref keeps
     * the session alive for the rest of this compound. */
    nfs4_session_bind_conn(req->conn, session);
    nfs4_session_put(session);

    /* Store session in request for use by subsequent operations */
    req->session = session;

    res->sr_status = NFS4_OK;

    memcpy(res->sr_resok4.sr_sessionid, session->nfs4_session_id,
           NFS4_SESSIONID_SIZE);
    res->sr_resok4.sr_sequenceid            = args->sa_sequenceid;
    res->sr_resok4.sr_slotid                = args->sa_slotid;
    res->sr_resok4.sr_highest_slotid        = session->nfs4_session_fore_attrs.ca_maxrequests;
    res->sr_resok4.sr_target_highest_slotid = session->nfs4_session_fore_attrs.ca_maxrequests;
    res->sr_resok4.sr_status_flags          = 0;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_reclaim_complete */