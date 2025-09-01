// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"

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