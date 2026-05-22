// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

void
chimera_nfs4_sequence(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SEQUENCE4args *args = &argop->opsequence;
    struct SEQUENCE4res  *res  = &resop->opsequence;
    struct nfs4_session  *session;
    bool                  is_replay = false;
    nfsstat4              status;

    session = nfs4_session_lookup(
        &thread->shared->nfs4_shared_clients,
        args->sa_sessionid);

    if (!session) {
        res->sr_status = NFS4ERR_BADSESSION;
        chimera_nfs4_compound_complete(req, NFS4ERR_BADSESSION);
        return;
    }

    /* Bind this session to the conn (idempotent if already bound) and
     * drop the +1 ref returned by nfs4_session_lookup -- the conn's ref
     * keeps the session alive for the rest of this compound. */
    nfs4_session_bind_conn(req->conn, session);
    nfs4_session_put(session);

    req->session = session;

    /* SEQUENCE is the 4.1+ lease tick (RFC 8881 §8.4); touch the owning
     * client's lease before consulting the slot table. */
    nfs_client_touch(session->client_unified);

    status = nfs4_replay_slot_acquire(session,
                                      args->sa_slotid,
                                      args->sa_sequenceid,
                                      args->sa_cachethis,
                                      req,
                                      &is_replay);

    if (status != NFS4_OK) {
        res->sr_status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    if (is_replay) {
        /* Phase 2 will short-circuit the compound dispatcher and send
         * the cached reply bytes verbatim.  Phase 1 has no cache so
         * this branch is unreachable today; nfs4_replay_slot_acquire
         * returns NFS4ERR_RETRY_UNCACHED_REP instead. */
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* RFC 8881 §2.10.6.4: a COMPOUND with more operations than the session's
     * negotiated fore-channel ca_maxoperations (SEQUENCE counts as one) is
     * rejected with NFS4ERR_TOO_MANY_OPS. */
    if (session->nfs4_session_fore_attrs.ca_maxoperations &&
        req->args_compound->num_argarray >
        session->nfs4_session_fore_attrs.ca_maxoperations) {
        res->sr_status = NFS4ERR_TOO_MANY_OPS;
        chimera_nfs4_compound_complete(req, NFS4ERR_TOO_MANY_OPS);
        return;
    }

    res->sr_status = NFS4_OK;
    memcpy(res->sr_resok4.sr_sessionid, session->nfs4_session_id,
           NFS4_SESSIONID_SIZE);
    res->sr_resok4.sr_sequenceid = args->sa_sequenceid;
    res->sr_resok4.sr_slotid     = args->sa_slotid;
    /* RFC 5661 18.46.3: sr_highest_slotid is the *maximum slot id*, not
     * the count.  Linux nfsd returns max_slots - 1. */
    res->sr_resok4.sr_highest_slotid =
        session->replay_max_slots ? session->replay_max_slots - 1 : 0;
    res->sr_resok4.sr_target_highest_slotid = res->sr_resok4.sr_highest_slotid;
    res->sr_resok4.sr_status_flags          = 0;

    /* RFC 8881 §2.10.6.3 / §18.46.3: signal the client that one or more of its
     * recallable objects (delegations) have been revoked, so it issues
     * TEST_STATEID / FREE_STATEID to recover.  Cleared once the client has
     * freed every revoked delegation. */
    if (session->client_unified &&
        atomic_load_explicit(&session->client_unified->revoked_deleg_count,
                             memory_order_acquire) > 0) {
        res->sr_resok4.sr_status_flags |= SEQ4_STATUS_RECALLABLE_STATE_REVOKED;
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_sequence */
