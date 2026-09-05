// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_drc.h"

/*
 * The SEQUENCE result of a successful slot acquisition.
 *
 * RFC 8881 Section 2.10.6.1.1 requires this reply to be cached, and permits a
 * replier to recompute the slot-usage fields rather than store them; that is
 * what this does, which is also why the uncached-retry path can be answered
 * from it: the reply the client should have received is reconstructible from
 * the session and the args alone.
 */
static void
nfs4_sequence_fill_resok(
    struct SEQUENCE4res  *res,
    struct nfs4_session  *session,
    struct SEQUENCE4args *args)
{
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
} /* nfs4_sequence_fill_resok */

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
        /* The client may have failed over from another node: its session
         * record can live in the shared KV store.  Reconstruct it lazily,
         * holding the client off with NFS4ERR_DELAY until the (idempotent)
         * scan settles -- the retry then finds the live session.  A sessionid
         * that is genuinely unknown resolves to NFS4ERR_BADSESSION. */
        nfsstat4 hs = (nfs4_drc_session_hydrate(thread, args->sa_sessionid) ==
                       NFS4_DRC_HYDRATE_INFLIGHT) ? NFS4ERR_DELAY
                                                  : NFS4ERR_BADSESSION;
        res->sr_status = hs;
        chimera_nfs4_compound_complete(req, hs);
        return;
    }

    /* Bind this session to the conn (idempotent if already bound) and
     * drop the +1 ref returned by nfs4_session_lookup -- the conn's ref
     * keeps the session alive for the rest of this compound. */
    nfs4_session_bind_conn(req->conn, session);
    nfs4_session_put(session);

    req->session = session;

    /*
     * The COMPOUND-shape limits are checked before the slot is touched.
     *
     * RFC 8881 §2.10.6.1.2 and §18.46.3: when SEQUENCE returns an error "the
     * sequence ID of the slot MUST NOT change" and the replier "MUST NOT
     * modify the reply cache entry for the slot".  Checking these after
     * nfs4_replay_slot_acquire left the slot CAS-advanced to the new seqid and
     * finalized as COMPLETED with no cached bytes, so the client's retry at the
     * old seqid was answered NFS4ERR_RETRY_UNCACHED_REP forever -- one
     * over-large COMPOUND permanently wedged the slot.  None of these limits
     * depend on slot state, so they can simply be evaluated first.
     */
    if (session->nfs4_session_fore_attrs.ca_maxrequestsize &&
        marshall_length_COMPOUND4args(req->args_compound) >
        (int) session->nfs4_session_fore_attrs.ca_maxrequestsize) {
        res->sr_status = NFS4ERR_REQ_TOO_BIG;
        chimera_nfs4_compound_complete(req, NFS4ERR_REQ_TOO_BIG);
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

    /* The reply is reconstructible from the session and the args alone, so it
     * can be built before the slot is claimed; that is what lets the
     * cacheability check below also run without disturbing the slot. */
    nfs4_sequence_fill_resok(res, session, args);

    if (args->sa_cachethis &&
        session->nfs4_session_fore_attrs.ca_maxresponsesize_cached &&
        marshall_length_nfs_resop4(resop) >
        (int) session->nfs4_session_fore_attrs.ca_maxresponsesize_cached) {
        res->sr_status = NFS4ERR_REP_TOO_BIG_TO_CACHE;
        chimera_nfs4_compound_complete(req, NFS4ERR_REP_TOO_BIG_TO_CACHE);
        return;
    }

    status = nfs4_replay_slot_acquire(session,
                                      args->sa_slotid,
                                      args->sa_sequenceid,
                                      args->sa_cachethis,
                                      req,
                                      &is_replay);

    /*
     * A retry of a request whose reply was not cached.
     *
     * RFC 8881 Section 2.10.6.1.1: "When a SEQUENCE or CB_SEQUENCE operation is
     * successfully executed, its reply MUST always be cached" -- even when the
     * client did not ask for the rest of the COMPOUND to be.  So a retry has a
     * SEQUENCE reply to be answered from, and Section 2.10.6.1.3 says what the
     * rest of the COMPOUND gets: the replier "enters into its reply cache a
     * reply consisting of the original results to the SEQUENCE ... operation,
     * and with the next operation in COMPOUND ... having the error
     * NFS4ERR_RETRY_UNCACHED_REP", and it "MUST NOT return
     * NFS4ERR_RETRY_UNCACHED_REP in reply to a Sequence operation if the
     * Sequence operation is the first operation" -- which it always is, since
     * a COMPOUND that does not begin with SEQUENCE is refused earlier.
     *
     * A lone SEQUENCE has no next operation, and nothing but the SEQUENCE reply
     * to cache, so the cached success is the whole answer.
     */
    if (status == NFS4ERR_RETRY_UNCACHED_REP) {
        /* res already carries the reconstructed SEQUENCE reply. */
        if (req->args_compound->num_argarray > 1) {
            req->index = 1;
            nfs4_fail_undispatched_op(thread,
                                      &req->args_compound->argarray[1],
                                      &req->res_compound.resarray[1],
                                      NFS4ERR_RETRY_UNCACHED_REP);
            chimera_nfs4_compound_complete(req, NFS4ERR_RETRY_UNCACHED_REP);
        } else {
            chimera_nfs4_compound_complete(req, NFS4_OK);
        }
        return;
    }

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
        nfs_client_touch(session->client_unified);
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* SEQUENCE is the 4.1+ lease tick (RFC 8881 §8.4).  §18.46.3 requires the
     * lease be renewed only when SEQUENCE returns NFS4_OK and the slot state
     * advances -- never on a BADSLOT/MISORDERED/REQ_TOO_BIG/TOO_MANY_OPS error
     * -- so the touch happens here, after every validation has passed, rather
     * than up front. */
    nfs_client_touch(session->client_unified);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_sequence */
