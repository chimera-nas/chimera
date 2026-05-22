// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * RFC 8881 §18.38: FREE_STATEID
 *
 * Frees a stateid whose state has already been released -- a lock stateid with
 * no remaining byte-range locks, or a stateid the server has revoked (in
 * particular a force-revoked delegation, reported via
 * SEQ4_STATUS_RECALLABLE_STATE_REVOKED / NFS4ERR_DELEG_REVOKED).  If state is
 * still held (an open, an active delegation, or a lock stateid that still owns
 * locks), the stateid cannot be freed and the server returns
 * NFS4ERR_LOCKS_HELD.  4.1+ only.
 */

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_stateid.h"

void
chimera_nfs4_free_stateid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct FREE_STATEID4args *args  = &argop->opfree_stateid;
    struct FREE_STATEID4res  *res   = &resop->opfree_stateid;
    struct nfs_state_table   *table = &thread->shared->nfs4_state_table;
    struct nfs_lock_state    *lock_state;
    void                     *state_void;
    uint8_t                   state_type;
    nfsstat4                  status;

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->fsa_stateid);

    /* The special anonymous/read-bypass stateids name no state to free. */
    if (nfs4_stateid_is_special(&args->fsa_stateid)) {
        res->fsr_status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, res->fsr_status);
        return;
    }

    status = nfs_state_table_acquire(table, &args->fsa_stateid, 0,
                                     &state_void, &state_type);

    /* A force-revoked delegation is reported by the lookup as DELEG_REVOKED
     * (the state itself is not handed back); FREE_STATEID is exactly how the
     * client disposes of it. */
    if (status == NFS4ERR_DELEG_REVOKED) {
        res->fsr_status = nfs_state_table_free_revoked_deleg(
            table, &args->fsa_stateid, thread->vfs_thread);
        chimera_nfs4_compound_complete(req, res->fsr_status);
        return;
    }

    if (status != NFS4_OK) {
        res->fsr_status = status;
        chimera_nfs4_compound_complete(req, res->fsr_status);
        return;
    }

    /* An open is released via CLOSE, and a still-valid delegation via
     * DELEGRETURN -- never FREE_STATEID. */
    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        nfs_state_table_release(table, state_void, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);
        res->fsr_status = NFS4ERR_LOCKS_HELD;
        chimera_nfs4_compound_complete(req, res->fsr_status);
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        nfs_state_table_release(table, state_void, NFS4_SLOT_TYPE_DELEG,
                                thread->vfs_thread);
        res->fsr_status = NFS4ERR_LOCKS_HELD;
        chimera_nfs4_compound_complete(req, res->fsr_status);
        return;
    }

    lock_state = state_void;

    /* A lock stateid that still owns byte-range locks cannot be freed. */
    if (lock_state->range_leases != NULL) {
        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                thread->vfs_thread);
        res->fsr_status = NFS4ERR_LOCKS_HELD;
        chimera_nfs4_compound_complete(req, res->fsr_status);
        return;
    }

    /* No locks remain: free the lock stateid.  destroy drops the lifetime
     * ref and marks the slot freed; the release below drops our acquire ref,
     * which then runs cleanup. */
    nfs_lock_state_destroy(lock_state, table, thread->vfs_thread);
    nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                            thread->vfs_thread);

    res->fsr_status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_free_stateid */
