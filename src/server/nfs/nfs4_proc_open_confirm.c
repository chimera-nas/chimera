// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * RFC 7530 §16.18: OPEN_CONFIRM
 *
 * Confirms a new open_owner.  Server sets OPEN4_RESULT_CONFIRM in the OPEN
 * response for an as-yet-unconfirmed owner; the client must follow with
 * OPEN_CONFIRM(seqid+1, open_stateid) before issuing any further state-
 * modifying op against that owner.  A successful OPEN_CONFIRM:
 *   - Verifies the open_owner is unconfirmed.
 *   - Validates the supplied stateid against the open_state.
 *   - Checks the seqid is owner.seqid + 1.
 *   - Flips owner.confirmed = true.
 *   - Bumps stateid.seqid and returns the new stateid.
 *
 * 4.1+ does not use this op; Phase 0's op_matrix already rejects it for
 * 4.1+ minorversions.
 */

#include "nfs4_procs.h"
#include "nfs4_state.h"
#include "vfs/vfs_release.h"

void
chimera_nfs4_open_confirm(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct OPEN_CONFIRM4args *args  = &argop->opopen_confirm;
    struct OPEN_CONFIRM4res  *res   = &resop->opopen_confirm;
    struct nfs_state_table   *table = &thread->shared->nfs4_state_table;
    void                     *state_void;
    uint8_t                   state_type;
    struct nfs_open_state    *open_state;
    struct nfs_open_owner    *owner;
    nfsstat4                  status;
    int                       seqid_class;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = nfs_state_table_acquire(table, &args->open_stateid,
                                     NFS4_SLOT_TYPE_OPEN,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    open_state = state_void;
    owner      = open_state->owner;

    pthread_mutex_lock(&owner->lock);

    seqid_class = nfs4_owner_seqid_classify(owner->seqid, &owner->replay,
                                            args->seqid);

    if (seqid_class == NFS4_SEQID_REPLAY) {
        /* RFC 7530 §9.1.7: return cached reply.  For OPEN_CONFIRM the
         * resok carries the confirmed stateid. */
        res->status              = owner->replay.status;
        res->resok4.open_stateid = owner->replay.stateid;
        pthread_mutex_unlock(&owner->lock);
        nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    if (seqid_class != NFS4_SEQID_NEW) {
        pthread_mutex_unlock(&owner->lock);
        nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);
        res->status = NFS4ERR_BAD_SEQID;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.18: OPEN_CONFIRM is only valid for an unconfirmed owner.
     * A (non-replayed) confirm of an already-confirmed owner is a stale use of
     * the stateid. */
    if (owner->confirmed) {
        pthread_mutex_unlock(&owner->lock);
        nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);
        res->status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* New op: confirm the owner and advance state.seqid. */
    owner->confirmed   = true;
    owner->seqid       = args->seqid;
    open_state->seqid += 1;

    nfs4_stateid_encode(&res->resok4.open_stateid,
                        open_state->seqid,
                        NFS4_STATEID_TYPE_OPEN,
                        open_state->shard, open_state->slot_idx,
                        open_state->generation,
                        table->epoch);

    nfs4_replay_record(&owner->replay, args->seqid, OP_OPEN_CONFIRM,
                       NFS4_OK, &res->resok4.open_stateid);

    pthread_mutex_unlock(&owner->lock);

    nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                            thread->vfs_thread);

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_open_confirm */
