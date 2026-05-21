// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * RFC 7530 §16.19: OPEN_DOWNGRADE
 *
 * Reduces the share_access / share_deny bits on an existing open_state
 * without closing it.  Args: open_stateid, seqid, share_access, share_deny.
 * The new bits must be a subset of the current bits (downgrade only).
 *
 * 4.1+ also has OPEN_DOWNGRADE (no seqid check), Phase 0 already routes it
 * for both minors.  This proc gates the seqid check on req->minorversion.
 */

#include "nfs4_procs.h"
#include "nfs4_state.h"
#include "vfs/vfs_release.h"

void
chimera_nfs4_open_downgrade(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct OPEN_DOWNGRADE4args *args  = &argop->opopen_downgrade;
    struct OPEN_DOWNGRADE4res  *res   = &resop->opopen_downgrade;
    struct nfs_state_table     *table = &thread->shared->nfs4_state_table;
    void                       *state_void;
    uint8_t                     state_type;
    struct nfs_open_state      *open_state;
    struct nfs_open_owner      *owner;
    nfsstat4                    status;
    bool                        is_v40 = (req->minorversion == 0);

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

    if (is_v40) {
        int seqid_class = nfs4_owner_seqid_classify(owner->seqid,
                                                    &owner->replay,
                                                    args->seqid);
        if (seqid_class == NFS4_SEQID_REPLAY) {
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

        /* RFC 7530 §9.1.4.2: reject a superseded (old) or never-issued
         * stateid seqid. */
        status = nfs4_stateid_check_seqid(open_state->seqid,
                                          args->open_stateid.seqid);
        if (status != NFS4_OK) {
            pthread_mutex_unlock(&owner->lock);
            nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                    thread->vfs_thread);
            res->status = status;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
    }

    /* RFC 7530 §16.19.4: new bits must be a non-empty subset of current. */
    if (args->share_access == 0 ||
        (args->share_access & ~open_state->share_access) ||
        (args->share_deny   & ~open_state->share_deny)) {
        pthread_mutex_unlock(&owner->lock);
        nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);
        res->status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    open_state->share_access = args->share_access;
    open_state->share_deny   = args->share_deny;
    open_state->seqid       += 1;

    nfs4_stateid_encode(&res->resok4.open_stateid,
                        open_state->seqid,
                        NFS4_STATEID_TYPE_OPEN,
                        open_state->shard, open_state->slot_idx,
                        open_state->generation,
                        table->epoch);

    if (is_v40) {
        owner->seqid = args->seqid;
        nfs4_replay_record(&owner->replay, args->seqid, OP_OPEN_DOWNGRADE,
                           NFS4_OK, &res->resok4.open_stateid);
    }

    pthread_mutex_unlock(&owner->lock);

    nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                            thread->vfs_thread);

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_open_downgrade */
