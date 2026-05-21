// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"

void
chimera_nfs4_locku(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCKU4args        *args      = &argop->oplocku;
    struct LOCKU4res         *res       = &resop->oplocku;
    struct nfs_state_table   *table     = &thread->shared->nfs4_state_table;
    struct chimera_vfs_state *vfs_state = thread->vfs->vfs_state;
    bool                      is_v40    = (req->minorversion == 0);
    void                     *state_void;
    uint8_t                   state_type;
    struct nfs_lock_state    *lock_state;
    struct nfs_lock_owner    *lock_owner;
    struct nfs4_range_lease  *rl, *prev, *match;
    uint64_t                  vfs_length;
    nfsstat4                  status;

    /* RFC 7530 §16.12.3: current filehandle must be set */
    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = nfs_state_table_acquire(table, &args->lock_stateid,
                                     NFS4_SLOT_TYPE_LOCK,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    lock_state          = state_void;
    lock_owner          = lock_state->lock_owner;
    req->nfs_state_ref  = lock_state;
    req->nfs_state_type = NFS4_SLOT_TYPE_LOCK;

    /* A LOCK stateid's lock_state always carries a lock_owner (set at
     * nfs_lock_state_create); the deref below relies on it. */
    chimera_nfs_abort_if(!lock_owner, "lock_state without lock_owner");

    if (is_v40 && lock_owner) {
        pthread_mutex_lock(&lock_owner->lock);
        int seqid_class = nfs4_owner_seqid_classify(lock_owner->seqid,
                                                    &lock_owner->replay,
                                                    args->seqid);
        if (seqid_class == NFS4_SEQID_REPLAY) {
            res->status       = lock_owner->replay.status;
            res->lock_stateid = lock_owner->replay.stateid;
            pthread_mutex_unlock(&lock_owner->lock);
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
            req->nfs_state_ref = NULL;
            chimera_nfs4_compound_complete(req, NFS4_OK);
            return;
        }
        if (seqid_class != NFS4_SEQID_NEW) {
            pthread_mutex_unlock(&lock_owner->lock);
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
            req->nfs_state_ref = NULL;
            res->status        = NFS4ERR_BAD_SEQID;
            chimera_nfs4_compound_complete(req, NFS4_OK);
            return;
        }
        pthread_mutex_unlock(&lock_owner->lock);
    }

    /* RFC 7530 §16.12.4: same length rules as LOCK */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                thread->vfs_thread);
        req->nfs_state_ref = NULL;
        res->status        = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* NFS uses UINT64_MAX to mean "to end of file"; vfs_state uses 0. */
    vfs_length = args->length == UINT64_MAX ? 0 : args->length;

    /* RFC 7530 §16.12.4: the unlock range must match a held range exactly.
     * Find and detach it from this lock_state. */
    prev  = NULL;
    match = NULL;
    for (rl = lock_state->range_leases; rl; rl = rl->next) {
        if (rl->lease.offset == args->offset && rl->lease.length == vfs_length) {
            match = rl;
            break;
        }
        prev = rl;
    }

    if (!match) {
        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                thread->vfs_thread);
        req->nfs_state_ref = NULL;
        res->status        = NFS4ERR_LOCK_RANGE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (prev) {
        prev->next = match->next;
    } else {
        lock_state->range_leases = match->next;
    }

    /* Release the lease (sync) and free the range entry. */
    chimera_vfs_lease_release(vfs_state, match->file_state, &match->lease);
    chimera_vfs_state_put(vfs_state, match->file_state);
    free(match);

    lock_state->seqid += 1;
    nfs4_stateid_encode(&res->lock_stateid, lock_state->seqid,
                        NFS4_STATEID_TYPE_LOCK,
                        lock_state->shard, lock_state->slot_idx,
                        lock_state->generation, table->epoch);
    res->status = NFS4_OK;

    /* RFC 7530 §9.1.7: record cached reply for the lock_owner. */
    if (is_v40 && lock_owner) {
        pthread_mutex_lock(&lock_owner->lock);
        lock_owner->seqid = args->seqid;
        nfs4_replay_record(&lock_owner->replay, args->seqid, OP_LOCKU,
                           NFS4_OK, &res->lock_stateid);
        pthread_mutex_unlock(&lock_owner->lock);
    }

    nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                            thread->vfs_thread);
    req->nfs_state_ref = NULL;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_locku */
