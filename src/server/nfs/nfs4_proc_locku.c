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
    struct nfs4_range_lease  *rl, *match;
    uint64_t                  vfs_length;
    nfsstat4                  status;

    /* RFC 7530 §16.12.3: current filehandle must be set */
    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->lock_stateid);

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
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
        if (seqid_class != NFS4_SEQID_NEW) {
            pthread_mutex_unlock(&lock_owner->lock);
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
            req->nfs_state_ref = NULL;
            res->status        = NFS4ERR_BAD_SEQID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
        pthread_mutex_unlock(&lock_owner->lock);
    }

    /* RFC 7530 §9.1.4.2: a stale lock-stateid seqid is NFS4ERR_OLD_STATEID
     * (and a never-issued one NFS4ERR_BAD_STATEID); this takes precedence over
     * the byte-range check below.  Gated on 4.0 (4.1+ drops seqid coupling). */
    if (is_v40) {
        status = nfs4_stateid_check_seqid(lock_state->seqid,
                                          args->lock_stateid.seqid);
        if (status != NFS4_OK) {
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
            req->nfs_state_ref = NULL;
            res->status        = status;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
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

    /* NFSv4 and the VFS range layer share UINT64_MAX as the to-EOF sentinel. */
    vfs_length = args->length;

    /* RFC 7530 §16.12.4 / POSIX: remove the unlocked region [u_start, u_end)
     * from every interval of this lock-owner that it overlaps, splitting an
     * interval into its left and/or right remainders as needed.  Unlocking a
     * region that holds no lock is a successful no-op (LOCKU always succeeds). */
    {
        uint64_t                  u_start = args->offset;
        uint64_t                  u_end   =
            (args->offset + vfs_length < args->offset)
            ? UINT64_MAX : args->offset + vfs_length;
        struct nfs4_range_lease  *hits = NULL;
        struct nfs4_range_lease **pp   = &lock_state->range_leases;

        /* Detach all intervals overlapping the unlock range. */
        while (*pp) {
            rl = *pp;
            uint64_t e_start = rl->lease.offset;
            uint64_t e_end   =
                (rl->lease.offset + rl->lease.length < rl->lease.offset)
                ? UINT64_MAX : rl->lease.offset + rl->lease.length;

            if (e_end <= u_start || u_end <= e_start) {
                pp = &rl->next;
                continue;
            }
            *pp      = rl->next;
            rl->next = hits;
            hits     = rl;
        }

        /* Re-insert the non-unlocked remainder(s) of each detached interval. */
        while (hits) {
            uint64_t                       e_start = hits->lease.offset;
            uint64_t                       e_end   =
                (hits->lease.offset + hits->lease.length < hits->lease.offset)
                ? UINT64_MAX : hits->lease.offset + hits->lease.length;
            uint8_t                        mode   = hits->lease.mode.granted;
            struct chimera_vfs_lease_owner owner  = hits->lease.owner;
            struct chimera_vfs_file_state *fs     = hits->file_state;
            bool                           reused = false;

            match = hits;
            hits  = hits->next;

            chimera_vfs_lease_release(vfs_state, fs, &match->lease);

            if (e_start < u_start) {
                /* Left remainder [e_start, u_start): reuse this entry. */
                match->lease.offset = e_start;
                match->lease.length = u_start - e_start;
                chimera_vfs_state_try_insert(vfs_state, fs, &match->lease, NULL);
                match->next              = lock_state->range_leases;
                lock_state->range_leases = match;
                reused                   = true;
            }
            if (e_end > u_end) {
                if (!reused) {
                    /* Right remainder [u_end, e_end): reuse this entry. */
                    match->lease.offset = u_end;
                    match->lease.length = (e_end == UINT64_MAX) ? UINT64_MAX :
                        e_end - u_end;
                    chimera_vfs_state_try_insert(vfs_state, fs, &match->lease,
                                                 NULL);
                    match->next              = lock_state->range_leases;
                    lock_state->range_leases = match;
                    reused                   = true;
                } else {
                    /* Both remainders: the right one needs its own entry and a
                     * fresh file_state reference. */
                    struct chimera_vfs_file_state *fs2 =
                        chimera_vfs_state_get(vfs_state, fs->fh, fs->fh_len,
                                              fs->fh_hash, true);
                    if (fs2) {
                        nfs4_range_lease_insert(vfs_state, lock_state, fs2,
                                                &owner, mode, u_end, e_end);
                    }
                }
            }
            if (!reused) {
                /* Interval fully covered by the unlock: drop it. */
                chimera_vfs_state_put(vfs_state, fs);
                free(match);
            }
        }
    }

    lock_state->seqid += 1;
    nfs4_stateid_encode(&res->lock_stateid, lock_state->seqid,
                        NFS4_STATEID_TYPE_LOCK,
                        lock_state->shard, lock_state->slot_idx,
                        lock_state->generation, table->epoch);
    res->status = NFS4_OK;
    chimera_nfs4_set_current_stateid(req, &res->lock_stateid);

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
