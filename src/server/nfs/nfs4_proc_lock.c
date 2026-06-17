// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"

/*
 * RFC 7530 §9.1.7 LOCK completion wrapper.  Advances the owner seqid(s)
 * and caches the reply for every outcome that nfs4_seqid_should_advance()
 * accepts.  Routing rules per the LOCK4args.locker.new_lock_owner flag:
 *
 *   new_lock_owner=true:
 *     - advance open_owner.seqid to args->locker.open_owner.open_seqid;
 *     - advance lock_owner.seqid to args->locker.open_owner.lock_seqid
 *       (this is the initialization step for a brand-new lock_owner);
 *     - cache the reply on the open_owner keyed by open_seqid (that is
 *       the seqid the client retransmits).
 *
 *   new_lock_owner=false:
 *     - advance lock_owner.seqid to args->locker.lock_owner.lock_seqid;
 *     - cache the reply on the lock_owner keyed by lock_seqid.
 *
 * Called instead of chimera_nfs4_compound_complete on every LOCK
 * response path (entry-time short-circuits, length validation, VFS
 * completion).  Safe to call when the lock_4_0_* fields are NULL --
 * that means classification didn't run (4.1+ or pre-classification
 * NOFILEHANDLE etc.), and the wrapper falls through.
 */
static void
chimera_nfs4_lock_finish(
    struct nfs_request *req,
    nfsstat4            status)
{
    if (req->minorversion == 0 &&
        nfs4_seqid_should_advance(status)) {

        struct LOCK4args      *args =
            &req->args_compound->argarray[req->index].oplock;
        struct LOCK4res       *res =
            &req->res_compound.resarray[req->index].oplock;
        const struct stateid4 *cache_stateid =
            (status == NFS4_OK) ? &res->resok4.lock_stateid : NULL;

        if (args->locker.new_lock_owner) {
            struct nfs_open_owner *oo         = req->lock_4_0_open_owner;
            struct nfs_lock_owner *lo         = req->lock_4_0_lock_owner;
            uint32_t               open_seqid =
                args->locker.open_owner.open_seqid;
            uint32_t               lock_seqid =
                args->locker.open_owner.lock_seqid;

            if (oo) {
                pthread_mutex_lock(&oo->lock);
                oo->seqid = open_seqid;
                nfs4_replay_record(&oo->replay, open_seqid, OP_LOCK,
                                   status, cache_stateid);
                pthread_mutex_unlock(&oo->lock);
            }
            if (lo) {
                pthread_mutex_lock(&lo->lock);
                lo->seqid = lock_seqid;
                /* The initial LOCK is replayed through the open_owner seqid,
                 * but the lock_owner must still be marked initialized so the
                 * next existing-lock-owner request cannot pick an arbitrary
                 * fresh seqid. */
                nfs4_replay_record(&lo->replay, lock_seqid, OP_LOCK,
                                   status, cache_stateid);
                pthread_mutex_unlock(&lo->lock);
            }
        } else {
            struct nfs_lock_owner *lo         = req->lock_4_0_lock_owner;
            uint32_t               lock_seqid =
                args->locker.lock_owner.lock_seqid;

            if (lo) {
                pthread_mutex_lock(&lo->lock);
                lo->seqid = lock_seqid;
                nfs4_replay_record(&lo->replay, lock_seqid, OP_LOCK,
                                   status, cache_stateid);
                pthread_mutex_unlock(&lo->lock);
            }
        }
    }

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_lock_finish */

static void
chimera_nfs4_lock_complete(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    struct nfs_request       *req        = private_data;
    struct LOCK4args         *args       = &req->args_compound->argarray[req->index].oplock;
    struct LOCK4res          *res        = &req->res_compound.resarray[req->index].oplock;
    struct nfs_state_table   *table      = &req->thread->shared->nfs4_state_table;
    struct nfs_lock_state    *lock_state = req->nfs_state_ref;
    struct nfs4_range_lease  *rl         = req->nfs_inflight_range;
    struct chimera_vfs_state *vfs_state  = req->thread->vfs->vfs_state;

    (void) granted;

    req->nfs_inflight_range = NULL;

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        /* POSIX consolidation (RFC 7530 §16.10.4): coalesce this newly-granted
         * range with any same-mode interval of the same lock-owner that it
         * overlaps or abuts, so a single merged interval is what LOCKT reports
         * and LOCKU operates on.  All entries on this lock_state share one
         * lock-owner, so only the lock mode must match. */
        uint64_t                  n_start = rl->lease.offset;
        /* The VFS range layer stores to-EOF as UINT64_MAX; computing the
         * exclusive end overflow-safe maps a to-EOF (or wrapping) length to
         * UINT64_MAX. */
        uint64_t                  n_end =
            (rl->lease.offset + rl->lease.length < rl->lease.offset)
            ? UINT64_MAX : rl->lease.offset + rl->lease.length;
        uint8_t                   n_mode     = rl->lease.mode.granted;
        uint64_t                  orig_start = n_start;
        uint64_t                  orig_end   = n_end;
        struct nfs4_range_lease **pp         = &lock_state->range_leases;

        while (*pp) {
            struct nfs4_range_lease *e       = *pp;
            uint64_t                 e_start = e->lease.offset;
            uint64_t                 e_end   =
                (e->lease.offset + e->lease.length < e->lease.offset)
                ? UINT64_MAX : e->lease.offset + e->lease.length;

            if (e->lease.mode.granted == n_mode &&
                e_start <= n_end && n_start <= e_end) {
                if (e_start < n_start) {
                    n_start = e_start;
                }
                if (e_end > n_end) {
                    n_end = e_end;
                }
                *pp = e->next;
                nfs4_range_lease_free(vfs_state, e);
            } else {
                pp = &e->next;
            }
        }

        if (n_start != orig_start || n_end != orig_end) {
            /* Grew via merge: re-take the lease at the coalesced extent. */
            chimera_vfs_lease_release(vfs_state, rl->file_state, &rl->lease);
            rl->lease.offset = n_start;
            rl->lease.length = (n_end == UINT64_MAX) ? UINT64_MAX : (n_end - n_start);
            chimera_vfs_state_try_insert(vfs_state, rl->file_state, &rl->lease,
                                         NULL);
        }

        /* Link the granted range lease onto the lock_state so LOCKU /
         * teardown can find and release it. */
        rl->next                 = lock_state->range_leases;
        lock_state->range_leases = rl;

        /* RFC 7530 §9.1.3: stateid.seqid starts at 1 for a new lock_stateid;
         * only increment when modifying an existing lock state. */
        if (!args->locker.new_lock_owner) {
            lock_state->seqid += 1;
        }

        nfs4_stateid_encode(&res->resok4.lock_stateid, lock_state->seqid,
                            NFS4_STATEID_TYPE_LOCK,
                            lock_state->shard, lock_state->slot_idx,
                            lock_state->generation, table->epoch);

        res->status = NFS4_OK;
        chimera_nfs4_set_current_stateid(req, &res->resok4.lock_stateid);

        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
        chimera_nfs4_lock_finish(req, NFS4_OK);
        return;
    }

    /* DENIED (or wait=false BREAKING): discard the half-built range lease. */
    chimera_vfs_state_put(vfs_state, rl->file_state);
    free(rl);

    /* Tear down the freshly-allocated lock_state if this was a new
     * lock_owner request; otherwise just drop the acquire ref. */
    if (args->locker.new_lock_owner) {
        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                req->thread->vfs_thread);
        nfs_lock_state_destroy(lock_state, table, req->thread->vfs_thread);
    } else {
        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                req->thread->vfs_thread);
    }
    req->nfs_state_ref = NULL;

    res->status        = NFS4ERR_DENIED;
    res->denied.offset = conflict ? conflict->offset : 0;
    /* conflict->length already uses UINT64_MAX for a to-EOF holder. */
    res->denied.length   = conflict ? conflict->length : UINT64_MAX;
    res->denied.locktype = (conflict && (conflict->mode.granted & CHIMERA_VFS_LEASE_MODE_W)) ?
        WRITE_LT : READ_LT;
    /* vfs_state does not expose the conflicting lock's NFS owner; zero it. */
    res->denied.owner.clientid   = 0;
    res->denied.owner.owner.len  = 0;
    res->denied.owner.owner.data = NULL;
    chimera_nfs4_lock_finish(req, res->status);
} /* chimera_nfs4_lock_complete */

void
chimera_nfs4_lock(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCK4args               *args  = &argop->oplock;
    struct LOCK4res                *res   = &resop->oplock;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    struct nfs_open_state          *open_state;
    struct nfs_lock_state          *lock_state;
    struct chimera_vfs_open_handle *handle;
    void                           *state_void;
    uint8_t                         state_type;
    uint32_t                        lock_type;
    nfsstat4                        status;

    /* RFC 7530 §16.10.3: current filehandle must be set */
    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_lock_finish(req, res->status);
        return;
    }

    if (args->locktype == READ_LT || args->locktype == READW_LT) {
        lock_type = CHIMERA_VFS_LOCK_READ;
    } else {
        lock_type = CHIMERA_VFS_LOCK_WRITE;
    }

    if (args->locker.new_lock_owner) {
        struct state_owner4   *lo_args = &args->locker.open_owner.lock_owner;
        struct nfs_client     *client;
        struct nfs_lock_owner *lock_owner;
        bool                   created;

        /* Validate the supplied open stateid. */
        status = nfs_state_table_acquire(table,
                                         &args->locker.open_owner.open_stateid,
                                         NFS4_SLOT_TYPE_OPEN,
                                         &state_void, &state_type);
        if (status != NFS4_OK) {
            res->status = status;
            chimera_nfs4_lock_finish(req, res->status);
            return;
        }
        open_state = state_void;
        client     = open_state->owner->client;

        /* RFC 7530 §9.1.4: the new lock-owner's clientid must be the client
         * that holds the open being locked.  A mismatched (e.g. stale)
         * clientid is a bad stateid. */
        if (req->minorversion == 0 &&
            lo_args->clientid != client->client_id) {
            nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                    thread->vfs_thread);
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_lock_finish(req, res->status);
            return;
        }

        /* RFC 7530 §9.1.4: lock_owner is scoped to the client.  Find or
         * create one and link the new lock_state to both the lock_owner and
         * the open_state. */
        lock_owner = nfs_lock_owner_find_or_create(client,
                                                   lo_args->owner.data,
                                                   lo_args->owner.len,
                                                   &created);

        /* RFC 7530 §9.1.7 entry-time seqid classification on the open_owner
         * (whose open_seqid the client advanced to perform this LOCK).
         * The lock_owner may be brand new; its seqid is initialized on
         * success in chimera_nfs4_lock_finish.  Done BEFORE the lock_state
         * is created so a replay short-circuits without leaving stray state. */
        if (req->minorversion == 0) {
            struct nfs_open_owner *oo = open_state->owner;
            pthread_mutex_lock(&oo->lock);
            int                    cls = nfs4_owner_seqid_classify(
                oo->seqid, &oo->replay,
                args->locker.open_owner.open_seqid);

            if (cls == NFS4_SEQID_REPLAY) {
                res->status              = oo->replay.status;
                res->resok4.lock_stateid = oo->replay.stateid;
                pthread_mutex_unlock(&oo->lock);
                nfs_state_table_release(table, open_state,
                                        NFS4_SLOT_TYPE_OPEN,
                                        thread->vfs_thread);
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
            if (cls != NFS4_SEQID_NEW) {
                pthread_mutex_unlock(&oo->lock);
                nfs_state_table_release(table, open_state,
                                        NFS4_SLOT_TYPE_OPEN,
                                        thread->vfs_thread);
                res->status = NFS4ERR_BAD_SEQID;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
            pthread_mutex_unlock(&oo->lock);

            /* RFC 7530 §9.1.4.2: the supplied open stateid must not be a
             * superseded (old) or never-issued seqid. */
            status = nfs4_stateid_check_seqid(
                open_state->seqid,
                args->locker.open_owner.open_stateid.seqid);
            if (status != NFS4_OK) {
                nfs_state_table_release(table, open_state,
                                        NFS4_SLOT_TYPE_OPEN,
                                        thread->vfs_thread);
                res->status = status;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }

            req->lock_4_0_open_owner = oo;
            req->lock_4_0_lock_owner = lock_owner;
        }

        /* Take a distinct +1 dup on the VFS handle: lock_state's lifetime
         * is independent of the open_state's. */
        handle = open_state->handle;
        chimera_vfs_dup_handle(thread->vfs_thread, handle);

        lock_state = nfs_lock_state_create(lock_owner, open_state, handle,
                                           table, &res->resok4.lock_stateid);

        /* Release the open_state acquire ref.  The lock_state holds its
         * own dup of the handle; the open_state's lifetime remains its
         * own concern. */
        nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);

        /* For the async VFS call, hold an acquire ref on the lock_state.
         * Acquire it via the slot table so the refcount machinery is
         * consistent (avoid manual increments). */
        struct stateid4 lock_stateid_for_acquire;
        nfs4_stateid_encode(&lock_stateid_for_acquire, lock_state->seqid,
                            NFS4_STATEID_TYPE_LOCK,
                            lock_state->shard, lock_state->slot_idx,
                            lock_state->generation,
                            table->epoch);
        status = nfs_state_table_acquire(table, &lock_stateid_for_acquire,
                                         NFS4_SLOT_TYPE_LOCK,
                                         &state_void, &state_type);
        chimera_nfs_abort_if(status != NFS4_OK,
                             "freshly-created lock_state not findable");

        req->nfs_state_ref  = lock_state;
        req->nfs_state_type = NFS4_SLOT_TYPE_LOCK;

    } else {
        /* Subsequent lock under an existing lock stateid. */
        status = nfs_state_table_acquire(table,
                                         &args->locker.lock_owner.lock_stateid,
                                         NFS4_SLOT_TYPE_LOCK,
                                         &state_void, &state_type);
        if (status != NFS4_OK) {
            res->status = status;
            chimera_nfs4_lock_finish(req, res->status);
            return;
        }
        lock_state          = state_void;
        handle              = lock_state->handle;
        req->nfs_state_ref  = lock_state;
        req->nfs_state_type = NFS4_SLOT_TYPE_LOCK;

        /* RFC 7530 §9.1.7 entry-time seqid classification on the lock_owner. */
        if (req->minorversion == 0) {
            struct nfs_lock_owner *lo = lock_state->lock_owner;
            pthread_mutex_lock(&lo->lock);
            int                    cls = nfs4_owner_seqid_classify(
                lo->seqid, &lo->replay,
                args->locker.lock_owner.lock_seqid);

            if (cls == NFS4_SEQID_REPLAY) {
                res->status              = lo->replay.status;
                res->resok4.lock_stateid = lo->replay.stateid;
                pthread_mutex_unlock(&lo->lock);
                nfs_state_table_release(table, lock_state,
                                        NFS4_SLOT_TYPE_LOCK,
                                        thread->vfs_thread);
                req->nfs_state_ref = NULL;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
            if (cls != NFS4_SEQID_NEW) {
                pthread_mutex_unlock(&lo->lock);
                nfs_state_table_release(table, lock_state,
                                        NFS4_SLOT_TYPE_LOCK,
                                        thread->vfs_thread);
                req->nfs_state_ref = NULL;
                res->status        = NFS4ERR_BAD_SEQID;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
            pthread_mutex_unlock(&lo->lock);

            /* RFC 7530 §9.1.4.2: the supplied lock stateid must not be a
             * superseded (old) or never-issued seqid. */
            status = nfs4_stateid_check_seqid(
                lock_state->seqid,
                args->locker.lock_owner.lock_stateid.seqid);
            if (status != NFS4_OK) {
                nfs_state_table_release(table, lock_state,
                                        NFS4_SLOT_TYPE_LOCK,
                                        thread->vfs_thread);
                req->nfs_state_ref = NULL;
                res->status        = status;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }

            req->lock_4_0_lock_owner = lo;
        }
    }

    /* RFC 7530 §16.10.4: length must not be zero; if not the all-ones
     * "to-EOF" sentinel, offset+length must not exceed UINT64_MAX. */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        if (args->locker.new_lock_owner) {
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
            nfs_lock_state_destroy(lock_state, table, thread->vfs_thread);
        } else {
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
        }
        req->nfs_state_ref = NULL;
        res->status        = NFS4ERR_INVAL;
        chimera_nfs4_lock_finish(req, res->status);
        return;
    }

    /* Acquire the byte-range lease through vfs_state for cross-protocol
     * (NLM / SMB) coordination.  NFS uses UINT64_MAX for "to EOF";
     * vfs_state uses 0. */
    {
        struct chimera_vfs_state      *vfs_state = thread->vfs->vfs_state;
        struct chimera_vfs_file_state *file_state;
        struct nfs4_range_lease       *rl;

        rl = calloc(1, sizeof(*rl));
        if (!rl) {
            if (args->locker.new_lock_owner) {
                nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                        thread->vfs_thread);
                nfs_lock_state_destroy(lock_state, table, thread->vfs_thread);
            } else {
                nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                        thread->vfs_thread);
            }
            req->nfs_state_ref = NULL;
            res->status        = NFS4ERR_RESOURCE;
            chimera_nfs4_lock_finish(req, res->status);
            return;
        }

        file_state = chimera_vfs_state_get(vfs_state,
                                           handle->fh, handle->fh_len,
                                           handle->fh_hash, true);
        if (!file_state) {
            free(rl);
            if (args->locker.new_lock_owner) {
                nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                        thread->vfs_thread);
                nfs_lock_state_destroy(lock_state, table, thread->vfs_thread);
            } else {
                nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                        thread->vfs_thread);
            }
            req->nfs_state_ref = NULL;
            res->status        = NFS4ERR_RESOURCE;
            chimera_nfs4_lock_finish(req, res->status);
            return;
        }

        rl->file_state         = file_state;
        rl->lease.kind         = CHIMERA_VFS_LEASE_RANGE;
        rl->lease.mode.granted = (lock_type == CHIMERA_VFS_LOCK_READ) ?
            CHIMERA_VFS_LEASE_MODE_R : CHIMERA_VFS_LEASE_MODE_W;
        rl->lease.offset = args->offset;
        /* NFSv4 and the VFS range layer share UINT64_MAX as the to-EOF
         * sentinel, so the wire length is stored unchanged. */
        rl->lease.length           = args->length;
        rl->lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_NFSV4;
        rl->lease.owner.client_key = lock_state->lock_owner->client->client_id;
        rl->lease.owner.owner_lo   = XXH3_64bits(lock_state->lock_owner->owner,
                                                 lock_state->lock_owner->owner_len);
        rl->lease.owner.owner_hi = 0;
        /* Courteous server: report this lock dead once the owning client's
         * lease lapses, so a conflicting acquire reclaims it; reclaim flags
         * the client for sweep teardown. */
        rl->lease.owner.is_alive_cb = nfs_client_lease_alive;
        rl->lease.owner.revoked_cb  = nfs_client_lease_revoked_cb;
        rl->lease.owner.cb_private  = lock_state->lock_owner->client;

        req->nfs_inflight_range = rl;

        /* wait=false: NFSv4 LOCK returns DENIED on conflict (matching the
         * prior backend behavior).  A cross-protocol breakable conflict
         * still kicks off the break inside try_insert. */
        chimera_vfs_lease_acquire(req->thread->vfs_thread, vfs_state, file_state,
                                  &rl->lease, &rl->ticket, false,
                                  chimera_nfs4_lock_complete, req);
    }
} /* chimera_nfs4_lock */
