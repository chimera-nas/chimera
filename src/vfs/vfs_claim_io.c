// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "vfs_claim.h"
#include "vfs_claim_internal.h"
#include "vfs_internal.h"
#include "common/macros.h"

/*
 * vfs_claim_io.c — the implicit INTERNAL claim, request parking and resume,
 * the parking namespace-recall wrappers, the mandatory-lock I/O predicate,
 * and the idle reaper.  Ported from the lease core with the claim
 * vocabulary; the fast-path shape (CAS handle attach, per-I/O borrow,
 * owning-thread doorbell resume — R63/R65) is preserved bit for bit.
 */

static void
chimera_vfs_implicit_break_cb(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed_mode,
    void                     *private_data);

static void
chimera_vfs_implicit_finish_drain(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file);

static void
chimera_vfs_io_try(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request);

/* Deterministic per-file owner for the implicit claim: never coalesces with
 * any real client owner, so it always recalls / is recalled by a client's
 * delegation, oplock, or deny-mode open.  (The node-uuid component the
 * projection phase needs is deferred with it.) */
static void
chimera_vfs_implicit_owner(
    struct chimera_vfs_file_state *file,
    struct chimera_claim_owner    *out)
{
    memset(out, 0, sizeof(*out));
    out->proto      = CHIMERA_CLAIM_PROTO_INTERNAL;
    out->client_key = 0;
    out->owner_lo   = file->fh_hash;
    out->owner_hi   = 0;
} /* chimera_vfs_implicit_owner */

static void
chimera_vfs_implicit_break_cb(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed_mode,
    void                     *private_data)
{
    struct chimera_vfs_state      *state = private_data;
    struct chimera_vfs_file_state *file  = claim->file;
    bool                           drop_now;

    (void) needed_mode;

    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    file->implicit_draining = 1;
    drop_now                = (file->implicit_inflight == 0);
    pthread_mutex_unlock(&file->lock);

    if (drop_now) {
        chimera_vfs_implicit_finish_drain(state, file);
    }
} /* chimera_vfs_implicit_break_cb */

static void
chimera_vfs_implicit_finish_drain(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    bool removed = false;

    pthread_mutex_lock(&file->lock);
    if (file->implicit_active) {
        chimera_vfs_claim_unlink_locked(file, &file->implicit_claim);
        file->implicit_active            = 0;
        file->implicit_claim.break_state = CHIMERA_CLAIM_BREAK_IDLE;
        removed                          = true;
    }
    file->implicit_draining = 0;
    pthread_mutex_unlock(&file->lock);

    if (removed) {
        chimera_vfs_claim_pump_pending(state, file);
        chimera_vfs_claim_pump_io(state, file);
        chimera_vfs_state_put(state, file);
    }
} /* chimera_vfs_implicit_finish_drain */

/* Drive begin_break / revoke on every breakable holder conflicting with
 * `probe` until none remain IDLE (the io-side mirror of try_acquire's
 * break loop).  Caller must NOT hold file->lock. */
static enum chimera_vfs_claim_result
chimera_vfs_claim_drive_breaks(
    struct chimera_vfs_state       *state,
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *probe)
{
    struct chimera_vfs_claim     *conflict = NULL;
    enum chimera_vfs_claim_result result;

    pthread_mutex_lock(&file->lock);
    result = chimera_vfs_claim_admit_locked(file, probe, &conflict);
    pthread_mutex_unlock(&file->lock);

    while (result == CHIMERA_CLAIM_BREAKING && conflict) {
        if (conflict->break_state == CHIMERA_CLAIM_BREAK_IDLE ||
            conflict->break_state == CHIMERA_CLAIM_BREAK_ACKED) {
            uint32_t deadline_ms =
                (conflict->owner.proto == CHIMERA_CLAIM_PROTO_NFSV4)
                ? CHIMERA_VFS_NFS_DELEG_RECALL_MS : 0;

            chimera_vfs_claim_begin_break_ex(
                state, conflict,
                chimera_vfs_claim_contended_floor(probe, conflict),
                deadline_ms, false);
        } else if (chimera_vfs_claim_deadline_passed(conflict)) {
            chimera_vfs_claim_revoke(conflict);
        } else {
            break;
        }
        conflict = NULL;
        pthread_mutex_lock(&file->lock);
        result = chimera_vfs_claim_admit_locked(file, probe, &conflict);
        pthread_mutex_unlock(&file->lock);
    }

    return result;
} /* chimera_vfs_claim_drive_breaks */

/* Park `request` on the file's I/O wait queue.  Caller holds file->lock. */
static void
chimera_vfs_io_park_locked(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request)
{
    struct chimera_vfs_pending_acquire *ticket = &request->io_lease_ticket;

    ticket->claim        = NULL;
    ticket->cb           = NULL;
    ticket->private_data = request;
    ticket->file         = file;
    ticket->queued       = true;
    ticket->next         = NULL;
    ticket->prev         = file->io_wait_tail;

    if (file->io_wait_tail) {
        file->io_wait_tail->next = ticket;
    } else {
        file->io_wait_head = ticket;
    }
    file->io_wait_tail = ticket;
} /* chimera_vfs_io_park_locked */

/* Retry every parked I/O request, marshaled back to its owning thread
 * (thread-local connection iovecs — the R63 arbiter contract). */
void
chimera_vfs_claim_pump_io(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_pending_acquire *head, *t, *next;

    (void) state;

    pthread_mutex_lock(&file->lock);
    head               = file->io_wait_head;
    file->io_wait_head = NULL;
    file->io_wait_tail = NULL;
    for (t = head; t; t = t->next) {
        t->queued = false;
    }
    pthread_mutex_unlock(&file->lock);

    for (t = head; t; t = next) {
        struct chimera_vfs_request *request = t->private_data;

        next    = t->next;
        t->prev = NULL;
        t->next = NULL;

        chimera_vfs_io_resume_post(request);
    }
} /* chimera_vfs_claim_pump_io */

SYMBOL_EXPORT void
chimera_vfs_state_io_resume(struct chimera_vfs_request *request)
{
    struct chimera_vfs_file_state *file = request->io_lease_file;

    if (!file) {
        return;
    }

    chimera_vfs_io_try(request->thread->vfs->vfs_state, file, request);
} /* chimera_vfs_state_io_resume */

static void
chimera_vfs_io_try(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request)
{
    struct chimera_vfs_claim      probe;
    enum chimera_vfs_claim_result result;
    uint8_t                       need;
    uint8_t                       target;
    bool                          was_active;
    bool                          activated = false;
    struct chimera_claim_owner    iowner;

    /* Single-step namespace recall (NS_UNLINK): exactly one break per peer
     * down to retain, await the real acks (smb2.lease.unlink). */
    if (request->io_recall_single) {
        if (chimera_vfs_claim_trigger_ns_unlink(state, file,
                                                request->io_handle,
                                                request->io_recall_retain)) {
            pthread_mutex_lock(&file->lock);
            chimera_vfs_io_park_locked(file, request);
            request->io_lease_file = file;
            pthread_mutex_unlock(&file->lock);
            return;
        }
        request->io_lease_file = NULL;
        chimera_vfs_state_put(state, file);
        request->io_next(request);
        return;
    }

    /* Full/flush namespace recall (NS_FULL / FLUSH). */
    if (request->io_recall_all) {
        if (chimera_vfs_claim_trigger_ns_full(state, file,
                                              request->io_handle,
                                              request->io_recall_flush_only)) {
            pthread_mutex_lock(&file->lock);
            chimera_vfs_io_park_locked(file, request);
            request->io_lease_file = file;
            pthread_mutex_unlock(&file->lock);
            return;
        }
        request->io_lease_file = NULL;
        chimera_vfs_state_put(state, file);
        request->io_next(request);
        return;
    }

    need = (request->opcode == CHIMERA_VFS_OP_WRITE)
        ? CHIMERA_CLAIM_W : CHIMERA_CLAIM_R;

    pthread_mutex_lock(&file->lock);

    if (file->implicit_draining) {
        chimera_vfs_io_park_locked(file, request);
        request->io_lease_file = file;
        pthread_mutex_unlock(&file->lock);
        return;
    }

    was_active = file->implicit_active;
    target     = was_active
        ? (uint8_t) (file->implicit_claim.used | need)
        : need;

    chimera_vfs_implicit_owner(file, &iowner);
    memset(&probe, 0, sizeof(probe));
    probe.construct  = CHIMERA_CONSTRUCT_IMPLICIT;
    probe.klass      = CHIMERA_CLAIM_CLASS_ACCESS;
    probe.used       = target;
    probe.advertised = target;
    probe.owner      = iowner;
    probe.length     = UINT64_MAX;
    probe.break_cb   = chimera_vfs_implicit_break_cb;
    probe.cb_private = state;

    {
        struct chimera_vfs_claim *conflict = NULL;

        result = chimera_vfs_claim_admit_locked(file, &probe, &conflict);
    }

    if (result == CHIMERA_CLAIM_GRANTED) {
        if (!was_active) {
            file->implicit_claim = probe;
            chimera_vfs_claim_link_locked(file, &file->implicit_claim);
            file->implicit_active = 1;
            activated             = true;
        } else {
            file->implicit_claim.used       = target;
            file->implicit_claim.advertised = target;
        }
        file->implicit_inflight++;
        file->implicit_last_used = chimera_vfs_now_ticks();
        pthread_mutex_unlock(&file->lock);

        if (activated) {
            /* Reference for the claim itself, dropped by finish_drain. */
            chimera_vfs_state_get(state, file->fh, file->fh_len,
                                  file->fh_hash, false);
        }

        request->io_lease_file = file;
        if (need == CHIMERA_CLAIM_W) {
            struct chimera_claim_actor actor = { 0 };

            actor.owner = iowner;
            chimera_vfs_claim_trigger_fire(state, file, CHIMERA_TRIGGER_WRITE,
                                           &actor, 0);
        }
        request->io_next(request);
        return;
    }

    if (result == CHIMERA_CLAIM_DENIED) {
        pthread_mutex_unlock(&file->lock);
        request->io_lease_file = NULL;
        if (request->io_owns_lease_ref) {
            chimera_vfs_state_put(state, file);
        }
        request->status = CHIMERA_VFS_EACCES;
        request->complete(request);
        return;
    }

    /* BREAKING: park atomically with the conflict observation, then drive
     * the recalls outside the lock (no lost wakeup). */
    chimera_vfs_io_park_locked(file, request);
    request->io_lease_file = file;
    pthread_mutex_unlock(&file->lock);

    if (chimera_vfs_claim_drive_breaks(state, file, &probe) !=
        CHIMERA_CLAIM_BREAKING) {
        chimera_vfs_claim_pump_io(state, file);
    }
} /* chimera_vfs_io_try */

SYMBOL_EXPORT void
chimera_vfs_io_claim_acquire(
    struct chimera_vfs_request       *request,
    const struct chimera_claim_actor *actor,
    void (                           *next )(struct chimera_vfs_request *request))
{
    struct chimera_vfs_state      *state = request->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file;

    request->io_next       = next;
    request->io_lease_file = NULL;

    /* A claim-holding client supplies its own actor: no implicit claim is
     * held; a write fires read-cache invalidation for other holders (R61). */
    if (actor) {
        if (request->opcode == CHIMERA_VFS_OP_WRITE) {
            chimera_vfs_claim_invalidate(state, request->fh, request->fh_len,
                                         request->fh_hash,
                                         CHIMERA_TRIGGER_WRITE, actor, 0);
        }
        next(request);
        return;
    }

    if (!state) {
        next(request);
        return;
    }

    /* Fast path: borrow the cached open handle's anchored reference (R65). */
    if (request->io_handle &&
        request->io_handle->cache_id != CHIMERA_VFS_OPEN_ID_SYNTHETIC) {
        struct chimera_vfs_open_handle *handle = request->io_handle;

        file = __atomic_load_n(&handle->file_state, __ATOMIC_ACQUIRE);
        if (!file) {
            struct chimera_vfs_file_state *expected = NULL;

            file = chimera_vfs_state_get(state, request->fh, request->fh_len,
                                         request->fh_hash, true);
            if (file &&
                !__atomic_compare_exchange_n(&handle->file_state, &expected,
                                             file, false, __ATOMIC_ACQ_REL,
                                             __ATOMIC_ACQUIRE)) {
                chimera_vfs_state_put(state, file);
                file = expected;
            }
        }

        if (file) {
            request->io_owns_lease_ref = 0;
            chimera_vfs_io_try(state, file, request);
            return;
        }
    }

    file = chimera_vfs_state_get(state, request->fh, request->fh_len,
                                 request->fh_hash, true);
    if (!file) {
        next(request);
        return;
    }

    request->io_owns_lease_ref = 1;
    chimera_vfs_io_try(state, file, request);
} /* chimera_vfs_io_claim_acquire */

SYMBOL_EXPORT void
chimera_vfs_io_recall(
    struct chimera_vfs_request *request,
    const uint8_t              *fh,
    uint8_t                     fh_len,
    uint64_t                    fh_hash,
    int                         flush_only,
    void (                     *next )(struct chimera_vfs_request *request))
{
    struct chimera_vfs_state      *state = request->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file;

    request->io_next              = next;
    request->io_lease_file        = NULL;
    request->io_recall_all        = 1;
    request->io_recall_flush_only = flush_only ? 1 : 0;
    request->io_owns_lease_ref    = 1;

    if (!state || fh_len == 0) {
        next(request);
        return;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        next(request);
        return;
    }

    chimera_vfs_io_try(state, file, request);
} /* chimera_vfs_io_recall */

SYMBOL_EXPORT void
chimera_vfs_io_recall_single(
    struct chimera_vfs_request *request,
    const uint8_t              *fh,
    uint8_t                     fh_len,
    uint64_t                    fh_hash,
    uint8_t                     retain,
    void (                     *next )(struct chimera_vfs_request *request))
{
    struct chimera_vfs_state      *state = request->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file;

    request->io_next           = next;
    request->io_lease_file     = NULL;
    request->io_recall_all     = 0;
    request->io_recall_single  = 1;
    request->io_recall_retain  = retain;
    request->io_owns_lease_ref = 1;

    if (!state || fh_len == 0) {
        next(request);
        return;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        next(request);
        return;
    }

    chimera_vfs_io_try(state, file, request);
} /* chimera_vfs_io_recall_single */

SYMBOL_EXPORT void
chimera_vfs_io_claim_release(struct chimera_vfs_request *request)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file = request->io_lease_file;
    bool                           finish;

    if (!file) {
        return;
    }

    request->io_lease_file = NULL;
    state                  = request->thread->vfs->vfs_state;

    pthread_mutex_lock(&file->lock);
    if (file->implicit_inflight > 0) {
        file->implicit_inflight--;
    }
    finish = (file->implicit_inflight == 0 &&
              file->implicit_draining &&
              file->implicit_active);
    pthread_mutex_unlock(&file->lock);

    if (finish) {
        chimera_vfs_implicit_finish_drain(state, file);
    }

    if (request->io_owns_lease_ref) {
        chimera_vfs_state_put(state, file);
    }
} /* chimera_vfs_io_claim_release */

/* -------------------------------------------------------------------- */
/* Mandatory-lock I/O predicate                                         */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT bool
chimera_vfs_claim_io_denied(
    struct chimera_vfs_state         *state,
    const uint8_t                    *fh,
    uint8_t                           fh_len,
    uint64_t                          fh_hash,
    uint64_t                          offset,
    uint64_t                          length,
    bool                              is_write,
    const struct chimera_claim_actor *actor)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim      *cur;
    bool                           conflict = false;

    /* Zero-length reads are exempt (R25's carve-out, owned here). */
    if (!is_write && length == 0) {
        return false;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_RANGE]; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_CLAIM_BREAK_REVOKED) {
            continue;
        }
        if (!chimera_vfs_claim_range_overlap(cur->offset, cur->length,
                                             offset, length)) {
            continue;
        }
        if (cur->used & CHIMERA_CLAIM_LW) {
            /* Exclusive lock: blocks all I/O from other actors; the lock's
             * own actor (owner, its grant's lease key, or its very handle
             * -- the HOLDER-exempt MAND row) passes. */
            bool self = actor &&
                (chimera_claim_owner_equal(&cur->owner, &actor->owner) ||
                 chimera_claim_owner_same_key(&cur->owner, &actor->owner) ||
                 (actor->op_handle && cur->op_handle == actor->op_handle));

            if (!self) {
                conflict = true;
                break;
            }
        } else {
            /* Shared lock: denies writes GLOBALLY, its own owner included
             * (MS-FSA). */
            if (is_write) {
                conflict = true;
                break;
            }
        }
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_state_put(state, file);
    return conflict;
} /* chimera_vfs_claim_io_denied */

/* -------------------------------------------------------------------- */
/* Idle reaper                                                          */
/* -------------------------------------------------------------------- */

static bool
chimera_vfs_claim_revoke_expired_breaks(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_claim *expired[CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH];
    struct chimera_vfs_claim *cur;
    int                       n = 0;
    int                       i;

    (void) state;

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        /* NFSv4 delegations are never force-revoked by the sweep: their
         * lifetime is governed by the NFSv4 client lease / CB_PATH_DOWN
         * path (DELEG6).  The sweep exists for SMB break-ack timeouts. */
        if (cur->owner.proto == CHIMERA_CLAIM_PROTO_NFSV4) {
            continue;
        }
        if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
            chimera_vfs_claim_deadline_passed(cur) &&
            n < CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH) {
            expired[n++] = cur;
        }
    }
    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_claim_revoke(expired[i]);
    }
    return n > 0;
} /* chimera_vfs_claim_revoke_expired_breaks */

SYMBOL_EXPORT void
chimera_vfs_state_reap_idle(
    struct chimera_vfs_state *state,
    uint64_t                  idle_ms)
{
#define CHIMERA_VFS_CLAIM_REAP_BATCH 256
    uint64_t now = chimera_vfs_now_ticks();
    int      b;

    for (b = 0; b < CHIMERA_VFS_STATE_NUM_SHARDS; b++) {
        struct chimera_vfs_state_shard *shard = &state->shards[b];
        struct chimera_vfs_file_state  *cand[CHIMERA_VFS_CLAIM_REAP_BATCH];
        struct chimera_vfs_file_state  *file;
        uint32_t                        s;
        int                             n = 0;
        int                             i;

        pthread_mutex_lock(&shard->lock);

        if (shard->count == 0) {
            pthread_mutex_unlock(&shard->lock);
            continue;
        }

        for (s = 0; s < shard->nslots && n < CHIMERA_VFS_CLAIM_REAP_BATCH; s++) {
            for (file = shard->slots[s];
                 file && n < CHIMERA_VFS_CLAIM_REAP_BATCH;
                 file = file->bucket_next) {
                if (file->implicit_active || file->io_wait_head ||
                    file->claims[CHIMERA_CLAIM_CLASS_CACHE]) {
                    file->refcount++;
                    cand[n++] = file;
                }
            }
        }
        pthread_mutex_unlock(&shard->lock);

        for (i = 0; i < n; i++) {
            bool reapable;
            bool has_waiters;
            bool has_caching;

            file = cand[i];

            pthread_mutex_lock(&file->lock);
            reapable = file->implicit_active &&
                !file->implicit_draining &&
                file->implicit_inflight == 0 &&
                chimera_vfs_claim_elapsed_ms(file->implicit_last_used, now) >=
                idle_ms;
            if (reapable) {
                file->implicit_draining = 1;
            }
            has_waiters = (file->io_wait_head != NULL);
            has_caching = (file->claims[CHIMERA_CLAIM_CLASS_CACHE] != NULL);
            pthread_mutex_unlock(&file->lock);

            if (has_caching) {
                chimera_vfs_claim_revoke_expired_breaks(state, file);
            }

            if (reapable) {
                chimera_vfs_implicit_finish_drain(state, file);
            } else if (has_waiters) {
                chimera_vfs_claim_pump_io(state, file);
            }

            chimera_vfs_state_put(state, file);
        }
    }
#undef CHIMERA_VFS_CLAIM_REAP_BATCH
} /* chimera_vfs_state_reap_idle */
