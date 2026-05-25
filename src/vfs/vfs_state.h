// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>

#include "vfs/vfs.h"
#include "vfs/vfs_fh.h"
#include "vfs/vfs_lease_types.h"

struct chimera_vfs_request;

/*
 * Unified VFS lease/lock state.
 *
 * One in-memory state object per file handle holds three lists of leases:
 * range locks (byte-range), share reservations (whole-file deny modes),
 * and caching leases (delegations / oplocks / SMB2 leases).  All three
 * protocols (NLM, NFSv4, SMB2) acquire and release leases through this
 * layer rather than maintaining their own per-protocol tables.
 *
 * In addition, the VFS core itself acquires an implicit SHARE lease on
 * behalf of actors that perform I/O without requesting a protocol lease
 * (NFSv3, S3, NFSv4 data I/O).  That lease is held lazily — kept across
 * operations and dropped only when it goes idle or another holder needs
 * it — so every read/write is lease-mediated.  See vfs_lease_types.h for
 * the shared lease vocabulary.
 *
 * Persistence is explicitly out of scope for this pass: all state lives
 * in memory and is lost on server crash.  Reclaim handshakes (NLM grace,
 * NFSv4 CLAIM_PREVIOUS, SMB3 DH2C) work across client disconnects within
 * a single server lifetime only.
 */

/* -------------------------------------------------------------------- */
/* Per-file state                                                       */
/* -------------------------------------------------------------------- */

#define CHIMERA_VFS_STATE_NUM_BUCKETS 64

struct chimera_vfs_file_state {
    uint8_t                             fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                             fh_len;
    uint64_t                            fh_hash;
    pthread_mutex_t                     lock;

    struct chimera_vfs_lease           *range_locks;
    struct chimera_vfs_lease           *share_resvs;
    struct chimera_vfs_lease           *caching_leases;

    /* Implicit lease held by chimera itself on behalf of leaseless actors
     * (NFSv3/S3/NFSv4 data I/O).  When active it is linked into share_resvs
     * like any other SHARE; it is kept across operations and dropped only
     * when idle-reaped or recalled by a conflicting holder.  Guarded by
     * file->lock.  See chimera_vfs_io_lease_acquire(). */
    struct chimera_vfs_lease            implicit_lease;
    uint8_t                             implicit_active;    /* linked in share_resvs */
    uint8_t                             implicit_draining;  /* recall in progress */
    uint32_t                            implicit_inflight;  /* in-flight ops pinning it */
    struct timespec                     implicit_last_used; /* CLOCK_MONOTONIC */

    /* FIFO queue of acquires waiting on a break to complete. */
    struct chimera_vfs_pending_acquire *pending_head;
    struct chimera_vfs_pending_acquire *pending_tail;

    /* FIFO queue of I/O requests waiting for the implicit lease to become
     * grantable (a conflicting holder is mid-recall, or a prior recall of
     * our own implicit lease is still draining).  Distinct from the
     * pending queue above, which carries protocol-lease acquires. */
    struct chimera_vfs_pending_acquire *io_wait_head;
    struct chimera_vfs_pending_acquire *io_wait_tail;

    /* Back-pointer set on creation so ack/revoke/remove can pump the
     * pending queue without changing public-API signatures. */
    struct chimera_vfs_state           *state;

    uint32_t                            refcount;
    struct chimera_vfs_file_state      *bucket_next;
};

struct chimera_vfs_state_bucket {
    struct chimera_vfs_file_state *files;
    pthread_mutex_t                lock;
};

struct chimera_vfs_state {
    struct chimera_vfs_state_bucket buckets[CHIMERA_VFS_STATE_NUM_BUCKETS];
    /* Default deadline applied by chimera_vfs_lease_begin_break() when the
     * caller passes 0.  Matches SMB2 lease-break timeout convention. */
    uint32_t                        default_break_deadline_ms;
    /* Implicit leases held this long without any I/O are dropped by
     * chimera_vfs_state_reap_idle(). */
    uint32_t                        implicit_idle_ms;
};

/* -------------------------------------------------------------------- */
/* Public API                                                           */
/* -------------------------------------------------------------------- */

/* Lifecycle.  One state subsystem per VFS instance, created at
* chimera_vfs_init() and destroyed at chimera_vfs_destroy(). */
struct chimera_vfs_state *
chimera_vfs_state_init(
    void);

void
chimera_vfs_state_destroy(
    struct chimera_vfs_state *state);

/* Lookup-or-create the per-file state object for a given FH.  On success
 * returns a refcounted pointer; caller must release via
 * chimera_vfs_state_put().  If create==false and no state exists, returns
 * NULL. */
struct chimera_vfs_file_state *
chimera_vfs_state_get(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash,
    bool                      create);

void
chimera_vfs_state_put(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file);

/* -------------------------------------------------------------------- */
/* Conflict matrix and lease insertion                                  */
/* -------------------------------------------------------------------- */

/* Pure test: would `probe` conflict with any existing lease on `file`?
 * Caller must hold file->lock.  On conflict, *conflict_out is set to the
 * first conflicting lease.  The probe is NOT inserted. */
enum chimera_vfs_lease_result
chimera_vfs_state_would_conflict(
    const struct chimera_vfs_file_state *file,
    const struct chimera_vfs_lease      *probe,
    struct chimera_vfs_lease           **conflict_out);

/* Atomically test and insert.  Caller must NOT hold file->lock; this
 * function takes it internally.  On GRANTED, ownership of `lease` transfers
 * to vfs_state until chimera_vfs_state_remove() is called.  On DENIED, the
 * lease is not inserted and conflict_out (if non-NULL) is set to a
 * conflicting holder.  On BREAKING, the lease is not inserted but the
 * conflicting holders have begin_break() called on them; caller is
 * expected to retry after the break ack/revoke completes. */
enum chimera_vfs_lease_result
chimera_vfs_state_try_insert(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease,
    struct chimera_vfs_lease     **conflict_out);

/* Remove a previously-inserted lease.  Caller must NOT hold file->lock. */
void
chimera_vfs_state_remove(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease);

/* -------------------------------------------------------------------- */
/* Async acquire/release                                                */
/* -------------------------------------------------------------------- */

/* Acquire a lease.  `lease` and `ticket` are caller-allocated; their
 * lifetime must extend until the callback fires.
 *
 * Outcomes:
 *   GRANTED  - lease was inserted; cb(GRANTED, lease, NULL, priv) fires
 *              synchronously inside this call.  Ownership of `lease`
 *              transfers to vfs_state until chimera_vfs_lease_release().
 *   DENIED   - lease conflicts with a non-breakable holder.  If wait
 *              is false, cb(DENIED, NULL, conflict, priv) fires
 *              synchronously.  If wait is true, the same DENIED result
 *              still fires synchronously — only breakable conflicts
 *              cause queueing.
 *   BREAKING - lease conflicts with a breakable holder.  If wait is
 *              true, the ticket is queued on the file's pending list;
 *              cb will fire asynchronously once the break completes
 *              (either GRANTED or DENIED at retry time).  If wait is
 *              false, cb(BREAKING, NULL, conflict, priv) fires
 *              synchronously and the caller is expected to retry. */
void
chimera_vfs_lease_acquire(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_lease           *lease,
    struct chimera_vfs_pending_acquire *ticket,
    bool                                wait,
    chimera_vfs_lease_acquire_cb_t      cb,
    void                               *private_data);

/* Release a previously-granted lease and free its slot in the file
 * state.  Equivalent to chimera_vfs_state_remove() but also pumps the
 * pending-acquire queue in case the released lease was holding back a
 * waiting acquire. */
void
chimera_vfs_lease_release(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease);

/* Cancel a queued acquire.  Returns true if the ticket was on the
 * pending queue and has been dequeued (cb will NOT fire); false if cb
 * has already fired or is about to fire. */
bool
chimera_vfs_lease_acquire_cancel(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket);

/* Pure synchronous conflict test.  Useful for LOCKT and SMB2_LOCK with
 * FAIL_IMMEDIATELY where the caller wants to probe without inserting.
 * `conflict_out`, if non-NULL, is set to the first conflicting holder
 * on DENIED / BREAKING. */
enum chimera_vfs_lease_result
chimera_vfs_lease_test(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_lease *probe,
    struct chimera_vfs_lease      **conflict_out);

/* SMB mandatory byte-range lock enforcement for an I/O.  Returns true if an
 * I/O of the given direction over [offset, offset+length) (length==0 means
 * to EOF) conflicts with a byte-range lock held on the file:
 *   - a shared lock denies writes from all owners (including the lock owner);
 *   - an exclusive lock denies reads and writes from any other owner.
 * `owner` identifies the open performing the I/O.  Looks up file state by FH;
 * a file with no state (no locks) never conflicts. */
bool
chimera_vfs_state_range_io_conflict(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    uint64_t                              offset,
    uint64_t                              length,
    bool                                  is_write,
    const struct chimera_vfs_lease_owner *owner);

/* -------------------------------------------------------------------- */
/* Break orchestration                                                  */
/* -------------------------------------------------------------------- */

/* Mark `lease` as breaking and invoke its owner's break_cb.  Idempotent —
 * a lease already in BREAKING state is left untouched.  deadline_ms==0
 * uses state->default_break_deadline_ms. */
void
chimera_vfs_lease_begin_break(
    struct chimera_vfs_state *state,
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    uint32_t                  deadline_ms);

/* Protocol server calls this when it has finished the protocol-specific
 * break and obtained the client's downgrade response.  `resulting` is the
 * mode the lease has been downgraded to; if resulting.granted == 0 the
 * lease is fully released and the caller must still call
 * chimera_vfs_state_remove() afterward to free it. */
void
chimera_vfs_lease_ack(
    struct chimera_vfs_lease     *lease,
    struct chimera_vfs_lease_mode resulting);

/* Forcibly revoke a lease — called by vfs_state when the break deadline
 * expires, or by the protocol server when it gives up on the client. */
void
chimera_vfs_lease_revoke(
    struct chimera_vfs_lease *lease);

/* Recall (begin_break) every caching lease held on the file identified by
 * `fh`, regardless of owner.  Used by namespace/metadata operations (REMOVE,
 * RENAME, LINK, SETATTR) that must invalidate a delegation/oplock before
 * proceeding.  Returns true if any caching lease is still present (the caller
 * should fail the op with a retryable error, e.g. NFS4ERR_DELAY, and retry
 * once the holders return); false if the file has no caching lease and the
 * op may proceed. */
bool
chimera_vfs_state_break_caching(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash);

/* -------------------------------------------------------------------- */
/* Implicit I/O lease (held by chimera on behalf of leaseless actors)   */
/* -------------------------------------------------------------------- */

/* Mediate request through the lease layer, then invoke next(request) to
 * proceed to dispatch.  Direction is taken from request->opcode (READ =>
 * read access, WRITE/metadata-mutation => write access).
 *
 * If `owner` is non-NULL the I/O is attributed to that owner (a lease-
 * holding client, so its own delegation/oplock is not self-recalled);
 * otherwise chimera acquires/holds an internal implicit SHARE on the
 * file's behalf.  Either way, conflicting holders are recalled and the
 * request waits (parks) until every break acks/revokes, after which
 * next(request) is invoked.  A failure (e.g. mandatory byte-range
 * conflict) sets request->status and invokes request->complete instead
 * of next. */
void
chimera_vfs_io_lease_acquire(
    struct chimera_vfs_request           *request,
    const struct chimera_vfs_lease_owner *owner,
    void (                               *next )(struct chimera_vfs_request *request));

/* Release the in-flight pin taken by chimera_vfs_io_lease_acquire().  Safe
 * to call when no implicit lease was taken (fast path).  Must be called
 * exactly once per acquire, from the operation's completion path. */
void
chimera_vfs_io_lease_release(
    struct chimera_vfs_request *request);

/* Drop every implicit lease that has been idle (no in-flight I/O) for at
 * least `idle_ms` milliseconds.  Driven by a periodic reaper. */
void
chimera_vfs_state_reap_idle(
    struct chimera_vfs_state *state,
    uint64_t                  idle_ms);

/* -------------------------------------------------------------------- */
/* Break-on-write                                                       */
/* -------------------------------------------------------------------- */

/* Break (to NONE) every read-caching lease invalidated by a write to the
 * file identified by (fh, fh_hash), except the writer's own write-caching
 * lease.  Drives the SMB2 oplock "self break to none" and cross-holder
 * read-cache invalidation.  `writer` is the issuing open's lease owner. */
void
chimera_vfs_state_break_on_write(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    const struct chimera_vfs_lease_owner *writer);
