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
 * Persistence is explicitly out of scope for this pass: all state lives
 * in memory and is lost on server crash.  Reclaim handshakes (NLM grace,
 * NFSv4 CLAIM_PREVIOUS, SMB3 DH2C) work across client disconnects within
 * a single server lifetime only.
 */

/* -------------------------------------------------------------------- */
/* Lease vocabulary                                                     */
/* -------------------------------------------------------------------- */

enum chimera_vfs_lease_kind {
    CHIMERA_VFS_LEASE_RANGE   = 0, /* byte-range lock */
    CHIMERA_VFS_LEASE_SHARE   = 1, /* whole-file share/deny reservation */
    CHIMERA_VFS_LEASE_CACHING = 2, /* breakable caching lease */
    CHIMERA_VFS_LEASE_KIND_MAX
};

/* Mode bits — SMB2 RWH triple, used as the lingua franca.
 *   R = read-cache / shared range lock / SMB FILE_READ_DATA share-allow
 *   W = write-cache / exclusive range lock / SMB FILE_WRITE_DATA share-allow
 *   H = handle-cache (SMB only)
 *   D = delete (SMB FILE_SHARE_DELETE)
 *
 * RANGE leases use {R} for shared, {W} for exclusive; H and D are ignored.
 * SHARE leases use granted = access bits, denied = deny bits.
 * CACHING leases use granted directly (e.g., {R,W,H} for an SMB lease).
 */
#define CHIMERA_VFS_LEASE_MODE_R      0x01
#define CHIMERA_VFS_LEASE_MODE_W      0x02
#define CHIMERA_VFS_LEASE_MODE_H      0x04
#define CHIMERA_VFS_LEASE_MODE_D      0x08

struct chimera_vfs_lease_mode {
    uint8_t granted; /* bits the holder has been granted */
    uint8_t denied;  /* SHARE only: bits this holder denies to others */
};

/* Protocol identifiers — used in owner.protocol so the conflict matrix
 * can apply protocol-specific same-owner coalescing rules. */
#define CHIMERA_VFS_LEASE_PROTO_NLM   1
#define CHIMERA_VFS_LEASE_PROTO_NFSV4 2
#define CHIMERA_VFS_LEASE_PROTO_SMB2  3

struct chimera_vfs_lease;

/* Break callback — invoked synchronously by vfs_state when this lease must
 * downgrade or release.  The callback is expected to kick off the protocol-
 * specific break (build SMB2 OPLOCK_BREAK, send NFSv4 CB_RECALL) and return
 * promptly; vfs_state does not wait inside the callback.  The protocol
 * server eventually calls chimera_vfs_lease_ack() (or chimera_vfs_lease_
 * revoke() on its own timeout) to unstick the pending acquire. */
typedef void (*chimera_vfs_lease_break_cb_t)(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data);

/* Liveness probe — vfs_state calls this to test whether a lease's owning
 * client/session is still considered alive.  Returns false on session
 * expiry, connection drop past grace, etc.  May be NULL (always alive). */
typedef bool (*chimera_vfs_lease_is_alive_cb_t)(
    const struct chimera_vfs_lease *lease,
    void                           *private_data);

/* Revocation notice — vfs_state calls this when it forcibly revokes a lease
 * (recall deadline elapsed, etc.) rather than the holder releasing it.  The
 * protocol layer uses it to mark its own state revoked (e.g. so a later use of
 * an NFSv4 delegation stateid reports NFS4ERR_DELEG_REVOKED).  May be NULL.
 * Invoked outside the file lock. */
typedef void (*chimera_vfs_lease_revoked_cb_t)(
    struct chimera_vfs_lease *lease,
    void                     *private_data);

struct chimera_vfs_lease_owner {
    uint32_t                        protocol;
    uint64_t                        client_key;
    uint64_t                        owner_lo;
    uint64_t                        owner_hi;
    chimera_vfs_lease_break_cb_t    break_cb;
    chimera_vfs_lease_is_alive_cb_t is_alive_cb;
    chimera_vfs_lease_revoked_cb_t  revoked_cb;
    void                           *cb_private;
};

enum chimera_vfs_break_state {
    CHIMERA_VFS_BREAK_IDLE     = 0, /* no break in progress */
    CHIMERA_VFS_BREAK_BREAKING = 1, /* break_cb invoked, awaiting ack */
    CHIMERA_VFS_BREAK_ACKED    = 2, /* protocol acked, lease downgraded */
    CHIMERA_VFS_BREAK_REVOKED  = 3, /* forcibly revoked (timeout or error) */
};

struct chimera_vfs_file_state;

struct chimera_vfs_lease {
    enum chimera_vfs_lease_kind kind;
    struct chimera_vfs_lease_mode  mode;
    uint64_t                       offset; /* RANGE only; SHARE/CACHING use 0 */
    uint64_t                       length; /* RANGE only; 0 = to EOF */
    struct chimera_vfs_lease_owner owner;
    struct chimera_vfs_file_state *file;

    enum chimera_vfs_break_state break_state;
    uint8_t                        break_needed_mode;
    struct timespec                break_deadline;

    /* For a SHARE probe only: a caching (handle) lease held under this same
     * key is the requester's own lease (SMB2 same-client, same lease key) and
     * must NOT be broken when acquiring the share — the opens coalesce.  Set by
     * the SMB server when a lease-bearing open takes its share reservation;
     * left zero (no skip) by every other caller. */
    uint8_t                        has_break_skip_key;
    uint64_t                       break_skip_lo;
    uint64_t                       break_skip_hi;

    /* Intrusive linkage on the appropriate file->{range,share,caching} list. */
    struct chimera_vfs_lease      *prev;
    struct chimera_vfs_lease      *next;
};

/* -------------------------------------------------------------------- */
/* Lease result enum                                                    */
/* -------------------------------------------------------------------- */

/* Conflict-test result.  Returned by chimera_vfs_state_would_conflict(),
 * chimera_vfs_state_try_insert(), and chimera_vfs_lease_test(). */
enum chimera_vfs_lease_result {
    CHIMERA_VFS_LEASE_GRANTED  = 0, /* no conflict; lease would be / was inserted */
    CHIMERA_VFS_LEASE_DENIED   = 1, /* hard conflict with non-breakable holder */
    CHIMERA_VFS_LEASE_BREAKING = 2, /* breakable holder must downgrade first */
};

/* -------------------------------------------------------------------- */
/* Async-acquire ticket                                                 */
/* -------------------------------------------------------------------- */

/* Result of an async acquire.  GRANTED carries `granted_lease`; DENIED
 * carries `conflict` (a pointer to a conflicting holder — owned by
 * vfs_state, valid only until the callback returns). */
typedef void (*chimera_vfs_lease_acquire_cb_t)(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted_lease,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data);

/* Caller-allocated ticket holding pending-acquire bookkeeping.  Its
 * lifetime must extend until the acquire callback fires.  Protocol
 * layers typically embed this in their existing per-lock state struct
 * (nlm_lock_entry, nfs4_state, smb_open_file's lock slot). */
struct chimera_vfs_pending_acquire {
    struct chimera_vfs_lease           *lease;
    chimera_vfs_lease_acquire_cb_t      cb;
    void                               *private_data;
    struct chimera_vfs_file_state      *file;
    bool                                queued;
    struct chimera_vfs_pending_acquire *prev;
    struct chimera_vfs_pending_acquire *next;
};

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

    /* FIFO queue of acquires waiting on a break to complete. */
    struct chimera_vfs_pending_acquire *pending_head;
    struct chimera_vfs_pending_acquire *pending_tail;

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
/* I/O hook                                                             */
/* -------------------------------------------------------------------- */

/* Called by VFS core before dispatching a READ or WRITE request.  In
 * Stage A this is a no-op pass-through that always returns 0 (success);
 * Stage B+ will enforce range-lock conflicts and break caching leases.
 * Returns 0 on permit, a CHIMERA_VFS_E* error code on deny. */
int
chimera_vfs_state_check_io(
    struct chimera_vfs_request *request);

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
