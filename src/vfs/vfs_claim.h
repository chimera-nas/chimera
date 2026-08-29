// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#include "vfs/vfs.h"
#include "vfs/sdk/vfs_fh.h"
#include "vfs/vfs_claim_types.h"

struct chimera_vfs_request;

/*
 * The claim core: unified lease/lock/share/cache arbitration.
 *
 * One in-memory anchor per file handle (chimera_vfs_file_state -- the name
 * and lifecycle survive from the previous lease core so the open-handle
 * cache's CAS attach and every state_get/put call site keep their shape).
 * The anchor holds three claim class lists (access / cache / range) walked
 * by the admission predicate, the trigger engine, and the mandatory-lock
 * I/O predicate.
 *
 * SYNC CONTRACT (local arbiter): every verb completes synchronously.
 * chimera_vfs_claim_acquire fires its callback inside the call for GRANTED
 * and DENIED; BREAKING+wait queues the ticket (blocked_cb fires exactly
 * once, synchronously, after enqueue); a hard-DENIED lock acquire with
 * wait_hard queues likewise.  Release is always synchronous -- callers may
 * free the embedding struct immediately after.  Admission-driven breaks are
 * kicked on the acquisition ATTEMPT regardless of wait and regardless of
 * whether the claim inserts (a wait=false BREAKING caller gets the recall
 * started plus a synchronous BREAKING result -- the NFSv4 LOCK triple).
 *
 * Persistence remains out of scope: all state is in memory.
 */

/* -------------------------------------------------------------------- */
/* Per-file anchor                                                      */
/* -------------------------------------------------------------------- */

#define CHIMERA_VFS_STATE_NUM_SHARDS    1024
#define CHIMERA_VFS_STATE_INITIAL_SLOTS 16

struct chimera_vfs_file_state {
    uint8_t                             fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                             fh_len;
    uint64_t                            fh_hash;
    pthread_mutex_t                     lock;

    /* Claim class lists (all guarded by file->lock). */
    struct chimera_vfs_claim           *claims[CHIMERA_CLAIM_CLASS_COUNT];

    /* Owner/key-indexed cache grants on this file; each grant's embedded
     * claim is also on claims[CACHE]. */
    struct chimera_vfs_claim_grant     *grants;

    /* Implicit I/O claim, embedded (no allocation on the I/O fast path). */
    struct chimera_vfs_claim            implicit_claim;
    uint8_t                             implicit_active;
    uint8_t                             implicit_draining;
    uint32_t                            implicit_inflight;
    uint64_t                            implicit_last_used;

    /* FIFO of protocol acquires waiting on a break. */
    struct chimera_vfs_pending_acquire *pending_head;
    struct chimera_vfs_pending_acquire *pending_tail;

    /* FIFO of parked I/O / namespace requests. */
    struct chimera_vfs_pending_acquire *io_wait_head;
    struct chimera_vfs_pending_acquire *io_wait_tail;

    struct chimera_vfs_state           *state;

    /* Backend lease projection (CHIMERA_VFS_CAP_LEASE).  One revocable
     * AGGREGATE token per file covering the union of local holders, held
     * lazily with escalate-or-reuse; state guarded by file->lock, the work
     * linkage by state->service_lock.  bl_want_used carries an implicit-I/O
     * need that parked before its claim activated, so the union it is
     * waiting for is already in the acquire. */
    uint8_t                             bl_state;      /* NONE/ACQUIRING/... */
    uint8_t                             bl_probed;     /* module resolved     */
    uint8_t                             bl_disabled;   /* module lacks CAP    */
    uint8_t                             bl_held_used;
    uint8_t                             bl_held_deny;
    uint8_t                             bl_want_used;
    uint8_t                             bl_refused;    /* bits the backend
                                                        * denied; cleared on
                                                        * any state change    */
    uint8_t                             bl_recall_retain;
    uint8_t                             bl_ref_held;   /* token holds a file
                                                        * self-reference      */
    uint64_t                            bl_token;
    uint64_t                            bl_last_used;  /* stopwatch ticks     */
    uint8_t                             bl_work_queued;
    struct chimera_vfs_file_state      *bl_work_next;

    /* SMB protocol annex (delete-on-close deferral, stream holders).  Kept
     * verbatim: honest protocol bookkeeping, not arbitration. */
    uint8_t                             delete_pending;
    uint32_t                            stream_holders;

    uint32_t                            refcount;
    struct chimera_vfs_file_state      *bucket_next;
};

struct chimera_vfs_state_shard {
    pthread_mutex_t                 lock;
    uint32_t                        count;
    uint32_t                        nslots;
    struct chimera_vfs_file_state **slots;
};

/* Backend-lease aggregate states. */
enum chimera_vfs_backend_lease_state {
    CHIMERA_VFS_BL_NONE = 0,
    CHIMERA_VFS_BL_ACQUIRING,
    CHIMERA_VFS_BL_HELD,
    CHIMERA_VFS_BL_RECALLING,
    CHIMERA_VFS_BL_RELEASING,
};

struct chimera_vfs_state {
    struct chimera_vfs_state_shard      shards[CHIMERA_VFS_STATE_NUM_SHARDS];
    uint32_t                            default_break_deadline_ms;
    uint32_t                            implicit_idle_ms;

    /* Backend lease projection service: work is posted here from any thread
     * and drained on the service thread (the VFS close thread), which owns
     * every backend lease dispatch for AGGREGATE tokens.  lease_capable is
     * false when no registered module declares CHIMERA_VFS_CAP_LEASE, making
     * every projection hook a cheap no-op. */
    uint8_t                             lease_capable;
    uint8_t                             lease_probed;  /* modules scanned (lazy: the
                                                       * close thread attaches
                                                       * before modules register) */
    struct chimera_vfs                 *vfs;
    struct chimera_claim_owner          node_owner;  /* this node's wire identity */
    pthread_mutex_t                     service_lock;
    struct chimera_vfs_file_state      *service_head;
    struct chimera_vfs_file_state      *service_tail;
    struct chimera_vfs_thread          *service_thread;
    struct evpl_doorbell               *service_doorbell;
    /* ONE ordered FIFO for every backend RANGE operation (acquire confirms
     * and token releases alike), drained in arrival order on the service
     * thread.  Ordering is load-bearing: an unlock's token release queued
     * before a subsequent lock's confirm MUST reach the backend first, or
     * the confirm collides with the stale record (smb2.lock.stacking /
     * unlock / zerobytelength, pynfs LOCK13). */
    struct chimera_vfs_bl_work         *work_head;
    struct chimera_vfs_bl_work         *work_tail;
    /* The TICKET the service thread is confirming RIGHT NOW (popped from the
     * FIFO but its callback not yet returned); guarded by service_lock.
     * chimera_vfs_claim_cancel waits on this so "cancel returned false"
     * strictly means the ticket's callback has fired -- the contract the
     * teardown paths (SMB abort-parked, NLM client reap) free memory on. */
    struct chimera_vfs_pending_acquire *work_active_ticket;
};

enum chimera_vfs_bl_work_type {
    CHIMERA_VFS_BL_WORK_TICKET = 1, /* deferred RANGE acquire confirm      */
    CHIMERA_VFS_BL_WORK_RELEASE,    /* fire-and-forget token release       */
};

struct chimera_vfs_bl_work {
    enum chimera_vfs_bl_work_type       type;
    struct chimera_vfs_pending_acquire *ticket; /* TICKET                  */
    uint8_t                             fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                             fh_len;
    uint64_t                            fh_hash;
    uint64_t                            token;  /* RELEASE                 */
    struct chimera_vfs_bl_work         *next;
};

/* -------------------------------------------------------------------- */
/* Lifecycle / lookup (unchanged shape)                                 */
/* -------------------------------------------------------------------- */

struct chimera_vfs_state *
chimera_vfs_state_init(
    void);

void
chimera_vfs_state_destroy(
    struct chimera_vfs_state *state);

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
/* Claim constructors — the admission mask table                        */
/* -------------------------------------------------------------------- */

/* Consumers build claims ONLY through these; each stamps the construct that
* drives the deny/MAND/revocability derivation in vfs_claim.c.  Bits use
* the CHIMERA_CLAIM_* vocabulary (data R/W/D; cache CR/CW/H; lock LR/LW). */

/* SMB share reservation: access ⊆ R|W|D, deny ⊆ R|W|D.  A zero/zero pair
 * builds the inert attribute-only registration (query-visible, conflicts
 * with nothing). */
void
chimera_vfs_claim_init_smb_open(
    struct chimera_vfs_claim         *claim,
    uint8_t                           access,
    uint8_t                           deny,
    const struct chimera_claim_owner *owner);

/* NFSv4 OPEN share/deny: access ⊆ R|W, deny ⊆ R|W. */
void
chimera_vfs_claim_init_nfs4_open(
    struct chimera_vfs_claim         *claim,
    uint8_t                           access,
    uint8_t                           deny,
    const struct chimera_claim_owner *owner);

/* SMB2 RqLs lease: used ⊆ CR|CW|H (owner->key carries the LeaseKey). */
void
chimera_vfs_claim_init_rqls(
    struct chimera_vfs_claim         *claim,
    uint8_t                           used,
    const struct chimera_claim_owner *owner);

/* Legacy oplock at LEVEL_II (CR), EXCLUSIVE (CR|CW), or BATCH (CR|CW|H) —
 * the construct is derived from `used`. */
void
chimera_vfs_claim_init_oplock(
    struct chimera_vfs_claim         *claim,
    uint8_t                           used,
    const struct chimera_claim_owner *owner);

/* SMB3 directory lease: used ⊆ CR|H. */
void
chimera_vfs_claim_init_dir_lease(
    struct chimera_vfs_claim         *claim,
    uint8_t                           used,
    const struct chimera_claim_owner *owner);

/* NFSv4 delegation.  A read delegation carries R|CR; a write delegation
 * R|W|CR|CW (delegations perform real I/O under their stateids, which is
 * also what makes deny-read opens conflict with them). */
void
chimera_vfs_claim_init_delegation(
    struct chimera_vfs_claim         *claim,
    bool                              write,
    const struct chimera_claim_owner *owner);

/* Byte-range lock.  smb selects the mandatory (LOCK_SMB) construct; the
* owner's key should carry the open's grant LeaseKey when it holds one so
* the fresh-cache denial self-exempts at KEY (brl2 across two opens). */
/* FUSE kernel read-cache grant (one per mount+file; owner.client_key =
 * mount identity): DELEG_R-shaped rows and awaited-class breaks, but
 * sweep-revocable at the break deadline (the liveness backstop). */
void
chimera_vfs_claim_init_fuse_grant(
    struct chimera_vfs_claim         *claim,
    const struct chimera_claim_owner *owner);

void
chimera_vfs_claim_init_range(
    struct chimera_vfs_claim         *claim,
    bool                              exclusive,
    bool                              smb,
    uint64_t                          offset,
    uint64_t                          length,
    const struct chimera_claim_owner *owner);

/* Transient deny-only probe (the SMB rename dp_probe: used 0, deny D). */
void
chimera_vfs_claim_init_deny_probe(
    struct chimera_vfs_claim         *claim,
    uint8_t                           deny,
    const struct chimera_claim_owner *owner);

/* -------------------------------------------------------------------- */
/* Acquire / release / test (single entrance)                           */
/* -------------------------------------------------------------------- */

/* Synchronous acquire: admit-or-break-or-deny, no ticket.  On GRANTED the
 * claim is inserted (ownership with the core until release).  On BREAKING
 * the recalls have been started; the caller retries (or uses the ticketed
 * form).  conflict_out (optional) is filled BY VALUE. */
enum chimera_vfs_claim_result
chimera_vfs_claim_try_acquire(
    struct chimera_vfs_state          *state,
    struct chimera_vfs_file_state     *file,
    struct chimera_vfs_claim          *claim,
    struct chimera_vfs_claim_conflict *conflict_out);

/* Ticketed acquire.  wait queues on BREAKING; wait_hard (lock claims)
 * additionally queues on hard DENIED.  blocked_cb (may be NULL) fires
 * exactly once, synchronously, iff the ticket queued.
 *
 * `thread` (may be NULL) enables backend RANGE projection on CAP_LEASE
 * files: a locally-granted byte-range lock is confirmed with the backend
 * BEFORE the callback fires GRANTED (optimistic local insert, rollback and
 * DENIED on backend refusal).  NULL skips projection (tests, callers with
 * no dispatch context); AGGREGATE projection is unaffected (it rides the
 * service thread). */
void
chimera_vfs_claim_acquire(
    struct chimera_vfs_thread          *thread,
    struct chimera_vfs_state           *state,
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_claim           *claim,
    struct chimera_vfs_pending_acquire *ticket,
    bool                                wait,
    bool                                wait_hard,
    chimera_vfs_claim_acquire_cb_t      cb,
    chimera_vfs_claim_blocked_cb_t      blocked_cb,
    void                               *private_data);

/* Always synchronous; pumps waiters.  Callers may free the embedding
 * struct immediately after. */
void
chimera_vfs_claim_release_ranged(
    struct chimera_vfs_thread     *thread,
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim);

void
chimera_vfs_claim_release(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim);

/* Shrink an inserted ACCESS claim's masks in place (truncating-open W drop,
 * OPEN_DOWNGRADE).  Never conflicts; pumps waiters. */
void
chimera_vfs_claim_shrink(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim,
    uint8_t                        new_used,
    uint8_t                        new_denied);

/* Pure probe (LOCKT / NLM TEST / F_GETLK / FAIL_IMMEDIATELY pre-check). */
enum chimera_vfs_claim_result
chimera_vfs_claim_test(
    struct chimera_vfs_file_state     *file,
    const struct chimera_vfs_claim    *probe,
    struct chimera_vfs_claim_conflict *conflict_out);

/* Exactly-once cancel of a queued ticket: true = dequeued, cb never fires;
 * false = the cb owns the entry (fired or firing). */
bool
chimera_vfs_claim_cancel(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket);

/* Atomic same-owner REPLACE carve for POSIX-geometry range sets: remove the
 * owner's coverage of [offset, offset+length).  v1 is carve-only —
 * new_mask MUST be 0 (aborts otherwise); the replacement extent, when any,
 * is the caller's subsequent acquire.  The core consumes fresh claim
 * structs from `spare` (caller-provided, up to two) for split remainders
 * and hands released fragments back via the release callback.  Used by the
 * POSIX client (and later NFSv4 LOCKU); SMB stays exact-stack. */
void
chimera_vfs_claim_range_replace(
    struct chimera_vfs_state         *state,
    struct chimera_vfs_file_state    *file,
    const struct chimera_claim_owner *owner,
    uint64_t                          offset,
    uint64_t                          length,
    uint8_t                           new_mask,
    struct chimera_vfs_claim         *spare[2],
    int                              *spare_used,
    void (                           *released_cb )(
        struct chimera_vfs_claim *claim,
        void                     *arg),
    void                             *released_arg);

/* -------------------------------------------------------------------- */
/* Cache grants                                                         */
/* -------------------------------------------------------------------- */

/* Grant acquisition flavors. */
enum chimera_vfs_claim_grant_flavor {
    CHIMERA_CLAIM_GRANT_EXACT = 0,     /* try the requested mode as-is      */
    CHIMERA_CLAIM_GRANT_BREAK_NONE,    /* decline instead of breaking       */
};

/* Coalesce-or-create.  The core allocates and frees grants; the claim
 * template supplies construct/owner/used and the break/alive/revoked
 * callbacks.  is_v2 selects epoch semantics.  R31's four sub-rules
 * (strict-superset upgrade; never mid-break + BREAK_IN_PROGRESS re-open;
 * ACKED-at-0 / dir re-arm with epoch bump; rescue only at refcount-1 +
 * sole cache) are implemented here verbatim.
 *
 * member_seed (optional): stored as a FRESH grant's members head BEFORE the
 * embedded claim is inserted, so a break callback can never observe a
 * memberless mid-insert grant (the old pre-registered-member discipline).
 * The seed must be walk-ready: its protocol next-link NULL, its state
 * consistent for a break callback's live-member scan.  *member_seeded (may
 * be NULL) reports whether the seed was consumed — false on a coalesce hit
 * or a racing-create collapse, where the caller registers its member on the
 * returned grant itself. */
enum chimera_vfs_claim_result
chimera_vfs_claim_grant_acquire(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_file_state      *file,
    const struct chimera_vfs_claim     *template_claim,
    int                                 upgrade_ok,
    uint8_t                             is_v2,
    enum chimera_vfs_claim_grant_flavor flavor,
    void                               *member_seed,
    bool                               *member_seeded,
    struct chimera_vfs_claim_grant    **grant_out,
    struct chimera_vfs_claim_conflict  *conflict_out);

/* Coalesce-only: returns an existing same-owner / same-LeaseKey grant with
 * its refcount bumped (upgrading per R31 when upgrade_ok), or NULL. */
struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_coalesce(
    struct chimera_vfs_file_state    *file,
    const struct chimera_claim_owner *owner,
    uint8_t                           want,
    int                               upgrade_ok);

/* Deferred-open rescue upgrade (refcount-1 + IDLE + sole cache claim). */
uint8_t
chimera_vfs_claim_grant_try_upgrade(
    struct chimera_vfs_file_state  *file,
    struct chimera_vfs_claim_grant *grant,
    uint8_t                         want_used);

/* Cap a requested cache mode to the largest subset grantable without
 * breaking another owner (MS-SMB2 3.3.5.9 "granting never breaks"); steps
 * W then H toward the CR floor.  strict returns 0 when even CR conflicts
 * (oplock-transparent stat-opens). */
uint8_t
chimera_vfs_claim_grant_cap_mode(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *template_claim,
    bool                            strict);

void
chimera_vfs_claim_grant_release(
    struct chimera_vfs_state       *state,
    struct chimera_vfs_claim_grant *grant,
    bool                            pump);

/* Same-client cache queries used by the SMB create path's grant-capping
 * policy (the sole-opener rule lives HERE, not in the admission masks). */
bool
chimera_vfs_claim_client_holds_handle_cache(
    struct chimera_vfs_file_state *file,
    uint64_t                       client_key);

bool
chimera_vfs_claim_client_holds_cache(
    struct chimera_vfs_file_state *file,
    uint64_t                       client_key);

/* -------------------------------------------------------------------- */
/* Break machinery                                                      */
/* -------------------------------------------------------------------- */

void
chimera_vfs_claim_ack(
    struct chimera_vfs_claim *claim,
    uint8_t                   resulting_used);

/* Revoke `claim` with the caller already holding file->lock, handing back the
 * revoked callback instead of firing it.
 *
 * An acquirer only ever borrows a conflicting claim: ACCESS and RANGE claims
 * carry no refcount (their `grant` is NULL), so the lock admission found the
 * conflict under is the only thing keeping it alive.  A caller that drops the
 * lock and then revokes races the holder's owner tearing it down -- the NFSv4
 * lease sweeper frees a lock-owner's range leases outright -- so the mutation
 * has to happen while the lock is still held.  Fire the returned callback
 * after unlocking; it must not dereference the claim, which may be gone by
 * then (all implementations take their state from cb_private).
 *
 * *out_cb is set only when this call is what newly revoked the claim.  The
 * caller owns the post-revoke pumps that chimera_vfs_claim_revoke() does. */
void
chimera_vfs_claim_revoke_locked(
    struct chimera_vfs_file_state  *file,
    struct chimera_vfs_claim       *claim,
    chimera_vfs_claim_revoked_cb_t *out_cb,
    void                          **out_private);

void
chimera_vfs_claim_revoke(
    struct chimera_vfs_claim *claim);

/* Durable park/unpark: masks the claim's advertised H and H denial while
 * parked (R48).  Takes file->lock; use the _locked form from a context that
 * already holds it (e.g. a grant member walk). */
void
chimera_vfs_claim_park(
    struct chimera_vfs_claim *claim,
    bool                      parked);

static inline void
chimera_vfs_claim_park_locked(
    struct chimera_vfs_claim *claim,
    bool                      parked)
{
    claim->parked = parked ? 1 : 0;
} /* chimera_vfs_claim_park_locked */

/* True while an ack-required break by another grant is outstanding (the
 * conflicting-CREATE park predicate, R39). */
bool
chimera_vfs_claim_ack_pending(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    const struct chimera_vfs_claim_grant *except);

/* Notified-edge machinery (R40): true while a begun break's notification
 * has not yet been sent; mark it sent by lease key. */
bool
chimera_vfs_claim_break_pending_notify(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash);

void
chimera_vfs_claim_mark_break_notified(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash,
    const uint8_t            *lease_key);

/* Revoke every mid-break cache claim except `except` (parked-open deadline
 * expiry). */
void
chimera_vfs_claim_revoke_breaks(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    const struct chimera_vfs_claim_grant *except);

/* -------------------------------------------------------------------- */
/* Trigger engine (Table 2)                                             */
/* -------------------------------------------------------------------- */

/* Fire a non-parking invalidation trigger.  `retain` applies to the rows
 * that take a caller floor (OPEN_H/OPEN_H_FORCE/OPEN_W/NS_UNLINK); other
 * rows fix their own.  actor may be NULL for a leaseless mutator (breaks
 * every eligible holder). */
void
chimera_vfs_claim_invalidate(
    struct chimera_vfs_state         *state,
    const uint8_t                    *fh,
    uint8_t                           fh_len,
    uint64_t                          fh_hash,
    enum chimera_claim_trigger        trigger,
    const struct chimera_claim_actor *actor,
    uint8_t                           retain);

/* NS_FULL as a synchronous query: kick the full recall and report whether
 * any holder still blocks (NFSv4 REMOVE/RENAME's NFS4ERR_DELAY loop). */
bool
chimera_vfs_claim_break_caching(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash);

/* -------------------------------------------------------------------- */
/* I/O path                                                             */
/* -------------------------------------------------------------------- */

/* Mediate `request` through the claim layer then invoke next(request).
 * A NULL actor means the implicit INTERNAL claim mediates; an actor with a
 * real owner self-exempts its own cache and fires WRITE invalidation for
 * writes (R61). */
void
chimera_vfs_io_claim_acquire(
    struct chimera_vfs_request       *request,
    const struct chimera_claim_actor *actor,
    void (                           *next )(
        struct chimera_vfs_request *request));

void
chimera_vfs_io_claim_release(
    struct chimera_vfs_request *request);

void
chimera_vfs_state_io_resume(
    struct chimera_vfs_request *request);

/* Parking namespace recalls (unchanged shape from the old core). */
void
chimera_vfs_io_recall(
    struct chimera_vfs_request *request,
    const uint8_t              *fh,
    uint8_t                     fh_len,
    uint64_t                    fh_hash,
    int                         flush_only,
    void (                     *next )(
        struct chimera_vfs_request *request));

void
chimera_vfs_io_recall_single(
    struct chimera_vfs_request *request,
    const uint8_t              *fh,
    uint8_t                     fh_len,
    uint64_t                    fh_hash,
    uint8_t                     retain,
    void (                     *next )(
        struct chimera_vfs_request *request));

/* Mandatory-lock I/O predicate (SMB data path only; inline local bool).
 * Zero-length reads are exempt before the walk.  MAND rows are stamped on
 * every range lock regardless of protocol: shared denies W globally (its
 * own owner included), exclusive denies R|W exempting its own actor. */
bool
chimera_vfs_claim_io_denied(
    struct chimera_vfs_state         *state,
    const uint8_t                    *fh,
    uint8_t                           fh_len,
    uint64_t                          fh_hash,
    uint64_t                          offset,
    uint64_t                          length,
    bool                              is_write,
    const struct chimera_claim_actor *actor);

/* Idle reaper (100ms sweep from the close thread; virtual-clock driven). */
void
chimera_vfs_state_reap_idle(
    struct chimera_vfs_state *state,
    uint64_t                  idle_ms);

/* -------------------------------------------------------------------- */
/* Backend lease projection (CHIMERA_VFS_CAP_LEASE)                     */
/* -------------------------------------------------------------------- */

/* Attach the projection service (called once from the close-thread setup):
 * scans the registered modules for CAP_LEASE, mints the node owner, and
 * records the service thread + doorbell.  Absent a CAP_LEASE module every
 * projection hook is a no-op. */
void
chimera_vfs_claim_backend_attach(
    struct chimera_vfs_state  *state,
    struct chimera_vfs        *vfs,
    struct chimera_vfs_thread *service_thread,
    struct evpl_doorbell      *service_doorbell);

/* Detach the service doorbell (called by the service thread before it
* destroys the doorbell): late posts from teardown traffic on other threads
* then enqueue without ringing instead of ringing a closed eventfd. */
void
chimera_vfs_claim_backend_detach(
    struct chimera_vfs_state *state);

/* Drain the projection work queue; runs ONLY on the service thread (wired
 * into the close thread's timer and doorbell). */
void
chimera_vfs_claim_backend_service(
    struct chimera_vfs_state *state);

/* Recompute the file's union {rev_used, bind_deny} from its local claims
 * and post service work when the backend cover must change.  Cheap no-op
 * when no CAP_LEASE module exists.  Called from every claim mutation site
 * (the single-entrance property: acquire/release/shrink/ack/revoke/drain
 * all funnel here). */
void
chimera_vfs_claim_backend_reeval(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file);

/* The recall upcall handed to backends (as acquire.recall_cb with
 * recall_arg = the chimera_vfs_state).  Any thread; marshals internally;
 * tolerates an unknown file or stale token as a no-op. */
void
chimera_vfs_lease_backend_recall(
    void          *recall_arg,
    const uint8_t *fh,
    uint8_t        fh_len,
    uint64_t       fh_hash,
    uint64_t       token,
    uint8_t        retain);

/* -------------------------------------------------------------------- */
/* Queries and SMB annex                                                */
/* -------------------------------------------------------------------- */

/* Filtered holder scan, by value + policy_tag, under file->lock; visit_cb
 * must not re-enter the core.  Covers the CB_GETATTR write-delegation
 * query, parked scans, and AppInstanceId resolution. */
struct chimera_vfs_claim_filter {
    uint8_t  klass_mask;      /* bitmask of (1 << CHIMERA_CLAIM_CLASS_*)   */
    uint8_t  used_any;        /* 0 = any                                   */
    uint8_t  proto;           /* 0 = any                                   */
    uint64_t exclude_client;  /* 0 = none                                  */
    uint8_t  breaking_only;
    uint8_t  include_inert;
};

typedef void (*chimera_vfs_claim_visit_cb_t)(
    const struct chimera_vfs_claim *claim,
    void                           *arg);

void
chimera_vfs_claim_scan(
    struct chimera_vfs_file_state         *file,
    const struct chimera_vfs_claim_filter *filter,
    chimera_vfs_claim_visit_cb_t           visit_cb,
    void                                  *arg);

bool
chimera_vfs_state_has_other_share_holder(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *exclude);

void
chimera_vfs_state_set_delete_pending(
    struct chimera_vfs_file_state *file);

void
chimera_vfs_state_clear_delete_pending(
    struct chimera_vfs_file_state *file);

bool
chimera_vfs_state_is_delete_pending(
    struct chimera_vfs_file_state *file);

void
chimera_vfs_state_stream_holder_inc(
    struct chimera_vfs_file_state *file);

void
chimera_vfs_state_stream_holder_dec(
    struct chimera_vfs_file_state *file);

uint32_t
chimera_vfs_state_stream_holders(
    struct chimera_vfs_file_state *file);
