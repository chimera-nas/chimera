// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#include <uthash.h>
#include <utlist.h>

#include "nfs4_xdr.h"
#include "nfs4_stateid.h"
#include "nfs4_lease.h"
#include "vfs/vfs.h"
#include "vfs/vfs_state.h"
#include "common/macros.h"

/* One held NFSv4 byte-range, tracked so it can be released on LOCKU or
 * cascaded at lock_state teardown.  vfs_state coordinates the range
 * across NLM and SMB; the lease lives here for the lock_state's lifetime. */
struct nfs4_range_lease {
    struct chimera_vfs_lease           lease;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_vfs_file_state     *file_state;
    struct nfs4_range_lease           *next;
};

/*
 * Unified NFSv4 state model.
 *
 * Shape is independent of minor version.  4.0 uses the seqid/replay fields
 * on the owners; 4.1+ leaves them at zero (its DRC lives on nfs4_session).
 * stateid.other is decoded through nfs_state_table (see nfs4_stateid.h),
 * not through any session.
 *
 * Hierarchy:
 *   nfs_client
 *     ├── nfs_open_owner            (per {client, owner_string})
 *     │     └── nfs_open_state      (per {open_owner, fh})
 *     │                  ↑
 *     │   nfs_lock_state ┘          (anchored to its open_state)
 *     │           ↑
 *     └── nfs_lock_owner            (per {client, lock_owner_string};
 *                                    holds locks that may span multiple
 *                                    open_owners — RFC 7530 §9.1.4)
 *
 * State objects allocate a slot in the global nfs_state_table; the slot
 * index and generation are embedded in stateid.other.  Slot reuse advances
 * the generation, so stale stateids return NFS4ERR_STALE_STATEID.
 */

struct nfs_client;
struct nfs_open_owner;
struct nfs_open_state;
struct nfs_lock_owner;
struct nfs_lock_state;
struct nfs_state_table;
struct chimera_vfs_thread;

/*
 * Per-owner replay cache.  RFC 7530 §9.1.7: when an owner-sequenced op is
 * retransmitted with the same seqid, the server must return the original
 * reply.  Phase 4 caches the structured fields most clients depend on
 * (status + stateid).  A fully byte-perfect replay (cinfo, attrset, ...)
 * is deferred; in practice Linux clients re-fetch those via GETATTR.
 */
struct nfs4_replay_cache {
    uint32_t        seqid;     /* seqid this entry replies to */
    uint32_t        op;        /* nfs_opnum4 of the cached op  */
    nfsstat4        status;
    struct stateid4 stateid;   /* for OPEN/CLOSE/LOCK/LOCKU/etc. responses */
    uint8_t         valid;
};

/*
 * RFC 7530 §9.1.7 seqid classification result.
 *   NFS4_SEQID_NEW       : seqid == owner.seqid + 1 (or owner.seqid == 0
 *                          and seqid == 1); caller should execute and
 *                          record the reply on completion.
 *   NFS4_SEQID_REPLAY    : seqid == owner.seqid; caller should return the
 *                          cached reply directly.
 *   NFS4_SEQID_BAD       : anything else; return NFS4ERR_BAD_SEQID.
 */
#define NFS4_SEQID_NEW    0
#define NFS4_SEQID_REPLAY 1
#define NFS4_SEQID_BAD    2

static inline int
nfs4_owner_seqid_classify(
    uint32_t                        owner_seqid,
    const struct nfs4_replay_cache *replay,
    uint32_t                        incoming)
{
    /* RFC 7530 §9.1.7: the first state-modifying op against a freshly
     * created open_owner/lock_owner uses whatever seqid the client picks --
     * the Linux client picks 0.  Detect "fresh" by an empty replay cache:
     * the completion wrapper seeds owner.seqid and replay together on the
     * first advancing outcome, so a populated replay implies the owner has
     * already been observed by the server. */
    if (replay && !replay->valid) {
        return NFS4_SEQID_NEW;
    }
    if (incoming == owner_seqid + 1) {
        return NFS4_SEQID_NEW;
    }
    if (replay && replay->valid && replay->seqid == incoming) {
        return NFS4_SEQID_REPLAY;
    }
    return NFS4_SEQID_BAD;
} /* nfs4_owner_seqid_classify */

static inline void
nfs4_replay_record(
    struct nfs4_replay_cache *replay,
    uint32_t                  seqid,
    uint32_t                  op,
    nfsstat4                  status,
    const struct stateid4    *stateid)
{
    replay->seqid  = seqid;
    replay->op     = op;
    replay->status = status;
    if (stateid) {
        replay->stateid = *stateid;
    } else {
        memset(&replay->stateid, 0, sizeof(replay->stateid));
    }
    replay->valid = 1;
} /* nfs4_replay_record */

/*
 * RFC 7530 §9.1.7: the owner-seqid is advanced and the replay cache is
 * updated for almost every outcome of an owner-sequenced op.  The only
 * exceptions are these "infrastructure" errors -- the request hasn't
 * really been "consumed" by the owner state machine, so the seqid is
 * left untouched and the client retries with the same seqid.
 */
static inline bool
nfs4_seqid_should_advance(nfsstat4 status)
{
    switch (status) {
        case NFS4ERR_STALE_CLIENTID:
        case NFS4ERR_STALE_STATEID:
        case NFS4ERR_BAD_STATEID:
        case NFS4ERR_BAD_SEQID:
        case NFS4ERR_BADXDR:
        case NFS4ERR_RESOURCE:
        case NFS4ERR_NOFILEHANDLE:
        case NFS4ERR_MOVED:
            return false;
        default:
            return true;
    } // switch
} /* nfs4_seqid_should_advance */

struct nfs_client {
    uint64_t               client_id;
    uint64_t               verifier;
    uint64_t               boot_id;
    uint8_t                owner_string[NFS4_OPAQUE_LIMIT];
    uint16_t               owner_len;
    uint8_t                confirmed     : 1;
    uint8_t                expired       : 1;
    uint8_t                recovering    : 1;
    uint8_t                soft_revoked  : 1;
    uint8_t                minor;  /* 0/1/2 */
    uint64_t               last_touch_ns;

    /* Owners are hashed by their byte string. */
    struct nfs_open_owner *open_owners_by_str;
    struct nfs_lock_owner *lock_owners_by_str;

    UT_hash_handle         hh_by_owner;
    UT_hash_handle         hh_by_id;

    _Atomic uint32_t       refcount;
    pthread_mutex_t        lock;
};

struct nfs_open_owner {
    struct nfs_client       *client;       /* borrowed; client outlives owners */
    uint8_t                  owner[NFS4_OPAQUE_LIMIT];
    uint16_t                 owner_len;
    uint32_t                 seqid;        /* 4.0 RFC 7530 §9.1.7; 4.1+ unused */
    bool                     confirmed;    /* 4.0 OPEN_CONFIRM gate */
    struct nfs4_replay_cache replay;       /* 4.0 only */
    struct nfs_open_state   *states_by_fh; /* uthash keyed on {fh, fh_len} */
    UT_hash_handle           hh;
    pthread_mutex_t          lock;
};

struct nfs_open_state {
    struct nfs_open_owner          *owner;
    uint8_t                         fh[NFS4_FHSIZE];
    uint16_t                        fh_len;
    uint32_t                        share_access;       /* OPEN4_SHARE_ACCESS_* */
    uint32_t                        share_deny;         /* OPEN4_SHARE_DENY_*   */
    uint32_t                        seqid;              /* stateid.seqid */

    /* Slot identity (decoded from stateid.other). */
    uint8_t                         shard;
    uint32_t                        slot_idx;
    uint32_t                        generation;

    struct chimera_vfs_open_handle *handle;             /* +1 via chimera_vfs_dup_handle */

    struct nfs_lock_state          *locks;              /* utlist via next_in_open */
    UT_hash_handle                  hh;                 /* by fh in owner->states_by_fh */

    /* vfs_state SHARE reservation for cross-protocol (NLM/SMB) share-mode
     * coordination.  Held while the open_state is alive; released in
     * open_state_cleanup. */
    struct chimera_vfs_lease        share_lease;
    struct chimera_vfs_file_state  *share_file_state;
    bool                            share_lease_held;

    /* Lifetime: starts at 1 for the state's slot; each acquire bumps it.
     * destroy() flips `destroyed` and drops the +1.  When refcount reaches
     * zero AND destroyed is set, the handle is released and the struct
     * freed by the last release. */
    _Atomic uint32_t                refcount;
    _Atomic uint8_t                 destroyed;
};

struct nfs_lock_owner {
    struct nfs_client       *client;       /* borrowed */
    uint8_t                  owner[NFS4_OPAQUE_LIMIT];
    uint16_t                 owner_len;
    uint32_t                 seqid;        /* 4.0 */
    struct nfs4_replay_cache replay;       /* 4.0 only */
    struct nfs_lock_state   *states;       /* utlist via next_in_owner */
    UT_hash_handle           hh;
    pthread_mutex_t          lock;
};

struct nfs_lock_state {
    struct nfs_lock_owner          *lock_owner;
    struct nfs_open_state          *open_state;
    uint32_t                        seqid;

    uint8_t                         shard;
    uint32_t                        slot_idx;
    uint32_t                        generation;

    struct chimera_vfs_open_handle *handle;             /* +1 distinct dup */

    /* vfs_state RANGE leases held by this lock_state, one per locked
     * byte-range.  LOCK appends, LOCKU removes the matching range, and
     * lock_state_cleanup drains the remainder. */
    struct nfs4_range_lease        *range_leases;

    /* utlist LL chains.  *_in_open is rooted on open_state->locks (CLOSE
     * cascade); *_in_owner is rooted on lock_owner->states (RELEASE_LOCKOWNER
     * cascade, wired up in Phase 4). */
    struct nfs_lock_state          *next_in_open;
    struct nfs_lock_state          *next_in_owner;

    _Atomic uint32_t                refcount;
    _Atomic uint8_t                 destroyed;
};

/*
 * Slot table.
 *
 * Each shard owns a dense slot array indexed by 24-bit slot_idx.  The slot
 * carries the state pointer, a type discriminator, and a generation counter
 * incremented on every (re)use to detect stale stateids (ABA).
 */

#define NFS4_SLOT_TYPE_FREE    0
#define NFS4_SLOT_TYPE_OPEN    1
#define NFS4_SLOT_TYPE_LOCK    2
/* A slot whose state was torn down because the owning client was purged
 * (lease expiry / reboot / DESTROY_CLIENTID).  Distinct from FREE so a stateid
 * minted by the purged client resolves to NFS4ERR_EXPIRED rather than
 * NFS4ERR_BAD_STATEID (RFC 7530 §8.1.3).  The slot is on the free list and
 * reverts to a normal type when reallocated. */
#define NFS4_SLOT_TYPE_EXPIRED 3

struct nfs_state_slot {
    void    *state;
    uint32_t generation;
    uint8_t  type;
};

struct nfs_state_shard {
    pthread_rwlock_t       lock;
    struct nfs_state_slot *slots;
    uint32_t               slots_capacity;
    uint32_t               slots_used;       /* high-water of allocated entries */
    uint32_t              *free_idx;
    uint32_t               free_count;
    uint32_t               free_capacity;
};

struct nfs_state_table {
    struct nfs_state_shard shards[NFS_STATE_NUM_SHARDS];
    /* Per-server-instance epoch stamped into every stateid; see
     * nfs4_stateid.h.  Set once at nfs_state_table_init. */
    uint32_t               epoch;
};

/*
 * Public API.
 */

SYMBOL_EXPORT void nfs_state_table_init(
    struct nfs_state_table *table);
SYMBOL_EXPORT void nfs_state_table_free(
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread);

/* Allocate a slot, returning shard / slot_idx / generation.  Caller fills in
 * the state pointer via nfs_state_table_install. */
SYMBOL_EXPORT int nfs_state_table_alloc(
    struct nfs_state_table *table,
    uint8_t                 type,
    uint8_t                *out_shard,
    uint32_t               *out_slot_idx,
    uint32_t               *out_generation);

/* Install a state pointer into a pre-allocated slot. */
SYMBOL_EXPORT void nfs_state_table_install(
    struct nfs_state_table *table,
    uint8_t                 shard,
    uint32_t                slot_idx,
    uint8_t                 type,
    void                   *state);

/* Release a slot back to the pool (advances generation, clears type). */
SYMBOL_EXPORT void nfs_state_table_free_slot(
    struct nfs_state_table *table,
    uint8_t                 shard,
    uint32_t                slot_idx);

/* Like free_slot but marks the slot EXPIRED (client purged); see
 * NFS4_SLOT_TYPE_EXPIRED. */
SYMBOL_EXPORT void nfs_state_table_expire_slot(
    struct nfs_state_table *table,
    uint8_t                 shard,
    uint32_t                slot_idx);

/* Hot-path lookup + ref.  Returns NFS4_OK with the state pointer and type
 * filled in, having bumped refcount.  Caller must invoke
 * nfs_state_table_release() when done.  On error returns NFS4ERR_BAD_STATEID
 * or NFS4ERR_STALE_STATEID and leaves the outputs untouched. */
SYMBOL_EXPORT nfsstat4 nfs_state_table_acquire(
    struct nfs_state_table *table,
    const struct stateid4  *sid,
    uint8_t                 want_type,                     /* 0 = ANY */
    void                  **out_state,
    uint8_t                *out_type);

/* Drop the ref taken by acquire.  If this was the last ref and the state has
 * been marked destroyed, performs cleanup (releases the VFS handle, frees
 * the struct). */
SYMBOL_EXPORT void nfs_state_table_release(
    struct nfs_state_table    *table,
    void                      *state,
    uint8_t                    type,
    struct chimera_vfs_thread *vfs_thread);

/* Validate without acquiring (used by TEST_STATEID). */
SYMBOL_EXPORT nfsstat4 nfs_state_table_validate(
    struct nfs_state_table *table,
    const struct stateid4  *sid);

/*
 * Lifecycle API for client / owner / state objects.  Phase 2 callers use
 * these in place of the old session-slot allocator.
 */

SYMBOL_EXPORT struct nfs_client *
nfs_client_alloc(
    uint64_t    client_id,
    const void *owner_string,
    uint16_t    owner_len,
    uint64_t    verifier,
    uint8_t     minor);

/* Tear down everything underneath a client (owners → states → locks),
 * releasing every dup'd VFS handle via vfs_thread, and freeing the struct.
 * Idempotent; safe to call on a fully-empty client. */
SYMBOL_EXPORT void
nfs_client_destroy(
    struct nfs_client         *client,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread);

/* Find an existing open_owner under `client` by byte-string, or create one.
 * Sets *out_created = true if a new one was allocated. */
SYMBOL_EXPORT struct nfs_open_owner *
nfs_open_owner_find_or_create(
    struct nfs_client *client,
    const void        *owner_bytes,
    uint16_t           owner_len,
    bool              *out_created);

/* Find an existing open_state on `owner` whose FH matches.  Returns NULL if
 * none.  Does NOT acquire a ref. */
SYMBOL_EXPORT struct nfs_open_state *
nfs_open_owner_find_state(
    struct nfs_open_owner *owner,
    const uint8_t         *fh,
    uint16_t               fh_len);

/* Create a new open_state under `owner`.  `handle_dup` must already be a
 * dup'd reference (caller invoked chimera_vfs_dup_handle).  Allocates a slot
 * in `table` and writes the encoded stateid to `out_stateid`.
 *
 * The returned state has refcount = 1 (its lifetime ref). */
SYMBOL_EXPORT struct nfs_open_state *
nfs_open_state_create(
    struct nfs_open_owner          *owner,
    const uint8_t                  *fh,
    uint16_t                        fh_len,
    uint32_t                        share_access,
    uint32_t                        share_deny,
    struct chimera_vfs_open_handle *handle_dup,
    struct nfs_state_table         *table,
    struct stateid4                *out_stateid);

/* RFC 7530 §9.10 share-mode conflict check.  Walks every open_owner
 * on `client` other than `requesting_owner` looking for a state on
 * `fh`.  Returns NFS4ERR_SHARE_DENIED if any peer state has access bits
 * the new OPEN is trying to deny (or vice versa), else NFS4_OK.
 *
 * NOTE: there is a brief TOCTOU window between this check returning OK
 * and the subsequent nfs_open_state_create()/_coalesce() call -- a
 * concurrent OPEN from a different owner against the same fh could
 * slip in.  In practice this requires two distinct clients/owners
 * racing on the same file within microseconds.  The common-case
 * Windows-via-NFS / Samba-on-NFS workload is correctly served. */
SYMBOL_EXPORT nfsstat4
nfs_client_check_share_conflict(
    struct nfs_client     *client,
    struct nfs_open_owner *requesting_owner,
    const uint8_t         *fh,
    uint16_t               fh_len,
    uint32_t               requested_access,
    uint32_t               requested_deny);

/* Re-open: merge share bits onto an existing state, bump its seqid, and
 * write the (now-updated) stateid to `out_stateid`. */
SYMBOL_EXPORT void
nfs_open_state_coalesce(
    struct nfs_open_state  *state,
    uint32_t                share_access,
    uint32_t                share_deny,
    struct nfs_state_table *table,
    struct stateid4        *out_stateid);

/* Mark a state for destruction and drop the lifetime ref.  Any walks
 * already holding an acquire-ref will complete their work; the actual
 * cleanup (VFS release, struct free) happens on the last release. */
SYMBOL_EXPORT void
nfs_open_state_destroy(
    struct nfs_open_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread);

/* Lock-owner / lock-state equivalents. */
SYMBOL_EXPORT struct nfs_lock_owner *
nfs_lock_owner_find_or_create(
    struct nfs_client *client,
    const void        *owner_bytes,
    uint16_t           owner_len,
    bool              *out_created);

SYMBOL_EXPORT struct nfs_lock_state *
nfs_lock_state_create(
    struct nfs_lock_owner          *lock_owner,
    struct nfs_open_state          *open_state,
    struct chimera_vfs_open_handle *handle_dup,
    struct nfs_state_table         *table,
    struct stateid4                *out_stateid);

SYMBOL_EXPORT void
nfs_lock_state_destroy(
    struct nfs_lock_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread);

/* Touch a client's lease.  Cheap relaxed store; called from any op that
 * counts as renewal evidence (state acquire, SEQUENCE, RENEW).  Phase 3. */
static inline void
nfs_client_touch(struct nfs_client *client)
{
    if (!client) {
        return;
    }
    atomic_store_explicit((_Atomic uint64_t *) &client->last_touch_ns,
                          nfs_lease_now_ns(), memory_order_relaxed);
} /* nfs_client_touch */


