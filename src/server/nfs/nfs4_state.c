// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "nfs4_state.h"
#include "nfs4_callback.h"
#include "nfs4_session.h"
#include "nfs4_layout_table.h"
#include "nfs_internal.h"
#include "vfs/vfs_release.h"

#define NFS4_SHARD_INITIAL_CAPACITY 64

static void
shard_init(struct nfs_state_shard *shard)
{
    pthread_rwlock_init(&shard->lock, NULL);
    shard->slots          = NULL;
    shard->slots_capacity = 0;
    shard->slots_used     = 0;
    shard->free_idx       = NULL;
    shard->free_count     = 0;
    shard->free_capacity  = 0;
} /* shard_init */

static void
shard_free(
    struct nfs_state_shard    *shard,
    struct chimera_vfs_thread *vfs_thread)
{
    /* Release any handles still held by live slots.  We do not free the
     * owning open_state / lock_state structs themselves here — that is the
     * caller's responsibility via the client / owner teardown path. */
    if (shard->slots && vfs_thread) {
        for (uint32_t i = 0; i < shard->slots_used; i++) {
            struct nfs_state_slot *slot = &shard->slots[i];
            if (slot->type == NFS4_SLOT_TYPE_OPEN) {
                struct nfs_open_state *os = slot->state;
                if (os && os->handle) {
                    chimera_vfs_release(vfs_thread, os->handle);
                    os->handle = NULL;
                }
            } else if (slot->type == NFS4_SLOT_TYPE_LOCK) {
                struct nfs_lock_state *ls = slot->state;
                if (ls && ls->handle) {
                    chimera_vfs_release(vfs_thread, ls->handle);
                    ls->handle = NULL;
                }
            }
        }
    }

    free(shard->slots);
    free(shard->free_idx);
    pthread_rwlock_destroy(&shard->lock);
} /* shard_free */

static int
shard_grow_locked(struct nfs_state_shard *shard)
{
    uint32_t               new_cap;
    struct nfs_state_slot *new_slots;

    new_cap = shard->slots_capacity ? shard->slots_capacity * 2
              : NFS4_SHARD_INITIAL_CAPACITY;

    /* slot_idx is 24-bit in the wire encoding. */
    if (new_cap > (1u << 24)) {
        return -1;
    }

    new_slots = realloc(shard->slots, new_cap * sizeof(*new_slots));
    if (!new_slots) {
        return -1;
    }

    memset(&new_slots[shard->slots_capacity], 0,
           (new_cap - shard->slots_capacity) * sizeof(*new_slots));

    shard->slots          = new_slots;
    shard->slots_capacity = new_cap;
    return 0;
} /* shard_grow_locked */

static int
shard_push_free_locked(
    struct nfs_state_shard *shard,
    uint32_t                slot_idx)
{
    if (shard->free_count == shard->free_capacity) {
        uint32_t  new_cap = shard->free_capacity ? shard->free_capacity * 2 : 16;
        uint32_t *new_arr = realloc(shard->free_idx, new_cap * sizeof(*new_arr));
        if (!new_arr) {
            return -1;
        }
        shard->free_idx      = new_arr;
        shard->free_capacity = new_cap;
    }
    shard->free_idx[shard->free_count++] = slot_idx;
    return 0;
} /* shard_push_free_locked */

void
nfs_state_table_init(
    struct nfs_state_table *table,
    uint16_t                node_id)
{
    for (int i = 0; i < NFS_STATE_NUM_SHARDS; i++) {
        shard_init(&table->shards[i]);
    }

    /* Per-instance epoch stamped into every stateid (see nfs4_stateid.h):
     *   bits 31..16 : node_id  -- attributes the stateid to its minting
     *                 instance, so peers sharing one backing store never mint
     *                 colliding epochs and a foreign stateid is recognisable.
     *   bits 15..1  : low 15 bits of boot time -- differs across this node's
     *                 restarts, so a previous instance's stateids read as stale.
     *   bit  0      : 1 -- with node_id >= 1 this keeps the epoch away from the
     *                 special all-zero/all-ones stateids and pynfs's small
     *                 "old epoch" probe values (replacing the old 0x80000000). */
    table->epoch = ((uint32_t) node_id << 16) |
        (((uint32_t) time(NULL) & 0x7FFFu) << 1) | 1u;
} /* nfs_state_table_init */

void
nfs_state_table_free(
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    for (int i = 0; i < NFS_STATE_NUM_SHARDS; i++) {
        shard_free(&table->shards[i], vfs_thread);
    }
} /* nfs_state_table_free */

/* Choose a shard index for new allocations.  Simple round-robin via an
 * atomic counter; the shard is private once chosen. */
static _Atomic uint32_t g_alloc_rr = 0;

int
nfs_state_table_alloc(
    struct nfs_state_table *table,
    uint8_t                 type,
    uint8_t                *out_shard,
    uint32_t               *out_slot_idx,
    uint32_t               *out_generation)
{
    uint32_t                rr;
    uint8_t                 shard_idx;
    struct nfs_state_shard *shard;
    uint32_t                slot_idx;
    struct nfs_state_slot  *slot;

    rr        = atomic_fetch_add_explicit(&g_alloc_rr, 1, memory_order_relaxed);
    shard_idx = (uint8_t) (rr % NFS_STATE_NUM_SHARDS);
    shard     = &table->shards[shard_idx];

    pthread_rwlock_wrlock(&shard->lock);

    if (shard->free_count) {
        slot_idx = shard->free_idx[--shard->free_count];
    } else {
        if (shard->slots_used == shard->slots_capacity) {
            if (shard_grow_locked(shard) != 0) {
                pthread_rwlock_unlock(&shard->lock);
                return -1;
            }
        }
        slot_idx = shard->slots_used++;
    }

    slot               = &shard->slots[slot_idx];
    slot->generation  += 1;         /* every (re)use advances generation */
    slot->type         = type;
    slot->state        = NULL;      /* installed separately */
    slot->replay.valid = 0;

    *out_shard      = shard_idx;
    *out_slot_idx   = slot_idx;
    *out_generation = slot->generation;

    pthread_rwlock_unlock(&shard->lock);
    return 0;
} /* nfs_state_table_alloc */

void
nfs_state_table_install(
    struct nfs_state_table *table,
    uint8_t                 shard_idx,
    uint32_t                slot_idx,
    uint8_t                 type,
    void                   *state)
{
    struct nfs_state_shard *shard = &table->shards[shard_idx];
    struct nfs_state_slot  *slot;

    pthread_rwlock_wrlock(&shard->lock);

    chimera_nfs_abort_if(slot_idx >= shard->slots_used,
                         "install on unallocated slot %u/%u",
                         slot_idx, shard->slots_used);

    slot        = &shard->slots[slot_idx];
    slot->type  = type;
    slot->state = state;

    pthread_rwlock_unlock(&shard->lock);
} /* nfs_state_table_install */

void
nfs_state_table_free_slot(
    struct nfs_state_table *table,
    uint8_t                 shard_idx,
    uint32_t                slot_idx)
{
    struct nfs_state_shard *shard = &table->shards[shard_idx];
    struct nfs_state_slot  *slot;

    pthread_rwlock_wrlock(&shard->lock);

    chimera_nfs_abort_if(slot_idx >= shard->slots_used,
                         "free of unallocated slot %u/%u",
                         slot_idx, shard->slots_used);

    slot               = &shard->slots[slot_idx];
    slot->replay.valid = 0;
    slot->type         = NFS4_SLOT_TYPE_FREE;
    slot->state        = NULL;
    slot->generation  += 1;         /* invalidate any extant stateid */

    (void) shard_push_free_locked(shard, slot_idx);

    pthread_rwlock_unlock(&shard->lock);
} /* nfs_state_table_free_slot */

static void
nfs_state_table_free_slot_replay(
    struct nfs_state_table         *table,
    uint8_t                         shard_idx,
    uint32_t                        slot_idx,
    const struct nfs4_replay_cache *replay)
{
    struct nfs_state_shard *shard = &table->shards[shard_idx];
    struct nfs_state_slot  *slot;

    pthread_rwlock_wrlock(&shard->lock);

    chimera_nfs_abort_if(slot_idx >= shard->slots_used,
                         "free of unallocated slot %u/%u",
                         slot_idx, shard->slots_used);

    slot = &shard->slots[slot_idx];
    if (replay && replay->valid) {
        slot->replay            = *replay;
        slot->replay_generation = slot->generation;
    } else {
        slot->replay.valid = 0;
    }
    slot->type        = NFS4_SLOT_TYPE_FREE;
    slot->state       = NULL;
    slot->generation += 1;

    (void) shard_push_free_locked(shard, slot_idx);

    pthread_rwlock_unlock(&shard->lock);
} /* nfs_state_table_free_slot_replay */

/* Like nfs_state_table_free_slot, but for a slot whose owning client was
 * purged.  The generation is left unchanged so a stateid the client still
 * holds continues to resolve to this slot -- now marked EXPIRED so the lookup
 * returns NFS4ERR_EXPIRED.  The slot is placed on the free list and reverts to
 * a normal type (bumping the generation) when it is next allocated. */
void
nfs_state_table_expire_slot(
    struct nfs_state_table *table,
    uint8_t                 shard_idx,
    uint32_t                slot_idx)
{
    struct nfs_state_shard *shard = &table->shards[shard_idx];
    struct nfs_state_slot  *slot;

    pthread_rwlock_wrlock(&shard->lock);

    chimera_nfs_abort_if(slot_idx >= shard->slots_used,
                         "expire of unallocated slot %u/%u",
                         slot_idx, shard->slots_used);

    slot        = &shard->slots[slot_idx];
    slot->type  = NFS4_SLOT_TYPE_EXPIRED;
    slot->state = NULL;

    (void) shard_push_free_locked(shard, slot_idx);

    pthread_rwlock_unlock(&shard->lock);
} /* nfs_state_table_expire_slot */

/*
 * Internal lookup: under shard rdlock, validate slot identity and (when
 * `bump_refcount` is true) increment the state's refcount before unlocking.
 * Doing the refcount bump under the shard rdlock prevents the destroy path
 * (which takes the shard wrlock first) from racing the cleanup.
 */
static nfsstat4
state_table_lookup_locked(
    struct nfs_state_table *table,
    const struct stateid4  *sid,
    uint8_t                 want_type,
    bool                    bump_refcount,
    void                  **out_state,
    uint8_t                *out_type)
{
    struct nfs4_stateid_view view;
    struct nfs_state_shard  *shard;
    struct nfs_state_slot   *slot;
    nfsstat4                 status = NFS4ERR_BAD_STATEID;

    *out_state = NULL;
    if (out_type) {
        *out_type = NFS4_SLOT_TYPE_FREE;
    }

    /* The special stateids (all-zero anonymous, all-ones read-bypass) are not
     * resolvable concrete state -- callers that accept them handle them before
     * calling here, so reaching the table with one is a bad stateid.  Checked
     * before the epoch test so an all-zero stateid is not misread as a stale
     * (wrong-epoch) one. */
    if (nfs4_stateid_is_special(sid)) {
        return NFS4ERR_BAD_STATEID;
    }

    nfs4_stateid_decode(&view, sid);

    /* RFC 7530 §9.1.4.2: a stateid whose epoch does not match this server
     * instance was minted before a restart -- NFS4ERR_STALE_STATEID.  Checked
     * before the structural fields so a "rebooted" stateid (whose other bytes
     * are otherwise meaningless) is not misreported as merely bad. */
    if (view.epoch != table->epoch) {
        return NFS4ERR_STALE_STATEID;
    }
    if (view.version != NFS4_STATEID_VERSION) {
        return NFS4ERR_BAD_STATEID;
    }
    if (view.shard >= NFS_STATE_NUM_SHARDS) {
        return NFS4ERR_BAD_STATEID;
    }

    shard = &table->shards[view.shard];

    pthread_rwlock_rdlock(&shard->lock);

    if (view.slot_idx >= shard->slots_used) {
        pthread_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    slot = &shard->slots[view.slot_idx];

    if (slot->type == NFS4_SLOT_TYPE_EXPIRED) {
        /* The owning client was purged.  A stateid still naming this slot's
         * generation gets NFS4ERR_EXPIRED; an older generation is stale. */
        status = (slot->generation == view.generation)
                 ? NFS4ERR_EXPIRED : NFS4ERR_STALE_STATEID;
    } else if (slot->type == NFS4_SLOT_TYPE_FREE) {
        status = NFS4ERR_BAD_STATEID;
    } else if (slot->generation != view.generation) {
        status = NFS4ERR_STALE_STATEID;
    } else if (want_type != 0 && slot->type != want_type) {
        status = NFS4ERR_BAD_STATEID;
    } else if (slot->type == NFS4_SLOT_TYPE_DELEG &&
               atomic_load_explicit(
                   &((struct nfs_delegation *) slot->state)->revoked,
                   memory_order_acquire)) {
        /* A force-revoked delegation: the stateid is still resolvable (so
         * TEST_STATEID/FREE_STATEID can act on it) but any use of it as a
         * delegation reports NFS4ERR_DELEG_REVOKED (RFC 8881 §8.2.4). */
        status = NFS4ERR_DELEG_REVOKED;
    } else if (slot->state) {
        /* Courteous-server reclaim: if a conflicting acquirer reclaimed this
         * state's (courtesy) client, the client has lost its lease.  Its slots
         * are not torn down until the next lease sweep, so report EXPIRED here
         * rather than handing back state the client no longer owns. */
        struct nfs_client *owner_client = NULL;

        if (slot->type == NFS4_SLOT_TYPE_OPEN) {
            struct nfs_open_state *os = slot->state;
            owner_client = os->owner ? os->owner->client : NULL;
        } else if (slot->type == NFS4_SLOT_TYPE_LOCK) {
            struct nfs_lock_state *ls = slot->state;
            owner_client = ls->lock_owner ? ls->lock_owner->client : NULL;
        } else if (slot->type == NFS4_SLOT_TYPE_DELEG) {
            owner_client = ((struct nfs_delegation *) slot->state)->client;
        }

        if (owner_client &&
            atomic_load_explicit(&owner_client->reclaim_pending,
                                 memory_order_acquire)) {
            pthread_rwlock_unlock(&shard->lock);
            return NFS4ERR_EXPIRED;
        }

        *out_state = slot->state;
        if (out_type) {
            *out_type = slot->type;
        }
        if (bump_refcount) {
            if (slot->type == NFS4_SLOT_TYPE_OPEN) {
                atomic_fetch_add_explicit(
                    &((struct nfs_open_state *) slot->state)->refcount, 1,
                    memory_order_acq_rel);
            } else if (slot->type == NFS4_SLOT_TYPE_LOCK) {
                atomic_fetch_add_explicit(
                    &((struct nfs_lock_state *) slot->state)->refcount, 1,
                    memory_order_acq_rel);
            } else if (slot->type == NFS4_SLOT_TYPE_DELEG) {
                atomic_fetch_add_explicit(
                    &((struct nfs_delegation *) slot->state)->refcount, 1,
                    memory_order_acq_rel);
            }
        }
        status = NFS4_OK;
    } else {
        /* Slot resolves but no state pointer is installed yet (e.g. the
         * validate path, which only confirms the slot exists).  Return OK with
         * a NULL state and no refcount bump, exactly as before. */
        *out_state = NULL;
        if (out_type) {
            *out_type = slot->type;
        }
        status = NFS4_OK;
    }

    pthread_rwlock_unlock(&shard->lock);
    return status;
} /* state_table_lookup_locked */

nfsstat4
nfs_state_table_acquire(
    struct nfs_state_table *table,
    const struct stateid4  *sid,
    uint8_t                 want_type,
    void                  **out_state,
    uint8_t                *out_type)
{
    nfsstat4 status = state_table_lookup_locked(table, sid, want_type, true,
                                                out_state, out_type);

    if (status == NFS4_OK) {
        /* Phase 3: any successful state acquire renews the owning client's
         * lease.  Both state types reach their client through their owner. */
        if (*out_type == NFS4_SLOT_TYPE_OPEN) {
            struct nfs_open_state *os = *out_state;
            if (os && os->owner) {
                nfs_client_touch(os->owner->client);
            }
        } else if (*out_type == NFS4_SLOT_TYPE_LOCK) {
            struct nfs_lock_state *ls = *out_state;
            if (ls && ls->lock_owner) {
                nfs_client_touch(ls->lock_owner->client);
            }
        } else if (*out_type == NFS4_SLOT_TYPE_DELEG) {
            struct nfs_delegation *d = *out_state;
            if (d) {
                nfs_client_touch(d->client);
            }
        }
    }

    return status;
} /* nfs_state_table_acquire */

nfsstat4
nfs_state_table_lookup_replay(
    struct nfs_state_table   *table,
    const struct stateid4    *sid,
    uint32_t                  op,
    uint32_t                  seqid,
    struct nfs4_replay_cache *out_replay)
{
    struct nfs4_stateid_view view;
    struct nfs_state_shard  *shard;
    struct nfs_state_slot   *slot;

    nfs4_stateid_decode(&view, sid);
    if (view.epoch != table->epoch) {
        return NFS4ERR_STALE_STATEID;
    }
    if (view.version != NFS4_STATEID_VERSION ||
        view.shard >= NFS_STATE_NUM_SHARDS) {
        return NFS4ERR_BAD_STATEID;
    }

    shard = &table->shards[view.shard];
    pthread_rwlock_rdlock(&shard->lock);

    if (view.slot_idx >= shard->slots_used) {
        pthread_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    slot = &shard->slots[view.slot_idx];
    if (!slot->replay.valid ||
        slot->replay_generation != view.generation ||
        slot->replay.op != op ||
        slot->replay.seqid != seqid) {
        pthread_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    *out_replay = slot->replay;
    pthread_rwlock_unlock(&shard->lock);
    return NFS4_OK;
} /* nfs_state_table_lookup_replay */

/* Forward decls for cleanup paths used by release. */
static void
open_state_cleanup(
    struct nfs_open_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread);
static void
lock_state_cleanup(
    struct nfs_lock_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread);
static void
delegation_cleanup(
    struct nfs_delegation     *deleg,
    struct chimera_vfs_thread *vfs_thread);
static void
delegation_destroy_common(
    struct nfs_delegation     *deleg,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread,
    bool                       expire,
    bool                       unlink_client);

void
nfs_state_table_release(
    struct nfs_state_table    *table,
    void                      *state,
    uint8_t                    type,
    struct chimera_vfs_thread *vfs_thread)
{
    uint32_t prev;

    if (type == NFS4_SLOT_TYPE_OPEN) {
        struct nfs_open_state *s = state;
        prev = atomic_fetch_sub_explicit(&s->refcount, 1,
                                         memory_order_acq_rel);
        chimera_nfs_abort_if(prev == 0,
                             "open_state refcount underflow on %p", s);
        if (prev == 1 && atomic_load_explicit(&s->destroyed,
                                              memory_order_acquire)) {
            open_state_cleanup(s, table, vfs_thread);
        }
    } else if (type == NFS4_SLOT_TYPE_LOCK) {
        struct nfs_lock_state *s = state;
        prev = atomic_fetch_sub_explicit(&s->refcount, 1,
                                         memory_order_acq_rel);
        chimera_nfs_abort_if(prev == 0,
                             "lock_state refcount underflow on %p", s);
        if (prev == 1 && atomic_load_explicit(&s->destroyed,
                                              memory_order_acquire)) {
            lock_state_cleanup(s, table, vfs_thread);
        }
    } else if (type == NFS4_SLOT_TYPE_DELEG) {
        struct nfs_delegation *d = state;
        prev = atomic_fetch_sub_explicit(&d->refcount, 1,
                                         memory_order_acq_rel);
        chimera_nfs_abort_if(prev == 0,
                             "delegation refcount underflow on %p", d);
        if (prev == 1 && atomic_load_explicit(&d->destroyed,
                                              memory_order_acquire)) {
            delegation_cleanup(d, vfs_thread);
        }
    } else if (type == NFS4_SLOT_TYPE_LAYOUT) {
        struct nfs_layout_state *s = state;
        prev = atomic_fetch_sub_explicit(&s->refcount, 1,
                                         memory_order_acq_rel);
        chimera_nfs_abort_if(prev == 0,
                             "layout_state refcount underflow on %p", s);
        if (prev == 1 && atomic_load_explicit(&s->destroyed,
                                              memory_order_acquire)) {
            free(s);
        }
    }
} /* nfs_state_table_release */

nfsstat4
nfs_state_table_validate(
    struct nfs_state_table *table,
    const struct stateid4  *sid)
{
    void   *unused_state;
    uint8_t unused_type;

    return state_table_lookup_locked(table, sid, 0, false,
                                     &unused_state, &unused_type);
} /* nfs_state_table_validate */

/* ---------------------------------------------------------------------- *
*  Lifecycle: client / open_owner / open_state / lock_owner / lock_state
* ---------------------------------------------------------------------- */

struct nfs_client *
nfs_client_alloc(
    uint64_t    client_id,
    const void *owner_string,
    uint16_t    owner_len,
    uint64_t    verifier,
    uint8_t     minor)
{
    struct nfs_client *c = calloc(1, sizeof(*c));

    chimera_nfs_abort_if(c == NULL, "nfs_client_alloc OOM");

    c->client_id     = client_id;
    c->verifier      = verifier;
    c->minor         = minor;
    c->last_touch_ns = nfs_lease_now_ns();
    if (owner_len > NFS4_OPAQUE_LIMIT) {
        owner_len = NFS4_OPAQUE_LIMIT;
    }
    memcpy(c->owner_string, owner_string, owner_len);
    c->owner_len = owner_len;
    atomic_init(&c->refcount, 1);
    pthread_mutex_init(&c->lock, NULL);
    return c;
} /* nfs_client_alloc */

/* Destroy a single open_state (unhash + unlink locks + free slot + release
 * lifetime ref).  Caller holds owner->lock or is in single-threaded teardown.
 *
 * Idempotent: gated by an atomic CAS on `destroyed`.  A losing CAS returns
 * without touching state, so concurrent destroyers (e.g. two CLOSEs on the
 * same stateid, or DESTROY_CLIENTID racing CLOSE) are safe. */
static void
open_state_destroy_locked(
    struct nfs_open_owner     *owner,
    struct nfs_open_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread,
    bool                       expire)
{
    uint8_t prev_destroyed;

    prev_destroyed = atomic_exchange_explicit(&state->destroyed, 1,
                                              memory_order_acq_rel);
    if (prev_destroyed != 0) {
        return;
    }

    /* Unhash from the owner so no new acquire can find it through the
     * owner walk. */
    HASH_DEL(owner->states_by_fh, state);

    /* Tear down every lock_state still rooted on this open_state.  Each
     * lock holds a distinct dup'd handle; its own cleanup will release it. */
    while (state->locks) {
        struct nfs_lock_state *ls = state->locks;
        uint8_t                ls_prev_destroyed;

        LL_DELETE2(state->locks, ls, next_in_open);
        if (ls->lock_owner) {
            LL_DELETE2(ls->lock_owner->states, ls, next_in_owner);
        }
        ls_prev_destroyed = atomic_exchange_explicit(&ls->destroyed, 1,
                                                     memory_order_acq_rel);
        if (ls_prev_destroyed == 0) {
            if (expire) {
                nfs_state_table_expire_slot(table, ls->shard, ls->slot_idx);
            } else {
                nfs_state_table_free_slot(table, ls->shard, ls->slot_idx);
            }
            uint32_t lprev = atomic_fetch_sub_explicit(&ls->refcount, 1,
                                                       memory_order_acq_rel);
            if (lprev == 1) {
                lock_state_cleanup(ls, table, vfs_thread);
            }
        }
    }

    if (expire) {
        nfs_state_table_expire_slot(table, state->shard, state->slot_idx);
    } else if (owner->replay.valid && owner->replay.op == OP_CLOSE) {
        nfs_state_table_free_slot_replay(table, state->shard, state->slot_idx,
                                         &owner->replay);
    } else {
        nfs_state_table_free_slot(table, state->shard, state->slot_idx);
    }

    uint32_t prev = atomic_fetch_sub_explicit(&state->refcount, 1,
                                              memory_order_acq_rel);
    if (prev == 1) {
        open_state_cleanup(state, table, vfs_thread);
    }
} /* open_state_destroy_locked */

static void
layout_state_destroy_locked(
    struct nfs_client         *client,
    struct nfs_layout_state   *st,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread);

void
nfs_client_destroy(
    struct nfs_client         *client,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread,
    bool                       synchronous)
{
    if (!client) {
        return;
    }

    pthread_mutex_lock(&client->lock);

    /* HASH_ITER + HASH_DELETE + free is the standard uthash teardown
     * pattern, but scan-build can't reason through the macro expansion
     * and flags a (false-positive) UAF on the bucket pointer.  Match
     * the rest of the codebase by hiding the walks from the analyzer. */
#ifndef __clang_analyzer__
    struct nfs_open_owner *oo, *oo_tmp;
    struct nfs_lock_owner *lo, *lo_tmp;

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        pthread_mutex_lock(&oo->lock);
        struct nfs_open_state *os, *os_tmp;
        HASH_ITER(hh, oo->states_by_fh, os, os_tmp)
        {
            /* The whole client is going away (lease expiry / reboot /
             * DESTROY_CLIENTID); mark its stateids EXPIRED, not free. */
            open_state_destroy_locked(oo, os, table, vfs_thread, true);
        }
        pthread_mutex_unlock(&oo->lock);

        HASH_DELETE(hh, client->open_owners_by_str, oo);
        pthread_mutex_destroy(&oo->lock);
        free(oo);
    }

    HASH_ITER(hh, client->lock_owners_by_str, lo, lo_tmp)
    {
        /* All states should have been torn down via the open_states above,
         * since each lock_state is on both lists. */
        chimera_nfs_abort_if(lo->states != NULL,
                             "lock_owner %p has leftover lock_states", lo);
        HASH_DELETE(hh, client->lock_owners_by_str, lo);
        pthread_mutex_destroy(&lo->lock);
        free(lo);
    }

    /* pNFS layouts held by the client (NFSv4.1+).  No recall is performed in
     * v1; the records simply drop with the client's lease. */
    struct nfs_layout_state *ly, *ly_tmp;
    HASH_ITER(hh, client->layouts_by_fh, ly, ly_tmp)
    {
        layout_state_destroy_locked(client, ly, table, vfs_thread);
    }
#endif /* ifndef __clang_analyzer__ */

    /* Revoke every outstanding delegation.  Slots are marked EXPIRED (the
     * client is going away) so a stateid still naming one resolves to
     * NFS4ERR_EXPIRED.  We already hold client->lock, so destroy with
     * unlink_client=false and splice the list head ourselves. */
    while (client->delegations) {
        struct nfs_delegation *deleg = client->delegations;
        LL_DELETE2(client->delegations, deleg, next_in_client);
        delegation_destroy_common(deleg, table, vfs_thread, true, false);
    }

    /* Free the delegation callback channel (and disconnect its outbound 4.0
     * connection) before the client struct goes away. */
    nfs4_cb_path_teardown(&client->cb_path, synchronous);

    pthread_mutex_unlock(&client->lock);
    pthread_mutex_destroy(&client->lock);
    free(client);
} /* nfs_client_destroy */

void
nfs_client_expire_state(
    struct nfs_client         *client,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    if (!client) {
        return;
    }

    pthread_mutex_lock(&client->lock);
    client->expired = 1;

#ifndef __clang_analyzer__
    struct nfs_open_owner *oo, *oo_tmp;
    struct nfs_lock_owner *lo, *lo_tmp;

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        pthread_mutex_lock(&oo->lock);
        struct nfs_open_state *os, *os_tmp;
        HASH_ITER(hh, oo->states_by_fh, os, os_tmp)
        {
            open_state_destroy_locked(oo, os, table, vfs_thread, true);
        }
        pthread_mutex_unlock(&oo->lock);

        HASH_DELETE(hh, client->open_owners_by_str, oo);
        pthread_mutex_destroy(&oo->lock);
        free(oo);
    }

    HASH_ITER(hh, client->lock_owners_by_str, lo, lo_tmp)
    {
        chimera_nfs_abort_if(lo->states != NULL,
                             "lock_owner %p has leftover lock_states", lo);
        HASH_DELETE(hh, client->lock_owners_by_str, lo);
        pthread_mutex_destroy(&lo->lock);
        free(lo);
    }

    struct nfs_layout_state *ly, *ly_tmp;
    HASH_ITER(hh, client->layouts_by_fh, ly, ly_tmp)
    {
        layout_state_destroy_locked(client, ly, table, vfs_thread);
    }
#endif /* ifndef __clang_analyzer__ */

    while (client->delegations) {
        struct nfs_delegation *deleg = client->delegations;
        LL_DELETE2(client->delegations, deleg, next_in_client);
        delegation_destroy_common(deleg, table, vfs_thread, true, false);
    }

    /* Runtime lease expiry: the owner thread is still alive, so marshal the
     * channel free to it rather than freeing in-line. */
    nfs4_cb_path_teardown(&client->cb_path, false);
    pthread_mutex_unlock(&client->lock);
} /* nfs_client_expire_state */

struct nfs_open_owner *
nfs_open_owner_find_or_create(
    struct nfs_client *client,
    const void        *owner_bytes,
    uint16_t           owner_len,
    bool              *out_created)
{
    struct nfs_open_owner *owner;

    if (owner_len > NFS4_OPAQUE_LIMIT) {
        owner_len = NFS4_OPAQUE_LIMIT;
    }

    pthread_mutex_lock(&client->lock);

    HASH_FIND(hh, client->open_owners_by_str, owner_bytes, owner_len, owner);

    if (owner) {
        if (out_created) {
            *out_created = false;
        }
        pthread_mutex_unlock(&client->lock);
        return owner;
    }

    owner = calloc(1, sizeof(*owner));
    chimera_nfs_abort_if(owner == NULL, "open_owner alloc OOM");
    owner->client = client;
    memcpy(owner->owner, owner_bytes, owner_len);
    owner->owner_len = owner_len;
    owner->seqid     = 0;
    owner->confirmed = false;
    pthread_mutex_init(&owner->lock, NULL);

    HASH_ADD_KEYPTR(hh, client->open_owners_by_str,
                    owner->owner, owner->owner_len, owner);

    if (out_created) {
        *out_created = true;
    }
    pthread_mutex_unlock(&client->lock);
    return owner;
} /* nfs_open_owner_find_or_create */

struct nfs_open_state *
nfs_open_owner_find_state(
    struct nfs_open_owner *owner,
    const uint8_t         *fh,
    uint16_t               fh_len)
{
    struct nfs_open_state *state = NULL;

    if (fh_len > NFS4_FHSIZE) {
        return NULL;
    }

    pthread_mutex_lock(&owner->lock);
    HASH_FIND(hh, owner->states_by_fh, fh, fh_len, state);
    pthread_mutex_unlock(&owner->lock);
    return state;
} /* nfs_open_owner_find_state */

static uint32_t
nfs_open_share_history(uint32_t share)
{
    share &= (OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WRITE);
    return share == (OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WRITE) ?
           0x04 : share;
} /* nfs_open_share_history */

struct nfs_open_state *
nfs_open_state_create(
    struct nfs_open_owner          *owner,
    uint32_t                        principal_flavor,
    const char                     *principal_machinename,
    uint32_t                        principal_machinename_len,
    const uint8_t                  *fh,
    uint16_t                        fh_len,
    uint32_t                        share_access,
    uint32_t                        share_deny,
    struct chimera_vfs_open_handle *handle_dup,
    struct nfs_state_table         *table,
    struct stateid4                *out_stateid)
{
    struct nfs_open_state *state;
    uint8_t                shard;
    uint32_t               slot_idx, gen;
    int                    rc;

    chimera_nfs_abort_if(fh_len > NFS4_FHSIZE,
                         "open_state_create fh_len %u", fh_len);

    state = calloc(1, sizeof(*state));
    chimera_nfs_abort_if(state == NULL, "open_state alloc OOM");

    rc = nfs_state_table_alloc(table, NFS4_SLOT_TYPE_OPEN, &shard, &slot_idx, &gen);
    chimera_nfs_abort_if(rc != 0, "state table exhausted");

    state->owner            = owner;
    state->principal_flavor = principal_flavor;
    if (principal_machinename_len > NFS4_OPAQUE_LIMIT) {
        principal_machinename_len = NFS4_OPAQUE_LIMIT;
    }
    state->principal_machinename_len = principal_machinename_len;
    if (principal_machinename && principal_machinename_len) {
        memcpy(state->principal_machinename, principal_machinename,
               principal_machinename_len);
    }
    memcpy(state->fh, fh, fh_len);
    state->fh_len            = fh_len;
    state->share_access      = share_access;
    state->share_deny        = share_deny;
    state->share_access_hist = nfs_open_share_history(share_access);
    state->share_deny_hist   = nfs_open_share_history(share_deny);
    state->seqid             = 1;
    state->shard             = shard;
    state->slot_idx          = slot_idx;
    state->generation        = gen;
    state->handle            = handle_dup;
    state->locks             = NULL;
    atomic_init(&state->refcount, 1);
    atomic_init(&state->destroyed, 0);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_OPEN, state);

    pthread_mutex_lock(&owner->lock);
    HASH_ADD_KEYPTR(hh, owner->states_by_fh, state->fh, state->fh_len, state);
    pthread_mutex_unlock(&owner->lock);

    nfs4_stateid_encode(out_stateid, state->seqid,
                        NFS4_STATEID_TYPE_OPEN, shard, slot_idx, gen,
                        table->epoch);
    return state;
} /* nfs_open_state_create */

nfsstat4
nfs_client_check_share_conflict(
    struct nfs_client     *client,
    struct nfs_open_owner *requesting_owner,
    const uint8_t         *fh,
    uint16_t               fh_len,
    uint32_t               requested_access,
    uint32_t               requested_deny)
{
    struct nfs_open_owner *oo, *oo_tmp;
    nfsstat4               status = NFS4_OK;

    if (fh_len > NFS4_FHSIZE) {
        return NFS4ERR_BAD_STATEID;
    }

    pthread_mutex_lock(&client->lock);

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        struct nfs_open_state *peer = NULL;

        if (oo == requesting_owner) {
            /* Same-owner OPEN merges via coalesce; no conflict possible. */
            continue;
        }

        pthread_mutex_lock(&oo->lock);
        HASH_FIND(hh, oo->states_by_fh, fh, fh_len, peer);
        if (peer) {
            /* RFC 7530 §9.10: SHARE_DENIED if my deny clashes with their
             * access OR their deny clashes with my access. */
            if ((peer->share_access & requested_deny) ||
                (requested_access & peer->share_deny)) {
                status = NFS4ERR_SHARE_DENIED;
            }
        }
        pthread_mutex_unlock(&oo->lock);

        if (status != NFS4_OK) {
            break;
        }
    }

    pthread_mutex_unlock(&client->lock);
    return status;
} /* nfs_client_check_share_conflict */

nfsstat4
nfs_client_check_io_denied(
    struct nfs_client     *client,
    struct nfs_open_owner *requesting_owner,
    const uint8_t         *fh,
    uint16_t               fh_len,
    uint32_t               requested_access)
{
    struct nfs_open_owner *oo, *oo_tmp;
    nfsstat4               status = NFS4_OK;

    if (fh_len > NFS4_FHSIZE) {
        return NFS4ERR_BAD_STATEID;
    }

    pthread_mutex_lock(&client->lock);

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        struct nfs_open_state *peer = NULL;

        if (oo == requesting_owner) {
            continue;
        }

        pthread_mutex_lock(&oo->lock);
        HASH_FIND(hh, oo->states_by_fh, fh, fh_len, peer);
        if (peer && (peer->share_deny & requested_access)) {
            status = NFS4ERR_LOCKED;
        }
        pthread_mutex_unlock(&oo->lock);

        if (status != NFS4_OK) {
            break;
        }
    }

    pthread_mutex_unlock(&client->lock);
    return status;
} /* nfs_client_check_io_denied */

nfsstat4
nfs_open_state_check_io_denied(
    struct nfs_open_state *requesting_state,
    uint32_t               requested_access)
{
    return nfs_client_check_io_denied(requesting_state->owner->client,
                                      requesting_state->owner,
                                      requesting_state->fh,
                                      requesting_state->fh_len,
                                      requested_access);
} /* nfs_open_state_check_io_denied */

void
nfs_open_state_coalesce(
    struct nfs_open_state  *state,
    uint32_t                share_access,
    uint32_t                share_deny,
    struct nfs_state_table *table,
    struct stateid4        *out_stateid)
{
    state->share_access      |= share_access;
    state->share_deny        |= share_deny;
    state->share_access_hist |= nfs_open_share_history(share_access);
    state->share_deny_hist   |= nfs_open_share_history(share_deny);
    state->seqid             += 1;

    nfs4_stateid_encode(out_stateid, state->seqid,
                        NFS4_STATEID_TYPE_OPEN,
                        state->shard, state->slot_idx, state->generation,
                        table->epoch);
} /* nfs_open_state_coalesce */

static void
open_state_cleanup(
    struct nfs_open_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct chimera_vfs_state *vfs_state = vfs_thread ? vfs_thread->vfs->vfs_state : NULL;

    (void) table;

    /* Release the cross-protocol SHARE reservation, if held. */
    if (vfs_state && state->share_lease_held) {
        chimera_vfs_lease_release(vfs_state, state->share_file_state,
                                  &state->share_lease);
        state->share_lease_held = false;
    }
    if (vfs_state && state->share_file_state) {
        chimera_vfs_state_put(vfs_state, state->share_file_state);
        state->share_file_state = NULL;
    }

    if (state->handle && vfs_thread) {
        chimera_vfs_release(vfs_thread, state->handle);
        state->handle = NULL;
    }
    free(state);
} /* open_state_cleanup */

void
nfs_open_state_destroy(
    struct nfs_open_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_open_owner *owner = state->owner;

    pthread_mutex_lock(&owner->lock);
    /* A single open being closed/rolled back -- its stateid becomes invalid
     * (free), not expired. */
    open_state_destroy_locked(owner, state, table, vfs_thread, false);
    pthread_mutex_unlock(&owner->lock);
} /* nfs_open_state_destroy */

/* --- pNFS layout state ------------------------------------------------- */

struct nfs_layout_state *
nfs_layout_state_find(
    struct nfs_client *client,
    const uint8_t     *fh,
    uint16_t           fh_len)
{
    struct nfs_layout_state *st;

    pthread_mutex_lock(&client->lock);
    HASH_FIND(hh, client->layouts_by_fh, fh, fh_len, st);
    pthread_mutex_unlock(&client->lock);

    return st;
} /* nfs_layout_state_find */

struct nfs_layout_state *
nfs_layout_state_create(
    struct nfs_client       *client,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    uint32_t                 iomode,
    uint32_t                 client_short_id,
    struct nfs_state_table  *table,
    struct nfs_layout_table *layout_table,
    struct stateid4         *out_stateid)
{
    struct nfs_layout_state *st;
    uint8_t                  shard;
    uint32_t                 slot_idx, gen;
    int                      rc;

    chimera_nfs_abort_if(fh_len > NFS4_FHSIZE,
                         "layout_state_create fh_len %u", fh_len);

    st = calloc(1, sizeof(*st));
    chimera_nfs_abort_if(st == NULL, "layout_state alloc OOM");

    rc = nfs_state_table_alloc(table, NFS4_SLOT_TYPE_LAYOUT, &shard, &slot_idx, &gen);
    chimera_nfs_abort_if(rc != 0, "state table exhausted");

    st->client = client;
    memcpy(st->fh, fh, fh_len);
    st->fh_len       = fh_len;
    st->seqid        = 1;
    st->iomode       = iomode;
    st->shard        = shard;
    st->slot_idx     = slot_idx;
    st->generation   = gen;
    st->global_table = layout_table;
    atomic_init(&st->refcount, 1);
    atomic_init(&st->destroyed, 0);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_LAYOUT, st);

    pthread_mutex_lock(&client->lock);
    HASH_ADD_KEYPTR(hh, client->layouts_by_fh, st->fh, st->fh_len, st);
    pthread_mutex_unlock(&client->lock);

    /* Publish into the server-wide fh->holder index so a conflicting op from
     * any client can find and recall this layout. */
    nfs_layout_table_register(layout_table, st);

    nfs4_stateid_encode(out_stateid, st->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        shard, slot_idx, gen, client_short_id);
    return st;
} /* nfs_layout_state_create */

void
nfs_layout_state_bump(
    struct nfs_layout_state *st,
    uint32_t                 client_short_id,
    struct stateid4         *out_stateid)
{
    /* RFC 8881 §12.5.3: the server owns the layout stateid seqid and advances
     * it on each successful LAYOUTGET / LAYOUTRETURN. */
    st->seqid++;
    nfs4_stateid_encode(out_stateid, st->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        st->shard, st->slot_idx, st->generation,
                        client_short_id);
} /* nfs_layout_state_bump */

static void
layout_state_destroy_locked(
    struct nfs_client         *client,
    struct nfs_layout_state   *st,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    uint8_t  prev_destroyed;
    uint32_t prev;

    (void) vfs_thread;

    prev_destroyed = atomic_exchange_explicit(&st->destroyed, 1,
                                              memory_order_acq_rel);
    if (prev_destroyed != 0) {
        return;
    }

    /* Drop from the server-wide fh->holder index; if this was the last holder
     * of the file, that resumes any operation deferred on its recall. */
    if (st->global_table) {
        nfs_layout_table_deregister(st->global_table, st);
    }

    HASH_DEL(client->layouts_by_fh, st);

    nfs_state_table_free_slot(table, st->shard, st->slot_idx);

    prev = atomic_fetch_sub_explicit(&st->refcount, 1, memory_order_acq_rel);
    if (prev == 1) {
        free(st);
    }
} /* layout_state_destroy_locked */

void
nfs_layout_state_get(struct nfs_layout_state *st)
{
    atomic_fetch_add_explicit(&st->refcount, 1, memory_order_acq_rel);
} /* nfs_layout_state_get */

void
nfs_layout_state_put(struct nfs_layout_state *st)
{
    if (atomic_fetch_sub_explicit(&st->refcount, 1, memory_order_acq_rel) == 1) {
        free(st);
    }
} /* nfs_layout_state_put */

void
nfs_layout_state_destroy(
    struct nfs_layout_state   *st,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_client *client = st->client;

    pthread_mutex_lock(&client->lock);
    layout_state_destroy_locked(client, st, table, vfs_thread);
    pthread_mutex_unlock(&client->lock);
} /* nfs_layout_state_destroy */

struct nfs_lock_owner *
nfs_lock_owner_find_or_create(
    struct nfs_client *client,
    const void        *owner_bytes,
    uint16_t           owner_len,
    bool              *out_created)
{
    struct nfs_lock_owner *owner;

    if (owner_len > NFS4_OPAQUE_LIMIT) {
        owner_len = NFS4_OPAQUE_LIMIT;
    }

    pthread_mutex_lock(&client->lock);

    HASH_FIND(hh, client->lock_owners_by_str, owner_bytes, owner_len, owner);

    if (owner) {
        if (out_created) {
            *out_created = false;
        }
        pthread_mutex_unlock(&client->lock);
        return owner;
    }

    owner = calloc(1, sizeof(*owner));
    chimera_nfs_abort_if(owner == NULL, "lock_owner alloc OOM");
    owner->client = client;
    memcpy(owner->owner, owner_bytes, owner_len);
    owner->owner_len = owner_len;
    owner->seqid     = 0;
    pthread_mutex_init(&owner->lock, NULL);

    HASH_ADD_KEYPTR(hh, client->lock_owners_by_str,
                    owner->owner, owner->owner_len, owner);

    if (out_created) {
        *out_created = true;
    }
    pthread_mutex_unlock(&client->lock);
    return owner;
} /* nfs_lock_owner_find_or_create */

struct nfs_lock_state *
nfs_lock_state_create(
    struct nfs_lock_owner          *lock_owner,
    struct nfs_open_state          *open_state,
    struct chimera_vfs_open_handle *handle_dup,
    struct nfs_state_table         *table,
    struct stateid4                *out_stateid)
{
    struct nfs_lock_state *state;
    uint8_t                shard;
    uint32_t               slot_idx, gen;
    int                    rc;

    state = calloc(1, sizeof(*state));
    chimera_nfs_abort_if(state == NULL, "lock_state alloc OOM");

    rc = nfs_state_table_alloc(table, NFS4_SLOT_TYPE_LOCK, &shard, &slot_idx, &gen);
    chimera_nfs_abort_if(rc != 0, "state table exhausted");

    state->lock_owner = lock_owner;
    state->open_state = open_state;
    state->seqid      = 1;
    state->shard      = shard;
    state->slot_idx   = slot_idx;
    state->generation = gen;
    state->handle     = handle_dup;
    atomic_init(&state->refcount, 1);
    atomic_init(&state->destroyed, 0);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_LOCK, state);

    /* Link onto both the lock_owner's list and the open_state's list. */
    pthread_mutex_lock(&lock_owner->lock);
    LL_PREPEND2(lock_owner->states, state, next_in_owner);
    pthread_mutex_unlock(&lock_owner->lock);

    pthread_mutex_lock(&open_state->owner->lock);
    LL_PREPEND2(open_state->locks, state, next_in_open);
    pthread_mutex_unlock(&open_state->owner->lock);

    nfs4_stateid_encode(out_stateid, state->seqid,
                        NFS4_STATEID_TYPE_LOCK, shard, slot_idx, gen,
                        table->epoch);
    return state;
} /* nfs_lock_state_create */

/* Insert a byte-range lock interval [start, end) (end == UINT64_MAX => to EOF)
 * of `mode` for `owner` on `file_state` (the ref's ownership transfers to the
 * returned entry), and link it onto lock_state->range_leases.  The interval is
 * always a sub-region or coalesced union of locks this owner already
 * coordinated, so the synchronous insert grants without conflict. */
struct nfs4_range_lease *
nfs4_range_lease_insert(
    struct chimera_vfs_state             *vfs_state,
    struct nfs_lock_state                *lock_state,
    struct chimera_vfs_file_state        *file_state,
    const struct chimera_vfs_lease_owner *owner,
    uint8_t                               mode,
    uint64_t                              start,
    uint64_t                              end)
{
    struct nfs4_range_lease *rl = calloc(1, sizeof(*rl));

    if (!rl) {
        chimera_vfs_state_put(vfs_state, file_state);
        return NULL;
    }

    rl->file_state         = file_state;
    rl->lease.kind         = CHIMERA_VFS_LEASE_RANGE;
    rl->lease.mode.granted = mode;
    rl->lease.mode.denied  = 0;
    rl->lease.owner        = *owner;
    rl->lease.offset       = start;
    /* The VFS range layer represents to-EOF as UINT64_MAX (length 0 is a
     * genuine zero-byte range), matching the [start, end) sentinel here. */
    rl->lease.length = (end == UINT64_MAX) ? UINT64_MAX : (end - start);

    chimera_vfs_state_try_insert(vfs_state, file_state, &rl->lease, NULL);

    rl->next                 = lock_state->range_leases;
    lock_state->range_leases = rl;
    return rl;
} /* nfs4_range_lease_insert */

/* Release the vfs_state lease backing `rl` and free it.  Caller has already
 * unlinked it from lock_state->range_leases. */
void
nfs4_range_lease_free(
    struct chimera_vfs_state *vfs_state,
    struct nfs4_range_lease  *rl)
{
    chimera_vfs_lease_release(vfs_state, rl->file_state, &rl->lease);
    chimera_vfs_state_put(vfs_state, rl->file_state);
    free(rl);
} /* nfs4_range_lease_free */

static void
lock_state_cleanup(
    struct nfs_lock_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct chimera_vfs_state *vfs_state = vfs_thread ? vfs_thread->vfs->vfs_state : NULL;
    struct nfs4_range_lease  *rl, *tmp;

    (void) table;

    /* Drain any byte-range leases still held (ranges not explicitly
     * released by LOCKU before CLOSE / lock-owner teardown). */
    rl                  = state->range_leases;
    state->range_leases = NULL;
    while (rl) {
        tmp = rl->next;
        if (vfs_state) {
            chimera_vfs_lease_release(vfs_state, rl->file_state, &rl->lease);
            chimera_vfs_state_put(vfs_state, rl->file_state);
        }
        free(rl);
        rl = tmp;
    }

    if (state->handle && vfs_thread) {
        chimera_vfs_release(vfs_thread, state->handle);
        state->handle = NULL;
    }
    free(state);
} /* lock_state_cleanup */

void
nfs_lock_state_destroy(
    struct nfs_lock_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_lock_owner *lock_owner = state->lock_owner;
    struct nfs_open_state *open_state = state->open_state;
    uint8_t                prev_destroyed;

    prev_destroyed = atomic_exchange_explicit(&state->destroyed, 1,
                                              memory_order_acq_rel);
    if (prev_destroyed != 0) {
        return;
    }

    pthread_mutex_lock(&lock_owner->lock);
    LL_DELETE2(lock_owner->states, state, next_in_owner);
    pthread_mutex_unlock(&lock_owner->lock);

    if (open_state) {
        pthread_mutex_lock(&open_state->owner->lock);
        LL_DELETE2(open_state->locks, state, next_in_open);
        pthread_mutex_unlock(&open_state->owner->lock);
    }

    nfs_state_table_free_slot(table, state->shard, state->slot_idx);

    uint32_t prev = atomic_fetch_sub_explicit(&state->refcount, 1,
                                              memory_order_acq_rel);
    if (prev == 1) {
        lock_state_cleanup(state, table, vfs_thread);
    }
} /* nfs_lock_state_destroy */

/* ---------------------------------------------------------------------- *
*  Lifecycle: delegations
* ---------------------------------------------------------------------- */

struct nfs_delegation *
nfs_delegation_create(
    struct nfs_client      *client,
    uint8_t                 deleg_type,
    const uint8_t          *fh,
    uint16_t                fh_len,
    uint64_t                fh_hash,
    struct nfs_state_table *table,
    struct stateid4        *out_stateid)
{
    struct nfs_delegation *deleg;
    uint8_t                shard;
    uint32_t               slot_idx, gen;
    int                    rc;

    chimera_nfs_abort_if(fh_len > NFS4_FHSIZE,
                         "delegation_create fh_len %u", fh_len);

    deleg = calloc(1, sizeof(*deleg));
    chimera_nfs_abort_if(deleg == NULL, "delegation alloc OOM");

    rc = nfs_state_table_alloc(table, NFS4_SLOT_TYPE_DELEG, &shard, &slot_idx, &gen);
    chimera_nfs_abort_if(rc != 0, "state table exhausted");

    deleg->client = client;
    deleg->type   = deleg_type;
    memcpy(deleg->fh, fh, fh_len);
    deleg->fh_len     = fh_len;
    deleg->fh_hash    = fh_hash;
    deleg->shard      = shard;
    deleg->slot_idx   = slot_idx;
    deleg->generation = gen;
    deleg->seqid      = 1;
    deleg->lease_held = false;
    atomic_init(&deleg->cb_recall_state, NFS4_DELEG_ACTIVE);
    atomic_init(&deleg->cb_recall_retries, 0);
    atomic_init(&deleg->refcount, 1);
    atomic_init(&deleg->destroyed, 0);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_DELEG, deleg);

    pthread_mutex_lock(&client->lock);
    LL_PREPEND2(client->delegations, deleg, next_in_client);
    pthread_mutex_unlock(&client->lock);

    nfs4_stateid_encode(out_stateid, deleg->seqid,
                        NFS4_STATEID_TYPE_DELEG, shard, slot_idx, gen,
                        table->epoch);
    return deleg;
} /* nfs_delegation_create */

static void
delegation_cleanup(
    struct nfs_delegation     *deleg,
    struct chimera_vfs_thread *vfs_thread)
{
    struct chimera_vfs_state *vfs_state =
        vfs_thread ? vfs_thread->vfs->vfs_state : NULL;

    if (vfs_state && deleg->lease_held) {
        chimera_vfs_lease_release(vfs_state, deleg->file_state, &deleg->lease);
        deleg->lease_held = false;
    }
    if (vfs_state && deleg->file_state) {
        chimera_vfs_state_put(vfs_state, deleg->file_state);
        deleg->file_state = NULL;
    }
    free(deleg);
} /* delegation_cleanup */

/* Mark a delegation for destruction and drop its lifetime ref.  Unlinks it
 * from the owning client first so no new lookup finds it.  `expire` marks the
 * slot EXPIRED (client purge) rather than FREE.  Idempotent via the destroyed
 * CAS. */
static void
delegation_destroy_common(
    struct nfs_delegation     *deleg,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread,
    bool                       expire,
    bool                       unlink_client)
{
    uint8_t  prev_destroyed;
    uint32_t prev;

    prev_destroyed = atomic_exchange_explicit(&deleg->destroyed, 1,
                                              memory_order_acq_rel);
    if (prev_destroyed != 0) {
        return;
    }

    /* If this delegation was force-revoked, drop it from the client's
    * revoked count so SEQ4_STATUS_RECALLABLE_STATE_REVOKED clears. */
    if (deleg->client &&
        atomic_load_explicit(&deleg->revoked, memory_order_acquire)) {
        atomic_fetch_sub_explicit(&deleg->client->revoked_deleg_count, 1,
                                  memory_order_acq_rel);
    }

    if (unlink_client && deleg->client) {
        pthread_mutex_lock(&deleg->client->lock);
        LL_DELETE2(deleg->client->delegations, deleg, next_in_client);
        pthread_mutex_unlock(&deleg->client->lock);
    }

    if (expire) {
        nfs_state_table_expire_slot(table, deleg->shard, deleg->slot_idx);
    } else {
        nfs_state_table_free_slot(table, deleg->shard, deleg->slot_idx);
    }

    prev = atomic_fetch_sub_explicit(&deleg->refcount, 1, memory_order_acq_rel);
    if (prev == 1) {
        delegation_cleanup(deleg, vfs_thread);
    }
} /* delegation_destroy_common */

void
nfs_delegation_destroy(
    struct nfs_delegation     *deleg,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    delegation_destroy_common(deleg, table, vfs_thread, false, true);
} /* nfs_delegation_destroy */

/* vfs_state revoked_cb (wired onto every delegation lease).  Marks the
 * delegation revoked so its stateid reports NFS4ERR_DELEG_REVOKED, and bumps
 * the owning client's revoked count (drives SEQ4_STATUS_RECALLABLE_STATE_
 * REVOKED).  Idempotent. */
SYMBOL_EXPORT void
nfs_delegation_revoked_cb(
    struct chimera_vfs_lease *lease,
    void                     *private_data)
{
    struct nfs_delegation *deleg    = private_data;
    uint8_t                expected = 0;

    (void) lease;

    if (atomic_load_explicit(&deleg->destroyed, memory_order_acquire)) {
        return;
    }
    if (atomic_compare_exchange_strong_explicit(
            &deleg->revoked, &expected, 1,
            memory_order_acq_rel, memory_order_acquire)) {
        atomic_fetch_add_explicit(&deleg->client->revoked_deleg_count, 1,
                                  memory_order_acq_rel);
    }
} /* nfs_delegation_revoked_cb */

SYMBOL_EXPORT bool
nfs_client_lease_alive(
    const struct chimera_vfs_lease *lease,
    void                           *private_data)
{
    const struct nfs_client *client = private_data;

    (void) lease;

    /* Dead (reclaimable) once the lease has lapsed into courtesy state. */
    return !atomic_load_explicit(&client->courtesy, memory_order_relaxed);
} /* nfs_client_lease_alive */

SYMBOL_EXPORT void
nfs_client_lease_revoked_cb(
    struct chimera_vfs_lease *lease,
    void                     *private_data)
{
    struct nfs_client *client = private_data;

    (void) lease;

    /* A conflicting acquirer reclaimed a lease this courtesy client still
     * held.  Flag it so the next lease sweep tears the client down; until
     * then its other leases stay reclaimable (still in courtesy). */
    atomic_store_explicit(&client->reclaim_pending, 1, memory_order_release);
} /* nfs_client_lease_revoked_cb */

SYMBOL_EXPORT bool
nfs_delegation_lease_alive(
    const struct chimera_vfs_lease *lease,
    void                           *private_data)
{
    const struct nfs_delegation *deleg = private_data;

    (void) lease;

    return !atomic_load_explicit(&deleg->client->courtesy, memory_order_relaxed);
} /* nfs_delegation_lease_alive */

/* FREE_STATEID support: if `sid` names a force-revoked delegation, tear it
* down (freeing the slot and clearing the client's revoked count) and return
* NFS4_OK.  Returns NFS4ERR_LOCKS_HELD for a still-live state (cannot be
* freed), or the usual NFS4ERR_BAD/STALE/EXPIRED for an invalid stateid. */
SYMBOL_EXPORT nfsstat4
nfs_state_table_free_revoked_deleg(
    struct nfs_state_table    *table,
    const struct stateid4     *sid,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs4_stateid_view view;
    struct nfs_state_shard  *shard;
    struct nfs_state_slot   *slot;
    struct nfs_delegation   *deleg = NULL;

    if (nfs4_stateid_is_special(sid)) {
        return NFS4ERR_BAD_STATEID;
    }

    nfs4_stateid_decode(&view, sid);

    if (view.epoch != table->epoch) {
        return NFS4ERR_STALE_STATEID;
    }
    if (view.version != NFS4_STATEID_VERSION ||
        view.shard >= NFS_STATE_NUM_SHARDS) {
        return NFS4ERR_BAD_STATEID;
    }

    shard = &table->shards[view.shard];

    pthread_rwlock_rdlock(&shard->lock);

    if (view.slot_idx >= shard->slots_used) {
        pthread_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    slot = &shard->slots[view.slot_idx];

    if (slot->type == NFS4_SLOT_TYPE_EXPIRED) {
        nfsstat4 st = (slot->generation == view.generation)
                      ? NFS4ERR_EXPIRED : NFS4ERR_STALE_STATEID;
        pthread_rwlock_unlock(&shard->lock);
        return st;
    }
    if (slot->type == NFS4_SLOT_TYPE_FREE) {
        pthread_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }
    if (slot->generation != view.generation) {
        pthread_rwlock_unlock(&shard->lock);
        return NFS4ERR_STALE_STATEID;
    }
    if (slot->type != NFS4_SLOT_TYPE_DELEG ||
        !atomic_load_explicit(&((struct nfs_delegation *) slot->state)->revoked,
                              memory_order_acquire)) {
        /* A live (non-revoked) state cannot be freed while in use. */
        pthread_rwlock_unlock(&shard->lock);
        return NFS4ERR_LOCKS_HELD;
    }

    deleg = slot->state;
    atomic_fetch_add_explicit(&deleg->refcount, 1, memory_order_acq_rel);
    pthread_rwlock_unlock(&shard->lock);

    /* Our +1 ref above keeps `deleg` alive across destroy, which drops only
     * the lifetime ref (refcount stays >0); the release then drops our ref and
     * is what actually frees it.  scan-build inlines the conditional free in
     * delegation_destroy_common/delegation_cleanup (same TU) and can't follow
     * the refcount, so it flags a false-positive use-after-free on the release.
     * Hide the sequence from the analyzer, matching nfs_client_destroy. */
#ifndef __clang_analyzer__
    nfs_delegation_destroy(deleg, table, vfs_thread);
    nfs_state_table_release(table, deleg, NFS4_SLOT_TYPE_DELEG, vfs_thread);
#endif /* ifndef __clang_analyzer__ */
    return NFS4_OK;
} /* nfs_state_table_free_revoked_deleg */
