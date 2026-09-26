// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
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

void
nfs4_replay_record_lock(
    struct nfs4_replay_cache *replay,
    uint32_t                  seqid,
    const struct LOCK4res    *response)
{
    nfs4_replay_record(replay, seqid, OP_LOCK, response->status,
                       response->status == NFS4_OK ? &response->resok4.lock_stateid : NULL);
    if (response->status == NFS4ERR_DENIED) {
        const struct LOCK4denied *denied = &response->denied;
        chimera_nfs_abort_if(denied->owner.owner.len > NFS4_OPAQUE_LIMIT ||
                             (denied->owner.owner.len && !denied->owner.owner.data), "invalid LOCK replay owner");
        replay->denied.offset    = denied->offset;
        replay->denied.length    = denied->length;
        replay->denied.locktype  = denied->locktype;
        replay->denied.clientid  = denied->owner.clientid;
        replay->denied.owner_len = denied->owner.owner.len;
        if (denied->owner.owner.len) {
            memcpy(replay->denied.owner, denied->owner.owner.data, denied->owner.owner.len);
        }
        replay->lock_denied_valid = true;
    }
} /* nfs4_replay_record_lock */

bool
nfs4_replay_fill_lock(
    const struct nfs4_replay_cache *replay,
    struct LOCK4res                *response,
    uint8_t                        *owner_storage)
{
    if (!replay->valid || replay->op != OP_LOCK ||
        (replay->status == NFS4ERR_DENIED && (!replay->lock_denied_valid ||
                                              replay->denied.owner_len > NFS4_OPAQUE_LIMIT || (replay->denied.owner_len
                                                                                               && !owner_storage)))) {
        return false;
    }
    memset(response, 0, sizeof(*response));
    response->status = replay->status;
    if (replay->status == NFS4_OK) {
        response->resok4.lock_stateid = replay->stateid;
    } else if (replay->status == NFS4ERR_DENIED) {
        response->denied.offset           = replay->denied.offset;
        response->denied.length           = replay->denied.length;
        response->denied.locktype         = replay->denied.locktype;
        response->denied.owner.clientid   = replay->denied.clientid;
        response->denied.owner.owner.len  = replay->denied.owner_len;
        response->denied.owner.owner.data = owner_storage;
        if (replay->denied.owner_len) {
            memcpy(owner_storage, replay->denied.owner, replay->denied.owner_len);
        }
    }
    return true;
} /* nfs4_replay_fill_lock */

bool
nfs4_replay_record_open(
    struct nfs4_replay_cache *replay,
    uint32_t                  seqid,
    const struct OPEN4res    *response,
    const uint8_t            *fh,
    uint32_t                  fh_len)
{
    nfs4_replay_record(replay, seqid, OP_OPEN, response->status,
                       response->status == NFS4_OK ? &response->resok4.stateid : NULL);
    if (response->status != NFS4_OK) {
        return true;
    }
    const struct OPEN4resok *opened = &response->resok4;
    replay->rflags               = opened->rflags;
    replay->open.delegation_type = opened->delegation.delegation_type;
    const struct nfsace4    *permissions = NULL;
    switch (opened->delegation.delegation_type) {
        case OPEN_DELEGATE_NONE:
        case OPEN_DELEGATE_NONE_EXT:
            break;
        case OPEN_DELEGATE_READ:
            permissions = &opened->delegation.read.permissions;
            break;
        case OPEN_DELEGATE_WRITE:
            permissions = &opened->delegation.write.permissions;
            break;
        default:
            return false;
    } /* switch */
    if ((permissions && (permissions->who.len > NFS4_OPAQUE_LIMIT ||
                         (permissions->who.len && !permissions->who.data))) ||
        opened->num_attrset > 3 || (opened->num_attrset && !opened->attrset) ||
        !fh || !fh_len || fh_len > NFS4_FHSIZE) {
        return false;
    }
    replay->open.delegation         = opened->delegation;
    replay->open.delegation_who_len = permissions ? permissions->who.len : 0;
    if (permissions) {
        if (permissions->who.len) {
            memcpy(replay->open.delegation_who, permissions->who.data, permissions->who.len);
        }
        /* No interior pointers: caches are copied into journals/tombstones. */
        if (opened->delegation.delegation_type == OPEN_DELEGATE_READ) {
            replay->open.delegation.read.permissions.who.data = NULL;
        } else {
            replay->open.delegation.write.permissions.who.data = NULL;
        }
    }
    replay->open.cinfo       = opened->cinfo;
    replay->open.num_attrset = opened->num_attrset;
    if (opened->num_attrset) {
        memcpy(replay->open.attrset, opened->attrset, opened->num_attrset * sizeof(uint32_t));
    }
    memcpy(replay->open.fh, fh, fh_len);
    replay->open.fh_len = fh_len;
    replay->open_valid  = true;
    return true;
} /* nfs4_replay_record_open */

bool
nfs4_replay_fill_open(
    const struct nfs4_replay_cache *replay,
    struct OPEN4res                *response,
    uint32_t                        attr_storage[3],
    uint8_t                         fh_storage[NFS4_FHSIZE],
    uint32_t                       *fh_len,
    uint8_t                         who_storage[NFS4_OPAQUE_LIMIT])
{
    if (!replay->valid || replay->op != OP_OPEN ||
        (replay->status == NFS4_OK && (!replay->open_valid || !fh_storage ||
                                       !replay->open.fh_len || replay->open.fh_len > NFS4_FHSIZE || replay->open.
                                       num_attrset > 3 ||
                                       (replay->open.num_attrset && !attr_storage) ||
                                       replay->open.delegation_who_len > NFS4_OPAQUE_LIMIT ||
                                       (replay->open.delegation_who_len && !who_storage)))) {
        return false;
    }
    memset(response, 0, sizeof(*response));
    response->status = replay->status;
    *fh_len          = 0;
    if (replay->status == NFS4_OK) {
        struct OPEN4resok *opened = &response->resok4;
        opened->stateid     = replay->stateid;
        opened->rflags      = replay->rflags;
        opened->cinfo       = replay->open.cinfo;
        opened->num_attrset = replay->open.num_attrset;
        opened->attrset     = attr_storage;
        if (opened->num_attrset) {
            memcpy(attr_storage, replay->open.attrset, opened->num_attrset * sizeof(uint32_t));
        }
        opened->delegation = replay->open.delegation;
        if (replay->open.delegation_who_len) {
            memcpy(who_storage, replay->open.delegation_who, replay->open.delegation_who_len);
        }
        if (opened->delegation.delegation_type == OPEN_DELEGATE_READ) {
            opened->delegation.read.permissions.who.data = who_storage;
        } else if (opened->delegation.delegation_type == OPEN_DELEGATE_WRITE) {
            opened->delegation.write.permissions.who.data = who_storage;
        }
        memcpy(fh_storage, replay->open.fh, replay->open.fh_len);
        *fh_len = replay->open.fh_len;
    }
    return true;
} /* nfs4_replay_fill_open */

bool
nfs4_owner_replay_record_open(
    struct nfs4_owner_replay_journal *journal,
    uint32_t                          incoming,
    const struct OPEN4res            *response,
    const uint8_t                    *fh,
    uint32_t                          fh_len)
{
    struct nfs4_replay_cache replay = { 0 };

    if (!nfs4_seqid_should_advance(response->status)) {
        return true;
    }
    if (!nfs4_replay_record_open(&replay, incoming, response, fh, fh_len)) {
        return false;
    }
    journal->seqid  = incoming;
    journal->replay = replay;
    journal->dirty  = true;
    return true;
} /* nfs4_owner_replay_record_open */

void
nfs4_owner_replay_reset(struct nfs4_owner_replay_journal *journal)
{
    journal->seqid     = journal->initial_seqid;
    journal->replay    = journal->initial_replay;
    journal->dirty     = false;
    journal->confirmed = journal->initial_confirmed;
} /* nfs4_owner_replay_reset */

int
nfs4_owner_replay_classify(
    const struct nfs4_owner_replay_journal *journal,
    uint32_t                                incoming,
    uint32_t                                op)
{
    int classification = nfs4_owner_seqid_classify(journal->seqid, &journal->replay, incoming);

    if (classification == NFS4_SEQID_REPLAY && journal->replay.op != op) {
        return NFS4_SEQID_BAD;
    }
    return classification;
} /* nfs4_owner_replay_classify */

void
nfs4_owner_replay_record(
    struct nfs4_owner_replay_journal *journal,
    uint32_t                          incoming,
    uint32_t                          op,
    nfsstat4                          status,
    const struct stateid4            *stateid)
{
    if (!nfs4_seqid_should_advance(status)) {
        return;
    }
    journal->seqid = incoming;
    nfs4_replay_record(&journal->replay, incoming, op, status, stateid);
    journal->dirty = true;
} /* nfs4_owner_replay_record */

void
nfs4_owner_replay_record_lock(
    struct nfs4_owner_replay_journal *journal,
    uint32_t                          incoming,
    const struct LOCK4res            *response)
{
    if (!nfs4_seqid_should_advance(response->status)) {
        return;
    }
    journal->seqid = incoming;
    nfs4_replay_record_lock(&journal->replay, incoming, response);
    journal->dirty = true;
} /* nfs4_owner_replay_record_lock */

void
nfs_open_owner_publish_replay(struct nfs_open_owner_reservation *reservation)
{
    struct nfs_open_owner            *owner   = reservation->owner;
    struct nfs4_owner_replay_journal *journal = &reservation->owner_replay;

    if (!journal->dirty) {
        return;
    }
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_reservation != reservation || owner->client->minor,
                         "invalid compound OPEN owner replay publication");
    owner->seqid     = journal->seqid;
    owner->replay    = journal->replay;
    owner->confirmed = journal->confirmed;
    evpl_mutex_unlock(&owner->lock);
} /* nfs_open_owner_publish_replay */

void
nfs_open_owner_confirm_compound(
    struct nfs_open_owner_reservation *reservation,
    struct nfs_open_state             *target,
    uint32_t                           seqid)
{
    struct nfs_open_owner *owner = reservation->owner;

    evpl_mutex_lock(&owner->client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->client->minor || owner->compound_reservation != reservation ||
                         target->owner != owner || target->compound_reservation != reservation ||
                         !atomic_load(&target->compound_reserved) || atomic_load(&target->destroyed) ||
                         !reservation->owner_replay.confirmed,
                         "invalid compound OPEN_CONFIRM publication");
    target->seqid = seqid;
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&owner->client->lock);
} /* nfs_open_owner_confirm_compound */

void
nfs_lock_owner_publish_replay(struct nfs_lock_owner_reservation *reservation)
{
    struct nfs_lock_owner            *owner   = reservation->owner;
    struct nfs4_owner_replay_journal *journal = &reservation->owner_replay;

    if (!journal->dirty) {
        return;
    }
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_group != reservation->cookie || !atomic_load(&owner->compound_pins) ||
                         owner->client->minor, "invalid compound LOCK owner replay publication");
    owner->seqid  = journal->seqid;
    owner->replay = journal->replay;
    evpl_mutex_unlock(&owner->lock);
} /* nfs_lock_owner_publish_replay */

static void
shard_init(struct nfs_state_shard *shard)
{
    evpl_rwlock_init(&shard->lock, NULL);
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
    chimera_rwlock_destroy(&shard->lock);
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
    table->change_table = nfs4_change_table_create();
    for (int i = 0; i < NFS_STATE_NUM_SHARDS; i++) {
        shard_init(&table->shards[i]);
    }

    /* Per-instance epoch stamped into every stateid (see nfs4_stateid.h):
     *   bits 31..16 : node_id  -- attributes the stateid to its minting
     *                 instance, so peers sharing one backing store never mint
     *                 colliding epochs and a foreign stateid is recognisable.
     *   bits 15..1  : 15-bit boot discriminator -- differs across this node's
     *                 restarts, so a previous instance's stateids read as stale.
     *   bit  0      : 1 -- with node_id >= 1 this keeps the epoch away from the
     *                 special all-zero/all-ones stateids and pynfs's small
     *                 "old epoch" probe values (replacing the old 0x80000000). */
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);

    /* Fold the full-resolution boot time (seconds AND nanoseconds) into the
     * 15-bit discriminator.  A bare `time(NULL) & 0x7FFF` collides whenever two
     * restarts' wall-clock seconds are congruent mod 32768 (~9.1h) -- a
     * deterministic, easily-hit alias.  Mixing the high seconds bits and the
     * nanosecond field makes a collision a ~1/32768 chance instead, so a
     * previous instance's stateids reliably read as STALE_STATEID on reboot. */
    uint32_t boot = (uint32_t) ts.tv_sec ^ ((uint32_t) ts.tv_sec >> 15) ^
        (uint32_t) ts.tv_nsec ^ ((uint32_t) ts.tv_nsec >> 15);

    table->epoch = ((uint32_t) node_id << 16) |
        ((boot & 0x7FFFu) << 1) | 1u;
} /* nfs_state_table_init */

void
nfs_state_table_free(
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    for (int i = 0; i < NFS_STATE_NUM_SHARDS; i++) {
        shard_free(&table->shards[i], vfs_thread);
    }
    nfs4_change_table_free(table->change_table);
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

    evpl_rwlock_wrlock(&shard->lock);

    if (shard->free_count) {
        slot_idx = shard->free_idx[--shard->free_count];
    } else {
        if (shard->slots_used == shard->slots_capacity) {
            if (shard_grow_locked(shard) != 0) {
                evpl_rwlock_unlock(&shard->lock);
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

    evpl_rwlock_unlock(&shard->lock);
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

    evpl_rwlock_wrlock(&shard->lock);

    chimera_nfs_abort_if(slot_idx >= shard->slots_used,
                         "install on unallocated slot %u/%u",
                         slot_idx, shard->slots_used);

    slot        = &shard->slots[slot_idx];
    slot->type  = type;
    slot->state = state;

    evpl_rwlock_unlock(&shard->lock);
} /* nfs_state_table_install */

void
nfs_state_table_free_slot(
    struct nfs_state_table *table,
    uint8_t                 shard_idx,
    uint32_t                slot_idx)
{
    struct nfs_state_shard *shard = &table->shards[shard_idx];
    struct nfs_state_slot  *slot;

    evpl_rwlock_wrlock(&shard->lock);

    chimera_nfs_abort_if(slot_idx >= shard->slots_used,
                         "free of unallocated slot %u/%u",
                         slot_idx, shard->slots_used);

    slot               = &shard->slots[slot_idx];
    slot->replay.valid = 0;
    slot->type         = NFS4_SLOT_TYPE_FREE;
    slot->state        = NULL;
    slot->generation  += 1;         /* invalidate any extant stateid */

    (void) shard_push_free_locked(shard, slot_idx);

    evpl_rwlock_unlock(&shard->lock);
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

    evpl_rwlock_wrlock(&shard->lock);

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

    evpl_rwlock_unlock(&shard->lock);
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

    evpl_rwlock_wrlock(&shard->lock);

    chimera_nfs_abort_if(slot_idx >= shard->slots_used,
                         "expire of unallocated slot %u/%u",
                         slot_idx, shard->slots_used);

    slot        = &shard->slots[slot_idx];
    slot->type  = NFS4_SLOT_TYPE_EXPIRED;
    slot->state = NULL;

    (void) shard_push_free_locked(shard, slot_idx);

    evpl_rwlock_unlock(&shard->lock);
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

    evpl_rwlock_rdlock(&shard->lock);

    if (view.slot_idx >= shard->slots_used) {
        evpl_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    slot = &shard->slots[view.slot_idx];

    if (slot->type == NFS4_SLOT_TYPE_EXPIRED) {
        /* The owning client was purged.  A stateid still naming this slot's
         * generation gets NFS4ERR_EXPIRED; an older generation is stale. */
        status = (slot->generation == view.generation)
                 ? NFS4ERR_EXPIRED : NFS4ERR_STALE_STATEID;
    } else if (slot->type == NFS4_SLOT_TYPE_FREE ||
               slot->type == NFS4_SLOT_TYPE_RESERVED) {
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
            evpl_rwlock_unlock(&shard->lock);
            return NFS4ERR_EXPIRED;
        }

        if (bump_refcount && slot->type == NFS4_SLOT_TYPE_OPEN &&
            (atomic_load_explicit(&((struct nfs_open_state *) slot->state)->compound_closing,
                                  memory_order_acquire) ||
             atomic_load_explicit(&((struct nfs_open_state *) slot->state)->compound_reserved,
                                  memory_order_acquire))) {
            evpl_rwlock_unlock(&shard->lock);
            return NFS4ERR_DELAY;
        }
        if (bump_refcount && slot->type == NFS4_SLOT_TYPE_LOCK &&
            (atomic_load_explicit(&((struct nfs_lock_state *) slot->state)->compound_reserved, memory_order_acquire) ||
             atomic_load_explicit(&((struct nfs_lock_state *) slot->state)->lock_owner->compound_pins,
                                  memory_order_acquire))) {
            evpl_rwlock_unlock(&shard->lock);
            return NFS4ERR_DELAY;
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
            } else if (slot->type == NFS4_SLOT_TYPE_LAYOUT) {
                atomic_fetch_add_explicit(
                    &((struct nfs_layout_state *) slot->state)->refcount, 1,
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

    evpl_rwlock_unlock(&shard->lock);
    return status;
} /* state_table_lookup_locked */

nfsstat4
nfs_state_table_acquire_no_renew(
    struct nfs_state_table *table,
    const struct stateid4  *sid,
    uint8_t                 want_type,
    void                  **out_state,
    uint8_t                *out_type)
{
    return state_table_lookup_locked(table, sid, want_type, true,
                                     out_state, out_type);
} /* nfs_state_table_acquire_no_renew */

nfsstat4
nfs_state_table_acquire(
    struct nfs_state_table *table,
    const struct stateid4  *sid,
    uint8_t                 want_type,
    void                  **out_state,
    uint8_t                *out_type)
{
    nfsstat4 status = nfs_state_table_acquire_no_renew(table, sid, want_type,
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
    evpl_rwlock_rdlock(&shard->lock);

    if (view.slot_idx >= shard->slots_used) {
        evpl_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    slot = &shard->slots[view.slot_idx];
    if (!slot->replay.valid ||
        slot->replay_generation != view.generation ||
        slot->replay.op != op ||
        slot->replay.seqid != seqid) {
        evpl_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    *out_replay = slot->replay;
    evpl_rwlock_unlock(&shard->lock);
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
        nfs_layout_state_put(state);
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

struct nfs_state_advise_check {
    const uint8_t *fh;
    uint32_t       fh_len;
    uint32_t       principal_flavor;
    const char    *principal_name;
    uint32_t       principal_len;
};

static nfsstat4
nfs_state_table_snapshot(
    struct nfs_state_table              *table,
    const struct stateid4               *sid,
    const struct nfs_client             *session_client,
    const struct nfs_state_advise_check *advise)
{
    struct nfs4_stateid_view view;
    struct nfs_state_shard  *shard;
    struct nfs_state_slot   *slot;
    struct nfs_client       *client;
    uint32_t                 seqid;
    nfsstat4                 status = NFS4ERR_BAD_STATEID;

    if (!session_client || nfs4_stateid_is_special(sid)) {
        return status;
    }
    nfs4_stateid_decode(&view, sid);
    /* TEST_STATEID maps stale identities to BAD_STATEID, including restart. */
    if (view.epoch != table->epoch) {
        return advise ? NFS4ERR_STALE_STATEID : NFS4ERR_BAD_STATEID;
    }
    if (view.version != NFS4_STATEID_VERSION ||
        view.shard >= NFS_STATE_NUM_SHARDS) {
        return status;
    }
    shard = &table->shards[view.shard];
    /* Slot teardown requires this shard's write lock before it can drop the
     * lifetime reference. Keep the read lock through every dereference: no
     * borrowed pointer escapes, and no final put can trigger cleanup here.
     * Sequence scalars are atomic because legacy writers use owner/client
     * locks; taking those under the shard lock would invert their lock order. */
    evpl_rwlock_rdlock(&shard->lock);
    if (view.slot_idx >= shard->slots_used) {
        goto out;
    }
    slot = &shard->slots[view.slot_idx];
    if (slot->generation != view.generation) {
        status = advise ? NFS4ERR_STALE_STATEID : NFS4ERR_BAD_STATEID;
        goto out;
    }
    if (slot->type == NFS4_SLOT_TYPE_EXPIRED) {
        status = NFS4ERR_EXPIRED;
        goto out;
    }
    if (!slot->state) {
        goto out;
    }
    if (advise && slot->type != NFS4_SLOT_TYPE_OPEN && slot->type != NFS4_SLOT_TYPE_LOCK) {
        goto out;
    }
    client = nfs_state_owner_client(slot->state, slot->type);
    if (client != session_client) {
        goto out;
    }
    if (atomic_load_explicit(&client->reclaim_pending, memory_order_acquire)) {
        status = NFS4ERR_EXPIRED;
        goto out;
    }
    switch (slot->type) {
        case NFS4_SLOT_TYPE_OPEN:
            if (view.type != NFS4_STATEID_TYPE_OPEN ||
                atomic_load_explicit(&((struct nfs_open_state *) slot->state)->destroyed, memory_order_acquire)) {
                goto out;
            }
            seqid = atomic_load_explicit(&((struct nfs_open_state *) slot->state)->seqid, memory_order_acquire);
            break;
        case NFS4_SLOT_TYPE_LOCK:
            if (view.type != NFS4_STATEID_TYPE_LOCK ||
                atomic_load_explicit(&((struct nfs_lock_state *) slot->state)->destroyed, memory_order_acquire)) {
                goto out;
            }
            seqid = atomic_load_explicit(&((struct nfs_lock_state *) slot->state)->seqid, memory_order_acquire);
            break;
        case NFS4_SLOT_TYPE_DELEG:
            if (view.type != NFS4_STATEID_TYPE_DELEG ||
                atomic_load_explicit(&((struct nfs_delegation *) slot->state)->destroyed, memory_order_acquire)) {
                goto out;
            }
            if (atomic_load_explicit(&((struct nfs_delegation *) slot->state)->revoked, memory_order_acquire)) {
                status = NFS4ERR_DELEG_REVOKED;
                goto out;
            }
            seqid = atomic_load_explicit(&((struct nfs_delegation *) slot->state)->seqid, memory_order_acquire);
            break;
        case NFS4_SLOT_TYPE_LAYOUT:
            if (view.type != NFS4_STATEID_TYPE_LAYOUT ||
                atomic_load_explicit(&((struct nfs_layout_state *) slot->state)->destroyed, memory_order_acquire)) {
                goto out;
            }
            seqid = atomic_load_explicit(&((struct nfs_layout_state *) slot->state)->seqid, memory_order_acquire);
            break;
        default:
            goto out;
    } /* switch */
    if (advise) {
        struct nfs_open_state *open_state;
        if (slot->type == NFS4_SLOT_TYPE_OPEN) {
            open_state = slot->state;
        } else if (slot->type == NFS4_SLOT_TYPE_LOCK) {
            /* The lock state's lifetime pins its parent OPEN until lock
             * cleanup. Keeping the lock slot live therefore protects both
             * objects. Parent identity/principal are immutable once public. */
            open_state = ((struct nfs_lock_state *) slot->state)->open_state;
        } else {
            goto out;
        }
        if (!open_state || atomic_load_explicit(&open_state->destroyed, memory_order_acquire) ||
            open_state->owner->client != session_client || open_state->fh_len != advise->fh_len ||
            !advise->fh || memcmp(open_state->fh, advise->fh, advise->fh_len)) {
            goto out;
        }
        if (!nfs_open_state_check_principal(open_state, advise->principal_flavor,
                                            advise->principal_name, advise->principal_len)) {
            status = NFS4ERR_ACCESS;
            goto out;
        }
    }
    status = nfs4_stateid_check_seqid(seqid, sid->seqid);
 out:
    evpl_rwlock_unlock(&shard->lock);
    return status;
} /* nfs_state_table_snapshot */

nfsstat4
nfs_state_table_test_stateid(
    struct nfs_state_table  *table,
    const struct stateid4   *sid,
    const struct nfs_client *session_client)
{
    return nfs_state_table_snapshot(table, sid, session_client, NULL);
} /* nfs_state_table_test_stateid */

nfsstat4
nfs_state_table_advise(
    struct nfs_state_table  *table,
    const struct stateid4   *sid,
    const struct nfs_client *session_client,
    const uint8_t           *fh,
    uint32_t                 fh_len,
    uint32_t                 principal_flavor,
    const char              *principal_name,
    uint32_t                 principal_len)
{
    const struct nfs_state_advise_check check = {
        .fh = fh,             .fh_len
            =
                fh_len,        .
        principal_flavor
            =
                principal_flavor,
        .principal_name = principal_name, .principal_len
                        =
                principal_len
            ,
    };

    return nfs_state_table_snapshot(table, sid, session_client, &check);
} /* nfs_state_table_advise */

nfsstat4
nfs_state_table_delegation_io(
    struct nfs_state_table     *table,
    const struct stateid4      *sid,
    const struct nfs_client    *session_client,
    const uint8_t              *fh,
    uint32_t                    fh_len,
    uint32_t                    want_access,
    struct chimera_claim_actor *out_actor)
{
    struct nfs4_stateid_view view;
    struct nfs_state_shard  *shard;
    struct nfs_state_slot   *slot;
    struct nfs_delegation   *deleg;
    nfsstat4                 status = NFS4ERR_BAD_STATEID;

    memset(out_actor, 0, sizeof(*out_actor));
    if (!session_client || !fh || !fh_len || fh_len > NFS4_FHSIZE ||
        nfs4_stateid_is_special(sid)) {
        return status;
    }
    if (!want_access || (want_access & ~OPEN4_SHARE_ACCESS_BOTH)) {
        return NFS4ERR_INVAL;
    }
    nfs4_stateid_decode(&view, sid);
    if (view.version != NFS4_STATEID_VERSION || view.type != NFS4_STATEID_TYPE_DELEG ||
        view.shard >= NFS_STATE_NUM_SHARDS) {
        return status;
    }
    if (view.epoch != table->epoch) {
        return NFS4ERR_STALE_STATEID;
    }
    shard = &table->shards[view.shard];
    /* Teardown cannot free slot-owned state while this read lock is held.
     * Identity, FH, delegation type and actor identity are immutable. */
    evpl_rwlock_rdlock(&shard->lock);
    if (view.slot_idx >= shard->slots_used) {
        goto out;
    }
    slot = &shard->slots[view.slot_idx];
    if (slot->generation != view.generation) {
        status = NFS4ERR_STALE_STATEID;
        goto out;
    }
    if (slot->type == NFS4_SLOT_TYPE_EXPIRED) {
        status = NFS4ERR_EXPIRED;
        goto out;
    }
    if (slot->type != NFS4_SLOT_TYPE_DELEG || !slot->state) {
        goto out;
    }
    deleg = slot->state;
    if (deleg->client != session_client ||
        atomic_load_explicit(&deleg->destroyed, memory_order_acquire) ||
        deleg->fh_len != fh_len || memcmp(deleg->fh, fh, fh_len)) {
        goto out;
    }
    if (atomic_load_explicit(&session_client->reclaim_pending, memory_order_acquire)) {
        status = NFS4ERR_EXPIRED;
        goto out;
    }
    if (atomic_load_explicit(&deleg->revoked, memory_order_acquire)) {
        status = NFS4ERR_DELEG_REVOKED;
        goto out;
    }
    status = nfs4_stateid_check_seqid(atomic_load_explicit(&deleg->seqid, memory_order_acquire), sid->seqid);
    if (status != NFS4_OK) {
        goto out;
    }
    if ((deleg->type != OPEN_DELEGATE_READ && deleg->type != OPEN_DELEGATE_WRITE) ||
        ((want_access & OPEN4_SHARE_ACCESS_WRITE) && deleg->type != OPEN_DELEGATE_WRITE)) {
        status = NFS4ERR_OPENMODE;
        goto out;
    }
    out_actor->owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    out_actor->owner.client_key = session_client->client_id;
    out_actor->owner.owner_lo   = deleg->fh_hash;
 out:
    evpl_rwlock_unlock(&shard->lock);
    return status;
} /* nfs_state_table_delegation_io */

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
    evpl_mutex_init(&c->lock, NULL);
    return c;
} /* nfs_client_alloc */

bool
nfs_client_reserve_compound(struct nfs_client *client)
{
    bool reserved = false;

    if (!client) {
        return false;
    }
    evpl_mutex_lock(&client->lock);
    if (!client->teardown_started && !client->expired && !client->compound_destroy_pending && !atomic_load(&client->
                                                                                                           reclaim_pending))
    {
        atomic_fetch_add(&client->compound_pins, 1);
        reserved = true;
    }
    evpl_mutex_unlock(&client->lock);
    return reserved;
} /* nfs_client_reserve_compound */

void
nfs_client_duplicate_compound_pin(struct nfs_client *client)
{
    evpl_mutex_lock(&client->lock);
    chimera_nfs_abort_if(!atomic_load(&client->compound_pins), "duplicate requires an existing client pin");
    atomic_fetch_add(&client->compound_pins, 1);
    evpl_mutex_unlock(&client->lock);
} /* nfs_client_duplicate_compound_pin */

void
nfs_client_finish_compound(
    struct nfs_client         *client,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    bool destroy;

    evpl_mutex_lock(&client->lock);
    chimera_nfs_abort_if(!atomic_load(&client->compound_pins), "compound client pin underflow");
    destroy = atomic_fetch_sub(&client->compound_pins, 1) == 1 && client->compound_destroy_pending;
    evpl_mutex_unlock(&client->lock);
    if (destroy) {
        nfs_client_destroy(client, table, vfs_thread, false);
    }
} /* nfs_client_finish_compound */

static void
nfs_client_memory_put(struct nfs_client *client)
{
    if (atomic_fetch_sub_explicit(&client->refcount, 1, memory_order_acq_rel) == 1) {
        evpl_mutex_destroy(&client->lock);
        free(client);
    }
} /* nfs_client_memory_put */

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
        ls->on_open_list = 0;
        if (ls->lock_owner) {
            /* lock_owner->states is mutated under lock_owner->lock everywhere
             * else (nfs_lock_state_create / nfs_lock_state_destroy); take it
             * here too or a concurrent create corrupts the list.
             * Ordering: owner->lock (held by caller) -> lock_owner->lock.
             * Flag-guarded: a racing nfs_lock_state_destroy may have unlinked
             * this side already. */
            evpl_mutex_lock(&ls->lock_owner->lock);
            if (ls->on_owner_list) {
                LL_DELETE2(ls->lock_owner->states, ls, next_in_owner);
                ls->on_owner_list = 0;
            }
            evpl_mutex_unlock(&ls->lock_owner->lock);
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

    evpl_mutex_lock(&client->lock);
    if (client->teardown_started) {
        evpl_mutex_unlock(&client->lock);
        return;
    }
    if (client->compound_pins) {
        client->compound_destroy_pending = true;
        evpl_mutex_unlock(&client->lock);
        return;
    }
    client->teardown_started = true;
    /* HASH_ITER + HASH_DELETE + free is the standard uthash teardown
     * pattern, but scan-build can't reason through the macro expansion
     * and flags a (false-positive) UAF on the bucket pointer.  Match
     * the rest of the codebase by hiding the walks from the analyzer. */
#ifndef __clang_analyzer__
    struct nfs_open_owner *oo, *oo_tmp;
    struct nfs_lock_owner *lo, *lo_tmp;

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        evpl_mutex_lock(&oo->lock);
        struct nfs_open_state *os, *os_tmp;
        HASH_ITER(hh, oo->states_by_fh, os, os_tmp)
        {
            /* The whole client is going away (lease expiry / reboot /
             * DESTROY_CLIENTID); mark its stateids EXPIRED, not free. */
            open_state_destroy_locked(oo, os, table, vfs_thread, true);
        }
        evpl_mutex_unlock(&oo->lock);

        /* Unpublish, then drop the hash-table slot ref; a borrowing in-flight
         * request defers the free to its own put(). */
        HASH_DELETE(hh, client->open_owners_by_str, oo);
        nfs_open_owner_put(oo);
    }

    HASH_ITER(hh, client->lock_owners_by_str, lo, lo_tmp)
    {
        /* All states should have been torn down via the open_states above,
         * since each lock_state is on both lists. */
        chimera_nfs_abort_if(lo->states != NULL,
                             "lock_owner %p has leftover lock_states", lo);
        HASH_DELETE(hh, client->lock_owners_by_str, lo);
        nfs_lock_owner_put(lo);
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

    evpl_mutex_unlock(&client->lock);
    nfs_client_memory_put(client);
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

    evpl_mutex_lock(&client->lock);
    if (client->compound_pins) {
        evpl_mutex_unlock(&client->lock);
        return;
    }
    client->expired = 1;

#ifndef __clang_analyzer__
    struct nfs_open_owner *oo, *oo_tmp;
    struct nfs_lock_owner *lo, *lo_tmp;

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        evpl_mutex_lock(&oo->lock);
        struct nfs_open_state *os, *os_tmp;
        HASH_ITER(hh, oo->states_by_fh, os, os_tmp)
        {
            open_state_destroy_locked(oo, os, table, vfs_thread, true);
        }
        evpl_mutex_unlock(&oo->lock);

        /* Unpublish, then drop the hash-table slot ref.  If an in-flight OPEN
         * still borrows this owner, its ref keeps the struct alive until its
         * own put(); the last put() destroys the lock and frees. */
        HASH_DELETE(hh, client->open_owners_by_str, oo);
        nfs_open_owner_put(oo);
    }

    HASH_ITER(hh, client->lock_owners_by_str, lo, lo_tmp)
    {
        chimera_nfs_abort_if(lo->states != NULL,
                             "lock_owner %p has leftover lock_states", lo);
        HASH_DELETE(hh, client->lock_owners_by_str, lo);
        nfs_lock_owner_put(lo);
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
    evpl_mutex_unlock(&client->lock);
} /* nfs_client_expire_state */

struct nfs_open_owner *
nfs_open_owner_find_or_adopt(
    struct nfs_client     *client,
    struct nfs_open_owner *adopt,
    const void            *owner_bytes,
    uint16_t               owner_len,
    bool                  *out_created)
{
    struct nfs_open_owner *owner;

    if (owner_len > NFS4_OPAQUE_LIMIT) {
        owner_len = NFS4_OPAQUE_LIMIT;
    }

    evpl_mutex_lock(&client->lock);

    HASH_FIND(hh, client->open_owners_by_str, owner_bytes, owner_len, owner);

    if (owner) {
        if (out_created) {
            *out_created = false;
        }
        /* Take the caller ref while still holding client->lock, which is
         * serialized against teardown's HASH_DELETE. */
        nfs_open_owner_get(owner);
        evpl_mutex_unlock(&client->lock);
        return owner;
    }

    if (adopt) {
        /* Republish a still-pinned owner that a mid-flight lease sweep
         * unpublished, so an OPEN completing after the sweep keeps its
         * seqid/replay state on a single object instead of splitting it
         * across a fresh one.  One get() re-takes the hash-table
         * slot ref, the other is the caller ref. */
        nfs_open_owner_get(adopt);
        nfs_open_owner_get(adopt);
        HASH_ADD_KEYPTR(hh, client->open_owners_by_str,
                        adopt->owner, adopt->owner_len, adopt);
        if (out_created) {
            *out_created = false;
        }
        evpl_mutex_unlock(&client->lock);
        return adopt;
    }

    owner = calloc(1, sizeof(*owner));
    chimera_nfs_abort_if(owner == NULL, "open_owner alloc OOM");
    owner->client = client;
    memcpy(owner->owner, owner_bytes, owner_len);
    owner->owner_len = owner_len;
    owner->seqid     = 0;
    owner->confirmed = false;
    evpl_mutex_init(&owner->lock, NULL);
    /* refcount 2: one for the hash-table slot, one for the caller. */
    atomic_init(&owner->refcount, 2);

    HASH_ADD_KEYPTR(hh, client->open_owners_by_str,
                    owner->owner, owner->owner_len, owner);

    if (out_created) {
        *out_created = true;
    }
    evpl_mutex_unlock(&client->lock);
    return owner;
} /* nfs_open_owner_find_or_adopt */

struct nfs_open_owner *
nfs_open_owner_find_or_create(
    struct nfs_client *client,
    const void        *owner_bytes,
    uint16_t           owner_len,
    bool              *out_created)
{
    return nfs_open_owner_find_or_adopt(client, NULL, owner_bytes, owner_len,
                                        out_created);
} /* nfs_open_owner_find_or_create */

void
nfs_open_owner_get(struct nfs_open_owner *owner)
{
    uint32_t prev = atomic_fetch_add_explicit(&owner->refcount, 1,
                                              memory_order_acq_rel);

    chimera_nfs_abort_if(prev == 0, "open_owner get after free on %p", owner);
} /* nfs_open_owner_get */

void
nfs_open_owner_put(struct nfs_open_owner *owner)
{
    uint32_t prev = atomic_fetch_sub_explicit(&owner->refcount, 1,
                                              memory_order_acq_rel);

    chimera_nfs_abort_if(prev == 0, "open_owner refcount underflow on %p",
                         owner);
    if (prev == 1) {
        evpl_mutex_destroy(&owner->lock);
        free(owner);
    }
} /* nfs_open_owner_put */

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

    evpl_mutex_lock(&owner->lock);
    HASH_FIND(hh, owner->states_by_fh, fh, fh_len, state);
    evpl_mutex_unlock(&owner->lock);
    return state;
} /* nfs_open_owner_find_state */

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
    struct nfs_client     *client = owner->client;
    struct nfs_open_owner *published;
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
    state->fh_len       = fh_len;
    state->share_access = share_access;
    state->share_deny   = share_deny;
    state->share_combos = nfs_open_combo_bit(share_access, share_deny);
    state->seqid        = 1;
    state->shard        = shard;
    state->slot_idx     = slot_idx;
    state->generation   = gen;
    state->handle       = handle_dup;
    state->locks        = NULL;
    atomic_init(&state->refcount, 1);
    atomic_init(&state->destroyed, 0);

    atomic_init(&state->compound_closing, 0);
    atomic_init(&state->compound_reserved, 0);

    /* Serialize installation against client teardown, mirroring
     * nfs_lock_state_create: if the lease sweeper already
     * unpublished this owner, installing a fresh open_state on it would
     * orphan the state (and any lock_state later rooted on it) forever --
     * no teardown walk can reach it through the client's owner hash. */
    evpl_mutex_lock(&client->lock);

    HASH_FIND(hh, client->open_owners_by_str,
              owner->owner, owner->owner_len, published);
    if (published != owner || owner->compound_pending || owner->compound_close_count || owner->compound_reservation) {
        evpl_mutex_unlock(&client->lock);
        nfs_state_table_free_slot(table, shard, slot_idx);
        free(state);
        return NULL;
    }

    /* Pin the owner for the state's lifetime so an acquire-held open_state
     * always has a valid ->owner, even if the lease sweeper reaps the client
     * mid-flight.  Released in open_state_cleanup. */
    nfs_open_owner_get(owner);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_OPEN, state);

    evpl_mutex_lock(&owner->lock);
    HASH_ADD_KEYPTR(hh, owner->states_by_fh, state->fh, state->fh_len, state);
    evpl_mutex_unlock(&owner->lock);

    /* Encode before dropping client->lock: once published, a concurrent
     * expire may destroy and free the state. */
    nfs4_stateid_encode(out_stateid, state->seqid,
                        NFS4_STATEID_TYPE_OPEN, shard, slot_idx, gen,
                        table->epoch);

    evpl_mutex_unlock(&client->lock);
    return state;
} /* nfs_open_state_create */

static nfsstat4
nfs_open_owner_reserve_compound_internal(
    struct nfs_client                  *client,
    const void                         *owner_bytes,
    uint16_t                            owner_len,
    uint32_t                            principal_flavor,
    const char                         *principal_name,
    uint32_t                            principal_len,
    const void                         *cookie,
    uint32_t                            num_candidates,
    struct nfs_state_table             *table,
    struct chimera_vfs_thread          *vfs_thread,
    struct nfs_open_owner_reservation **out_reservation,
    bool                                allow_locks)
{
    struct nfs_open_owner_reservation *reservation;
    struct nfs_open_owner             *owner, *published;
    struct nfs_open_state             *state, *tmp;
    nfsstat4                           status = NFS4_OK;
    bool                               created;

    *out_reservation = NULL;
    if (!client) {
        return NFS4ERR_NOTSUPP;
    }
    if (!cookie || num_candidates > NFS4_COMPOUND_OWNER_MAX_STATES) {
        return NFS4ERR_INVAL;
    }
    reservation = calloc(1, sizeof(*reservation));
    if (!reservation) {
        return NFS4ERR_RESOURCE;
    }
    owner               = nfs_open_owner_find_or_create(client, owner_bytes, owner_len, &created);
    reservation->owner  = owner;
    reservation->cookie = cookie;
    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    HASH_FIND(hh, client->open_owners_by_str, owner->owner, owner->owner_len, published);
    if (client->expired || client->compound_destroy_pending ||
        atomic_load_explicit(&client->reclaim_pending, memory_order_acquire)) {
        status = NFS4ERR_EXPIRED;
    } else if (published != owner || owner->compound_pending || owner->compound_close_count ||
               owner->compound_reservation || HASH_COUNT(owner->states_by_fh) > NFS4_COMPOUND_OWNER_MAX_STATES ||
               atomic_load_explicit(&owner->refcount, memory_order_acquire) !=
               2 + HASH_COUNT(owner->states_by_fh)) {
        status = NFS4ERR_DELAY;
    }
    if (status != NFS4_OK) {
        evpl_mutex_unlock(&owner->lock);
        evpl_mutex_unlock(&client->lock);
        nfs_open_owner_put(owner);
        free(reservation);
        return status;
    }

    /* The owner borrow excludes a legacy coalesce already in flight; the
     * owner reservation excludes new ones. Freeze each slot under its shard
     * lock so an ordinary acquire cannot enter between checking and pinning. */
    owner->compound_reservation                 = reservation;
    reservation->owner_replay.initial_seqid     = owner->seqid;
    reservation->owner_replay.initial_replay    = owner->replay;
    reservation->owner_replay.initial_confirmed = owner->confirmed;
    nfs4_owner_replay_reset(&reservation->owner_replay);
    client->compound_pins++;
    HASH_ITER(hh, owner->states_by_fh, state, tmp)
    {
        struct nfs_state_shard *shard = &table->shards[state->shard];
        struct nfs_lock_state  *lock_state;
        uint32_t                num_locks = 0;

        for (lock_state = state->locks; lock_state; lock_state = lock_state->next_in_open) {
            num_locks++;
        }

        evpl_rwlock_wrlock(&shard->lock);
        if ((!allow_locks && state->locks) || state->base_stream_file_state ||
            atomic_load_explicit(&state->destroyed, memory_order_acquire) ||
            atomic_load_explicit(&state->compound_closing, memory_order_acquire) ||
            atomic_load_explicit(&state->compound_reserved, memory_order_acquire) ||
            atomic_load_explicit(&state->refcount, memory_order_acquire) != 1 + num_locks) {
            status = NFS4ERR_DELAY;
        } else if (!nfs_open_state_check_principal(state, principal_flavor, principal_name, principal_len)) {
            status = NFS4ERR_ACCESS;
        } else {
            atomic_fetch_add_explicit(&state->refcount, 1, memory_order_relaxed);
            atomic_store_explicit(&state->compound_reserved, 1, memory_order_release);
            state->compound_reservation                        = reservation;
            reservation->existing[reservation->num_existing++] = state;
        }
        evpl_rwlock_unlock(&shard->lock);
        if (status != NFS4_OK) {
            break;
        }
        for (lock_state = state->locks; lock_state; lock_state = lock_state->next_in_open) {
            struct nfs_lock_owner *lo = lock_state->lock_owner;
            struct nfs_lock_owner *published_lock_owner;
            struct nfs_lock_state *ls;
            uint32_t               owner_states = 0;
            if (reservation->num_locks >= NFS4_COMPOUND_OWNER_MAX_STATES) {
                status = NFS4ERR_RESOURCE;
                break;
            }
            evpl_mutex_lock(&lo->lock);
            HASH_FIND(hh, client->lock_owners_by_str, lo->owner, lo->owner_len, published_lock_owner);
            for (ls = lo->states; ls; ls = ls->next_in_owner) {
                owner_states++;
            }
            shard = &table->shards[lock_state->shard];
            evpl_rwlock_wrlock(&shard->lock);
            if (published_lock_owner != lo ||
                atomic_load_explicit(&lock_state->destroyed, memory_order_acquire) ||
                atomic_load_explicit(&lock_state->compound_reserved, memory_order_acquire) ||
                atomic_load_explicit(&lock_state->refcount, memory_order_acquire) != 1 ||
                (atomic_load_explicit(&lo->compound_pins, memory_order_acquire) && lo->compound_group != cookie) ||
                atomic_load_explicit(&lo->refcount, memory_order_acquire) != 1 + owner_states) {
                status = NFS4ERR_DELAY;
            } else {
                atomic_fetch_add_explicit(&lock_state->refcount, 1, memory_order_relaxed);
                lock_state->compound_group = cookie;
                atomic_store_explicit(&lock_state->compound_reserved, 1, memory_order_release);
                lo->compound_group = cookie;
                atomic_fetch_add_explicit(&lo->compound_pins, 1, memory_order_release);
                reservation->locks[reservation->num_locks++] = lock_state;
            }
            evpl_rwlock_unlock(&shard->lock);
            evpl_mutex_unlock(&lo->lock);
            if (status != NFS4_OK) {
                break;
            }
        }
        if (status != NFS4_OK) {
            break;
        }
    }
    for (uint32_t i = 0; status == NFS4_OK && i < num_candidates; i++) {
        state = calloc(1, sizeof(*state));
        if (!state) {
            status = NFS4ERR_RESOURCE;
            break;
        }
        if (nfs_state_table_alloc(table, NFS4_SLOT_TYPE_RESERVED, &state->shard,
                                  &state->slot_idx, &state->generation) != 0) {
            free(state);
            status = NFS4ERR_RESOURCE;
            break;
        }
        nfs_open_owner_get(owner);
        state->owner                     = owner;
        state->principal_flavor          = principal_flavor;
        state->principal_machinename_len = principal_len > NFS4_OPAQUE_LIMIT ? NFS4_OPAQUE_LIMIT : principal_len;
        if (principal_name && state->principal_machinename_len) {
            memcpy(state->principal_machinename, principal_name, state->principal_machinename_len);
        }
        state->seqid = 1;
        atomic_init(&state->refcount, 1);
        atomic_init(&state->destroyed, 0);
        atomic_init(&state->compound_closing, 0);
        atomic_init(&state->compound_reserved, 1);
        state->compound_reservation                            = reservation;
        reservation->candidates[reservation->num_candidates++] = state;
    }
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    if (status != NFS4_OK) {
        nfs_open_owner_finish_compound(reservation, table, vfs_thread);
        return status;
    }
    *out_reservation = reservation;
    return NFS4_OK;
} /* nfs_open_owner_reserve_compound_internal */

nfsstat4
nfs_open_owner_reserve_compound(
    struct nfs_client                  *client,
    const void                         *owner_bytes,
    uint16_t                            owner_len,
    uint32_t                            principal_flavor,
    const char                         *principal_name,
    uint32_t                            principal_len,
    const void                         *cookie,
    uint32_t                            num_candidates,
    struct nfs_state_table             *table,
    struct chimera_vfs_thread          *vfs_thread,
    struct nfs_open_owner_reservation **out_reservation)
{
    return nfs_open_owner_reserve_compound_internal(client, owner_bytes, owner_len, principal_flavor,
                                                    principal_name, principal_len, cookie, num_candidates, table,
                                                    vfs_thread, out_reservation, false);
} /* nfs_open_owner_reserve_compound */

nfsstat4
nfs_open_owner_reserve_compound_locks(
    struct nfs_client                  *client,
    const void                         *owner_bytes,
    uint16_t                            owner_len,
    uint32_t                            principal_flavor,
    const char                         *principal_name,
    uint32_t                            principal_len,
    const void                         *cookie,
    uint32_t                            num_candidates,
    struct nfs_state_table             *table,
    struct chimera_vfs_thread          *vfs_thread,
    struct nfs_open_owner_reservation **out_reservation)
{
    return nfs_open_owner_reserve_compound_internal(client, owner_bytes, owner_len, principal_flavor,
                                                    principal_name, principal_len, cookie, num_candidates, table,
                                                    vfs_thread, out_reservation, true);
} /* nfs_open_owner_reserve_compound_locks */

static int
nfs_open_owner_candidate_index(
    const struct nfs_open_owner_reservation *reservation,
    const struct nfs_open_state             *state)
{
    for (uint32_t i = 0; i < reservation->num_candidates; i++) {
        if (reservation->candidates[i] == state) {
            return (int) i;
        }
    }
    return -1;
} /* nfs_open_owner_candidate_index */

void
nfs_open_owner_apply_compound(
    struct nfs_open_owner_reservation           *reservation,
    struct nfs_open_state                       *target,
    const struct nfs_open_state_compound_update *update,
    struct nfs_state_table                      *table,
    struct chimera_vfs_thread                   *vfs_thread)
{
    struct nfs_open_owner          *owner  = reservation->owner;
    struct nfs_client              *client = owner->client;
    struct chimera_vfs_open_handle *old_handle;
    struct chimera_vfs_file_state  *old_file;
    int                             candidate      = nfs_open_owner_candidate_index(reservation, target);
    bool                            publish        = candidate >= 0 && !reservation->candidate_published[candidate];
    bool                            claim_narrowed = false;

    chimera_nfs_abort_if(target->compound_reservation != reservation ||
                         !atomic_load_explicit(&target->compound_reserved, memory_order_acquire) ||
                         atomic_load_explicit(&target->destroyed, memory_order_acquire) ||
                         !update->fh || !update->fh_len || update->fh_len > NFS4_FHSIZE ||
                         (!publish && (target->fh_len != update->fh_len ||
                                       memcmp(target->fh, update->fh, update->fh_len))),
                         "invalid compound OPEN publication target");
    old_file = publish ? NULL : target->share_file_state;
    chimera_nfs_abort_if((target->share_claim_held && old_file != update->claim_file) ||
                         (!!update->claim_file != !!update->admitted_claim),
                         "compound OPEN claim file changed");
    if (update->admitted_claim) {
        claim_narrowed = chimera_vfs_claim_replace_deferred(update->claim_file, &target->share_claim,
                                                            update->admitted_claim);
    } else {
        chimera_nfs_abort_if(target->share_claim_held, "compound OPEN discarded its share claim");
    }

    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_reservation != reservation,
                         "compound OPEN lost owner reservation");
    old_handle               = publish ? NULL : target->handle;
    target->handle           = update->handle;
    target->share_file_state = update->claim_file;
    target->share_claim_held = update->admitted_claim != NULL;
    target->share_access     = update->share_access;
    target->share_deny       = update->share_deny;
    target->share_combos     = update->share_combos;
    target->seqid            = update->seqid;
    if (publish) {
        struct nfs_open_state *existing;
        HASH_FIND(hh, owner->states_by_fh, update->fh, update->fh_len, existing);
        chimera_nfs_abort_if(existing, "compound OPEN duplicate file publication");
        memcpy(target->fh, update->fh, update->fh_len);
        target->fh_len = update->fh_len;
        atomic_fetch_add_explicit(&target->refcount, 1, memory_order_relaxed);
        nfs_state_table_install(table, target->shard, target->slot_idx, NFS4_SLOT_TYPE_OPEN, target);
        HASH_ADD_KEYPTR(hh, owner->states_by_fh, target->fh, target->fh_len, target);
        reservation->candidate_published[candidate] = true;
    }
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    if (claim_narrowed) {
        chimera_vfs_claim_replacement_complete(update->claim_file);
    }
    if (old_handle && vfs_thread) {
        chimera_vfs_release(vfs_thread, old_handle);
    }
    if (old_file && vfs_thread) {
        chimera_vfs_state_put(vfs_thread->vfs->vfs_state, old_file);
    }
} /* nfs_open_owner_apply_compound */

void
nfs_open_owner_close_compound(
    struct nfs_open_owner_reservation *reservation,
    struct nfs_open_state             *target,
    uint32_t                           final_seqid,
    struct nfs_state_table            *table,
    struct chimera_vfs_thread         *vfs_thread)
{
    struct nfs_open_owner *owner     = reservation->owner;
    struct nfs_client     *client    = owner->client;
    int                    candidate = nfs_open_owner_candidate_index(reservation, target);

    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_reservation != reservation ||
                         target->compound_reservation != reservation ||
                         !atomic_load_explicit(&target->compound_reserved, memory_order_acquire) ||
                         atomic_load_explicit(&target->destroyed, memory_order_acquire) ||
                         (candidate >= 0 && !reservation->candidate_published[candidate]),
                         "invalid compound CLOSE publication target");
    target->seqid = final_seqid;
    open_state_destroy_locked(owner, target, table, vfs_thread, false);
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
} /* nfs_open_owner_close_compound */

void
nfs_open_owner_finish_compound(
    struct nfs_open_owner_reservation *reservation,
    struct nfs_state_table            *table,
    struct chimera_vfs_thread         *vfs_thread)
{
    struct nfs_open_owner *owner  = reservation->owner;
    struct nfs_client     *client = owner->client;
    bool                   destroy;

    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_reservation != reservation || !client->compound_pins,
                         "compound owner reservation underflow");
    for (uint32_t i = 0; i < reservation->num_locks; i++) {
        struct nfs_lock_state  *ls    = reservation->locks[i];
        struct nfs_lock_owner  *lo    = ls->lock_owner;
        struct nfs_state_shard *shard = &table->shards[ls->shard];
        evpl_mutex_lock(&lo->lock);
        evpl_rwlock_wrlock(&shard->lock);
        ls->compound_group = NULL;
        atomic_store_explicit(&ls->compound_reserved, 0, memory_order_release);
        if (atomic_fetch_sub_explicit(&lo->compound_pins, 1, memory_order_acq_rel) == 1) {
            lo->compound_group = NULL;
        }
        evpl_rwlock_unlock(&shard->lock);
        evpl_mutex_unlock(&lo->lock);
    }
    for (uint32_t i = 0; i < reservation->num_existing + reservation->num_candidates; i++) {
        struct nfs_open_state  *state = i < reservation->num_existing ? reservation->existing[i] :
            reservation->candidates[i - reservation->num_existing];
        struct nfs_state_shard *shard = &table->shards[state->shard];
        evpl_rwlock_wrlock(&shard->lock);
        state->compound_reservation = NULL;
        atomic_store_explicit(&state->compound_reserved, 0, memory_order_release);
        evpl_rwlock_unlock(&shard->lock);
    }
    owner->compound_reservation = NULL;
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    for (uint32_t i = 0; i < reservation->num_locks; i++) {
        nfs_state_table_release(table, reservation->locks[i], NFS4_SLOT_TYPE_LOCK, vfs_thread);
    }
    for (uint32_t i = 0; i < reservation->num_existing; i++) {
        nfs_state_table_release(table, reservation->existing[i], NFS4_SLOT_TYPE_OPEN, vfs_thread);
    }
    for (uint32_t i = 0; i < reservation->num_candidates; i++) {
        struct nfs_open_state *state = reservation->candidates[i];
        if (reservation->candidate_published[i]) {
            nfs_state_table_release(table, state, NFS4_SLOT_TYPE_OPEN, vfs_thread);
        } else {
            nfs_state_table_free_slot(table, state->shard, state->slot_idx);
            nfs_open_owner_put(owner);
            free(state);
        }
    }
    nfs_open_owner_put(owner);
    evpl_mutex_lock(&client->lock);
    client->compound_pins--;
    destroy = !client->compound_pins && client->compound_destroy_pending;
    evpl_mutex_unlock(&client->lock);
    free(reservation);
    if (destroy) {
        nfs_client_destroy(client, table, vfs_thread, false);
    }
} /* nfs_open_owner_finish_compound */

struct nfs_open_state *
nfs_open_state_reserve_compound(
    struct nfs_client      *client,
    const void             *owner_bytes,
    uint16_t                owner_len,
    uint32_t                principal_flavor,
    const char             *principal_name,
    uint32_t                principal_len,
    uint32_t                access,
    uint32_t                deny,
    struct nfs_state_table *table,
    struct stateid4        *stateid)
{
    struct nfs_open_owner *owner, *published;
    struct nfs_open_state *state;
    bool                   created;

    owner = nfs_open_owner_find_or_create(client, owner_bytes, owner_len, &created);
    state = calloc(1, sizeof(*state));
    if (!state) {
        nfs_open_owner_put(owner);
        return NULL;
    }
    if (nfs_state_table_alloc(table, NFS4_SLOT_TYPE_RESERVED, &state->shard,
                              &state->slot_idx, &state->generation) != 0) {
        free(state);
        nfs_open_owner_put(owner);
        return NULL;
    }
    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    HASH_FIND(hh, client->open_owners_by_str, owner->owner, owner->owner_len, published);
    if (published != owner || owner->states_by_fh || owner->compound_pending || owner->compound_close_count ||
        owner->compound_reservation ||
        client->expired || client->compound_destroy_pending ||
        atomic_load_explicit(&client->reclaim_pending, memory_order_relaxed)) {
        evpl_mutex_unlock(&owner->lock);
        evpl_mutex_unlock(&client->lock);
        nfs_state_table_free_slot(table, state->shard, state->slot_idx);
        free(state);
        nfs_open_owner_put(owner);
        return NULL;
    }
    state->owner            = owner; /* transfer the construction reference */
    state->principal_flavor = principal_flavor;
    if (principal_len > NFS4_OPAQUE_LIMIT) {
        principal_len = NFS4_OPAQUE_LIMIT;
    }
    state->principal_machinename_len = principal_len;
    if (principal_name && principal_len) {
        memcpy(state->principal_machinename, principal_name, principal_len);
    }
    state->share_access = access;
    state->share_deny   = deny;
    state->share_combos = nfs_open_combo_bit(access, deny);
    state->seqid        = 1;
    atomic_init(&state->refcount, 1);
    atomic_init(&state->destroyed, 0);
    owner->compound_pending = state;
    atomic_init(&state->compound_closing, 0);
    atomic_init(&state->compound_reserved, 0);
    client->compound_pins++;
    nfs4_stateid_encode(stateid, 1, NFS4_STATEID_TYPE_OPEN, state->shard,
                        state->slot_idx, state->generation, table->epoch);
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    return state;
} /* nfs_open_state_reserve_compound */

void
nfs_open_state_publish_compound(
    struct nfs_open_state  *state,
    struct nfs_state_table *table)
{
    struct nfs_open_owner *owner  = state->owner;
    struct nfs_client     *client = owner->client;

    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_pending != state || owner->states_by_fh,
                         "compound OPEN lost its publication reservation");
    /* Keep a construction reference until finish_compound even if another
     * worker destroys the newly published state before reply assembly ends. */
    atomic_fetch_add_explicit(&state->refcount, 1, memory_order_relaxed);
    nfs_state_table_install(table, state->shard, state->slot_idx,
                            NFS4_SLOT_TYPE_OPEN, state);
    HASH_ADD_KEYPTR(hh, owner->states_by_fh, state->fh, state->fh_len, state);
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
} /* nfs_open_state_publish_compound */

void
nfs_open_state_finish_compound(
    struct nfs_open_state     *state,
    bool                       published,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_open_owner *owner  = state->owner;
    struct nfs_client     *client = owner->client;
    bool                   destroy;

    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_pending != state || !client->compound_pins,
                         "compound OPEN reservation underflow");
    owner->compound_pending = NULL;
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    /* A concurrently destroyed published state may lose its final reference
     * here. Claim release and handle cleanup can invoke callbacks, so retain
     * the client pin while releasing resources outside protocol locks. */
    if (published) {
        nfs_state_table_release(table, state, NFS4_SLOT_TYPE_OPEN, vfs_thread);
    } else {
        nfs_state_table_free_slot(table, state->shard, state->slot_idx);
        nfs_open_owner_put(owner);
        free(state);
    }
    evpl_mutex_lock(&client->lock);
    client->compound_pins--;
    destroy = !client->compound_pins && client->compound_destroy_pending;
    evpl_mutex_unlock(&client->lock);
    if (destroy) {
        nfs_client_destroy(client, table, vfs_thread, false);
    }
} /* nfs_open_state_finish_compound */

nfsstat4
nfs_open_state_reserve_close_compound(
    struct nfs_state_table    *table,
    const struct stateid4     *stateid,
    struct nfs_client         *client,
    const uint8_t             *fh,
    uint16_t                   fh_len,
    uint32_t                   principal_flavor,
    const char                *principal_name,
    uint32_t                   principal_len,
    const void                *reservation_group,
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_open_state    **out_state,
    struct stateid4           *out_reply_stateid)
{
    struct nfs_open_state  *state = NULL;
    struct nfs_open_owner  *owner, *published;
    struct nfs_state_shard *shard;
    void                   *found;
    uint8_t                 type;
    nfsstat4                status;

    *out_state = NULL;
    if (!reservation_group) {
        return NFS4ERR_INVAL;
    }
    if (!client || client->minor == 0) {
        return NFS4ERR_NOTSUPP;
    }
    if (!fh || !fh_len || fh_len > NFS4_FHSIZE) {
        return NFS4ERR_NOFILEHANDLE;
    }

    /* Session ownership supplies a live client; pin under its teardown lock
     * before allowing execution to leave this callback. The internal lookup
     * takes a state reference without the ordinary acquire's lease renewal. */
    evpl_mutex_lock(&client->lock);
    status = state_table_lookup_locked(table, stateid, NFS4_SLOT_TYPE_OPEN,
                                       true, &found, &type);
    if (status != NFS4_OK) {
        evpl_mutex_unlock(&client->lock);
        return status;
    }
    state = found;
    if (!state) {
        evpl_mutex_unlock(&client->lock);
        return NFS4ERR_BAD_STATEID;
    }
    owner = state->owner;
    if (owner->client != client || state->fh_len != fh_len ||
        memcmp(state->fh, fh, fh_len)) {
        status = NFS4ERR_BAD_STATEID;
        goto release;
    }
    if (!nfs_open_state_check_principal(state, principal_flavor,
                                        principal_name, principal_len)) {
        status = NFS4ERR_ACCESS;
        goto release;
    }
    evpl_mutex_lock(&owner->lock);
    shard = &table->shards[state->shard];
    evpl_rwlock_wrlock(&shard->lock);
    HASH_FIND(hh, client->open_owners_by_str, owner->owner, owner->owner_len, published);
    if (client->expired || client->compound_destroy_pending ||
        atomic_load_explicit(&client->reclaim_pending, memory_order_acquire)) {
        status = NFS4ERR_EXPIRED;
    } else if (published != owner || atomic_load_explicit(&state->destroyed, memory_order_acquire)) {
        status = NFS4ERR_BAD_STATEID;
    } else if (owner->compound_pending || owner->compound_reservation ||
               (owner->compound_close_count && owner->compound_close_group != reservation_group) ||
               state->locks ||
               atomic_load_explicit(&state->refcount, memory_order_acquire) != 2 ||
               atomic_load_explicit(&owner->refcount, memory_order_acquire) !=
               1 + HASH_COUNT(owner->states_by_fh)) {
        /* Slot + our reference must be the only state users. Legacy OPEN
         * coalesces via an owner borrow rather than a state acquire, so also
         * require only the owner hash and its states to hold owner references.
         * New OPEN callers then see the pending CLOSE group before coalescing. */
        status = NFS4ERR_DELAY;
    } else if ((status = nfs4_stateid_check_seqid(state->seqid, stateid->seqid)) == NFS4_OK) {
        atomic_store_explicit(&state->compound_closing, 1, memory_order_release);
        state->compound_close_group = reservation_group;
        owner->compound_close_group = reservation_group;
        owner->compound_close_count++;
        client->compound_pins++;
        nfs4_stateid_encode(out_reply_stateid, state->seqid + 1,
                            NFS4_STATEID_TYPE_OPEN, state->shard, state->slot_idx,
                            state->generation, table->epoch);
        *out_state = state;
    }
    evpl_rwlock_unlock(&shard->lock);
    evpl_mutex_unlock(&owner->lock);
 release:
    evpl_mutex_unlock(&client->lock);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state, NFS4_SLOT_TYPE_OPEN, vfs_thread);
    }
    return status;
} /* nfs_open_state_reserve_close_compound */

void
nfs_open_state_finish_close_compound(
    struct nfs_open_state     *state,
    bool                       accepted,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_open_owner  *owner  = state->owner;
    struct nfs_client      *client = owner->client;
    struct nfs_state_shard *shard  = &table->shards[state->shard];
    bool                    destroy;

    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(!owner->compound_close_count ||
                         owner->compound_close_group != state->compound_close_group ||
                         !atomic_load_explicit(&state->compound_closing, memory_order_acquire) ||
                         !client->compound_pins, "compound CLOSE reservation underflow");
    if (accepted) {
        /* Keep acquires excluded until the slot is removed. Destroy's own
         * shard write lock then makes public invalidation atomic to lookup. */
        state->seqid++;
        nfs_client_touch(client);
        open_state_destroy_locked(owner, state, table, vfs_thread, false);
    } else {
        evpl_rwlock_wrlock(&shard->lock);
        atomic_store_explicit(&state->compound_closing, 0, memory_order_release);
        evpl_rwlock_unlock(&shard->lock);
    }
    state->compound_close_group = NULL;
    if (!--owner->compound_close_count) {
        owner->compound_close_group = NULL;
    }
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);

    /* Final release can wake VFS claim waiters and close backend handles.
     * Keep the client pin, but drop protocol locks before those callbacks. */
    nfs_state_table_release(table, state, NFS4_SLOT_TYPE_OPEN, vfs_thread);

    evpl_mutex_lock(&client->lock);
    client->compound_pins--;
    destroy = !client->compound_pins && client->compound_destroy_pending;
    evpl_mutex_unlock(&client->lock);
    if (destroy) {
        nfs_client_destroy(client, table, vfs_thread, false);
    }
} /* nfs_open_state_finish_close_compound */

static bool
nfs_open_state_is_excluded(
    struct nfs_open_state        *state,
    struct nfs_open_state *const *closed,
    uint32_t                      num_closed)
{
    for (uint32_t i = 0; i < num_closed; i++) {
        if (state == closed[i]) {
            return true;
        }
    }
    return false;
} /* nfs_open_state_is_excluded */

nfsstat4
nfs_client_check_share_conflict_except(
    struct nfs_client            *client,
    struct nfs_open_owner        *requesting_owner,
    const uint8_t                *fh,
    uint16_t                      fh_len,
    uint32_t                      requested_access,
    uint32_t                      requested_deny,
    struct nfs_open_state *const *closed,
    uint32_t                      num_closed)
{
    struct nfs_open_owner *oo, *oo_tmp;
    nfsstat4               status = NFS4_OK;

    if (fh_len > NFS4_FHSIZE) {
        return NFS4ERR_BAD_STATEID;
    }

    evpl_mutex_lock(&client->lock);

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        struct nfs_open_state *peer = NULL;

        if (oo == requesting_owner) {
            /* Same-owner OPEN merges via coalesce; no conflict possible. */
            continue;
        }

        evpl_mutex_lock(&oo->lock);
        HASH_FIND(hh, oo->states_by_fh, fh, fh_len, peer);
        if (peer && !nfs_open_state_is_excluded(peer, closed, num_closed)) {
            /* RFC 7530 §9.10: SHARE_DENIED if my deny clashes with their
             * access OR their deny clashes with my access. */
            if ((peer->share_access & requested_deny) ||
                (requested_access & peer->share_deny)) {
                status = NFS4ERR_SHARE_DENIED;
            }
        }
        evpl_mutex_unlock(&oo->lock);

        if (status != NFS4_OK) {
            break;
        }
    }

    evpl_mutex_unlock(&client->lock);
    return status;
} /* nfs_client_check_share_conflict_except */

nfsstat4
nfs_client_check_io_denied_except(
    struct nfs_client            *client,
    struct nfs_open_owner        *requesting_owner,
    const uint8_t                *fh,
    uint16_t                      fh_len,
    uint32_t                      requested_access,
    struct nfs_open_state *const *closed,
    uint32_t                      num_closed)
{
    struct nfs_open_owner *oo, *oo_tmp;
    nfsstat4               status = NFS4_OK;

    if (fh_len > NFS4_FHSIZE) {
        return NFS4ERR_BAD_STATEID;
    }

    evpl_mutex_lock(&client->lock);

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        struct nfs_open_state *peer = NULL;

        if (oo == requesting_owner) {
            continue;
        }

        evpl_mutex_lock(&oo->lock);
        HASH_FIND(hh, oo->states_by_fh, fh, fh_len, peer);
        if (peer && !nfs_open_state_is_excluded(peer, closed, num_closed) &&
            (peer->share_deny & requested_access)) {
            status = NFS4ERR_LOCKED;
        }
        evpl_mutex_unlock(&oo->lock);

        if (status != NFS4_OK) {
            break;
        }
    }

    evpl_mutex_unlock(&client->lock);
    return status;
} /* nfs_client_check_io_denied_except */

nfsstat4
nfs_client_check_share_conflict(
    struct nfs_client     *client,
    struct nfs_open_owner *requesting_owner,
    const uint8_t         *fh,
    uint16_t               fh_len,
    uint32_t               requested_access,
    uint32_t               requested_deny)
{
    return nfs_client_check_share_conflict_except(client, requesting_owner, fh, fh_len,
                                                  requested_access, requested_deny, NULL, 0);
} /* nfs_client_check_share_conflict */

nfsstat4
nfs_client_check_io_denied(
    struct nfs_client     *client,
    struct nfs_open_owner *requesting_owner,
    const uint8_t         *fh,
    uint16_t               fh_len,
    uint32_t               requested_access)
{
    return nfs_client_check_io_denied_except(client, requesting_owner, fh, fh_len,
                                             requested_access, NULL, 0);
} /* nfs_client_check_io_denied */

bool
nfs_client_has_open_state_for_fh_except(
    struct nfs_client            *client,
    const uint8_t                *fh,
    uint16_t                      fh_len,
    struct nfs_open_state *const *closed,
    uint32_t                      num_closed)
{
    struct nfs_open_owner *oo, *oo_tmp;
    bool                   found = false;

    if (fh_len > NFS4_FHSIZE) {
        return false;
    }

    evpl_mutex_lock(&client->lock);

    HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
    {
        struct nfs_open_state *state = NULL;

        evpl_mutex_lock(&oo->lock);
        HASH_FIND(hh, oo->states_by_fh, fh, fh_len, state);
        found = state && !nfs_open_state_is_excluded(state, closed, num_closed);
        evpl_mutex_unlock(&oo->lock);

        if (found) {
            break;
        }
    }

    evpl_mutex_unlock(&client->lock);
    return found;
} /* nfs_client_has_open_state_for_fh_except */

bool
nfs_client_has_open_state_for_fh(
    struct nfs_client *client,
    const uint8_t     *fh,
    uint16_t           fh_len)
{
    return nfs_client_has_open_state_for_fh_except(client, fh, fh_len, NULL, 0);
} /* nfs_client_has_open_state_for_fh */

/* Does this client hold any leased state at all -- opens, locks, delegations
 * or layouts?  DESTROY_CLIENTID (RFC 8881 §18.50.3) must answer
 * NFS4ERR_CLIENTID_BUSY while any of it remains.
 *
 * The owner hashes are deliberately NOT the test: an open/lock owner record
 * outlives its last CLOSE/LOCKU (it caches the 4.0 seqid and replay slot), so
 * a non-empty owner hash does not mean the client still holds state.  Walk
 * down to the per-owner state containers instead.
 *
 * Lock order is client->lock then owner->lock, matching
 * nfs_client_has_open_state_for_fh; callers may hold nfs4_ct_lock, as
 * nfs4_clients_check_io_denied already does. */
bool
nfs_client_has_leased_state(struct nfs_client *client)
{
    struct nfs_open_owner *oo, *oo_tmp;
    struct nfs_lock_owner *lo, *lo_tmp;
    bool                   found;

    evpl_mutex_lock(&client->lock);

    found = (client->delegations != NULL) || (client->layouts_by_fh != NULL);

    if (!found) {
        HASH_ITER(hh, client->open_owners_by_str, oo, oo_tmp)
        {
            evpl_mutex_lock(&oo->lock);
            found = (oo->states_by_fh != NULL);
            evpl_mutex_unlock(&oo->lock);

            if (found) {
                break;
            }
        }
    }

    if (!found) {
        HASH_ITER(hh, client->lock_owners_by_str, lo, lo_tmp)
        {
            evpl_mutex_lock(&lo->lock);
            found = (lo->states != NULL);
            evpl_mutex_unlock(&lo->lock);

            if (found) {
                break;
            }
        }
    }

    evpl_mutex_unlock(&client->lock);
    return found;
} /* nfs_client_has_leased_state */

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
    state->share_access |= share_access;
    state->share_deny   |= share_deny;
    state->share_combos |= nfs_open_combo_bit(share_access, share_deny);
    state->seqid        += 1;

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
    if (vfs_state && state->share_claim_held) {
        chimera_vfs_claim_release(vfs_state, state->share_file_state,
                                  &state->share_claim);
        state->share_claim_held = false;
    }
    if (vfs_state && state->share_file_state) {
        chimera_vfs_state_put(vfs_state, state->share_file_state);
        state->share_file_state = NULL;
    }

    /* Drop the base-file stream holder taken for a named-attribute open. */
    if (vfs_state && state->base_stream_file_state) {
        chimera_vfs_state_stream_holder_dec(state->base_stream_file_state);
        chimera_vfs_state_put(vfs_state, state->base_stream_file_state);
        state->base_stream_file_state = NULL;
    }

    if (state->handle && vfs_thread) {
        chimera_vfs_release(vfs_thread, state->handle);
        state->handle = NULL;
    }
    /* The read-only handle a write-share coalesce superseded (parked so
     * in-flight I/O borrowing it stayed valid). */
    if (state->handle_superseded && vfs_thread) {
        chimera_vfs_release(vfs_thread, state->handle_superseded);
        state->handle_superseded = NULL;
    }
    /* Release the owner ref taken in nfs_open_state_create. */
    nfs_open_owner_put(state->owner);
    free(state);
} /* open_state_cleanup */

void
nfs_open_state_destroy(
    struct nfs_open_state     *state,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_open_owner *owner = state->owner;

    evpl_mutex_lock(&owner->lock);
    /* A single open being closed/rolled back -- its stateid becomes invalid
     * (free), not expired. */
    if (!atomic_load_explicit(&state->compound_closing, memory_order_acquire) &&
        !atomic_load_explicit(&state->compound_reserved, memory_order_acquire)) {
        open_state_destroy_locked(owner, state, table, vfs_thread, false);
    }
    evpl_mutex_unlock(&owner->lock);
} /* nfs_open_state_destroy */

/* --- pNFS layout state ------------------------------------------------- */

struct nfs_layout_state *
nfs_layout_state_find(
    struct nfs_client *client,
    const uint8_t     *fh,
    uint16_t           fh_len)
{
    struct nfs_layout_state *st;

    evpl_mutex_lock(&client->lock);
    HASH_FIND(hh, client->layouts_by_fh, fh, fh_len, st);
    evpl_mutex_unlock(&client->lock);

    return st;
} /* nfs_layout_state_find */

static struct nfs_layout_state *
layout_state_create_locked(
    struct nfs_client       *client,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    uint16_t                 export_id,
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

    st->client    = client;
    st->client_id = client->client_id;
    atomic_fetch_add_explicit(&client->refcount, 1, memory_order_relaxed);
    memcpy(st->fh, fh, fh_len);
    st->fh_len       = fh_len;
    st->export_id    = export_id;
    st->seqid        = 1;
    st->iomode       = iomode;
    st->shard        = shard;
    st->slot_idx     = slot_idx;
    st->generation   = gen;
    st->global_table = layout_table;
    atomic_init(&st->refcount, 1);
    atomic_init(&st->destroyed, 0);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_LAYOUT, st);

    HASH_ADD_KEYPTR(hh, client->layouts_by_fh, st->fh, st->fh_len, st);

    /* Publish into the server-wide fh->holder index so a conflicting op from
     * any client can find and recall this layout. */
    nfs_layout_table_register(layout_table, st);

    nfs4_stateid_encode(out_stateid, st->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        shard, slot_idx, gen, table->epoch);
    return st;
} /* layout_state_create_locked */

struct nfs_layout_state *
nfs_layout_state_create(
    struct nfs_client       *client,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    uint16_t                 export_id,
    uint32_t                 iomode,
    uint32_t                 client_short_id,
    struct nfs_state_table  *table,
    struct nfs_layout_table *layout_table,
    struct stateid4         *out_stateid)
{
    evpl_mutex_lock(&client->lock);
    struct nfs_layout_state *layout = layout_state_create_locked(client, fh, fh_len,
                                                                 export_id, iomode, client_short_id, table, layout_table
                                                                 , out_stateid);
    evpl_mutex_unlock(&client->lock);
    return layout;
} /* nfs_layout_state_create */

static nfsstat4
layout_state_check_locked(
    struct nfs_layout_state *layout,
    const struct stateid4   *sid,
    struct nfs_state_table  *table)
{
    struct nfs4_stateid_view view;
    struct stateid4          current;

    if (nfs4_stateid_is_special(sid)) {
        return NFS4ERR_BAD_STATEID;
    }
    nfs4_stateid_decode(&view, sid);
    if (view.type != NFS4_STATEID_TYPE_LAYOUT) {
        return NFS4_OK; /* Initial OPEN/LOCK/DELEG identity checked by caller. */
    }
    if (!layout) {
        return NFS4ERR_BAD_STATEID;
    }
    nfs4_stateid_encode(&current, layout->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        layout->shard, layout->slot_idx, layout->generation, table->epoch);
    if (memcmp(current.other, sid->other, sizeof(sid->other)) || sid->seqid > current.seqid) {
        return NFS4ERR_BAD_STATEID;
    }
    return NFS4_OK; /* Pipelined LAYOUTGET may carry an earlier issued seqid. */
} /* layout_state_check_locked */

nfsstat4
nfs_layout_state_check(
    struct nfs_client      *client,
    const uint8_t          *fh,
    uint16_t                fh_len,
    const struct stateid4  *sid,
    struct nfs_state_table *table)
{
    struct nfs_layout_state *layout;

    evpl_mutex_lock(&client->lock);
    HASH_FIND(hh, client->layouts_by_fh, fh, fh_len, layout);
    nfsstat4                 status = layout_state_check_locked(layout, sid, table);
    evpl_mutex_unlock(&client->lock);
    return status;
} /* nfs_layout_state_check */

nfsstat4
nfs_layout_state_grant(
    struct nfs_client       *client,
    const uint8_t           *fh,
    uint16_t                 fh_len,
    uint16_t                 export_id,
    uint32_t                 iomode,
    uint32_t                 layout_type,
    const struct stateid4   *input,
    struct nfs_state_table  *table,
    struct nfs_layout_table *layout_table,
    struct stateid4         *output)
{
    struct nfs_layout_state *layout;

    evpl_mutex_lock(&client->lock);
    HASH_FIND(hh, client->layouts_by_fh, fh, fh_len, layout);
    nfsstat4                 status = layout_state_check_locked(layout, input, table);
    if (status == NFS4_OK) {
        if (layout) {
            if (iomode == LAYOUTIOMODE4_RW) {
                layout->iomode = iomode;
            }
            nfs_layout_state_bump(layout, table->epoch, output);
        } else {
            layout = layout_state_create_locked(client, fh, fh_len, export_id, iomode,
                                                0, table, layout_table, output);
        }
        layout->layout_type = layout_type;
    }
    evpl_mutex_unlock(&client->lock);
    return status;
} /* nfs_layout_state_grant */

void
nfs_layout_state_bump(
    struct nfs_layout_state *st,
    uint32_t                 epoch,
    struct stateid4         *out_stateid)
{
    /* RFC 8881 §12.5.3: the server owns the layout stateid seqid and advances
     * it on each successful LAYOUTGET / LAYOUTRETURN. */
    st->seqid++;
    nfs4_stateid_encode(out_stateid, st->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        st->shard, st->slot_idx, st->generation,
                        epoch);
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
        nfs_client_memory_put(st->client);
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
    uint32_t prev = atomic_fetch_sub_explicit(&st->refcount, 1, memory_order_acq_rel);

    chimera_nfs_abort_if(prev == 0, "layout_state refcount underflow on %p", st);
    if (prev == 1) {
        nfs_client_memory_put(st->client);
        free(st);
    }
} /* nfs_layout_state_put */

struct nfs_client *
nfs_layout_state_reserve_client(struct nfs_layout_state *st)
{
    struct nfs_client *client   = st->client;
    bool               reserved = false;

    evpl_mutex_lock(&client->lock);
    if (!client->teardown_started && !atomic_load_explicit(&st->destroyed, memory_order_acquire)) {
        atomic_fetch_add(&client->compound_pins, 1);
        reserved = true;
    }
    evpl_mutex_unlock(&client->lock);
    return reserved ? client : NULL;
} /* nfs_layout_state_reserve_client */

void
nfs_layout_state_destroy(
    struct nfs_layout_state   *st,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_client *client = st->client;

    evpl_mutex_lock(&client->lock);
    layout_state_destroy_locked(client, st, table, vfs_thread);
    evpl_mutex_unlock(&client->lock);
} /* nfs_layout_state_destroy */

nfsstat4
nfs_layout_state_return_file(
    struct nfs_client         *client,
    const uint8_t             *fh,
    uint16_t                   fh_len,
    const struct stateid4     *sid,
    uint32_t                   layout_type,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_layout_state *layout;
    struct stateid4          current;
    nfsstat4                 status = NFS4_OK;

    evpl_mutex_lock(&client->lock);
    HASH_FIND(hh, client->layouts_by_fh, fh, fh_len, layout);
    if (layout) {
        nfs_layout_state_get(layout);
        nfs4_stateid_encode(&current, layout->seqid, NFS4_STATEID_TYPE_LAYOUT,
                            layout->shard, layout->slot_idx, layout->generation, table->epoch);
        if (!sid->seqid || memcmp(sid->other, current.other, sizeof(sid->other))) {
            status = NFS4ERR_BAD_STATEID;
        } else if (sid->seqid < current.seqid) {
            status = NFS4ERR_OLD_STATEID;
        } else if (sid->seqid > current.seqid) {
            status = NFS4ERR_BAD_STATEID;
        } else if (layout->layout_type && layout_type != layout->layout_type) {
            status = NFS4ERR_BADLAYOUT;
        } else {
            layout_state_destroy_locked(client, layout, table, vfs_thread);
        }
    }
    evpl_mutex_unlock(&client->lock);
    if (layout) {
        nfs_layout_state_put(layout);
    }
    return status;
} /* nfs_layout_state_return_file */

void
nfs_layout_state_destroy_all(
    struct nfs_client         *client,
    struct nfs_state_table    *table,
    struct chimera_vfs_thread *vfs_thread)
{
    evpl_mutex_lock(&client->lock);

    /* The uthash delete-during-iteration idiom trips scan-build's
     * use-after-free checker; guard it the way the client teardown above
     * guards the identical loop. */
#ifndef __clang_analyzer__
    struct nfs_layout_state *ly, *ly_tmp;

    HASH_ITER(hh, client->layouts_by_fh, ly, ly_tmp)
    {
        layout_state_destroy_locked(client, ly, table, vfs_thread);
    }
#endif /* ifndef __clang_analyzer__ */

    evpl_mutex_unlock(&client->lock);
} /* nfs_layout_state_destroy_all */

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

    evpl_mutex_lock(&client->lock);

    HASH_FIND(hh, client->lock_owners_by_str, owner_bytes, owner_len, owner);

    if (owner) {
        if (out_created) {
            *out_created = false;
        }
        /* Take the caller ref while still holding client->lock. */
        nfs_lock_owner_get(owner);
        evpl_mutex_unlock(&client->lock);
        return owner;
    }

    owner = calloc(1, sizeof(*owner));
    chimera_nfs_abort_if(owner == NULL, "lock_owner alloc OOM");
    owner->client = client;
    memcpy(owner->owner, owner_bytes, owner_len);
    owner->owner_len = owner_len;
    owner->seqid     = 0;
    evpl_mutex_init(&owner->lock, NULL);
    /* refcount 2: one for the hash-table slot, one for the caller. */
    atomic_init(&owner->refcount, 2);
    atomic_init(&owner->compound_pins, 0);

    HASH_ADD_KEYPTR(hh, client->lock_owners_by_str,
                    owner->owner, owner->owner_len, owner);

    if (out_created) {
        *out_created = true;
    }
    evpl_mutex_unlock(&client->lock);
    return owner;
} /* nfs_lock_owner_find_or_create */

void
nfs_lock_owner_get(struct nfs_lock_owner *owner)
{
    uint32_t prev = atomic_fetch_add_explicit(&owner->refcount, 1,
                                              memory_order_acq_rel);

    chimera_nfs_abort_if(prev == 0, "lock_owner get after free on %p", owner);
} /* nfs_lock_owner_get */

void
nfs_lock_owner_put(struct nfs_lock_owner *owner)
{
    uint32_t prev = atomic_fetch_sub_explicit(&owner->refcount, 1,
                                              memory_order_acq_rel);

    chimera_nfs_abort_if(prev == 0, "lock_owner refcount underflow on %p",
                         owner);
    if (prev == 1) {
        evpl_mutex_destroy(&owner->lock);
        free(owner);
    }
} /* nfs_lock_owner_put */

nfsstat4
nfs_lock_owner_compound_parents(
    struct nfs_client      *client,
    const void             *owner_bytes,
    uint16_t                owner_len,
    const void             *cookie,
    struct nfs_state_table *table,
    struct stateid4        *out,
    uint32_t                capacity,
    uint32_t               *count)
{
    struct nfs_lock_owner *owner;
    struct nfs_lock_state *state;
    nfsstat4               status = NFS4_OK;

    *count = 0;
    if (!client || !cookie || owner_len > NFS4_OPAQUE_LIMIT) {
        return NFS4ERR_INVAL;
    }
    evpl_mutex_lock(&client->lock);
    HASH_FIND(hh, client->lock_owners_by_str, owner_bytes, owner_len, owner);
    if (!owner) {
        evpl_mutex_unlock(&client->lock);
        return NFS4_OK;
    }
    evpl_mutex_lock(&owner->lock);
    for (state = owner->states; state; state = state->next_in_owner) {
        struct nfs_open_state *parent = state->open_state;
        struct stateid4        sid;
        bool                   duplicate = false;
        if (atomic_load(&state->destroyed) || !parent || atomic_load(&parent->destroyed)) {
            status = NFS4ERR_DELAY;
            break;
        }
        if (parent->compound_reservation && parent->compound_reservation->cookie == cookie) {
            continue;
        }
        nfs4_stateid_encode(&sid, 0, NFS4_STATEID_TYPE_OPEN, parent->shard,
                            parent->slot_idx, parent->generation, table->epoch);
        for (uint32_t i = 0; i < *count; i++) {
            duplicate |= !memcmp(out[i].other, sid.other, sizeof(sid.other));
        }
        if (duplicate) {
            continue;
        }
        if (*count == capacity) {
            status = NFS4ERR_RESOURCE;
            break;
        }
        out[(*count)++] = sid;
    }
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    return status;
} /* nfs_lock_owner_compound_parents */

nfsstat4
nfs_lock_owner_reserve_compound(
    struct nfs_client                  *client,
    const void                         *owner_bytes,
    uint16_t                            owner_len,
    const void                         *cookie,
    uint32_t                            num_candidates,
    struct nfs_state_table             *table,
    struct chimera_vfs_thread          *vfs_thread,
    struct nfs_lock_owner_reservation **out_reservation)
{
    struct nfs_lock_owner_reservation *reservation;
    struct nfs_lock_owner             *owner, *published;
    struct nfs_lock_state             *state;
    uint32_t                           count = 0;
    bool                               created;
    nfsstat4                           status = NFS4_OK;

    *out_reservation = NULL;
    if (!client) {
        return NFS4ERR_NOTSUPP;
    }
    if (!cookie || num_candidates > NFS4_COMPOUND_OWNER_MAX_STATES) {
        return NFS4ERR_INVAL;
    }
    reservation = calloc(1, sizeof(*reservation));
    if (!reservation) {
        return NFS4ERR_RESOURCE;
    }
    owner               = nfs_lock_owner_find_or_create(client, owner_bytes, owner_len, &created);
    reservation->owner  = owner;
    reservation->cookie = cookie;
    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    HASH_FIND(hh, client->lock_owners_by_str, owner->owner, owner->owner_len, published);
    if (client->expired || client->compound_destroy_pending || atomic_load(&client->reclaim_pending)) {
        status = NFS4ERR_EXPIRED;
    } else if (published != owner ||
               (atomic_load(&owner->compound_pins) && owner->compound_group != cookie)) {
        status = NFS4ERR_DELAY;
    }
    for (state = owner->states; status == NFS4_OK && state; state = state->next_in_owner) {
        count++;
        /* Parent OPEN groups own these pins and must outlive this journal. */
        if (count > NFS4_COMPOUND_OWNER_MAX_STATES ||
            !atomic_load(&state->compound_reserved) || state->compound_group != cookie ||
            atomic_load(&state->refcount) != 2 || atomic_load(&state->destroyed)) {
            status = NFS4ERR_DELAY;
            break;
        }
        reservation->existing[reservation->num_existing++] = state;
    }
    if (status == NFS4_OK && atomic_load(&owner->refcount) != 2 + count) {
        status = NFS4ERR_DELAY;
    }
    if (status != NFS4_OK) {
        evpl_mutex_unlock(&owner->lock);
        evpl_mutex_unlock(&client->lock);
        nfs_lock_owner_put(owner);
        free(reservation);
        return status;
    }
    owner->compound_group                    = cookie;
    reservation->owner_replay.initial_seqid  = owner->seqid;
    reservation->owner_replay.initial_replay = owner->replay;
    nfs4_owner_replay_reset(&reservation->owner_replay);
    atomic_fetch_add(&owner->compound_pins, 1);
    client->compound_pins++;
    for (uint32_t i = 0; i < num_candidates; i++) {
        state = calloc(1, sizeof(*state));
        if (!state || nfs_state_table_alloc(table, NFS4_SLOT_TYPE_RESERVED, &state->shard,
                                            &state->slot_idx, &state->generation)) {
            free(state);
            status = NFS4ERR_RESOURCE;
            break;
        }
        state->lock_owner     = owner;
        state->compound_group = cookie;
        atomic_init(&state->seqid, 1);
        atomic_init(&state->refcount, 1);
        atomic_init(&state->destroyed, 0);
        atomic_init(&state->compound_reserved, 1);
        nfs_lock_owner_get(owner);
        reservation->candidates[reservation->num_candidates++] = state;
    }
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    if (status != NFS4_OK) {
        nfs_lock_owner_finish_compound(reservation, table, vfs_thread);
        return status;
    }
    *out_reservation = reservation;
    return NFS4_OK;
} /* nfs_lock_owner_reserve_compound */

void
nfs_lock_owner_finish_compound(
    struct nfs_lock_owner_reservation *reservation,
    struct nfs_state_table            *table,
    struct chimera_vfs_thread         *vfs_thread)
{
    struct nfs_lock_owner *owner  = reservation->owner;
    struct nfs_client     *client = owner->client;
    bool                   destroy;

    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_group != reservation->cookie ||
                         !atomic_load(&owner->compound_pins) || !client->compound_pins,
                         "lock owner reservation underflow");
    for (uint32_t i = 0; i < reservation->num_candidates; i++) {
        struct nfs_lock_state  *state = reservation->candidates[i];
        struct nfs_state_shard *shard = &table->shards[state->shard];
        evpl_rwlock_wrlock(&shard->lock);
        state->compound_group = NULL;
        atomic_store(&state->compound_reserved, 0);
        evpl_rwlock_unlock(&shard->lock);
    }
    if (atomic_fetch_sub(&owner->compound_pins, 1) == 1) {
        owner->compound_group = NULL;
    }
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&client->lock);
    for (uint32_t i = 0; i < reservation->num_candidates; i++) {
        struct nfs_lock_state *state = reservation->candidates[i];
        if (reservation->candidate_published[i]) {
            nfs_state_table_release(table, state, NFS4_SLOT_TYPE_LOCK, vfs_thread);
        } else {
            nfs_state_table_free_slot(table, state->shard, state->slot_idx);
            nfs_lock_owner_put(owner);
            free(state);
        }
    }
    nfs_lock_owner_put(owner);
    evpl_mutex_lock(&client->lock);
    client->compound_pins--;
    destroy = !client->compound_pins && client->compound_destroy_pending;
    evpl_mutex_unlock(&client->lock);
    free(reservation);
    if (destroy) {
        nfs_client_destroy(client, table, vfs_thread, false);
    }
} /* nfs_lock_owner_finish_compound */

/* Each mutation can add at most two interval boundaries. All storage,
 * including the nodes eventually installed in public state, is reserved
 * before dispatch; the attempt callbacks only rewrite these private arrays. */
#define NFS4_COMPOUND_MAX_ORIGINAL_RANGES 256

struct nfs_lock_range_interval {
    uint64_t    start;
    __uint128_t end;
    bool        write;
};

struct nfs_lock_range_journal {
    uint32_t                        capacity, max_modifications, num_original, num_previous, count;
    bool                            published;
    struct chimera_vfs_claim      **previous;
    struct chimera_vfs_claim      **current;
    struct nfs4_range_lease       **nodes;
    struct nfs_lock_range_interval *original, *intervals, *scratch;
};

static __uint128_t
nfs_lock_range_end(
    uint64_t offset,
    uint64_t length)
{
    __uint128_t eof = ((__uint128_t) 1) << 64;
    __uint128_t end = (__uint128_t) offset + length;

    return length == UINT64_MAX || end > eof ? eof : end;
} /* nfs_lock_range_end */

static void
nfs_lock_range_refresh(struct nfs_lock_range_journal *journal)
{
    const struct chimera_vfs_claim *template = journal->num_previous ? journal->previous[0] : NULL;

    for (uint32_t i = 0; i < journal->count; i++) {
        struct nfs_lock_range_interval *range = &journal->intervals[i];
        struct chimera_vfs_claim       *claim = &journal->nodes[i]->claim;
        chimera_nfs_abort_if(!template, "compound range has no admitted source");
        chimera_vfs_claim_init_range(claim, range->write, false, range->start,
                                     range->end == (((__uint128_t) 1) << 64) ? UINT64_MAX : (uint64_t) (range->end -
                                                                                                        range->start),
                                     &template->owner);
        claim->is_alive_cb  = template->is_alive_cb;
        claim->revoked_cb   = template->revoked_cb;
        claim->cb_private   = template->cb_private;
        claim->policy_tag   = template->policy_tag;
        journal->current[i] = claim;
    }
} /* nfs_lock_range_refresh */

/* Sort and merge without allocation (even libc qsort may allocate). */
static bool
nfs_lock_range_normalize(
    struct nfs_lock_range_interval *ranges,
    uint32_t                       *count)
{
    for (uint32_t i = 1; i < *count; i++) {
        struct nfs_lock_range_interval range = ranges[i];
        uint32_t                       j     = i;
        while (j && ranges[j - 1].start > range.start) {
            ranges[j] = ranges[j - 1];
            j--;
        }
        ranges[j] = range;
    }
    uint32_t out = 0;
    for (uint32_t i = 0; i < *count; i++) {
        if (out && ranges[out - 1].end > ranges[i].start &&
            ranges[out - 1].write != ranges[i].write) {
            return false;
        }
        if (out && ranges[out - 1].end >= ranges[i].start &&
            ranges[out - 1].write == ranges[i].write) {
            if (ranges[i].end > ranges[out - 1].end) {
                ranges[out - 1].end = ranges[i].end;
            }
        } else {
            ranges[out++] = ranges[i];
        }
    }
    *count = out;
    return true;
} /* nfs_lock_range_normalize */

struct nfs_lock_range_journal *
nfs_lock_range_journal_alloc(
    struct nfs_lock_state *target,
    uint32_t               max_modifications)
{
    struct nfs_lock_range_journal *journal;
    struct nfs4_range_lease       *lease;
    uint32_t                       count = 0;

    if (max_modifications > NFS4_COMPOUND_OWNER_MAX_STATES) {
        return NULL;
    }
    for (lease = target ? target->range_leases : NULL; lease; lease = lease->next) {
        if (++count > NFS4_COMPOUND_MAX_ORIGINAL_RANGES || lease->claim.backend_token ||
            lease->claim.break_cb || lease->claim.parked || !lease->claim.file ||
            !lease->claim.length || lease->claim.construct != CHIMERA_CONSTRUCT_LOCK_ADVISORY) {
            return NULL;
        }
    }
    journal = calloc(1, sizeof(*journal));
    if (!journal) {
        return NULL;
    }
    journal->capacity          = count + 2 * max_modifications + 1;
    journal->max_modifications = max_modifications;
    journal->num_original      = count;
    journal->previous          = calloc(count + max_modifications + 1, sizeof(*journal->previous));
    journal->current           = calloc(journal->capacity, sizeof(*journal->current));
    journal->nodes             = calloc(journal->capacity, sizeof(*journal->nodes));
    journal->original          = calloc(count + 1, sizeof(*journal->original));
    journal->intervals         = calloc(journal->capacity, sizeof(*journal->intervals));
    journal->scratch           = calloc(journal->capacity, sizeof(*journal->scratch));
    if (!journal->previous || !journal->current || !journal->nodes || !journal->original ||
        !journal->intervals || !journal->scratch) {
        nfs_lock_range_journal_free(journal);
        return NULL;
    }
    for (uint32_t i = 0; i < journal->capacity; i++) {
        journal->nodes[i] = calloc(1, sizeof(*journal->nodes[i]));
        if (!journal->nodes[i]) {
            nfs_lock_range_journal_free(journal);
            return NULL;
        }
    }
    count = 0;
    for (lease = target ? target->range_leases : NULL; lease; lease = lease->next) {
        journal->previous[count]   = &lease->claim;
        journal->original[count++] = (struct nfs_lock_range_interval) {
            .start = lease->claim.offset,
            .end   = nfs_lock_range_end(lease->claim.offset, lease->claim.length),
            .write = (lease->claim.used & CHIMERA_CLAIM_LW) != 0
        };
    }
    memcpy(journal->intervals, journal->original, count * sizeof(*journal->intervals));
    /* Legacy mixed-mode overlapping lists have no unambiguous final mode;
     * keep those on the existing protocol path rather than guess. */
    if (!nfs_lock_range_normalize(journal->intervals, &count)) {
        nfs_lock_range_journal_free(journal);
        return NULL;
    }
    nfs_lock_range_journal_reset(journal);
    return journal;
} /* nfs_lock_range_journal_alloc */

void
nfs_lock_range_journal_reset(struct nfs_lock_range_journal *journal)
{
    chimera_nfs_abort_if(journal->published, "reset published compound ranges");
    journal->num_previous = journal->num_original;
    journal->count        = journal->num_original;
    memcpy(journal->intervals, journal->original, journal->count * sizeof(*journal->intervals));
    chimera_nfs_abort_if(!nfs_lock_range_normalize(journal->intervals, &journal->count),
                         "invalid compound range snapshot");
    nfs_lock_range_refresh(journal);
} /* nfs_lock_range_journal_reset */

void
nfs_lock_range_journal_free(struct nfs_lock_range_journal *journal)
{
    if (!journal) {
        return;
    }
    for (uint32_t i = 0; journal->nodes && i < journal->capacity; i++) {
        free(journal->nodes[i]);
    }
    free(journal->previous);
    free(journal->current);
    free(journal->nodes);
    free(journal->original);
    free(journal->intervals);
    free(journal->scratch);
    free(journal);
} /* nfs_lock_range_journal_free */

static bool
nfs_lock_range_transform(
    struct nfs_lock_range_journal *journal,
    uint64_t                       offset,
    uint64_t                       length,
    bool                           add,
    bool                           write,
    struct chimera_vfs_claim      *admitted)
{
    uint32_t    count = 0;
    __uint128_t end   = nfs_lock_range_end(offset, length);

    if (journal->published || !length ||
        (add && (!admitted || journal->num_previous == journal->num_original + journal->max_modifications))) {
        return false;
    }
    for (uint32_t i = 0; i < journal->count; i++) {
        struct nfs_lock_range_interval old = journal->intervals[i];
        if (old.end <= offset || old.start >= end) {
            if (count == journal->capacity) {
                return false;
            }
            journal->scratch[count++] = old;
        } else {
            if (old.start < offset) {
                if (count == journal->capacity) {
                    return false;
                }
                journal->scratch[count++] = (struct nfs_lock_range_interval) { old.start, offset, old.write };
            }
            if (old.end > end) {
                if (count == journal->capacity) {
                    return false;
                }
                journal->scratch[count++] = (struct nfs_lock_range_interval) { (uint64_t) end, old.end, old.write };
            }
        }
    }
    if (add) {
        if (count == journal->capacity) {
            return false;
        }
        journal->scratch[count++] = (struct nfs_lock_range_interval) { offset, end, write };
    }
    if (!nfs_lock_range_normalize(journal->scratch, &count)) {
        return false;
    }
    if (add) {
        journal->previous[journal->num_previous++] = admitted;
    }
    memcpy(journal->intervals, journal->scratch, count * sizeof(*journal->intervals));
    journal->count = count;
    nfs_lock_range_refresh(journal);
    return true;
} /* nfs_lock_range_transform */

bool
nfs_lock_range_journal_lock(
    struct nfs_lock_range_journal *journal,
    uint64_t                       offset,
    uint64_t                       length,
    bool                           write,
    struct chimera_vfs_claim      *admitted)
{
    return nfs_lock_range_transform(journal, offset, length, true, write, admitted);
} /* nfs_lock_range_journal_lock */

bool
nfs_lock_range_journal_unlock(
    struct nfs_lock_range_journal *journal,
    uint64_t                       offset,
    uint64_t                       length)
{
    return nfs_lock_range_transform(journal, offset, length, false, false, NULL);
} /* nfs_lock_range_journal_unlock */

const struct chimera_vfs_claim * const *
nfs_lock_range_journal_previous(
    const struct nfs_lock_range_journal *journal,
    uint32_t                            *count)
{
    *count = journal->num_previous;
    return (const struct chimera_vfs_claim *const *) journal->previous;
} /* nfs_lock_range_journal_previous */

const struct chimera_vfs_claim * const *
nfs_lock_range_journal_current(
    const struct nfs_lock_range_journal *journal,
    uint32_t                            *count)
{
    *count = journal->count;
    return (const struct chimera_vfs_claim *const *) journal->current;
} /* nfs_lock_range_journal_current */

bool
nfs_lock_range_journal_empty(const struct nfs_lock_range_journal *journal)
{
    return journal->count == 0;
} /* nfs_lock_range_journal_empty */

struct chimera_vfs_file_state *
nfs_lock_range_journal_retire(
    struct nfs_lock_range_journal *journal,
    struct chimera_vfs_thread     *vfs_thread)
{
    struct chimera_vfs_file_state *file = journal->num_previous ? journal->previous[0]->file : NULL;

    chimera_nfs_abort_if(journal->published || (journal->num_previous && (!file || !vfs_thread)),
                         "invalid compound LOCK retirement");
    if (file) {
        chimera_nfs_abort_if(chimera_vfs_state_get(vfs_thread->vfs->vfs_state,
                                                   file->fh, file->fh_len, file->fh_hash, false) != file,
                             "compound LOCK lost retirement file");
        chimera_vfs_claim_range_retire(file, journal->previous, journal->num_previous);
    }
    journal->published = true;
    journal->count     = 0;
    return file;
} /* nfs_lock_range_journal_retire */

void
nfs_lock_state_apply_compound(
    struct nfs_lock_owner_reservation *reservation,
    struct nfs_lock_state             *target,
    struct nfs_open_state             *parent,
    struct chimera_vfs_open_handle    *handle,
    uint32_t                           seqid,
    struct nfs_lock_range_journal     *journal,
    struct nfs_state_table            *table,
    struct chimera_vfs_thread         *vfs_thread)
{
    struct nfs_lock_owner          *owner     = reservation->owner;
    struct nfs_client              *client    = owner->client;
    struct chimera_vfs_state       *vfs_state = vfs_thread ? vfs_thread->vfs->vfs_state : NULL;
    struct chimera_vfs_file_state  *file      = journal->num_previous ? journal->previous[0]->file : NULL;
    struct nfs4_range_lease        *old_ranges = target->range_leases, *head = NULL;
    struct chimera_vfs_open_handle *old_handle = target->handle;
    int                             candidate  = -1;
    bool                            existing   = false;

    for (uint32_t i = 0; i < reservation->num_candidates; i++) {
        if (reservation->candidates[i] == target) {
            candidate = (int) i;
        }
    }
    for (uint32_t i = 0; i < reservation->num_existing; i++) {
        if (reservation->existing[i] == target) {
            existing = true;
        }
    }
    bool publish = candidate >= 0 && !reservation->candidate_published[candidate];
    chimera_nfs_abort_if((candidate < 0 && !existing) || journal->published ||
                         target->lock_owner != owner || target->compound_group != reservation->cookie ||
                         !atomic_load(&target->compound_reserved) || atomic_load(&target->destroyed) ||
                         !parent || !parent->compound_reservation ||
                         parent->compound_reservation->cookie != reservation->cookie ||
                         !atomic_load(&parent->compound_reserved) || atomic_load(&parent->destroyed) ||
                         (!publish && target->open_state != parent) || (journal->num_previous && (!file || !vfs_state)),
                         "invalid compound LOCK publication");
    for (uint32_t i = 0; i < journal->count; i++) {
        struct nfs4_range_lease *lease = journal->nodes[i];
        lease->file_state = chimera_vfs_state_get(vfs_state, file->fh, file->fh_len, file->fh_hash, false);
        chimera_nfs_abort_if(lease->file_state != file, "compound LOCK lost pinned file");
        lease->next = head;
        head        = lease;
    }
    if (file) {
        chimera_nfs_abort_if(chimera_vfs_state_get(vfs_state, file->fh, file->fh_len,
                                                   file->fh_hash, false) != file,
                             "compound LOCK lost publication file");
        chimera_vfs_claim_range_publish(file, journal->previous, journal->num_previous,
                                        journal->current, journal->count);
    }
    evpl_mutex_lock(&client->lock);
    evpl_mutex_lock(&parent->owner->lock);
    evpl_mutex_lock(&owner->lock);
    chimera_nfs_abort_if(owner->compound_group != reservation->cookie,
                         "compound LOCK lost owner reservation");
    target->open_state   = parent;
    target->handle       = handle;
    target->seqid        = seqid;
    target->range_leases = head;
    if (publish) {
        atomic_fetch_add(&parent->refcount, 1);
        atomic_fetch_add(&target->refcount, 1);
        LL_PREPEND2(owner->states, target, next_in_owner);
        LL_PREPEND2(parent->locks, target, next_in_open);
        target->on_owner_list = target->on_open_list = 1;
        nfs_state_table_install(table, target->shard, target->slot_idx, NFS4_SLOT_TYPE_LOCK, target);
        reservation->candidate_published[candidate] = true;
    }
    evpl_mutex_unlock(&owner->lock);
    evpl_mutex_unlock(&parent->owner->lock);
    evpl_mutex_unlock(&client->lock);
    for (uint32_t i = 0; i < journal->count; i++) {
        journal->nodes[i] = NULL;
    }
    journal->published = true;
    while (old_ranges) {
        struct nfs4_range_lease *next = old_ranges->next;
        chimera_vfs_state_put(vfs_state, old_ranges->file_state);
        free(old_ranges);
        old_ranges = next;
    }
    if (file) {
        chimera_vfs_claim_replacement_complete(file);
        chimera_vfs_state_put(vfs_state, file);
    }
    if (old_handle && vfs_thread) {
        chimera_vfs_release(vfs_thread, old_handle);
    }
} /* nfs_lock_state_apply_compound */

struct nfs_lock_state *
nfs_lock_state_create(
    struct nfs_lock_owner          *lock_owner,
    struct nfs_open_state          *open_state,
    struct chimera_vfs_open_handle *handle_dup,
    struct nfs_state_table         *table,
    struct stateid4                *out_stateid)
{
    struct nfs_client     *client = lock_owner->client;
    struct nfs_lock_owner *published;
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
    atomic_init(&state->compound_reserved, 0);

    /* Serialize installation against client teardown.  Under
     * client->lock the lease sweeper cannot be mid-walk, so either the
     * lock_owner is still published and the open_state alive (install
     * proceeds and a later sweep finds the state on both lists), or the
     * client was already expired (fail: installing now would orphan the
     * state forever, since teardown has already walked these lists). */
    evpl_mutex_lock(&client->lock);

    HASH_FIND(hh, client->lock_owners_by_str,
              lock_owner->owner, lock_owner->owner_len, published);
    if (published != lock_owner || atomic_load_explicit(&lock_owner->compound_pins, memory_order_acquire)) {
        evpl_mutex_unlock(&client->lock);
        nfs_state_table_free_slot(table, shard, slot_idx);
        free(state);
        return NULL;
    }

    /* Both list links happen under open_state->owner->lock so that
     * open_state_destroy_locked (CLOSE / expiry), which walks and unlinks
     * under that same lock, sees either no links or both -- never a
     * lock_state on one list only.  The destroyed check must share that
     * critical section: destroy_locked flips the flag under owner->lock
     * before walking. */
    evpl_mutex_lock(&open_state->owner->lock);
    if (atomic_load_explicit(&open_state->destroyed, memory_order_acquire) ||
        atomic_load_explicit(&open_state->compound_closing, memory_order_acquire) ||
        atomic_load_explicit(&open_state->compound_reserved, memory_order_acquire)) {
        evpl_mutex_unlock(&open_state->owner->lock);
        evpl_mutex_unlock(&client->lock);
        nfs_state_table_free_slot(table, shard, slot_idx);
        free(state);
        return NULL;
    }

    /* Pin the lock_owner for the state's lifetime; released in
     * lock_state_cleanup. */
    nfs_lock_owner_get(lock_owner);

    /* Pin the open_state too: nfs_lock_state_destroy dereferences
     * state->open_state (owner lock, locks list) and can race the expire
     * cascade freeing it.  Released in lock_state_cleanup via
     * nfs_state_table_release, which defers the open_state cleanup to the
     * last reference as usual. */
    atomic_fetch_add_explicit(&open_state->refcount, 1, memory_order_acq_rel);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_LOCK, state);

    evpl_mutex_lock(&lock_owner->lock);
    LL_PREPEND2(lock_owner->states, state, next_in_owner);
    state->on_owner_list = 1;
    evpl_mutex_unlock(&lock_owner->lock);

    LL_PREPEND2(open_state->locks, state, next_in_open);
    state->on_open_list = 1;
    evpl_mutex_unlock(&open_state->owner->lock);

    /* Encode before dropping client->lock: once published, a concurrent
     * expire may destroy and free the state. */
    nfs4_stateid_encode(out_stateid, state->seqid,
                        NFS4_STATEID_TYPE_LOCK, shard, slot_idx, gen,
                        table->epoch);

    evpl_mutex_unlock(&client->lock);
    return state;
} /* nfs_lock_state_create */

/* Insert a byte-range lock interval [start, end) (end == UINT64_MAX => to EOF)
 * for `owner` on `file_state` (the ref's ownership transfers to the
 * returned entry), and link it onto lock_state->range_leases.  The interval is
 * always a sub-region or coalesced union of locks this owner already
 * coordinated, so the synchronous acquire grants without conflict. */
struct nfs4_range_lease *
nfs4_range_lease_insert(
    struct chimera_vfs_state         *vfs_state,
    struct nfs_lock_state            *lock_state,
    struct chimera_vfs_file_state    *file_state,
    const struct chimera_claim_owner *owner,
    bool                              exclusive,
    uint64_t                          start,
    uint64_t                          end,
    void                             *cb_private)
{
    struct nfs4_range_lease *rl = calloc(1, sizeof(*rl));

    if (!rl) {
        chimera_vfs_state_put(vfs_state, file_state);
        return NULL;
    }

    rl->file_state = file_state;
    /* The claim core represents to-EOF as UINT64_MAX (length 0 is a
     * genuine zero-byte range), matching the [start, end) sentinel here. */
    chimera_vfs_claim_init_range(&rl->claim, exclusive, /*smb=*/ false,
                                 start,
                                 (end == UINT64_MAX) ? UINT64_MAX : (end - start),
                                 owner);
    /* Courteous server: report this lock dead once the owning client's lease
     * lapses, so a conflicting acquire reclaims it. */
    rl->claim.is_alive_cb = nfs_client_lease_alive;
    rl->claim.revoked_cb  = nfs_client_lease_revoked_cb;
    rl->claim.cb_private  = cb_private;

    chimera_vfs_claim_try_acquire(vfs_state, file_state, &rl->claim, NULL);

    rl->next                 = lock_state->range_leases;
    lock_state->range_leases = rl;
    return rl;
} /* nfs4_range_lease_insert */

/*
 * Release the SHARE reservation `share` holds and free it.
 *
 * Both halves are conditional because a lease can exist without either: one
 * built for a sequenced OPEN whose CLAIM was never reached holds no claim and
 * owns no file state, and freeing it is the whole of giving it back.
 */

/* Release the claim backing `rl` and free it.  Caller has already
 * unlinked it from lock_state->range_leases. */
void
nfs4_range_lease_free(
    struct chimera_vfs_state *vfs_state,
    struct nfs4_range_lease  *rl)
{
    chimera_vfs_claim_release(vfs_state, rl->file_state, &rl->claim);
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

    /* Drain any byte-range claims still held (ranges not explicitly
     * released by LOCKU before CLOSE / lock-owner teardown). */
    rl                  = state->range_leases;
    state->range_leases = NULL;
    while (rl) {
        tmp = rl->next;
        if (vfs_state) {
            chimera_vfs_claim_release(vfs_state, rl->file_state, &rl->claim);
            chimera_vfs_state_put(vfs_state, rl->file_state);
        }
        free(rl);
        rl = tmp;
    }

    if (state->handle && vfs_thread) {
        chimera_vfs_release(vfs_thread, state->handle);
        state->handle = NULL;
    }
    /* Release the owner and open_state pins taken in
     * nfs_lock_state_create.  The open_state release goes through the table so a
     * destroyed-and-otherwise-unreferenced open_state is cleaned up here. */
    nfs_lock_owner_put(state->lock_owner);
    if (state->open_state) {
        nfs_state_table_release(table, state->open_state,
                                NFS4_SLOT_TYPE_OPEN, vfs_thread);
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

    /* Flag-guarded unlinks: the expire/CLOSE cascade in
     * open_state_destroy_locked may race this and unlink either side
     * first. */
    evpl_mutex_lock(&lock_owner->lock);
    if (state->on_owner_list) {
        LL_DELETE2(lock_owner->states, state, next_in_owner);
        state->on_owner_list = 0;
    }
    evpl_mutex_unlock(&lock_owner->lock);

    if (open_state) {
        evpl_mutex_lock(&open_state->owner->lock);
        if (state->on_open_list) {
            LL_DELETE2(open_state->locks, state, next_in_open);
            state->on_open_list = 0;
        }
        evpl_mutex_unlock(&open_state->owner->lock);
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

void
nfs_delegation_combine_reset(struct nfs_delegation_combine_journal *journal)
{
    journal->sc      = journal->initial_sc;
    journal->last    = journal->initial_last;
    journal->valid   = journal->initial_valid;
    journal->dirty   = journal->initial_dirty;
    journal->applied = false;
} /* nfs_delegation_combine_reset */

nfsstat4
nfs_delegation_combine_reserve(
    struct nfs_delegation                 *deleg,
    const void                            *cookie,
    struct nfs_delegation_combine_journal *journal)
{
    nfsstat4 status = NFS4ERR_DELAY;

    if (!deleg || !cookie || !journal) {
        return NFS4ERR_INVAL;
    }
    evpl_mutex_lock(&deleg->combine_lock);
    if (!deleg->combine_reservation &&
        !atomic_load_explicit(&deleg->destroyed, memory_order_acquire) &&
        !atomic_load_explicit(&deleg->revoked, memory_order_acquire)) {
        memset(journal, 0, sizeof(*journal));
        journal->deleg         = deleg;
        journal->cookie        = cookie;
        journal->initial_sc    = deleg->combine_sc;
        journal->initial_last  = deleg->combine_last;
        journal->initial_valid = deleg->combine_valid;
        journal->initial_dirty = deleg->combine_dirty;
        nfs_delegation_combine_reset(journal);
        atomic_fetch_add_explicit(&deleg->refcount, 1, memory_order_relaxed);
        deleg->combine_reservation = journal;
        status                     = NFS4_OK;
    } else if (deleg->combine_reservation == journal && journal->cookie == cookie) {
        status = NFS4_OK;
    }
    evpl_mutex_unlock(&deleg->combine_lock);
    return status;
} /* nfs_delegation_combine_reserve */

nfsstat4
nfs_delegation_combine_apply(
    struct nfs_delegation_combine_journal *journal,
    uint64_t                               client_change,
    bool                                  *modified,
    uint64_t                              *server_change)
{
    *modified      = false;
    *server_change = 0;
    if (!journal->deleg) {
        return NFS4ERR_INVAL;
    }
    if (!journal->valid) {
        journal->sc    = journal->last = client_change;
        journal->valid = true;
    }
    if (journal->dirty || client_change != journal->sc) {
        uint64_t floor = journal->sc > journal->last ? journal->sc : journal->last;
        if (floor == UINT64_MAX) {
            return NFS4ERR_RESOURCE;
        }
        journal->sc    = journal->last = client_change > floor ? client_change : floor + 1;
        journal->dirty = true;
        *modified      = true;
        *server_change = journal->sc;
    }
    journal->applied = true;
    return NFS4_OK;
} /* nfs_delegation_combine_apply */

nfsstat4
nfs_delegation_combine_attrs(
    struct nfs_delegation_combine_journal *journal,
    uint64_t                               client_change,
    bool                                   got_size,
    uint64_t                               client_size,
    const struct timespec                 *query_time,
    struct chimera_vfs_attrs              *attrs)
{
    struct nfs_delegation_combine_journal pending      = *journal;
    uint64_t                              local_change = (attrs->va_set_mask & CHIMERA_VFS_ATTR_CHANGE) ?
        attrs->va_change : (uint64_t) attrs->va_ctime.tv_sec * 1000000000ULL + attrs->va_ctime.tv_nsec;
    uint64_t                              server_change;
    bool                                  modified;

    /* CLAIM_FH may not have captured a grant-time baseline. Compare the
     * callback against local metadata, never adopt a dirty client's cc as an
     * apparently clean baseline. */
    if (!pending.valid) {
        pending.sc    = local_change;
        pending.last  = pending.sc;
        pending.valid = true;
    }
    if (local_change > pending.last) {
        pending.last = local_change;
    }
    if (got_size && (attrs->va_set_mask & CHIMERA_VFS_ATTR_SIZE) && client_size != attrs->va_size) {
        pending.dirty = true;
    }
    nfsstat4 status = nfs_delegation_combine_apply(&pending, client_change, &modified, &server_change);
    if (status != NFS4_OK) {
        return status;
    }
    if (modified) {
        attrs->va_ctime     = attrs->va_mtime = *query_time;
        attrs->va_set_mask |= CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_MTIME;
        attrs->va_change    = server_change;
        attrs->va_set_mask |= CHIMERA_VFS_ATTR_CHANGE;
        if (got_size) {
            attrs->va_size      = client_size;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        }
    }
    *journal = pending;
    return NFS4_OK;
} /* nfs_delegation_combine_attrs */

void
nfs_delegation_combine_finish(
    struct nfs_delegation_combine_journal *journal,
    bool                                   accepted,
    struct nfs_state_table                *table,
    struct chimera_vfs_thread             *vfs_thread)
{
    struct nfs_delegation *deleg = journal->deleg;

    if (!deleg) {
        return;
    }
    evpl_mutex_lock(&deleg->combine_lock);
    chimera_nfs_abort_if(deleg->combine_reservation != journal, "delegation combine reservation lost");
    if (accepted && journal->applied) {
        deleg->combine_sc    = journal->sc;
        deleg->combine_last  = journal->last;
        deleg->combine_valid = journal->valid;
        deleg->combine_dirty = journal->dirty;
    }
    deleg->combine_reservation = NULL;
    evpl_mutex_unlock(&deleg->combine_lock);
    journal->deleg = NULL;
    nfs_state_table_release(table, deleg, NFS4_SLOT_TYPE_DELEG, vfs_thread);
} /* nfs_delegation_combine_finish */

struct nfs_delegation *
nfs_delegation_create(
    struct nfs_client      *client,
    uint8_t                 deleg_type,
    const uint8_t          *fh,
    uint16_t                fh_len,
    uint64_t                fh_hash,
    uint16_t                export_id,
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
    deleg->export_id  = export_id;
    deleg->shard      = shard;
    deleg->slot_idx   = slot_idx;
    deleg->generation = gen;
    deleg->seqid      = 1;
    deleg->lease_held = false;
    /* RFC 7530/8881 §10.4.3 combine state.  sc is captured at grant by the
     * OPEN path; combine_valid stays false until then (lazy capture on first
     * CB_GETATTR otherwise). */
    evpl_mutex_init(&deleg->combine_lock, NULL);
    deleg->combine_sc    = 0;
    deleg->combine_last  = 0;
    deleg->combine_valid = false;
    atomic_init(&deleg->cb_recall_state, NFS4_DELEG_ACTIVE);
    atomic_init(&deleg->cb_recall_retries, 0);
    atomic_init(&deleg->refcount, 1);
    atomic_init(&deleg->destroyed, 0);

    nfs_state_table_install(table, shard, slot_idx, NFS4_SLOT_TYPE_DELEG, deleg);

    evpl_mutex_lock(&client->lock);
    LL_PREPEND2(client->delegations, deleg, next_in_client);
    evpl_mutex_unlock(&client->lock);

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
        chimera_vfs_claim_release(vfs_state, deleg->file_state, &deleg->claim);
        deleg->lease_held = false;
    }
    if (vfs_state && deleg->file_state) {
        chimera_vfs_state_put(vfs_state, deleg->file_state);
        deleg->file_state = NULL;
    }
    evpl_mutex_destroy(&deleg->combine_lock);
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
        evpl_mutex_lock(&deleg->client->lock);
        LL_DELETE2(deleg->client->delegations, deleg, next_in_client);
        evpl_mutex_unlock(&deleg->client->lock);
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

/* Claim revoked_cb (wired onto every delegation claim).  Marks the
 * delegation revoked so its stateid reports NFS4ERR_DELEG_REVOKED, and bumps
 * the owning client's revoked count (drives SEQ4_STATUS_RECALLABLE_STATE_
 * REVOKED).  Idempotent. */
SYMBOL_EXPORT void
nfs_delegation_revoked_cb(
    struct chimera_vfs_claim *claim,
    void                     *private_data)
{
    struct nfs_delegation *deleg    = private_data;
    uint8_t                expected = 0;

    (void) claim;

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
    const struct chimera_vfs_claim *claim,
    void                           *private_data)
{
    const struct nfs_client *client = private_data;

    (void) claim;

    /* Dead (reclaimable) once the lease has lapsed into courtesy state. */
    return atomic_load_explicit(&client->compound_pins, memory_order_relaxed) ||
           !atomic_load_explicit(&client->courtesy, memory_order_relaxed);
} /* nfs_client_lease_alive */

SYMBOL_EXPORT void
nfs_client_lease_revoked_cb(
    struct chimera_vfs_claim *claim,
    void                     *private_data)
{
    struct nfs_client *client = private_data;

    (void) claim;

    /* A conflicting acquirer reclaimed a claim this courtesy client still
     * held.  Flag it so the next lease sweep tears the client down; until
     * then its other claims stay reclaimable (still in courtesy). */
    atomic_store_explicit(&client->reclaim_pending, 1, memory_order_release);
} /* nfs_client_lease_revoked_cb */

SYMBOL_EXPORT bool
nfs_delegation_lease_alive(
    const struct chimera_vfs_claim *claim,
    void                           *private_data)
{
    const struct nfs_delegation *deleg = private_data;

    (void) claim;

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

    evpl_rwlock_rdlock(&shard->lock);

    if (view.slot_idx >= shard->slots_used) {
        evpl_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }

    slot = &shard->slots[view.slot_idx];

    if (slot->type == NFS4_SLOT_TYPE_EXPIRED) {
        nfsstat4 st = (slot->generation == view.generation)
                      ? NFS4ERR_EXPIRED : NFS4ERR_STALE_STATEID;
        evpl_rwlock_unlock(&shard->lock);
        return st;
    }
    if (slot->type == NFS4_SLOT_TYPE_FREE) {
        evpl_rwlock_unlock(&shard->lock);
        return NFS4ERR_BAD_STATEID;
    }
    if (slot->generation != view.generation) {
        evpl_rwlock_unlock(&shard->lock);
        return NFS4ERR_STALE_STATEID;
    }
    if (slot->type != NFS4_SLOT_TYPE_DELEG ||
        !atomic_load_explicit(&((struct nfs_delegation *) slot->state)->revoked,
                              memory_order_acquire)) {
        /* A live (non-revoked) state cannot be freed while in use. */
        evpl_rwlock_unlock(&shard->lock);
        return NFS4ERR_LOCKS_HELD;
    }

    deleg = slot->state;
    atomic_fetch_add_explicit(&deleg->refcount, 1, memory_order_acq_rel);
    evpl_rwlock_unlock(&shard->lock);

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
