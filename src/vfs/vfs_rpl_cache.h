// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include <urcu.h>
#include <urcu/urcu-memb.h>

/*
 * RPL (Reverse Path Lookup) Cache
 *
 * Caches child_fh -> (parent_fh, name) mappings to accelerate
 * subtree change notification resolution.
 *
 * Two indices provide efficient access in both directions:
 *   Forward:  hash(child_fh)           -> entry  (resolver lookup)
 *   Reverse:  hash(parent_fh) ^ hash(name) -> entry  (invalidation)
 */

struct chimera_vfs_rpl_cache_entry {
    uint64_t        fwd_key;        /* hash(child_fh) */
    uint64_t        rev_key;        /* hash(parent_fh) ^ hash(name) */
    uint16_t        child_fh_len;
    uint16_t        parent_fh_len;
    uint16_t        name_len;
    int64_t         score;
    struct timespec expiration;
    struct rcu_head rcu;
    union {
        struct chimera_vfs_rpl_cache_entry *next;  /* when on free list */
        struct chimera_vfs_rpl_cache_shard *shard; /* when active */
    };
    uint8_t         child_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t         parent_fh[CHIMERA_VFS_FH_SIZE];
    char            name[CHIMERA_VFS_NAME_MAX];
};

struct chimera_vfs_rpl_cache_shard {
    struct chimera_vfs_rpl_cache_entry **fwd_entries; /* forward index slots */
    struct chimera_vfs_rpl_cache_entry **rev_entries; /* reverse index slots */
    struct chimera_vfs_rpl_cache_entry  *free_entries;
    pthread_mutex_t                      entry_lock;
    pthread_mutex_t                      free_lock;
};

struct chimera_vfs_rpl_cache {
    uint8_t                             num_slots_bits;
    uint8_t                             num_shards_bits;
    uint8_t                             num_entries_bits;
    uint64_t                            num_slots;
    uint32_t                            num_shards;
    uint32_t                            num_entries;
    uint64_t                            num_slots_mask;
    uint32_t                            num_shards_mask;
    uint32_t                            num_entries_mask;
    uint64_t                            ttl;
    struct chimera_vfs_rpl_cache_shard *shards;
};

static inline struct chimera_vfs_rpl_cache *
chimera_vfs_rpl_cache_create(
    uint8_t  num_shards_bits,
    uint8_t  num_slots_bits,
    uint8_t  entries_per_slot_bits,
    uint64_t ttl)
{
    struct chimera_vfs_rpl_cache       *cache;
    struct chimera_vfs_rpl_cache_shard *shard;
    struct chimera_vfs_rpl_cache_entry *entry;
    int                                 i, j;

    cache = calloc(1, sizeof(*cache));

    cache->num_shards_bits  = num_shards_bits;
    cache->num_slots_bits   = num_slots_bits;
    cache->num_entries_bits = entries_per_slot_bits;
    cache->ttl              = ttl;
    cache->num_shards       = 1 << num_shards_bits;
    cache->num_slots        = 1 << num_slots_bits;
    cache->num_entries      = 1 << entries_per_slot_bits;

    cache->num_slots_mask   = cache->num_slots - 1;
    cache->num_shards_mask  = cache->num_shards - 1;
    cache->num_entries_mask = cache->num_entries - 1;

    cache->shards = calloc(cache->num_shards, sizeof(*cache->shards));

    for (i = 0; i < cache->num_shards; i++) {

        shard = &cache->shards[i];

        shard->fwd_entries = calloc(cache->num_slots * cache->num_entries,
                                    sizeof(struct chimera_vfs_rpl_cache_entry *));
        shard->rev_entries = calloc(cache->num_slots * cache->num_entries,
                                    sizeof(struct chimera_vfs_rpl_cache_entry *));

        for (j = 0; j < cache->num_slots * cache->num_entries; j++) {
            entry = calloc(1, sizeof(*entry));
            LL_PREPEND(shard->free_entries, entry);
        }

        pthread_mutex_init(&shard->entry_lock, NULL);
        pthread_mutex_init(&shard->free_lock, NULL);
    }

    return cache;
} /* chimera_vfs_rpl_cache_create */

static inline void
chimera_vfs_rpl_cache_destroy(struct chimera_vfs_rpl_cache *cache)
{
    struct chimera_vfs_rpl_cache_shard *shard;
    struct chimera_vfs_rpl_cache_entry *entry;
    int                                 i, j;

    if (!cache) {
        return;
    }

    rcu_barrier();

    for (i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        for (j = 0; j < cache->num_slots * cache->num_entries; j++) {
            if (shard->fwd_entries[j]) {
                free(shard->fwd_entries[j]);
            }
        }

        free(shard->fwd_entries);
        free(shard->rev_entries);

        while (shard->free_entries) {
            entry               = shard->free_entries;
            shard->free_entries = entry->next;
            free(entry);
        }

        pthread_mutex_destroy(&shard->entry_lock);
        pthread_mutex_destroy(&shard->free_lock);
    }

    free(cache->shards);
    free(cache);
} /* chimera_vfs_rpl_cache_destroy */

static inline void
chimera_vfs_rpl_cache_free_entry_rcu(struct rcu_head *head)
{
    struct chimera_vfs_rpl_cache_entry *entry =
        container_of(head, struct chimera_vfs_rpl_cache_entry, rcu);
    struct chimera_vfs_rpl_cache_shard *shard = entry->shard;

    pthread_mutex_lock(&shard->free_lock);
    LL_PREPEND(shard->free_entries, entry);
    pthread_mutex_unlock(&shard->free_lock);
} /* chimera_vfs_rpl_cache_free_entry_rcu */

/*
 * Forward lookup: child_fh -> (parent_fh, name)
 * Returns 0 on hit, -1 on miss.
 */
static inline int
chimera_vfs_rpl_cache_lookup(
    struct chimera_vfs_rpl_cache *cache,
    uint64_t                      child_fh_hash,
    const void                   *child_fh,
    int                           child_fh_len,
    void                         *r_parent_fh,
    uint16_t                     *r_parent_fh_len,
    char                         *r_name,
    uint16_t                     *r_name_len)
{
    struct chimera_vfs_rpl_cache_entry  *entry;
    struct chimera_vfs_rpl_cache_shard  *shard;
    struct chimera_vfs_rpl_cache_entry **slot, **slot_end;
    uint64_t                             key = child_fh_hash;
    struct timespec                      now;

    clock_gettime(CLOCK_MONOTONIC, &now);

    shard = &cache->shards[key & cache->num_shards_mask];

    slot     = &shard->fwd_entries[(key & cache->num_slots_mask) << cache->num_entries_bits];
    slot_end = slot + cache->num_entries;

    urcu_memb_read_lock();

    while (slot < slot_end) {
        entry = rcu_dereference(*slot);

        if (entry && entry->fwd_key == key &&
            chimera_timespec_cmp(&entry->expiration, &now) >= 0 &&
            chimera_memequal(entry->child_fh, entry->child_fh_len,
                             child_fh, child_fh_len)) {

            *r_parent_fh_len = entry->parent_fh_len;
            memcpy(r_parent_fh, entry->parent_fh, entry->parent_fh_len);
            *r_name_len = entry->name_len;
            memcpy(r_name, entry->name, entry->name_len);
            entry->score++;

            urcu_memb_read_unlock();
            return 0;
        }

        slot++;
    }

    urcu_memb_read_unlock();
    return -1;
} /* chimera_vfs_rpl_cache_lookup */

/*
 * Remove an entry from the reverse index.
 * Caller must hold shard->entry_lock and be in an RCU read section.
 */
static inline void
chimera_vfs_rpl_cache_rev_remove(
    struct chimera_vfs_rpl_cache       *cache,
    struct chimera_vfs_rpl_cache_shard *shard,
    struct chimera_vfs_rpl_cache_entry *target)
{
    struct chimera_vfs_rpl_cache_entry **slot, **slot_end;

    slot     = &shard->rev_entries[(target->rev_key & cache->num_slots_mask) << cache->num_entries_bits];
    slot_end = slot + cache->num_entries;

    while (slot < slot_end) {
        if (*slot == target) {
            rcu_assign_pointer(*slot, NULL);
            return;
        }
        slot++;
    }
} /* chimera_vfs_rpl_cache_rev_remove */

/*
 * Insert an entry into the reverse index.
 * Caller must hold shard->entry_lock and be in an RCU read section.
 * Uses LRU eviction if all slots are occupied.
 * Returns evicted entry or NULL.
 */
static inline struct chimera_vfs_rpl_cache_entry *
chimera_vfs_rpl_cache_rev_insert(
    struct chimera_vfs_rpl_cache       *cache,
    struct chimera_vfs_rpl_cache_shard *shard,
    struct chimera_vfs_rpl_cache_entry *entry)
{
    struct chimera_vfs_rpl_cache_entry **slot, **slot_end, **slot_best;
    struct chimera_vfs_rpl_cache_entry  *old_entry, *best_entry;

    slot     = &shard->rev_entries[(entry->rev_key & cache->num_slots_mask) << cache->num_entries_bits];
    slot_end = slot + cache->num_entries;

    slot_best  = slot;
    best_entry = *slot_best;

    while (slot < slot_end) {
        old_entry = *slot;

        if (!old_entry) {
            rcu_assign_pointer(*slot, entry);
            return NULL;
        }

        if (old_entry == entry) {
            /* Already present */
            return NULL;
        }

        if (best_entry && best_entry->score > old_entry->score) {
            best_entry = old_entry;
            slot_best  = slot;
        }

        slot++;
    }

    /* All slots full — evict lowest-score entry from reverse index only.
     * The forward index entry remains; it just won't be invalidatable
     * by parent+name until TTL expires. */
    rcu_assign_pointer(*slot_best, entry);
    return NULL;
} /* chimera_vfs_rpl_cache_rev_insert */

/*
 * Insert: cache child_fh -> (parent_fh, name)
 */
static inline void
chimera_vfs_rpl_cache_insert(
    struct chimera_vfs_rpl_cache *cache,
    uint64_t                      child_fh_hash,
    const void                   *child_fh,
    int                           child_fh_len,
    const void                   *parent_fh,
    int                           parent_fh_len,
    uint64_t                      parent_fh_hash,
    uint64_t                      name_hash,
    const char                   *name,
    int                           name_len)
{
    struct chimera_vfs_rpl_cache_entry  *entry, *old_entry, *best_entry;
    struct chimera_vfs_rpl_cache_shard  *shard;
    struct chimera_vfs_rpl_cache_entry **slot, **slot_end, **slot_best;
    uint64_t                             fwd_key = child_fh_hash;
    uint64_t                             rev_key = parent_fh_hash ^ name_hash;
    struct timespec                      now;

    clock_gettime(CLOCK_MONOTONIC, &now);

    shard = &cache->shards[fwd_key & cache->num_shards_mask];

    /* Allocate entry from free list */
    pthread_mutex_lock(&shard->free_lock);
    entry = shard->free_entries;
    if (entry) {
        LL_DELETE(shard->free_entries, entry);
    }
    pthread_mutex_unlock(&shard->free_lock);

    if (!entry) {
        entry = calloc(1, sizeof(*entry));
    }

    entry->fwd_key       = fwd_key;
    entry->rev_key       = rev_key;
    entry->child_fh_len  = child_fh_len;
    entry->parent_fh_len = parent_fh_len;
    entry->name_len      = name_len;
    entry->shard         = shard;
    entry->score         = 0;

    entry->expiration.tv_sec  = now.tv_sec + cache->ttl;
    entry->expiration.tv_nsec = now.tv_nsec;

    memcpy(entry->child_fh, child_fh, child_fh_len);
    memcpy(entry->parent_fh, parent_fh, parent_fh_len);
    memcpy(entry->name, name, name_len);

    /* Insert into forward index */
    urcu_memb_read_lock();
    pthread_mutex_lock(&shard->entry_lock);

    slot     = &shard->fwd_entries[(fwd_key & cache->num_slots_mask) << cache->num_entries_bits];
    slot_end = slot + cache->num_entries;

    slot_best  = slot;
    best_entry = *slot_best;

    while (slot < slot_end) {
        old_entry = *slot;

        /* Replace existing entry for same child_fh */
        if (old_entry && old_entry->fwd_key == fwd_key &&
            chimera_memequal(old_entry->child_fh, old_entry->child_fh_len,
                             child_fh, child_fh_len)) {
            best_entry = old_entry;
            slot_best  = slot;
            break;
        }

        if (!best_entry) {
            slot++;
            continue;
        }

        if (!old_entry) {
            best_entry = old_entry;
            slot_best  = slot;
            slot++;
            continue;
        }

        if (chimera_timespec_cmp(&old_entry->expiration, &now) < 0) {
            old_entry->score = -1;
        }

        if ((best_entry->score > old_entry->score) ||
            (best_entry->score == old_entry->score &&
             chimera_timespec_cmp(&best_entry->expiration, &old_entry->expiration) < 0)) {
            best_entry = old_entry;
            slot_best  = slot;
        }

        slot++;
    }

    /* Remove old entry from reverse index if we're evicting one */
    if (best_entry) {
        chimera_vfs_rpl_cache_rev_remove(cache, shard, best_entry);
    }

    rcu_assign_pointer(*slot_best, entry);

    /* Insert into reverse index */
    chimera_vfs_rpl_cache_rev_insert(cache, shard, entry);

    pthread_mutex_unlock(&shard->entry_lock);
    urcu_memb_read_unlock();

    if (best_entry) {
        call_rcu(&best_entry->rcu, chimera_vfs_rpl_cache_free_entry_rcu);
    }
} /* chimera_vfs_rpl_cache_insert */

/*
 * Invalidate: remove the entry for a child that had (parent_fh, name).
 * Called when a rename or remove makes a cached mapping stale.
 */
static inline void
chimera_vfs_rpl_cache_invalidate(
    struct chimera_vfs_rpl_cache *cache,
    uint64_t                      parent_fh_hash,
    const void                   *parent_fh,
    int                           parent_fh_len,
    uint64_t                      name_hash,
    const char                   *name,
    int                           name_len)
{
    struct chimera_vfs_rpl_cache_entry  *entry, *removed_entry = NULL;
    struct chimera_vfs_rpl_cache_shard  *shard;
    struct chimera_vfs_rpl_cache_entry **slot, **slot_end;
    uint64_t                             rev_key = parent_fh_hash ^ name_hash;
    int                                  shard_idx;

    /* Entries are sharded by fwd_key (the child's FH hash) so the forward
     * lookup hot path is O(1).  We don't have fwd_key during invalidate
     * (the caller knows only the parent_fh + name), so scan every shard's
     * reverse index.  Invalidation runs on rename/remove which is far
     * less frequent than lookup. */
    urcu_memb_read_lock();

    for (shard_idx = 0; shard_idx < cache->num_shards; shard_idx++) {
        shard = &cache->shards[shard_idx];

        pthread_mutex_lock(&shard->entry_lock);

        slot     = &shard->rev_entries[(rev_key & cache->num_slots_mask) << cache->num_entries_bits];
        slot_end = slot + cache->num_entries;

        while (slot < slot_end) {
            entry = *slot;

            if (entry && entry->rev_key == rev_key &&
                chimera_memequal(entry->parent_fh, entry->parent_fh_len,
                                 parent_fh, parent_fh_len) &&
                chimera_memequal(entry->name, entry->name_len, name, name_len)) {

                removed_entry = entry;

                /* Remove from reverse index */
                rcu_assign_pointer(*slot, NULL);

                /* Remove from forward index (same shard — entries are
                 * stored together in the shard chosen by fwd_key). */
                {
                    struct chimera_vfs_rpl_cache_entry **fwd_slot, **fwd_end;

                    fwd_slot = &shard->fwd_entries[
                        (entry->fwd_key & cache->num_slots_mask) << cache->num_entries_bits];
                    fwd_end = fwd_slot + cache->num_entries;

                    while (fwd_slot < fwd_end) {
                        if (*fwd_slot == entry) {
                            rcu_assign_pointer(*fwd_slot, NULL);
                            break;
                        }
                        fwd_slot++;
                    }
                }
                break;
            }

            slot++;
        }

        pthread_mutex_unlock(&shard->entry_lock);

        if (removed_entry) {
            break;
        }
    }

    urcu_memb_read_unlock();

    if (removed_entry) {
        call_rcu(&removed_entry->rcu, chimera_vfs_rpl_cache_free_entry_rcu);
    }
} /* chimera_vfs_rpl_cache_invalidate */
