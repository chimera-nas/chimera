#pragma once

#include "vfs/vfs.h"
#include <urcu.h>
#include <urcu/urcu-memb.h>

struct chimera_vfs_attr_cache_entry {
    uint64_t                 key;
    uint64_t                 score;
    struct rcu_head          rcu;
    struct timespec          expiration;
    union {
        struct chimera_vfs_attr_cache_entry *next; /* when on the free list */
        struct chimera_vfs_attr_cache_shard *shard; /* when not in the free list */
    };
    struct chimera_vfs_attrs attr;
};

struct chimera_vfs_attr_cache_shard {
    struct chimera_vfs_attr_cache_entry **entries;
    struct chimera_vfs_attr_cache_entry  *free_entries;
    uint64_t                              num_lookups;
    uint64_t                              num_hits;
    uint64_t                              num_inserts;
    pthread_mutex_t                       entry_lock;
    pthread_mutex_t                       free_lock;
};

struct chimera_vfs_attr_cache {
    uint8_t                              num_slots_bits;
    uint8_t                              num_shards_bits;
    uint8_t                              num_entries_bits;
    uint64_t                             num_slots;
    uint32_t                             num_shards;
    uint32_t                             num_entries;
    uint64_t                             num_slots_mask;
    uint32_t                             num_shards_mask;
    uint32_t                             num_entries_mask;
    uint64_t                             ttl;
    struct chimera_vfs_attr_cache_shard *shards;
};

static inline struct chimera_vfs_attr_cache *
chimera_vfs_attr_cache_create(
    uint8_t  num_shards_bits,
    uint8_t  num_slots_bits,
    uint8_t  entries_per_slot_bits,
    uint64_t ttl)
{
    struct chimera_vfs_attr_cache       *cache;
    struct chimera_vfs_attr_cache_shard *shard;
    struct chimera_vfs_attr_cache_entry *entry;
    int                                  i, j;

    cache = calloc(1, sizeof(struct chimera_vfs_attr_cache));

    cache->num_shards_bits  = num_shards_bits;
    cache->num_slots_bits   = num_slots_bits;
    cache->num_entries_bits = entries_per_slot_bits;
    cache->ttl              = ttl;

    cache->num_shards  = 1 << num_shards_bits;
    cache->num_slots   = 1 << num_slots_bits;
    cache->num_entries = 1 << entries_per_slot_bits;

    cache->num_slots_mask   = cache->num_slots - 1;
    cache->num_shards_mask  = cache->num_shards - 1;
    cache->num_entries_mask = cache->num_entries - 1;

    cache->shards = calloc(cache->num_shards, sizeof(struct chimera_vfs_attr_cache_shard));

    for (i = 0; i < cache->num_shards; i++) {

        shard          = &cache->shards[i];
        shard->entries = calloc(cache->num_slots * cache->num_entries, sizeof(struct chimera_vfs_attr_cache_entry *));

        for (j = 0; j < cache->num_slots * cache->num_entries; j++) {
            entry = calloc(1, sizeof(struct chimera_vfs_attr_cache_entry));
            LL_PREPEND(shard->free_entries, entry);
        }

        pthread_mutex_init(&shard->entry_lock, NULL);
        pthread_mutex_init(&shard->free_lock, NULL);
    }

    return cache;
} /* chimera_vfs_name_cache_create */

static inline void
chimera_vfs_attr_cache_destroy(struct chimera_vfs_attr_cache *cache)
{
    struct chimera_vfs_attr_cache_shard *shard;
    struct chimera_vfs_attr_cache_entry *entry;
    int                                  i, j;
    uint64_t                             total_lookups = 0, total_hits = 0, total_inserts = 0;

    for (i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        total_lookups += shard->num_lookups;
        total_hits    += shard->num_hits;
        total_inserts += shard->num_inserts;

        for (j = 0; j < cache->num_slots * cache->num_entries; j++) {
            if (shard->entries[j]) {
                entry = shard->entries[j];
                free(entry);
            }
        }

        free(shard->entries);

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

    chimera_vfs_info("ATTR_CACHE stats: lookups %lu hits %lu inserts %lu",
                     total_lookups,
                     total_hits,
                     total_inserts);
} /* chimera_vfs_attr_cache_destroy */

static inline int
chimera_vfs_attr_cache_lookup(
    struct chimera_vfs_attr_cache *cache,
    uint64_t                       fh_hash,
    const void                    *fh,
    int                            fh_len,
    struct chimera_vfs_attrs      *r_attr)
{
    struct chimera_vfs_attr_cache_entry  *entry;
    struct chimera_vfs_attr_cache_shard  *shard;
    struct chimera_vfs_attr_cache_entry **slot, **slot_end;
    int                                   rc;
    struct timespec                       now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    shard = &cache->shards[fh_hash & cache->num_shards_mask];

    slot = &shard->entries[(fh_hash & cache->num_slots_mask) << cache->num_entries_bits];

    slot_end = slot + cache->num_entries;

    rc = -1;

    urcu_memb_read_lock();

    while (slot < slot_end) {
        entry = rcu_dereference(*slot);

        if (entry &&
            entry->key == fh_hash &&
            chimera_timespec_cmp(&entry->expiration, &now) >= 0 &&
            chimera_memequal(entry->attr.va_fh, entry->attr.va_fh_len, fh, fh_len)) {

            *r_attr = entry->attr;
            entry->score++;
            rc = 0;
            break;
        }

        slot++;
    }

    urcu_memb_read_unlock();

    shard->num_lookups++;

    if (rc == 0) {
        shard->num_hits++;
    }

    return rc;
} /* chimera_vfs_name_cache_lookup */

static inline void
chimera_vfs_attr_cache_free_entry_rcu(struct rcu_head *head)
{
    struct chimera_vfs_attr_cache_entry *entry = container_of(head, struct chimera_vfs_attr_cache_entry, rcu);
    struct chimera_vfs_attr_cache_shard *shard = entry->shard;

    pthread_mutex_lock(&shard->free_lock);
    LL_PREPEND(shard->free_entries, entry);
    pthread_mutex_unlock(&shard->free_lock);
} /* chimera_name_cache_free_entry_rcu */

static inline void
chimera_vfs_attr_cache_insert(
    struct chimera_vfs_attr_cache *cache,
    uint64_t                       fh_hash,
    const void                    *fh,
    int                            fh_len,
    struct chimera_vfs_attrs      *attr)
{
    struct chimera_vfs_attr_cache_entry  *entry, *old_entry, *best_entry;
    struct chimera_vfs_attr_cache_shard  *shard;
    struct chimera_vfs_attr_cache_entry **slot, **slot_end, **slot_best;

    shard = &cache->shards[fh_hash & cache->num_shards_mask];

    slot = &shard->entries[(fh_hash & cache->num_slots_mask) << cache->num_entries_bits];

    slot_end = slot + cache->num_entries;

    slot_best = slot;

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MASK_STAT) == CHIMERA_VFS_ATTR_MASK_STAT) {

        pthread_mutex_lock(&shard->free_lock);

        entry = shard->free_entries;

        if (entry) {
            LL_DELETE(shard->free_entries, entry);
        }

        pthread_mutex_unlock(&shard->free_lock);


        if (!entry) {
            entry = calloc(1, sizeof(struct chimera_vfs_attr_cache_entry));
        }

        entry->key   = fh_hash;
        entry->shard = shard;
        entry->score = 0;
        entry->attr  = *attr;

        clock_gettime(CLOCK_MONOTONIC, &entry->expiration);
        entry->expiration.tv_sec += cache->ttl;

        entry->attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
        memcpy(entry->attr.va_fh, fh, fh_len);
        entry->attr.va_fh_len = fh_len;

    } else {
        entry = NULL;
    }

    urcu_memb_read_lock();

    pthread_mutex_lock(&shard->entry_lock);

    best_entry = *slot_best;

    while (slot < slot_end) {
        old_entry = *slot;

        if (old_entry && old_entry->key == fh_hash) {
            best_entry = old_entry;
            slot_best  = slot;
            break;
        }

        if ((best_entry && !old_entry) || (best_entry && old_entry && best_entry->score > old_entry->score)) {
            best_entry = old_entry;
            slot_best  = slot;
        }

        slot++;
    }

    rcu_assign_pointer(*slot_best, entry);

    shard->num_inserts++;

    pthread_mutex_unlock(&shard->entry_lock);

    urcu_memb_read_unlock();

    if (best_entry) {
        call_rcu(&best_entry->rcu, chimera_vfs_attr_cache_free_entry_rcu);
    }

} /* chimera_vfs_attr_cache_insert */