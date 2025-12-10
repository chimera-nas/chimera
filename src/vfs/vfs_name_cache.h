// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs.h"
#include <urcu.h>
#include <urcu/urcu-memb.h>

struct chimera_vfs_name_cache_entry {
    uint64_t        key;
    uint8_t         parent_fh_len;
    uint8_t         child_fh_len;
    uint16_t        name_len;
    int64_t         score;
    struct timespec expiration;
    struct rcu_head rcu;
    union {
        struct chimera_vfs_name_cache_entry *next; /* when on the free list */
        struct chimera_vfs_name_cache_shard *shard; /* when not in the free list */
    };
    uint8_t         parent_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t         child_fh[CHIMERA_VFS_FH_SIZE];
    char            child_name[CHIMERA_VFS_NAME_MAX];
};

struct chimera_vfs_name_cache_shard {
    struct chimera_vfs_name_cache_entry **entries;
    struct chimera_vfs_name_cache_entry  *free_entries;
    pthread_mutex_t                       entry_lock;
    pthread_mutex_t                       free_lock;
    struct prometheus_counter_instance   *miss;
    struct prometheus_counter_instance   *hit;
    struct prometheus_counter_instance   *insert;
};

struct chimera_vfs_name_cache {
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
    struct chimera_vfs_name_cache_shard *shards;
    struct prometheus_metrics           *metrics;
    struct prometheus_counter           *name_cache;
    struct prometheus_counter_series    *miss_series;
    struct prometheus_counter_series    *hit_series;
    struct prometheus_counter_series    *insert_series;
};

static inline struct chimera_vfs_name_cache *
chimera_vfs_name_cache_create(
    uint8_t                    num_shards_bits,
    uint8_t                    num_slots_bits,
    uint8_t                    entries_per_slot_bits,
    uint64_t                   ttl,
    struct prometheus_metrics *metrics)
{
    struct chimera_vfs_name_cache       *cache;
    struct chimera_vfs_name_cache_shard *shard;
    struct chimera_vfs_name_cache_entry *entry;
    int                                  i, j;

    cache = calloc(1, sizeof(struct chimera_vfs_name_cache));

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

    cache->shards = calloc(cache->num_shards, sizeof(struct chimera_vfs_name_cache_shard));

    if (metrics) {
        cache->metrics    = metrics;
        cache->name_cache = prometheus_metrics_create_counter(metrics, "chimera_name_cache",
                                                              "Operations on the chimera VFS name cache");

        cache->miss_series = prometheus_counter_create_series(cache->name_cache,
                                                              (const char *[]) { "op" },
                                                              (const char *[]) { "miss" }, 1);
        cache->hit_series = prometheus_counter_create_series(cache->name_cache,
                                                             (const char *[]) { "op" },
                                                             (const char *[]) { "hit" }, 1);
        cache->insert_series = prometheus_counter_create_series(cache->name_cache,
                                                                (const char *[]) { "op" },
                                                                (const char *[]) { "insert" }, 1);

    }

    for (i = 0; i < cache->num_shards; i++) {

        shard          = &cache->shards[i];
        shard->entries = calloc(cache->num_slots * cache->num_entries, sizeof(struct chimera_vfs_name_cache_entry *));

        for (j = 0; j < cache->num_slots * cache->num_entries; j++) {
            entry = calloc(1, sizeof(struct chimera_vfs_name_cache_entry));
            LL_PREPEND(shard->free_entries, entry);
        }

        pthread_mutex_init(&shard->entry_lock, NULL);
        pthread_mutex_init(&shard->free_lock, NULL);

        shard->miss   = prometheus_counter_series_create_instance(cache->miss_series);
        shard->hit    = prometheus_counter_series_create_instance(cache->hit_series);
        shard->insert = prometheus_counter_series_create_instance(cache->insert_series);
    }

    return cache;
} /* chimera_vfs_name_cache_create */

static inline void
chimera_vfs_name_cache_destroy(struct chimera_vfs_name_cache *cache)
{
    struct chimera_vfs_name_cache_shard *shard;
    struct chimera_vfs_name_cache_entry *entry;
    int                                  i, j;

    rcu_barrier();

    for (i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        if (cache->metrics) {
            prometheus_counter_series_destroy_instance(cache->miss_series, shard->miss);
            prometheus_counter_series_destroy_instance(cache->hit_series, shard->hit);
            prometheus_counter_series_destroy_instance(cache->insert_series, shard->insert);
        }

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

    if (cache->metrics) {
        prometheus_counter_destroy_series(cache->name_cache, cache->miss_series);
        prometheus_counter_destroy_series(cache->name_cache, cache->hit_series);
        prometheus_counter_destroy_series(cache->name_cache, cache->insert_series);
        prometheus_counter_destroy(cache->metrics, cache->name_cache);
    }

    free(cache->shards);
    free(cache);

} /* chimera_vfs_name_cache_destroy */

static inline int
chimera_vfs_name_cache_lookup(
    struct chimera_vfs_name_cache *cache,
    uint64_t                       fh_hash,
    const void                    *fh,
    int                            fh_len,
    uint64_t                       name_hash,
    const char                    *name,
    int                            name_len,
    void                          *r_child_fh,
    uint32_t                      *r_child_fh_len)
{
    struct chimera_vfs_name_cache_entry  *entry;
    struct chimera_vfs_name_cache_shard  *shard;
    struct chimera_vfs_name_cache_entry **slot, **slot_end;
    uint64_t                              key = fh_hash ^ name_hash;
    int                                   rc;
    struct timespec                       now;

    clock_gettime(CLOCK_MONOTONIC, &now);

    shard = &cache->shards[key & cache->num_shards_mask];

    slot = &shard->entries[(key & cache->num_slots_mask) << cache->num_entries_bits];

    slot_end = slot + cache->num_entries;

    rc = -1;

    urcu_memb_read_lock();

    while (slot < slot_end) {
        entry = rcu_dereference(*slot);

        if (entry && entry->key == key &&
            chimera_timespec_cmp(&entry->expiration, &now) >= 0 &&
            chimera_memequal(entry->parent_fh, entry->parent_fh_len, fh, fh_len) &&
            chimera_memequal(entry->child_name, entry->name_len, name, name_len)) {

            *r_child_fh_len = entry->child_fh_len;
            memcpy(r_child_fh, entry->child_fh, entry->child_fh_len);
            entry->score++;

            rc = 0;
            break;
        }

        slot++;
    }

    urcu_memb_read_unlock();

    if (rc == 0) {
        prometheus_counter_increment(shard->hit);
    } else {
        prometheus_counter_increment(shard->miss);
    }

    return rc;
} /* chimera_vfs_name_cache_lookup */

static inline void
chimera_name_cache_free_entry_rcu(struct rcu_head *head)
{
    struct chimera_vfs_name_cache_entry *entry = container_of(head, struct chimera_vfs_name_cache_entry, rcu);
    struct chimera_vfs_name_cache_shard *shard = entry->shard;

    pthread_mutex_lock(&shard->free_lock);
    LL_PREPEND(shard->free_entries, entry);
    pthread_mutex_unlock(&shard->free_lock);
} /* chimera_name_cache_free_entry_rcu */

static inline void
chimera_vfs_name_cache_insert(
    struct chimera_vfs_name_cache *cache,
    uint64_t                       fh_hash,
    const void                    *fh,
    int                            fh_len,
    uint64_t                       name_hash,
    const char                    *name,
    int                            name_len,
    const void                    *child_fh,
    int                            child_fh_len)
{
    struct chimera_vfs_name_cache_entry  *entry, *old_entry, *best_entry;
    struct chimera_vfs_name_cache_shard  *shard;
    struct chimera_vfs_name_cache_entry **slot, **slot_end, **slot_best;
    uint64_t                              key = fh_hash ^ name_hash;
    struct timespec                       now;

    clock_gettime(CLOCK_MONOTONIC, &now);

    shard = &cache->shards[key & cache->num_shards_mask];

    slot = &shard->entries[(key & cache->num_slots_mask) << cache->num_entries_bits];

    slot_end = slot + cache->num_entries;

    slot_best = slot;

    pthread_mutex_lock(&shard->free_lock);

    entry = shard->free_entries;

    if (entry) {
        LL_DELETE(shard->free_entries, entry);
    }

    pthread_mutex_unlock(&shard->free_lock);


    if (!entry) {
        entry = calloc(1, sizeof(struct chimera_vfs_name_cache_entry));
    }

    entry->key           = key;
    entry->parent_fh_len = fh_len;
    entry->child_fh_len  = child_fh_len;
    entry->name_len      = name_len;
    entry->shard         = shard;
    entry->score         = 0;

    entry->expiration.tv_sec  = now.tv_sec + cache->ttl;
    entry->expiration.tv_nsec = now.tv_nsec;

    memcpy(entry->parent_fh, fh, fh_len);

    if (child_fh_len) {
        memcpy(entry->child_fh, child_fh, child_fh_len);
    }

    if (name_len) {
        memcpy(entry->child_name, name, name_len);
    }

    urcu_memb_read_lock();

    pthread_mutex_lock(&shard->entry_lock);

    best_entry = *slot_best;

    while (slot < slot_end) {
        old_entry = *slot;

        if (old_entry && old_entry->key == key &&
            chimera_memequal(old_entry->parent_fh, old_entry->parent_fh_len, fh, fh_len) &&
            chimera_memequal(old_entry->child_name, old_entry->name_len, name, name_len)) {

            /* IF same FH/name is in cache, we must replace it */

            best_entry = old_entry;
            slot_best  = slot;
            break;
        }

        if (!best_entry) {
            /* Otherwise if we found an empty slot there's no need to check further */
            slot++;
            continue;
        }

        if (!old_entry) {
            /* empty is always better than not empty */
            best_entry = old_entry;
            slot_best  = slot;
            slot++;
            continue;
        }

        if (chimera_timespec_cmp(&best_entry->expiration, &now) < 0) {
            /* This entry is expired, so its effectively empty */
            old_entry->score = -1;
        }

        /* Otherwise old_entry is better than best_entry if it has a lower score,
         * or if its score is the same and it expires later (ie it is newer)
         */

        if ((best_entry->score > old_entry->score) ||
            (best_entry->score == old_entry->score &&
             chimera_timespec_cmp(&best_entry->expiration, &old_entry->expiration) < 0)) {
            best_entry = old_entry;
            slot_best  = slot;
        }

        slot++;
    }

    rcu_assign_pointer(*slot_best, entry);

    prometheus_counter_increment(shard->insert);

    pthread_mutex_unlock(&shard->entry_lock);

    urcu_memb_read_unlock();

    if (best_entry) {
        call_rcu(&best_entry->rcu, chimera_name_cache_free_entry_rcu);
    }

} /* chimera_vfs_name_cache_insert */

static inline void
chimera_vfs_name_cache_remove(
    struct chimera_vfs_name_cache *cache,
    uint64_t                       fh_hash,
    const void                    *fh,
    int                            fh_len,
    uint64_t                       name_hash,
    const char                    *name,
    int                            name_len)
{
    struct chimera_vfs_name_cache_entry  *entry, *removed_entry = NULL;
    struct chimera_vfs_name_cache_shard  *shard;
    struct chimera_vfs_name_cache_entry **slot, **slot_end;
    uint64_t                              key = fh_hash ^ name_hash;

    shard = &cache->shards[key & cache->num_shards_mask];

    slot = &shard->entries[(key & cache->num_slots_mask) << cache->num_entries_bits];

    slot_end = slot + cache->num_entries;

    urcu_memb_read_lock();

    pthread_mutex_lock(&shard->entry_lock);

    while (slot < slot_end) {

        entry = *slot;

        if (entry && entry->key == key &&
            chimera_memequal(entry->parent_fh, entry->parent_fh_len, fh, fh_len) &&
            chimera_memequal(entry->child_name, entry->name_len, name, name_len)) {

            removed_entry = entry;
            rcu_assign_pointer(*slot, NULL);
            break;
        }

        slot++;
    }

    //prometheus_counter_increment(shard->remove);

    pthread_mutex_unlock(&shard->entry_lock);

    urcu_memb_read_unlock();

    if (removed_entry) {
        call_rcu(&removed_entry->rcu, chimera_name_cache_free_entry_rcu);
    }

} /* chimera_vfs_name_cache_insert */