// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs.h"
#include "vfs/vfs_rcu_pool.h"
#include <urcu/urcu-qsbr.h>
#include "prometheus-c.h"

struct chimera_vfs_attr_cache_entry {
    struct chimera_rcu_node  rnode; /* must be first: aliases the entry pointer */
    uint64_t                 key;
    uint64_t                 score;
    uint64_t                 expiration; /* stopwatch ticks */
    struct chimera_vfs_attrs attr;
};

struct chimera_vfs_attr_cache_shard {
    struct chimera_vfs_attr_cache_entry **entries;
    pthread_mutex_t                       entry_lock;
    struct prometheus_counter_instance   *insert;
    struct prometheus_counter_instance   *hit;
    struct prometheus_counter_instance   *miss;
    struct prometheus_counter_instance   *skip;
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
    struct chimera_rcu_pool              pool;
    struct chimera_vfs_attr_cache_shard *shards;
    struct prometheus_metrics           *metrics;
    struct prometheus_counter           *attr_cache;
    struct prometheus_counter_series    *insert_series;
    struct prometheus_counter_series    *hit_series;
    struct prometheus_counter_series    *miss_series;
    struct prometheus_counter_series    *skip_series;
};

static inline struct chimera_vfs_attr_cache *
chimera_vfs_attr_cache_create(
    uint8_t                    num_shards_bits,
    uint8_t                    num_slots_bits,
    uint8_t                    entries_per_slot_bits,
    uint64_t                   ttl,
    struct prometheus_metrics *metrics)
{
    struct chimera_vfs_attr_cache       *cache;
    struct chimera_vfs_attr_cache_shard *shard;
    int                                  i;

    cache = calloc(1, sizeof(struct chimera_vfs_attr_cache));

    cache->num_shards_bits  = num_shards_bits;
    cache->num_slots_bits   = num_slots_bits;
    cache->num_entries_bits = entries_per_slot_bits;
    cache->ttl              = ttl;

    chimera_rcu_pool_init(&cache->pool, CHIMERA_RCU_POOL_ATTR,
                          sizeof(struct chimera_vfs_attr_cache_entry));

    cache->num_shards  = 1 << num_shards_bits;
    cache->num_slots   = 1 << num_slots_bits;
    cache->num_entries = 1 << entries_per_slot_bits;

    cache->num_slots_mask   = cache->num_slots - 1;
    cache->num_shards_mask  = cache->num_shards - 1;
    cache->num_entries_mask = cache->num_entries - 1;

    cache->shards = calloc(cache->num_shards, sizeof(struct chimera_vfs_attr_cache_shard));

    if (metrics) {
        cache->metrics    = metrics;
        cache->attr_cache = prometheus_metrics_create_counter(metrics, "chimera_attr_cache",
                                                              "Operations on the chimera VFS attribute cache");

        cache->insert_series = prometheus_counter_create_series(cache->attr_cache,
                                                                (const char *[]) { "op" },
                                                                (const char *[]) { "insert" }, 1);
        cache->hit_series = prometheus_counter_create_series(cache->attr_cache,
                                                             (const char *[]) { "op" },
                                                             (const char *[]) { "hit" }, 1);
        cache->miss_series = prometheus_counter_create_series(cache->attr_cache,
                                                              (const char *[]) { "op" },
                                                              (const char *[]) { "miss" }, 1);
        cache->skip_series = prometheus_counter_create_series(cache->attr_cache,
                                                              (const char *[]) { "op" },
                                                              (const char *[]) { "skip" }, 1);
    }

    for (i = 0; i < cache->num_shards; i++) {

        shard          = &cache->shards[i];
        shard->entries = calloc(cache->num_slots * cache->num_entries, sizeof(struct chimera_vfs_attr_cache_entry *));

        pthread_mutex_init(&shard->entry_lock, NULL);

        if (metrics) {
            shard->insert = prometheus_counter_series_create_instance(cache->insert_series);
            shard->hit    = prometheus_counter_series_create_instance(cache->hit_series);
            shard->miss   = prometheus_counter_series_create_instance(cache->miss_series);
            shard->skip   = prometheus_counter_series_create_instance(cache->skip_series);
        }
    }

    return cache;
} /* chimera_vfs_name_cache_create */

static inline void
chimera_vfs_attr_cache_destroy(struct chimera_vfs_attr_cache *cache)
{
    struct chimera_vfs_attr_cache_shard *shard;
    struct chimera_vfs_attr_cache_entry *entry;
    int                                  i, j;

    rcu_barrier();

    for (i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        prometheus_counter_series_destroy_instance(cache->insert_series, shard->insert);
        prometheus_counter_series_destroy_instance(cache->hit_series, shard->hit);
        prometheus_counter_series_destroy_instance(cache->miss_series, shard->miss);
        prometheus_counter_series_destroy_instance(cache->skip_series, shard->skip);

        for (j = 0; j < cache->num_slots * cache->num_entries; j++) {
            if (shard->entries[j]) {
                entry = shard->entries[j];
                free(entry);
            }
        }

        free(shard->entries);

        pthread_mutex_destroy(&shard->entry_lock);
    }

    chimera_rcu_pool_destroy(&cache->pool);

    if (cache->metrics) {
        prometheus_counter_destroy_series(cache->attr_cache, cache->insert_series);
        prometheus_counter_destroy_series(cache->attr_cache, cache->hit_series);
        prometheus_counter_destroy_series(cache->attr_cache, cache->miss_series);
        prometheus_counter_destroy_series(cache->attr_cache, cache->skip_series);
        prometheus_counter_destroy(cache->metrics, cache->attr_cache);
    }

    free(cache->shards);
    free(cache);
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
    uint64_t                              now = chimera_vfs_now_ticks();

    shard = &cache->shards[fh_hash & cache->num_shards_mask];

    slot = &shard->entries[(fh_hash & cache->num_slots_mask) << cache->num_entries_bits];

    slot_end = slot + cache->num_entries;

    rc = -1;

    urcu_qsbr_read_lock();

    while (slot < slot_end) {
        entry = rcu_dereference(*slot);

        if (entry &&
            entry->key == fh_hash &&
            entry->expiration >= now &&
            chimera_memequal(entry->attr.va_fh, entry->attr.va_fh_len, fh, fh_len)) {

            *r_attr = entry->attr;
            entry->score++;
            rc = 0;
            break;
        }

        slot++;
    }

    urcu_qsbr_read_unlock();

    if (rc == 0) {
        prometheus_counter_increment(shard->hit);
    } else {
        prometheus_counter_increment(shard->miss);
    }

    return rc;
} /* chimera_vfs_name_cache_lookup */

static inline void
chimera_vfs_attr_cache_insert(
    struct chimera_vfs_thread     *thread,
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

        entry = (struct chimera_vfs_attr_cache_entry *)
            chimera_rcu_pool_alloc(&thread->rcu_magazines[CHIMERA_RCU_POOL_ATTR], &cache->pool);

        entry->key   = fh_hash;
        entry->score = 0;
        entry->attr  = *attr;

        entry->expiration = chimera_vfs_now_ticks() +
            chimera_vfs_ns_to_ticks((uint64_t) cache->ttl * 1000000000ULL);

        entry->attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
        memcpy(entry->attr.va_fh, fh, fh_len);
        entry->attr.va_fh_len = fh_len;

    } else {
        entry = NULL;
    }

    urcu_qsbr_read_lock();

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

    prometheus_counter_increment(shard->insert);

    pthread_mutex_unlock(&shard->entry_lock);

    urcu_qsbr_read_unlock();

    if (best_entry) {
        call_rcu(&best_entry->rnode.rcu, chimera_rcu_pool_retire);
    }

} /* chimera_vfs_attr_cache_insert */

/*
 * Do two attr sets carry the same change-significant fields?  ctime is the
 * metadata catch-all (it advances on any mode/uid/gid/size/mtime change), mtime
 * covers data writes, size guards truncate/append, and atime is included so a
 * relatime atime bump is treated as a change and refreshes the entry.
 */
static inline int
chimera_vfs_attrs_times_equal(
    const struct chimera_vfs_attrs *a,
    const struct chimera_vfs_attrs *b)
{
    return a->va_size == b->va_size &&
           a->va_mtime.tv_sec == b->va_mtime.tv_sec &&
           a->va_mtime.tv_nsec == b->va_mtime.tv_nsec &&
           a->va_ctime.tv_sec == b->va_ctime.tv_sec &&
           a->va_ctime.tv_nsec == b->va_ctime.tv_nsec &&
           a->va_atime.tv_sec == b->va_atime.tv_sec &&
           a->va_atime.tv_nsec == b->va_atime.tv_nsec;
} /* chimera_vfs_attrs_times_equal */

/*
 * Refresh the cached attrs for a non-mutating op (read/getattr/lookup/commit).
 * Such ops almost never change the attributes, so instead of unconditionally
 * inserting (alloc + shard mutex + RCU retire) we first do a lock-free RCU
 * lookup; if a live entry already holds these exact change-significant fields,
 * there is nothing to do.  Only on a miss / expiry / genuine change do we fall
 * through to the full insert.  Mutating ops should call _insert directly -- for
 * them the compare would always miss and just waste an RCU scan.
 *
 * Skipping does not refresh the entry's TTL (the entry was already live), so a
 * hot read-only object re-inserts at most once per TTL.
 */
static inline void
chimera_vfs_attr_cache_refresh(
    struct chimera_vfs_thread     *thread,
    struct chimera_vfs_attr_cache *cache,
    uint64_t                       fh_hash,
    const void                    *fh,
    int                            fh_len,
    struct chimera_vfs_attrs      *attr)
{
    struct chimera_vfs_attr_cache_entry  *entry;
    struct chimera_vfs_attr_cache_shard  *shard;
    struct chimera_vfs_attr_cache_entry **slot, **slot_end;
    int                                   unchanged = 0;
    uint64_t                              now       = chimera_vfs_now_ticks();

    /* Only stat-complete attrs are cacheable (same gate as _insert). */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MASK_STAT) != CHIMERA_VFS_ATTR_MASK_STAT) {
        return;
    }

    shard = &cache->shards[fh_hash & cache->num_shards_mask];

    slot = &shard->entries[(fh_hash & cache->num_slots_mask) << cache->num_entries_bits];

    slot_end = slot + cache->num_entries;

    urcu_qsbr_read_lock();

    while (slot < slot_end) {
        entry = rcu_dereference(*slot);

        if (entry &&
            entry->key == fh_hash &&
            entry->expiration >= now &&
            chimera_memequal(entry->attr.va_fh, entry->attr.va_fh_len, fh, fh_len)) {

            unchanged = chimera_vfs_attrs_times_equal(&entry->attr, attr);
            break;
        }

        slot++;
    }

    urcu_qsbr_read_unlock();

    if (unchanged) {
        prometheus_counter_increment(shard->skip);
        return;
    }

    chimera_vfs_attr_cache_insert(thread, cache, fh_hash, fh, fh_len, attr);
} /* chimera_vfs_attr_cache_refresh */