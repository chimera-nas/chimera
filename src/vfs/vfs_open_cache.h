#pragma once

#include <pthread.h>

#include "uthash/uthash.h"
#include "common/format.h"
#include "common/misc.h"
#include "vfs.h"
#include "vfs_procs.h"

struct vfs_open_cache_shard {
    pthread_mutex_t                 lock;
    struct chimera_vfs_open_handle *open_files;
    struct chimera_vfs_open_handle *free_handles;
    uint64_t                        num_lookups;
    uint64_t                        num_inserts;
};

struct vfs_open_cache {
    unsigned int                 num_shards;
    unsigned int                 shard_mask;
    struct vfs_open_cache_shard *shards;
};

static inline struct vfs_open_cache *
chimera_vfs_open_cache_init(int num_shard_bits)
{
    struct vfs_open_cache *cache;

    cache             = calloc(1, sizeof(*cache));
    cache->num_shards = 1 << num_shard_bits;
    cache->shard_mask = cache->num_shards - 1;

    cache->shards = calloc(cache->num_shards, sizeof(*cache->shards));

    for (int i = 0; i < cache->num_shards; i++) {
        pthread_mutex_init(&cache->shards[i].lock, NULL);
        cache->shards[i].open_files   = NULL;
        cache->shards[i].free_handles = NULL;
    }

    return cache;
} /* vfs_open_cache_init */

static inline void
chimera_vfs_open_cache_destroy(struct vfs_open_cache *cache)
{
    struct vfs_open_cache_shard *shard;
    uint64_t                     total_lookups = 0;
    uint64_t                     total_inserts = 0;

    for (int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];
        pthread_mutex_destroy(&shard->lock);
        total_lookups += shard->num_lookups;
        total_inserts += shard->num_inserts;
    }
    free(cache->shards);
    free(cache);

    chimera_vfs_info("open cache total lookups %lu total inserts %lu",
                     total_lookups, total_inserts);
} /* vfs_open_cache_destroy */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_alloc(struct vfs_open_cache_shard *shard)
{
    struct chimera_vfs_open_handle *handle;

    handle = shard->free_handles;
    if (handle) {
        LL_DELETE(shard->free_handles, handle);
    } else {
        handle = calloc(1, sizeof(*handle));
    }

    handle->timestamp.tv_sec  = 0;
    handle->timestamp.tv_nsec = 0;

    return handle;
} /* chimera_vfs_open_cache_alloc */

static inline void
chimera_vfs_open_cache_free(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    LL_PREPEND(shard->free_handles, handle);
} /* chimera_vfs_open_cache_free */



static inline void
chimera_vfs_open_cache_release(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle)
{
    struct vfs_open_cache_shard *shard;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);
    handle->opencnt--;
    pthread_mutex_unlock(&shard->lock);

} /* vfs_open_cache_release */
static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_insert(
    struct chimera_vfs_thread *thread,
    struct vfs_open_cache     *cache,
    struct chimera_vfs_module *module,
    const void                *fh,
    uint32_t                   fhlen,
    uint64_t                   fh_hash,
    uint64_t                   vfs_private)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *existing;

    shard = &cache->shards[fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    shard->num_inserts++;

    handle              = chimera_vfs_open_cache_alloc(shard);
    handle->vfs_module  = module;
    handle->fh_hash     = fh_hash & 0xFFFFFFFF;
    handle->fh_len      = fhlen;
    handle->opencnt     = 1;
    handle->vfs_private = vfs_private;

    memcpy(handle->fh, fh, fhlen);

    HASH_REPLACE_BYHASHVALUE(hh, shard->open_files, fh, handle->fh_len, handle->fh_hash, handle, existing);

    if (unlikely(existing)) {

        /* We lost a race to open this file handle */

        /* Remove the new we just added and put the original one back */
        HASH_DEL(shard->open_files, handle);
        HASH_ADD_BYHASHVALUE(hh, shard->open_files, fh, handle->fh_len, handle->fh_hash, existing);

        /* Add our reference to the existing handle */
        existing->opencnt++;
        existing->timestamp.tv_sec  = 0;
        existing->timestamp.tv_nsec = 0;

        /* Close our own duplicate handle*/
        chimera_vfs_close(thread, module, fh, fhlen, vfs_private, NULL, NULL);
        chimera_vfs_open_cache_free(shard, handle);

    }

    pthread_mutex_unlock(&shard->lock);

    return handle;
} /* chimera_vfs_open_cache_insert */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_lookup(
    struct vfs_open_cache     *cache,
    struct chimera_vfs_module *module,
    const void                *fh,
    uint32_t                   fhlen,
    uint64_t                   fh_hash)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle;
    uint32_t                        _hash = fh_hash & 0xFFFFFFFF;

    shard = &cache->shards[fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    shard->num_lookups++;

    HASH_FIND_BYHASHVALUE(hh, shard->open_files, fh, fhlen, _hash, handle);

    if (handle) {
        handle->opencnt++;
        handle->timestamp.tv_sec  = 0;
        handle->timestamp.tv_nsec = 0;
    }

    pthread_mutex_unlock(&shard->lock);

    return handle;
} /* chimera_vfs_open_cache_lookup */

static inline uint64_t
chimera_vfs_open_cache_size(struct vfs_open_cache *cache)
{
    struct vfs_open_cache_shard *shard;
    uint64_t                     size = 0;

    for (int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];
        pthread_mutex_lock(&shard->lock);
        size += HASH_COUNT(shard->open_files);
        pthread_mutex_unlock(&shard->lock);
    }

    return size;
} /* chimera_vfs_open_cache_size */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_defer_close(
    struct vfs_open_cache *cache,
    struct timespec       *timestamp,
    uint64_t               min_age,
    uint64_t              *r_count)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *tmp, *closed = NULL;
    uint64_t                        elapsed, count = 0;

    for (int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        pthread_mutex_lock(&shard->lock);

        HASH_ITER(hh, shard->open_files, handle, tmp)
        {

            count++;

            if (handle->opencnt) {
                continue;
            }

            if (handle->timestamp.tv_sec == 0) {
                handle->timestamp = *timestamp;
            }

            elapsed = chimera_get_elapsed_ns(timestamp, &handle->timestamp);

            if (elapsed > min_age) {
                LL_PREPEND(closed, handle);
                HASH_DEL(shard->open_files, handle);
            }
        }
        pthread_mutex_unlock(&shard->lock);
    }
    *r_count = count;
    return closed;
} /* vfs_open_cache_defer_close */