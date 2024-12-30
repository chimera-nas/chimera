#pragma once

#include <pthread.h>

#include "uthash/uthash.h"
#include "common/format.h"
#include "common/misc.h"
#include "vfs.h"
#include "vfs_procs.h"
#include "vfs_internal.h"

struct vfs_open_cache_shard {
    pthread_mutex_t                 lock;
    struct chimera_vfs_open_handle *open_files;
    struct chimera_vfs_open_handle *pending_close;
    struct chimera_vfs_open_handle *free_handles;
    uint8_t                         cache_id;
    uint32_t                        max_open_files;
    uint32_t                        open_handles;
    uint64_t                        num_lookups;
    uint64_t                        num_inserts;
};

struct vfs_open_cache {
    unsigned int                 num_shards;
    unsigned int                 shard_mask;
    struct vfs_open_cache_shard *shards;
};

static inline struct vfs_open_cache *
chimera_vfs_open_cache_init(
    uint8_t cache_id,
    int     num_shard_bits,
    int     max_open_files)
{
    struct vfs_open_cache *cache;
    int                    max_per_shard;

    cache             = calloc(1, sizeof(*cache));
    cache->num_shards = 1 << num_shard_bits;
    cache->shard_mask = cache->num_shards - 1;

    max_per_shard = max_open_files / cache->num_shards;

    cache->shards = calloc(cache->num_shards, sizeof(*cache->shards));

    for (int i = 0; i < cache->num_shards; i++) {
        pthread_mutex_init(&cache->shards[i].lock, NULL);
        cache->shards[i].open_files     = NULL;
        cache->shards[i].free_handles   = NULL;
        cache->shards[i].max_open_files = max_per_shard;
        cache->shards[i].cache_id       = cache_id;
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
        handle           = calloc(1, sizeof(*handle));
        handle->cache_id = shard->cache_id;
    }

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

    chimera_vfs_abort_if(handle->cache_id != shard->cache_id, "handle released by wrong cache");

    pthread_mutex_lock(&shard->lock);

    handle->opencnt--;

    if (handle->opencnt == 0) {
        clock_gettime(CLOCK_MONOTONIC, &handle->timestamp);
        DL_APPEND(shard->pending_close, handle);
    }

    pthread_mutex_unlock(&shard->lock);

} /* vfs_open_cache_release */

static void
chimera_vfs_open_cache_insert_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_open_handle *handle = private_data;

    chimera_vfs_abort_if(error_code, "open cache failed to close existing handle");

    handle->close_callback(handle, handle->close_private);
} /* chimera_vfs_open_cache_insert_callback */

static inline void
chimera_vfs_open_cache_insert(
    struct chimera_vfs_thread *thread,
    struct vfs_open_cache *cache,
    struct chimera_vfs_module *module,
    const void *fh,
    uint32_t fhlen,
    uint64_t fh_hash,
    uint64_t vfs_private,
    void ( *callback )(struct chimera_vfs_open_handle *handle, void *private_data),
    void *private_data)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *existing;
    int                             done = 1;

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

        if (existing->opencnt == 0) {
            DL_DELETE(shard->pending_close, existing);
        }

        /* Add our reference to the existing handle */
        existing->opencnt++;

        /* Close our own duplicate handle */
        chimera_vfs_close(thread, fh, fhlen, vfs_private, NULL, NULL);
        chimera_vfs_open_cache_free(shard, handle);

        handle = existing;
    } else {

        if (shard->open_handles < shard->max_open_files) {
            shard->open_handles++;
        } else {
            chimera_vfs_abort_if(!shard->pending_close, "open cache exhausted with referenced handles");

            existing = shard->pending_close;

            DL_DELETE(shard->pending_close, existing);
            HASH_DEL(shard->open_files, existing);

            handle->close_callback = callback;
            handle->close_private  = private_data;

            chimera_vfs_close(thread, existing->fh, existing->fh_len, existing->vfs_private,
                              chimera_vfs_open_cache_insert_callback, handle);

            chimera_vfs_open_cache_free(shard, existing);

            done = 0;
        }
    }

    pthread_mutex_unlock(&shard->lock);

    if (done) {
        callback(handle, private_data);
    }
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

        if (handle->opencnt == 0) {
            DL_DELETE(shard->pending_close, handle);
        }

        handle->opencnt++;
    }

    pthread_mutex_unlock(&shard->lock);

    return handle;
} /* chimera_vfs_open_cache_lookup */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_defer_close(
    struct vfs_open_cache *cache,
    struct timespec       *timestamp,
    uint64_t               min_age,
    uint64_t              *r_count)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *closed = NULL;
    uint64_t                        elapsed, count = 0;

    for (int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        pthread_mutex_lock(&shard->lock);

        count += shard->open_handles;

        while (shard->pending_close) {

            handle = shard->pending_close;

            elapsed = chimera_get_elapsed_ns(timestamp, &handle->timestamp);

            if (elapsed < min_age) {
                break;
            }

            DL_DELETE(shard->pending_close, handle);
            LL_PREPEND(closed, handle);
            HASH_DEL(shard->open_files, handle);
            shard->open_handles--;
        }
        pthread_mutex_unlock(&shard->lock);
    }
    *r_count = count;
    return closed;
} /* vfs_open_cache_defer_close */