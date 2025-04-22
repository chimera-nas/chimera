#pragma once

#include <pthread.h>

#include "uthash/uthash.h"
#include "common/format.h"
#include "common/misc.h"
#include "vfs.h"
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "prometheus-c.h"

struct vfs_open_cache_shard {
    pthread_mutex_t                     lock;
    struct chimera_vfs_open_handle     *open_files;
    struct chimera_vfs_open_handle     *pending_close;
    struct chimera_vfs_open_handle     *free_handles;
    uint8_t                             cache_id;
    uint32_t                            max_open_files;
    uint32_t                            open_handles;
    struct prometheus_counter_instance *acquire;
    struct prometheus_counter_instance *insert;
    struct prometheus_gauge_instance   *num_handles;
};

struct vfs_open_cache {
    unsigned int                      num_shards;
    unsigned int                      shard_mask;
    struct vfs_open_cache_shard      *shards;
    struct prometheus_metrics        *metrics;
    struct prometheus_counter        *open_cache;
    struct prometheus_counter_series *open_cache_acquire;
    struct prometheus_counter_series *open_cache_insert;
};


static inline struct vfs_open_cache *
chimera_vfs_open_cache_init(
    uint8_t                    cache_id,
    int                        num_shard_bits,
    int                        max_open_files,
    struct prometheus_metrics *metrics,
    const char                *cache_name)
{
    struct vfs_open_cache *cache;
    int                    max_per_shard;

    cache             = calloc(1, sizeof(*cache));
    cache->num_shards = 1 << num_shard_bits;
    cache->shard_mask = cache->num_shards - 1;

    max_per_shard = max_open_files / cache->num_shards;

    cache->shards = calloc(cache->num_shards, sizeof(*cache->shards));

    if (metrics) {
        cache->metrics    = metrics;
        cache->open_cache = prometheus_metrics_create_counter(metrics, "chimera_vfs_open_cache",
                                                              "Chimera VFS open cache operations");
        cache->open_cache_insert = prometheus_counter_create_series(cache->open_cache,
                                                                    (const char *[]) { "name", "op" },
                                                                    (const char *[]) { cache_name, "insert" }, 2);
        cache->open_cache_acquire = prometheus_counter_create_series(cache->open_cache,
                                                                     (const char *[]) { "name", "op" },
                                                                     (const char *[]) { cache_name, "acquire" }, 2);
    }

    for (int i = 0; i < cache->num_shards; i++) {
        pthread_mutex_init(&cache->shards[i].lock, NULL);
        cache->shards[i].open_files     = NULL;
        cache->shards[i].free_handles   = NULL;
        cache->shards[i].max_open_files = max_per_shard;
        cache->shards[i].cache_id       = cache_id;

        if (metrics) {
            cache->shards[i].insert  = prometheus_counter_series_create_instance(cache->open_cache_insert);
            cache->shards[i].acquire = prometheus_counter_series_create_instance(cache->open_cache_acquire);
        }
    }

    return cache;
} /* vfs_open_cache_init */

static inline void
chimera_vfs_open_cache_destroy(struct vfs_open_cache *cache)
{
    struct vfs_open_cache_shard *shard;

    for (int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];
        pthread_mutex_destroy(&shard->lock);
    }

    if (cache->metrics) {
        prometheus_counter_destroy_series(cache->open_cache, cache->open_cache_acquire);
        prometheus_counter_destroy_series(cache->open_cache, cache->open_cache_insert);
        prometheus_counter_destroy(cache->metrics, cache->open_cache);
    }

    free(cache->shards);
    free(cache);
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
chimera_vfs_open_cache_release_blocked(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *requests,
    int                         failed)
{
    struct chimera_vfs_thread  *request_thread;
    struct chimera_vfs_request *request;


    while (requests) {

        request = requests;

        LL_DELETE(requests, request);

        if (failed) {
            request->pending_handle = NULL;
        }

        request_thread = request->thread;

        if (request_thread == thread) {
            /* This is a request from the same thread, so we can dispatch it immediately */
            request->unblock_callback(request, request->pending_handle);
        } else {
            /* This is a request from a different thread, so we need to send it home */
            pthread_mutex_lock(&request_thread->lock);
            LL_PREPEND(request_thread->unblocked_requests, request);
            pthread_mutex_unlock(&request_thread->lock);

            /* Wake up the thread */
            evpl_ring_doorbell(&request_thread->doorbell);
        }
    }

} /* chimera_vfs_open_cache_release_blocked */

static inline void
chimera_vfs_open_cache_release(
    struct chimera_vfs_thread      *thread,
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle)
{
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_request  *requests;
    int                          failed;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    chimera_vfs_abort_if(handle->cache_id != shard->cache_id, "handle released by wrong cache");

    pthread_mutex_lock(&shard->lock);

    handle->exclusive = 0;

    /* If we are releasing a handle that is still in pending state
     * then we failed to do the open
     */

    failed = handle->pending;

    requests                 = handle->blocked_requests;
    handle->blocked_requests = NULL;

    if (failed) {
        /* We failed to open the file, so we need to remove it from the cache
         * and we'll give everyone else NULL callbaks so they'll never
         * release themselves.  Therefore we can free the handle now
         */
        handle->opencnt = 0;
        HASH_DEL(shard->open_files, handle);
        chimera_vfs_open_cache_free(shard, handle);

    } else {

        handle->opencnt--;

        if (handle->opencnt == 0) {
            clock_gettime(CLOCK_MONOTONIC, &handle->timestamp);
            DL_APPEND(shard->pending_close, handle);
        }
    }

    pthread_mutex_unlock(&shard->lock);

    chimera_vfs_open_cache_release_blocked(thread, requests, failed);

} /* vfs_open_cache_release */

static void
chimera_vfs_open_cache_close_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_open_handle *handle = private_data;

    chimera_vfs_abort_if(error_code, "open cache failed to close existing handle");

    handle->callback(handle->request, handle);
} /* chimera_vfs_open_cache_insert_callback */

static inline void
chimera_vfs_open_cache_populate(
    struct chimera_vfs_thread      *thread,
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        vfs_private_data)
{
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_request  *requests = NULL;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    handle->vfs_private = vfs_private_data;
    handle->pending     = 0;

    if (!handle->exclusive) {
        requests                 = handle->blocked_requests;
        handle->blocked_requests = NULL;
    }

    pthread_mutex_unlock(&shard->lock);

    if (requests) {
        chimera_vfs_open_cache_release_blocked(thread, requests, 0);
    }

} /* chimera_vfs_open_cache_populate */

static inline void
chimera_vfs_open_cache_acquire(
    struct chimera_vfs_thread *thread,
    struct vfs_open_cache *cache,
    struct chimera_vfs_module *module,
    struct chimera_vfs_request *request,
    const void *fh,
    uint32_t fhlen,
    uint64_t fh_hash,
    uint64_t vfs_private_data,
    int exclusive,
    void ( *callback )(struct chimera_vfs_request *request, struct chimera_vfs_open_handle *handle))
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *existing;
    int                             done = 0;

    shard = &cache->shards[fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    HASH_FIND_BYHASHVALUE(hh, shard->open_files, fh, fhlen, (fh_hash & 0xFFFFFFFF), handle);

    if (handle) {

        chimera_vfs_abort_if(handle->pending && vfs_private_data != UINT64_MAX,
                             "open cache pending handle with vfs private data");

        if (handle->opencnt == 0) {
            DL_DELETE(shard->pending_close, handle);
        }

        handle->opencnt++;

        if (handle->exclusive || handle->pending) {
            request->unblock_callback = callback;
            request->pending_handle   = handle;
            LL_PREPEND(handle->blocked_requests, request);
            done = 0;
        } else {
            done = 1;
        }
    } else {

        prometheus_counter_increment(shard->insert);

        handle                   = chimera_vfs_open_cache_alloc(shard);
        handle->vfs_module       = module;
        handle->fh_hash          = fh_hash;
        handle->fh_len           = fhlen;
        handle->opencnt          = 1;
        handle->exclusive        = exclusive;
        handle->callback         = callback;
        handle->request          = request;
        handle->vfs_private      = vfs_private_data;
        handle->blocked_requests = NULL;

        if (handle->vfs_private == UINT64_MAX) {
            handle->pending = 1;
        } else {
            handle->pending = 0;
        }

        memcpy(handle->fh, fh, fhlen);

        HASH_ADD_BYHASHVALUE(hh, shard->open_files, fh, handle->fh_len, (handle->fh_hash & 0xFFFFFFFF), handle);

        if (shard->open_handles < shard->max_open_files) {
            shard->open_handles++;
            done = 1;
        } else {
            chimera_vfs_abort_if(!shard->pending_close, "open cache exhausted with referenced handles");

            existing = shard->pending_close;

            DL_DELETE(shard->pending_close, existing);
            HASH_DEL(shard->open_files, existing);

            chimera_vfs_close(thread, existing->fh, existing->fh_len, existing->vfs_private,
                              chimera_vfs_open_cache_close_callback, handle);

            chimera_vfs_open_cache_free(shard, existing);

            done = 0;
        }

    }

    prometheus_counter_increment(shard->acquire);

    pthread_mutex_unlock(&shard->lock);

    if (done) {
        callback(request, handle);
    }
} /* chimera_vfs_open_cache_acquire */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_defer_close(
    struct vfs_open_cache *cache,
    struct timespec       *timestamp,
    uint64_t               min_age,
    uint64_t              *r_count)
{

#ifndef __clang_analyzer__

    /* HASH_DEL blows clangs mind so we disable this block under analyzer */

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
#else  /* ifndef __clang_analyzer__ */
    *r_count = 0;
    return NULL;
#endif /* ifndef __clang_analyzer__ */
} /* vfs_open_cache_defer_close */