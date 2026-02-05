// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>

#include "common/format.h"
#include "common/misc.h"
#include "vfs.h"
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "prometheus-c.h"

struct vfs_open_cache_shard {
    pthread_mutex_t                     lock;
    struct chimera_vfs_open_handle     *handles;
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

/* --- Shard linked-list helpers --- */

static inline void
chimera_vfs_open_cache_shard_insert(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    handle->bucket_prev = NULL;
    handle->bucket_next = shard->handles;
    if (shard->handles) {
        shard->handles->bucket_prev = handle;
    }
    shard->handles = handle;
} // chimera_vfs_open_cache_shard_insert

static inline void
chimera_vfs_open_cache_shard_remove(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    if (handle->bucket_prev) {
        handle->bucket_prev->bucket_next = handle->bucket_next;
    } else {
        shard->handles = handle->bucket_next;
    }
    if (handle->bucket_next) {
        handle->bucket_next->bucket_prev = handle->bucket_prev;
    }
    handle->bucket_next = NULL;
    handle->bucket_prev = NULL;
} // chimera_vfs_open_cache_shard_remove

/*
 * Find a handle in the shard matching fh and access_mode.
 *
 * For RW requests, only an exact RW match is returned.
 * For RO requests, any matching handle (RW or RO) is returned,
 * since a RW handle can satisfy reads.
 */
static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_shard_find(
    struct vfs_open_cache_shard *shard,
    const void                  *fh,
    uint32_t                     fhlen,
    uint8_t                      access_mode)
{
    struct chimera_vfs_open_handle *h;

    for (h = shard->handles; h; h = h->bucket_next) {
        if (h->fh_len == fhlen && memcmp(h->fh, fh, fhlen) == 0) {
            if (h->access_mode == CHIMERA_VFS_ACCESS_MODE_RW ||
                access_mode == CHIMERA_VFS_ACCESS_MODE_RO) {
                return h;
            }
        }
    }
    return NULL;
} /* chimera_vfs_open_cache_shard_find */

/* Derive access mode from open flags */
static inline uint8_t
chimera_vfs_open_access_mode(unsigned int open_flags)
{
    return (open_flags & CHIMERA_VFS_OPEN_READ_ONLY) ?
           CHIMERA_VFS_ACCESS_MODE_RO : CHIMERA_VFS_ACCESS_MODE_RW;
} // chimera_vfs_open_access_mode

/* --- Init / Destroy --- */

static inline struct vfs_open_cache *
chimera_vfs_open_cache_init(
    uint8_t                    cache_id,
    int                        num_shard_bits,
    int                        max_open_files,
    struct prometheus_metrics *metrics,
    const char                *cache_name)
{
    struct vfs_open_cache *cache;
    int                    num_shard_bits_actual;
    int                    max_per_shard;

    cache = calloc(1, sizeof(*cache));

    num_shard_bits_actual = num_shard_bits + 4;
    cache->num_shards     = 1 << num_shard_bits_actual;
    cache->shard_mask     = cache->num_shards - 1;

    max_per_shard = max_open_files / cache->num_shards;
    if (max_per_shard < 4) {
        max_per_shard = 4;
    }

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

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        pthread_mutex_init(&cache->shards[i].lock, NULL);
        cache->shards[i].handles        = NULL;
        cache->shards[i].free_handles   = NULL;
        cache->shards[i].pending_close  = NULL;
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
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *tmp;

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        /* Free any handles remaining on the free list */
        handle = shard->free_handles;
        while (handle) {
            tmp = handle->next;
            free(handle);
            handle = tmp;
        }

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

/* --- Handle alloc / free --- */

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

/* --- Blocked request handling --- */

static inline void
chimera_vfs_open_cache_release_blocked(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *requests,
    enum chimera_vfs_error      error_code)
{
    struct chimera_vfs_thread  *request_thread;
    struct chimera_vfs_request *request;
    int                         count = 0;

    while (requests) {

        request = requests;

        LL_DELETE(requests, request);

        request->status = error_code;

        if (error_code) {
            request->pending_handle = NULL;
        }

        request_thread = request->thread;

        if (request_thread == thread) {
            /* This is a request from the same thread, so we can dispatch it immediately */
            chimera_vfs_debug("release_blocked: UNBLOCKING request=%p handle=%p SAME thread, calling directly",
                              request, request->pending_handle);
            request->unblock_callback(request, request->pending_handle);
        } else {
            /* This is a request from a different thread, so we need to send it home */
            chimera_vfs_debug("release_blocked: UNBLOCKING request=%p handle=%p DIFFERENT thread, queueing",
                              request, request->pending_handle);
            pthread_mutex_lock(&request_thread->lock);
            LL_PREPEND(request_thread->unblocked_requests, request);
            pthread_mutex_unlock(&request_thread->lock);

            /* Wake up the thread */
            evpl_ring_doorbell(&request_thread->doorbell);
        }
        count++;
    }

    if (count > 0) {
        chimera_vfs_debug("release_blocked: unblocked %d requests", count);
    }

} /* chimera_vfs_open_cache_release_blocked */

/* --- Release --- */

static inline void
chimera_vfs_open_cache_release(
    struct chimera_vfs_thread      *thread,
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    enum chimera_vfs_error          error_code)
{
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_request  *requests;
    int                          had_blocked;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    chimera_vfs_abort_if(handle->cache_id != shard->cache_id, "handle released by wrong cache");

    pthread_mutex_lock(&shard->lock);

    chimera_vfs_debug("open_cache_release: handle=%p fh_hash=%lx opencnt=%d flags_before=%x error=%d",
                      handle, handle->fh_hash, handle->opencnt, handle->flags, error_code);

    handle->flags &= ~CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE;

    requests                 = handle->blocked_requests;
    handle->blocked_requests = NULL;
    had_blocked              = (requests != NULL);

    if (error_code) {
        /* We failed to open the file, so we need to remove it from the cache
         * and we'll give everyone else NULL callbacks so they'll never
         * release themselves.  Therefore we can free the handle now
         */
        chimera_vfs_debug("open_cache_release: ERROR path, removing from cache");
        handle->opencnt = 0;
        if (!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED)) {
            chimera_vfs_open_cache_shard_remove(shard, handle);
        }
        chimera_vfs_open_cache_free(shard, handle);

    } else {

        handle->opencnt--;

        if (handle->opencnt == 0) {
            if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED) {
                /* Detached handle: close immediately, do not add to pending_close */
                chimera_vfs_debug("open_cache_release: DETACHED, closing immediately");
                pthread_mutex_unlock(&shard->lock);
                chimera_vfs_open_cache_release_blocked(thread, requests, error_code);
                chimera_vfs_close(thread, handle, NULL, NULL);
                pthread_mutex_lock(&shard->lock);
                chimera_vfs_open_cache_free(shard, handle);
                pthread_mutex_unlock(&shard->lock);
                return;
            }
            clock_gettime(CLOCK_MONOTONIC, &handle->timestamp);
            DL_APPEND(shard->pending_close, handle);
            chimera_vfs_debug("open_cache_release: opencnt=0, added to pending_close, flags_after=%x",
                              handle->flags);
        } else {
            chimera_vfs_debug("open_cache_release: opencnt now %d, flags_after=%x",
                              handle->opencnt, handle->flags);
        }
    }

    pthread_mutex_unlock(&shard->lock);

    if (had_blocked) {
        chimera_vfs_debug("open_cache_release: releasing blocked requests");
    }
    chimera_vfs_open_cache_release_blocked(thread, requests, error_code);

} /* vfs_open_cache_release */

/* Increment opencnt on an already-acquired handle (for dup operations) */
static inline void
chimera_vfs_open_cache_dup(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle)
{
    struct vfs_open_cache_shard *shard;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    chimera_vfs_abort_if(handle->cache_id != shard->cache_id, "handle duped by wrong cache");

    pthread_mutex_lock(&shard->lock);

    chimera_vfs_abort_if(handle->opencnt == 0, "dup on handle with zero opencnt");

    handle->opencnt++;

    pthread_mutex_unlock(&shard->lock);

} /* chimera_vfs_open_cache_dup */

/* --- Close callback for eviction --- */

static void
chimera_vfs_open_cache_close_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_open_handle *handle  = private_data;
    struct chimera_vfs_request     *request = handle->request;

    chimera_vfs_abort_if(error_code, "open cache failed to close existing handle");

    request->status = error_code;

    handle->callback(request, handle);
} /* chimera_vfs_open_cache_close_callback */

/* --- Populate (after pending open completes) --- */

static inline void
chimera_vfs_open_cache_populate(
    struct chimera_vfs_thread      *thread,
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        vfs_private_data)
{
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_request  *requests = NULL;
    int                          had_blocked;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    chimera_vfs_debug("open_cache_populate: handle=%p fh_hash=%lx flags_before=%x vfs_private=%lx",
                      handle, handle->fh_hash, handle->flags, vfs_private_data);

    handle->vfs_private = vfs_private_data;
    handle->flags      &= ~CHIMERA_VFS_OPEN_HANDLE_PENDING;

    had_blocked = (handle->blocked_requests != NULL);

    if (!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE)) {
        requests                 = handle->blocked_requests;
        handle->blocked_requests = NULL;
        chimera_vfs_debug("open_cache_populate: EXCLUSIVE not set, releasing blocked requests (had_blocked=%d)",
                          had_blocked);
    } else {
        chimera_vfs_debug("open_cache_populate: EXCLUSIVE still set, NOT releasing blocked requests (had_blocked=%d)",
                          had_blocked);
    }

    chimera_vfs_debug("open_cache_populate: handle=%p flags_after=%x", handle, handle->flags);

    pthread_mutex_unlock(&shard->lock);

    if (requests) {
        chimera_vfs_open_cache_release_blocked(thread, requests, 0);
    }

} /* chimera_vfs_open_cache_populate */

/* --- Acquire (cache-first, used by open-by-fh) --- */

static inline void
chimera_vfs_open_cache_acquire(
    struct chimera_vfs_thread  *thread,
    struct vfs_open_cache      *cache,
    struct chimera_vfs_module  *module,
    struct chimera_vfs_request *request,
    const void                 *fh,
    uint32_t                    fhlen,
    uint64_t                    fh_hash,
    uint64_t                    vfs_private_data,
    unsigned int                open_flags,
    int                         exclusive,
    void (                     *callback )(struct chimera_vfs_request     *request,
                                           struct chimera_vfs_open_handle *handle))
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *existing;
    uint8_t                         access_mode;
    int                             done = 0;

    access_mode = chimera_vfs_open_access_mode(open_flags);

    shard = &cache->shards[fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    handle = chimera_vfs_open_cache_shard_find(shard, fh, fhlen, access_mode);

    if (handle) {

        chimera_vfs_abort_if((handle->flags & CHIMERA_VFS_OPEN_HANDLE_PENDING) && vfs_private_data != UINT64_MAX,
                             "open cache pending handle with vfs private data");

        chimera_vfs_debug("open_cache_acquire: FOUND handle=%p fh_hash=%lx opencnt=%d flags=%x (E=%d P=%d D=%d)",
                          handle, fh_hash, handle->opencnt, handle->flags,
                          !!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE),
                          !!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_PENDING),
                          !!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED));

        if (handle->opencnt == 0) {
            DL_DELETE(shard->pending_close, handle);
            chimera_vfs_debug("open_cache_acquire: removed from pending_close");
        }

        handle->opencnt++;

        if (handle->flags & (CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE | CHIMERA_VFS_OPEN_HANDLE_PENDING)) {
            request->unblock_callback = callback;
            request->pending_handle   = handle;
            LL_PREPEND(handle->blocked_requests, request);
            done = 0;
            chimera_vfs_debug("open_cache_acquire: BLOCKING request=%p on handle=%p (flags=%x)",
                              request, handle, handle->flags);
        } else {
            done = 1;
            chimera_vfs_debug("open_cache_acquire: cache HIT, returning immediately");
        }
    } else {

        prometheus_counter_increment(shard->insert);

        handle                   = chimera_vfs_open_cache_alloc(shard);
        handle->vfs_module       = module;
        handle->fh_hash          = fh_hash;
        handle->fh_len           = fhlen;
        handle->opencnt          = 1;
        handle->access_mode      = access_mode;
        handle->flags            = exclusive ? CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE : 0;
        handle->callback         = callback;
        handle->request          = request;
        handle->vfs_private      = vfs_private_data;
        handle->blocked_requests = NULL;

        if (handle->vfs_private == UINT64_MAX) {
            handle->flags |= CHIMERA_VFS_OPEN_HANDLE_PENDING;
        }

        memcpy(handle->fh, fh, fhlen);

        chimera_vfs_open_cache_shard_insert(shard, handle);

        chimera_vfs_debug("open_cache_acquire: NEW handle=%p fh_hash=%lx flags=%x exclusive=%d",
                          handle, fh_hash, handle->flags, exclusive);

        if (shard->open_handles < shard->max_open_files) {
            shard->open_handles++;
            done = 1;
            chimera_vfs_debug("open_cache_acquire: shard has room, will dispatch");
        } else {
            chimera_vfs_abort_if(!shard->pending_close, "open cache exhausted with referenced handles");

            existing = shard->pending_close;

            DL_DELETE(shard->pending_close, existing);
            chimera_vfs_open_cache_shard_remove(shard, existing);

            chimera_vfs_debug("open_cache_acquire: shard FULL, evicting handle=%p to make room for handle=%p",
                              existing, handle);

            chimera_vfs_close(thread, existing, chimera_vfs_open_cache_close_callback, handle);

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

/* --- Close callback for insert eviction --- */

static void
chimera_vfs_open_cache_insert_close_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    /* Old handle evicted during insert; nothing to do on completion */
    (void) error_code;
    (void) private_data;
} /* chimera_vfs_open_cache_insert_close_callback */

/* --- Insert (always-insert, used by open_at / create_unlinked) --- */

static inline void
chimera_vfs_open_cache_insert(
    struct chimera_vfs_thread  *thread,
    struct vfs_open_cache      *cache,
    struct chimera_vfs_module  *module,
    struct chimera_vfs_request *request,
    const void                 *fh,
    uint32_t                    fhlen,
    uint64_t                    fh_hash,
    uint64_t                    vfs_private_data,
    unsigned int                open_flags,
    void (                     *callback )(struct chimera_vfs_request     *request,
                                           struct chimera_vfs_open_handle *handle))
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *existing;
    uint8_t                         access_mode;

    access_mode = chimera_vfs_open_access_mode(open_flags);

    shard = &cache->shards[fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    prometheus_counter_increment(shard->insert);

    /* Allocate and populate the new handle */
    handle                   = chimera_vfs_open_cache_alloc(shard);
    handle->vfs_module       = module;
    handle->fh_hash          = fh_hash;
    handle->fh_len           = fhlen;
    handle->opencnt          = 1;
    handle->access_mode      = access_mode;
    handle->flags            = 0;
    handle->callback         = callback;
    handle->request          = request;
    handle->vfs_private      = vfs_private_data;
    handle->blocked_requests = NULL;

    memcpy(handle->fh, fh, fhlen);

    /* Check for existing entry with same (fh, access_mode) */
    existing = chimera_vfs_open_cache_shard_find(shard, fh, fhlen, access_mode);

    if (existing) {
        if (existing->opencnt == 0) {
            /* On pending_close: remove from both lists, close old, free */
            DL_DELETE(shard->pending_close, existing);
            chimera_vfs_open_cache_shard_remove(shard, existing);
            chimera_vfs_close(thread, existing, chimera_vfs_open_cache_insert_close_callback, NULL);
            chimera_vfs_open_cache_free(shard, existing);
        } else {
            /* In use: detach. Current holders keep working; on release it closes immediately */
            chimera_vfs_open_cache_shard_remove(shard, existing);
            existing->flags |= CHIMERA_VFS_OPEN_HANDLE_DETACHED;
        }
    } else {
        /* No existing entry — we're adding a net new handle to the shard */
        if (shard->open_handles < shard->max_open_files) {
            shard->open_handles++;
        } else {
            /* Need to evict a pending_close entry to make room */
            if (shard->pending_close) {
                struct chimera_vfs_open_handle *victim = shard->pending_close;
                DL_DELETE(shard->pending_close, victim);
                chimera_vfs_open_cache_shard_remove(shard, victim);
                chimera_vfs_close(thread, victim, chimera_vfs_open_cache_insert_close_callback, NULL);
                chimera_vfs_open_cache_free(shard, victim);
            } else {
                /* All handles are active; just exceed the limit */
                shard->open_handles++;
            }
        }
    }

    /* Insert new handle into shard */
    chimera_vfs_open_cache_shard_insert(shard, handle);

    pthread_mutex_unlock(&shard->lock);

    callback(request, handle);
} /* chimera_vfs_open_cache_insert */

/* --- Deferred close (close thread) --- */

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

    for (unsigned int i = 0; i < cache->num_shards; i++) {
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
            chimera_vfs_open_cache_shard_remove(shard, handle);
            LL_PREPEND(closed, handle);
            shard->open_handles--;
        }
        pthread_mutex_unlock(&shard->lock);
    }
    *r_count = count;
    return closed;
} /* vfs_open_cache_defer_close */

/*
 * Count handles in the cache that belong to the given mount.
 * mount_id is the first 16 bytes of the file handle.
 * Returns count of handles with opencnt > 0 (actively referenced).
 */
static inline uint64_t
chimera_vfs_open_cache_count_by_mount(
    struct vfs_open_cache *cache,
    const uint8_t         *mount_id)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        count = 0;

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        pthread_mutex_lock(&shard->lock);

        for (handle = shard->handles; handle; handle = handle->bucket_next) {
            if (memcmp(handle->fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) == 0) {
                if (handle->opencnt > 0) {
                    count++;
                }
            }
        }

        pthread_mutex_unlock(&shard->lock);
    }

    return count;
} /* chimera_vfs_open_cache_count_by_mount */

/*
 * Mark all handles belonging to the given mount for immediate close.
 * This sets their timestamp to 0 so the close thread will close them
 * on the next sweep.
 * Returns count of handles marked.
 */
static inline uint64_t
chimera_vfs_open_cache_mark_for_close_by_mount(
    struct vfs_open_cache *cache,
    const uint8_t         *mount_id)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        count = 0;

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        pthread_mutex_lock(&shard->lock);

        for (handle = shard->handles; handle; handle = handle->bucket_next) {
            if (memcmp(handle->fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) == 0) {
                /* Set timestamp to 0 so it will be closed immediately */
                handle->timestamp.tv_sec  = 0;
                handle->timestamp.tv_nsec = 0;
                count++;
            }
        }

        pthread_mutex_unlock(&shard->lock);
    }

    return count;
} /* chimera_vfs_open_cache_mark_for_close_by_mount */

/*
 * Lookup a file handle in the open cache and increment its refcount if found.
 *
 * This is used for checking if a file is open (e.g., for silly rename on remove).
 * If the file is open (opencnt > 0), increments opencnt and returns the handle.
 * If not found or not open, returns NULL.
 *
 * The caller must call chimera_vfs_open_cache_release() when done with the handle.
 */
static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_lookup_ref(
    struct vfs_open_cache *cache,
    const uint8_t         *fh,
    uint32_t               fh_len,
    uint64_t               fh_hash)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *found = NULL;

    shard = &cache->shards[fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    /* Scan for any handle matching fh with opencnt > 0, regardless of access_mode */
    for (handle = shard->handles; handle; handle = handle->bucket_next) {
        if (handle->fh_len == fh_len &&
            memcmp(handle->fh, fh, fh_len) == 0 &&
            handle->opencnt > 0 &&
            !(handle->flags & CHIMERA_VFS_OPEN_HANDLE_PENDING)) {
            handle->opencnt++;
            found = handle;
            break;
        }
    }

    pthread_mutex_unlock(&shard->lock);

    return found;
} /* chimera_vfs_open_cache_lookup_ref */

/*
 * Check if a file handle exists in the open cache.
 *
 * Returns true if the handle exists (regardless of opencnt or pending state).
 */
static inline int
chimera_vfs_open_cache_exists(
    struct vfs_open_cache *cache,
    const uint8_t         *fh,
    uint32_t               fh_len,
    uint64_t               fh_hash)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle;
    int                             found = 0;

    shard = &cache->shards[fh_hash & cache->shard_mask];

    pthread_mutex_lock(&shard->lock);

    /* Scan for any handle matching fh, regardless of access_mode */
    for (handle = shard->handles; handle; handle = handle->bucket_next) {
        if (handle->fh_len == fh_len &&
            memcmp(handle->fh, fh, fh_len) == 0) {
            found = 1;
            break;
        }
    }

    pthread_mutex_unlock(&shard->lock);

    return found;
} /* chimera_vfs_open_cache_exists */
