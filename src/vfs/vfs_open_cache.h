#pragma once

#include <pthread.h>

#include "uthash/uthash.h"
#include "common/format.h"
#include "common/misc.h"
#include "vfs.h"

struct vfs_open_cache {
    pthread_mutex_t                 lock;
    struct chimera_vfs_open_handle *open_files;
    struct chimera_vfs_open_handle *free_handles;
};

static inline struct vfs_open_cache *
chimera_vfs_open_cache_init(void)
{
    struct vfs_open_cache *cache;

    cache             = calloc(1, sizeof(*cache));
    cache->open_files = NULL;
    pthread_mutex_init(&cache->lock, NULL);

    return cache;
} /* vfs_open_cache_init */

static inline void
chimera_vfs_open_cache_destroy(struct vfs_open_cache *cache)
{
    pthread_mutex_destroy(&cache->lock);
    free(cache);
} /* vfs_open_cache_destroy */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_alloc(struct vfs_open_cache *cache)
{
    struct chimera_vfs_open_handle *handle;

    handle = cache->free_handles;
    if (handle) {
        LL_DELETE(cache->free_handles, handle);
    } else {
        handle = calloc(1, sizeof(*handle));
    }

    handle->timestamp.tv_sec  = 0;
    handle->timestamp.tv_nsec = 0;

    return handle;
} /* chimera_vfs_open_cache_alloc */

static inline void
chimera_vfs_open_cache_free(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle)
{
    LL_APPEND(cache->free_handles, handle);
} /* chimera_vfs_open_cache_free */



static inline void
chimera_vfs_open_cache_release(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle)
{
    pthread_mutex_lock(&cache->lock);

    handle->opencnt--;

    if (handle->opencnt == 0 && handle->pending) {
        HASH_DEL(cache->open_files, handle);
        chimera_vfs_open_cache_free(cache, handle);
    }

    pthread_mutex_unlock(&cache->lock);

} /* vfs_open_cache_release */

static inline int
chimera_vfs_open_cache_acquire(
    struct chimera_vfs_open_handle **r_handle,
    struct vfs_open_cache           *cache,
    struct chimera_vfs_module       *module,
    const void                      *fh,
    uint32_t                         fhlen)
{
    struct chimera_vfs_open_handle *handle;
    int                             is_new;

    pthread_mutex_lock(&cache->lock);

    HASH_FIND(hh, cache->open_files, fh, fhlen, handle);

    if (handle) {
        handle->opencnt++;
        handle->timestamp.tv_sec  = 0;
        handle->timestamp.tv_nsec = 0;

        is_new = 0;
    } else {
        handle             = chimera_vfs_open_cache_alloc(cache);
        handle->vfs_module = module;
        handle->fh_len     = fhlen;
        handle->opencnt    = 1;
        handle->pending    = 1;
        is_new             = 1;

        memcpy(handle->fh, fh, fhlen);

        HASH_ADD(hh, cache->open_files, fh, handle->fh_len, handle);
    }

    pthread_mutex_unlock(&cache->lock);

    *r_handle = handle;

    return is_new;
} /* chimera_vfs_open_cache_acquire */

static inline uint64_t
chimera_vfs_open_cache_size(struct vfs_open_cache *cache)
{
    uint64_t size;

    pthread_mutex_lock(&cache->lock);
    size = HASH_COUNT(cache->open_files);
    pthread_mutex_unlock(&cache->lock);

    return size;
} /* chimera_vfs_open_cache_size */

static inline void
chimera_vfs_open_cache_ready(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        vfs_private)
{
    pthread_mutex_lock(&cache->lock);
    handle->pending     = 0;
    handle->vfs_private = vfs_private;
    pthread_mutex_unlock(&cache->lock);

} /* chimera_vfs_open_cache_ready */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_defer_close(
    struct vfs_open_cache *cache,
    struct timespec       *timestamp,
    uint64_t               min_age)
{
    struct chimera_vfs_open_handle *handle, *tmp, *closed = NULL;
    uint64_t                        elapsed;

    pthread_mutex_lock(&cache->lock);
    HASH_ITER(hh, cache->open_files, handle, tmp)
    {
        if (handle->opencnt) {
            continue;
        }

        if (handle->timestamp.tv_sec == 0) {
            handle->timestamp = *timestamp;
        }

        elapsed = chimera_get_elapsed_ns(timestamp, &handle->timestamp);

        if (elapsed > min_age) {
            LL_PREPEND(closed, handle);
            HASH_DEL(cache->open_files, handle);
        }
    }
    pthread_mutex_unlock(&cache->lock);

    return closed;
} /* vfs_open_cache_defer_close */