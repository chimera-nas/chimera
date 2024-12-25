#pragma once

#include <pthread.h>

#include "uthash/uthash.h"
#include "common/format.h"
#include "common/misc.h"
#include "vfs.h"
#include "vfs_procs.h"

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
    pthread_mutex_unlock(&cache->lock);

} /* vfs_open_cache_release */
static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_insert(
    struct chimera_vfs_thread *thread,
    struct vfs_open_cache     *cache,
    struct chimera_vfs_module *module,
    const void                *fh,
    uint32_t                   fhlen,
    uint64_t                   vfs_private)
{
    struct chimera_vfs_open_handle *handle;

    pthread_mutex_lock(&cache->lock);

    HASH_FIND(hh, cache->open_files, fh, fhlen, handle);

    if (handle) {

        chimera_vfs_close(thread, module, fh, fhlen, vfs_private, NULL, NULL);

        handle->opencnt++;
        handle->timestamp.tv_sec  = 0;
        handle->timestamp.tv_nsec = 0;

    } else {

        handle              = chimera_vfs_open_cache_alloc(cache);
        handle->vfs_module  = module;
        handle->fh_len      = fhlen;
        handle->opencnt     = 1;
        handle->vfs_private = vfs_private;

        memcpy(handle->fh, fh, fhlen);

        HASH_ADD(hh, cache->open_files, fh, handle->fh_len, handle);
    }

    pthread_mutex_unlock(&cache->lock);

    return handle;
} /* chimera_vfs_open_cache_insert */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_lookup(
    struct vfs_open_cache     *cache,
    struct chimera_vfs_module *module,
    const void                *fh,
    uint32_t                   fhlen)
{
    struct chimera_vfs_open_handle *handle;

    pthread_mutex_lock(&cache->lock);

    HASH_FIND(hh, cache->open_files, fh, fhlen, handle);

    if (handle) {
        handle->opencnt++;
        handle->timestamp.tv_sec  = 0;
        handle->timestamp.tv_nsec = 0;
    }

    pthread_mutex_unlock(&cache->lock);

    return handle;
} /* chimera_vfs_open_cache_lookup */

static inline uint64_t
chimera_vfs_open_cache_size(struct vfs_open_cache *cache)
{
    uint64_t size;

    pthread_mutex_lock(&cache->lock);
    size = HASH_COUNT(cache->open_files);
    pthread_mutex_unlock(&cache->lock);

    return size;
} /* chimera_vfs_open_cache_size */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_defer_close(
    struct vfs_open_cache *cache,
    struct timespec       *timestamp,
    uint64_t               min_age,
    uint64_t              *r_count)
{
    struct chimera_vfs_open_handle *handle, *tmp, *closed = NULL;
    uint64_t                        elapsed, count = 0;

    pthread_mutex_lock(&cache->lock);
    HASH_ITER(hh, cache->open_files, handle, tmp)
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
            HASH_DEL(cache->open_files, handle);
        }
    }
    pthread_mutex_unlock(&cache->lock);

    *r_count = count;
    return closed;
} /* vfs_open_cache_defer_close */