#pragma once

#include <pthread.h>

#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "uthash/uthash.h"
#include "common/format.h"

struct nfs3_open_file {
    uint8_t                        fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                       fhlen;
    struct chimera_vfs_open_handle handle;
    struct UT_hash_handle          hh;
};

struct nfs3_open_cache {
    pthread_mutex_t        lock;
    struct nfs3_open_file *open_files;
};

static inline void
nfs3_open_cache_init(struct nfs3_open_cache *cache)
{
    cache->open_files = NULL;
    pthread_mutex_init(&cache->lock, NULL);
} /* nfs3_open_cache_init */

static inline void
nfs3_open_cache_destroy(struct nfs3_open_cache *cache)
{
    chimera_nfs_abort_if(cache->open_files,
                         "Open cache is not empty at destruction");
    pthread_mutex_destroy(&cache->lock);
} /* nfs3_open_cache_destroy */

static inline void
nfs3_open_cache_insert(
    struct nfs3_open_cache         *cache,
    struct chimera_vfs_open_handle *handle)
{
    struct nfs3_open_file *file;

    file = calloc(1, sizeof(*file));

    file->handle = *handle;

    pthread_mutex_lock(&cache->lock);
    HASH_ADD_KEYPTR(hh,
                    cache->open_files,
                    file->handle.fh,
                    file->handle.fh_len,
                    file);
    pthread_mutex_unlock(&cache->lock);

} /* nfs3_open_cache_insert */

static inline void
nfs3_open_cache_remove(
    struct nfs3_open_cache *cache,
    struct nfs3_open_file  *file)
{
    pthread_mutex_lock(&cache->lock);
    HASH_DEL(cache->open_files, file);
    pthread_mutex_unlock(&cache->lock);

    free(file);
} /* nfs3_open_cache_remove */

static inline struct chimera_vfs_open_handle *
nfs3_open_cache_lookup(
    struct nfs3_open_cache *cache,
    uint8_t                *fh,
    uint32_t                fhlen)
{
    struct nfs3_open_file *file;

    pthread_mutex_lock(&cache->lock);

    HASH_FIND(hh, cache->open_files, fh, fhlen, file);

    pthread_mutex_unlock(&cache->lock);

    if (!file) {
        return NULL;
    }

    return &file->handle;
} /* nfs3_open_cache_lookup */

static inline void
nfs3_open_cache_iterate(
    struct nfs3_open_cache *cache,
    void (                 *callback )(struct nfs3_open_cache *cache,
                                       struct nfs3_open_file  *file,
                                       void                   *private_data),
    void                   *private_data)
{
    struct nfs3_open_file *file;
    struct nfs3_open_file *tmp;

    pthread_mutex_lock(&cache->lock);
    HASH_ITER(hh, cache->open_files, file, tmp)
    {
        pthread_mutex_unlock(&cache->lock);
        callback(cache, file, private_data);
        pthread_mutex_lock(&cache->lock);
    }
    pthread_mutex_unlock(&cache->lock);
} /* nfs3_open_cache_iterate */
