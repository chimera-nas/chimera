// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <uthash.h>

struct s3_bucket {
    char          *name;
    char          *path;
    int            pathlen;
    UT_hash_handle hh;
};

struct s3_bucket_map {
    struct s3_bucket *buckets;
    pthread_rwlock_t  rwlock;
};

struct s3_bucket_map *
s3_bucket_map_create(void)
{
    struct s3_bucket_map *map = calloc(1, sizeof(struct s3_bucket_map));

    map->buckets = NULL;
    pthread_rwlock_init(&map->rwlock, NULL);
    return map;
} /* s3_bucket_map_create */

void
s3_bucket_map_destroy(struct s3_bucket_map *map)
{
    struct s3_bucket *bucket, *tmp;

#ifndef __clang_analyzer__
    /* uthash blows clangs mind */
    HASH_ITER(hh, map->buckets, bucket, tmp)
    {
        HASH_DEL(map->buckets, bucket);
        free(bucket->name);
        free(bucket->path);
        free(bucket);
    }
#endif /* ifndef __clang_analyzer__ */

    pthread_rwlock_destroy(&map->rwlock);

    free(map);
} /* s3_bucket_map_destroy */

struct s3_bucket *
s3_bucket_map_get(
    struct s3_bucket_map *map,
    const char           *name,
    int                   namelen)
{
    struct s3_bucket *bucket;

    pthread_rwlock_rdlock(&map->rwlock);

    HASH_FIND(hh, map->buckets, name, namelen, bucket);

    return bucket;
} /* s3_bucket_map_get */

void
s3_bucket_map_release(struct s3_bucket_map *map)
{
    pthread_rwlock_unlock(&map->rwlock);
} /* s3_bucket_map_release */

void
s3_bucket_map_put(
    struct s3_bucket_map *map,
    const char           *name,
    int                   namelen,
    const char           *path)
{
    struct s3_bucket *bucket;

    pthread_rwlock_wrlock(&map->rwlock);
    HASH_FIND(hh, map->buckets, name, namelen, bucket);
    if (bucket == NULL) {
        bucket       = calloc(1, sizeof(struct s3_bucket));
        bucket->name = malloc(namelen + 1);
        memcpy(bucket->name, name, namelen);
        bucket->name[namelen] = '\0';
        bucket->path          = strdup(path);
        bucket->pathlen       = strlen(path);
        HASH_ADD(hh, map->buckets, name[0], namelen, bucket);
    } else {
        free(bucket->path);
        bucket->path    = strdup(path);
        bucket->pathlen = strlen(path);
    }
    pthread_rwlock_unlock(&map->rwlock);
} /* s3_bucket_map_put */


int
s3_bucket_map_remove(
    struct s3_bucket_map *map,
    const char           *name,
    int                   namelen)
{
    struct s3_bucket *bucket;
    int               found = 0;

    pthread_rwlock_wrlock(&map->rwlock);
    HASH_FIND(hh, map->buckets, name, namelen, bucket);
    if (bucket != NULL) {
        HASH_DEL(map->buckets, bucket);
        free(bucket->name);
        free(bucket->path);
        free(bucket);
        found = 1;
    }
    pthread_rwlock_unlock(&map->rwlock);

    return found ? 0 : -1;
} /* s3_bucket_map_remove */

typedef int (*s3_bucket_iterate_cb)(
    const struct s3_bucket *bucket,
    void                   *data);

void
s3_bucket_map_iterate(
    struct s3_bucket_map *map,
    s3_bucket_iterate_cb  callback,
    void                 *data)
{
    struct s3_bucket *bucket, *tmp;

    pthread_rwlock_rdlock(&map->rwlock);
#ifndef __clang_analyzer__
    /* uthash blows clangs mind */
    HASH_ITER(hh, map->buckets, bucket, tmp)
    {
        if (callback(bucket, data) != 0) {
            break;
        }
    }
#endif /* ifndef __clang_analyzer__ */
    pthread_rwlock_unlock(&map->rwlock);
} /* s3_bucket_map_iterate */


