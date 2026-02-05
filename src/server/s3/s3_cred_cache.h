// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <urcu.h>
#include <urcu/urcu-memb.h>
#include <xxhash.h>

#define CHIMERA_S3_ACCESS_KEY_MAX 128
#define CHIMERA_S3_SECRET_KEY_MAX 256

struct chimera_s3_cred {
    int                     access_key_len;
    struct timespec         expiration;
    int                     pinned;
    struct rcu_head         rcu;
    struct chimera_s3_cred *next;
    char                    access_key[CHIMERA_S3_ACCESS_KEY_MAX];
    char                    secret_key[CHIMERA_S3_SECRET_KEY_MAX];
};

struct chimera_s3_cred_cache_bucket {
    struct chimera_s3_cred *head;
    pthread_mutex_t         lock;
};

struct chimera_s3_cred_cache {
    int                                  num_buckets;
    int                                  ttl;
    int                                  num_credentials;
    struct chimera_s3_cred_cache_bucket *buckets;
    pthread_t                            expiry_thread;
    pthread_mutex_t                      expiry_lock;
    pthread_cond_t                       expiry_cond;
    int                                  shutdown;
};

static inline int
chimera_s3_cred_cache_has_credentials(struct chimera_s3_cred_cache *cache)
{
    return cache->num_credentials > 0;
} // chimera_s3_cred_cache_has_credentials

static inline unsigned int
chimera_s3_cred_cache_hash(
    const char *access_key,
    int         access_key_len,
    int         num_buckets)
{
    return XXH3_64bits(access_key, access_key_len) % num_buckets;
} // chimera_s3_cred_cache_hash

static void
chimera_s3_cred_cache_rcu_free(struct rcu_head *head)
{
    struct chimera_s3_cred *cred = caa_container_of(
        head, struct chimera_s3_cred, rcu);

    free(cred);
} // chimera_s3_cred_cache_rcu_free

static inline void
chimera_s3_cred_cache_remove_locked(
    struct chimera_s3_cred_cache *cache,
    struct chimera_s3_cred       *cred,
    unsigned int                  bucket_idx)
{
    struct chimera_s3_cred **pp;

    pp = &cache->buckets[bucket_idx].head;
    while (*pp) {
        if (*pp == cred) {
            rcu_assign_pointer(*pp, cred->next);
            __atomic_sub_fetch(&cache->num_credentials, 1, __ATOMIC_RELAXED);
            break;
        }
        pp = &(*pp)->next;
    }

    call_rcu(&cred->rcu, chimera_s3_cred_cache_rcu_free);
} // chimera_s3_cred_cache_remove_locked

static void *
chimera_s3_cred_cache_expiry_thread(void *arg)
{
    struct chimera_s3_cred_cache *cache = arg;
    struct chimera_s3_cred       *cred, *next;
    struct timespec               ts;
    int                           i;

    urcu_memb_register_thread();

    pthread_mutex_lock(&cache->expiry_lock);

    while (!cache->shutdown) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 60;

        pthread_cond_timedwait(&cache->expiry_cond, &cache->expiry_lock, &ts);

        if (cache->shutdown) {
            break;
        }

        clock_gettime(CLOCK_REALTIME, &ts);

        for (i = 0; i < cache->num_buckets; i++) {
            pthread_mutex_lock(&cache->buckets[i].lock);

            cred = cache->buckets[i].head;
            while (cred) {
                next = cred->next;
                if (!cred->pinned &&
                    (ts.tv_sec > cred->expiration.tv_sec ||
                     (ts.tv_sec == cred->expiration.tv_sec &&
                      ts.tv_nsec >= cred->expiration.tv_nsec))) {

                    chimera_s3_cred_cache_remove_locked(cache, cred, i);
                }
                cred = next;
            }

            pthread_mutex_unlock(&cache->buckets[i].lock);
        }
    }

    pthread_mutex_unlock(&cache->expiry_lock);

    urcu_memb_unregister_thread();

    return NULL;
} // chimera_s3_cred_cache_expiry_thread

static inline struct chimera_s3_cred_cache *
chimera_s3_cred_cache_create(
    int num_buckets,
    int ttl)
{
    struct chimera_s3_cred_cache *cache;
    int                           i;

    cache              = calloc(1, sizeof(*cache));
    cache->num_buckets = num_buckets;
    cache->ttl         = ttl;
    cache->shutdown    = 0;

    cache->buckets = calloc(num_buckets,
                            sizeof(struct chimera_s3_cred_cache_bucket));

    for (i = 0; i < num_buckets; i++) {
        pthread_mutex_init(&cache->buckets[i].lock, NULL);
    }

    pthread_mutex_init(&cache->expiry_lock, NULL);
    pthread_cond_init(&cache->expiry_cond, NULL);

    pthread_create(&cache->expiry_thread, NULL,
                   chimera_s3_cred_cache_expiry_thread, cache);

    return cache;
} // chimera_s3_cred_cache_create

static inline void
chimera_s3_cred_cache_destroy(struct chimera_s3_cred_cache *cache)
{
    struct chimera_s3_cred *cred, *next;
    int                     i;

    pthread_mutex_lock(&cache->expiry_lock);
    cache->shutdown = 1;
    pthread_cond_signal(&cache->expiry_cond);
    pthread_mutex_unlock(&cache->expiry_lock);

    pthread_join(cache->expiry_thread, NULL);

    urcu_memb_barrier();

    for (i = 0; i < cache->num_buckets; i++) {
        cred = cache->buckets[i].head;
        while (cred) {
            next = cred->next;
            free(cred);
            cred = next;
        }
        pthread_mutex_destroy(&cache->buckets[i].lock);
    }

    free(cache->buckets);

    pthread_mutex_destroy(&cache->expiry_lock);
    pthread_cond_destroy(&cache->expiry_cond);

    free(cache);
} // chimera_s3_cred_cache_destroy

static inline int
chimera_s3_cred_cache_add(
    struct chimera_s3_cred_cache *cache,
    const char                   *access_key,
    const char                   *secret_key,
    int                           pinned)
{
    struct chimera_s3_cred *cred, *existing;
    unsigned int            bucket_idx;
    struct timespec         now;
    int                     access_key_len;

    access_key_len = strlen(access_key);
    bucket_idx     = chimera_s3_cred_cache_hash(access_key, access_key_len,
                                                cache->num_buckets);

    cred = calloc(1, sizeof(*cred));
    strncpy(cred->access_key, access_key, sizeof(cred->access_key) - 1);
    strncpy(cred->secret_key, secret_key, sizeof(cred->secret_key) - 1);
    cred->access_key_len = access_key_len;
    cred->pinned         = pinned;

    if (!pinned) {
        clock_gettime(CLOCK_REALTIME, &now);
        cred->expiration.tv_sec  = now.tv_sec + cache->ttl;
        cred->expiration.tv_nsec = now.tv_nsec;
    }

    pthread_mutex_lock(&cache->buckets[bucket_idx].lock);

    /* Check for existing entry with same access_key and remove it */
    existing = cache->buckets[bucket_idx].head;
    while (existing) {
        if (strcmp(existing->access_key, access_key) == 0) {
            chimera_s3_cred_cache_remove_locked(cache, existing, bucket_idx);
            break;
        }
        existing = existing->next;
    }

    /* Insert into chain */
    cred->next = cache->buckets[bucket_idx].head;
    rcu_assign_pointer(cache->buckets[bucket_idx].head, cred);
    __atomic_add_fetch(&cache->num_credentials, 1, __ATOMIC_RELAXED);

    pthread_mutex_unlock(&cache->buckets[bucket_idx].lock);

    return 0;
} // chimera_s3_cred_cache_add

static inline int
chimera_s3_cred_cache_remove(
    struct chimera_s3_cred_cache *cache,
    const char                   *access_key)
{
    unsigned int            bucket_idx;
    struct chimera_s3_cred *cred;
    int                     access_key_len;

    access_key_len = strlen(access_key);
    bucket_idx     = chimera_s3_cred_cache_hash(access_key, access_key_len,
                                                cache->num_buckets);

    pthread_mutex_lock(&cache->buckets[bucket_idx].lock);

    cred = cache->buckets[bucket_idx].head;
    while (cred) {
        if (strcmp(cred->access_key, access_key) == 0) {
            chimera_s3_cred_cache_remove_locked(cache, cred, bucket_idx);
            pthread_mutex_unlock(&cache->buckets[bucket_idx].lock);
            return 0;
        }
        cred = cred->next;
    }

    pthread_mutex_unlock(&cache->buckets[bucket_idx].lock);
    return -1;
} // chimera_s3_cred_cache_remove

static inline const struct chimera_s3_cred *
chimera_s3_cred_cache_lookup(
    struct chimera_s3_cred_cache *cache,
    const char                   *access_key,
    int                           access_key_len)
{
    unsigned int            bucket_idx;
    struct chimera_s3_cred *cred;

    bucket_idx = chimera_s3_cred_cache_hash(access_key, access_key_len,
                                            cache->num_buckets);

    cred = rcu_dereference(cache->buckets[bucket_idx].head);
    while (cred) {
        if (cred->access_key_len == access_key_len &&
            memcmp(cred->access_key, access_key, access_key_len) == 0) {
            return cred;
        }
        cred = rcu_dereference(cred->next);
    }

    return NULL;
} // chimera_s3_cred_cache_lookup
