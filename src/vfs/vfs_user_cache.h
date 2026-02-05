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

#include "vfs_cred.h"
#include "vfs_internal.h"

// Maximum length for a Windows SID string (S-1-5-21-xxx-xxx-xxx-rid format)
#define CHIMERA_VFS_SID_MAX_LEN 80

struct chimera_vfs_user {
    uint32_t                 uid;
    uint32_t                 gid;
    uint32_t                 ngids;
    int                      username_len;
    struct timespec          expiration;
    int                      pinned;
    struct rcu_head          rcu;
    struct chimera_vfs_user *next_by_name;
    struct chimera_vfs_user *next_by_uid;
    struct chimera_vfs_user *next_builtin;
    uint32_t                 gids[CHIMERA_VFS_CRED_MAX_GIDS];
    char                     username[256];
    char                     password[256];
    char                     smbpasswd[256];
    char                     sid[CHIMERA_VFS_SID_MAX_LEN];
};

struct chimera_vfs_user_cache_bucket {
    struct chimera_vfs_user *head;
    pthread_mutex_t          lock;
};

struct chimera_vfs_user_cache {
    int                                   num_buckets;
    int                                   ttl;
    struct chimera_vfs_user_cache_bucket *name_buckets;
    struct chimera_vfs_user_cache_bucket *uid_buckets;
    struct chimera_vfs_user              *builtin_users;
    pthread_mutex_t                       builtin_lock;
    pthread_t                             expiry_thread;
    pthread_mutex_t                       expiry_lock;
    pthread_cond_t                        expiry_cond;
    int                                   shutdown;
};

static inline unsigned int
chimera_vfs_user_cache_hash_name(
    const char *name,
    int         name_len,
    int         num_buckets)
{
    return chimera_vfs_hash(name, name_len) % num_buckets;
} // chimera_vfs_user_cache_hash_name

static inline unsigned int
chimera_vfs_user_cache_hash_uid(
    uint32_t uid,
    int      num_buckets)
{
    return uid % num_buckets;
} // chimera_vfs_user_cache_hash_uid

static void
chimera_vfs_user_cache_rcu_free(struct rcu_head *head)
{
    struct chimera_vfs_user *user = caa_container_of(
        head, struct chimera_vfs_user, rcu);

    free(user);
} // chimera_vfs_user_cache_rcu_free

static inline void
chimera_vfs_user_cache_remove_locked(
    struct chimera_vfs_user_cache *cache,
    struct chimera_vfs_user       *user)
{
    unsigned int              name_idx, uid_idx;
    struct chimera_vfs_user **pp;

    name_idx = chimera_vfs_user_cache_hash_name(user->username,
                                                user->username_len,
                                                cache->num_buckets);
    uid_idx = chimera_vfs_user_cache_hash_uid(user->uid,
                                              cache->num_buckets);

    /* Remove from name chain */
    pp = &cache->name_buckets[name_idx].head;
    while (*pp) {
        if (*pp == user) {
            rcu_assign_pointer(*pp, user->next_by_name);
            break;
        }
        pp = &(*pp)->next_by_name;
    }

    /* Remove from uid chain */
    pp = &cache->uid_buckets[uid_idx].head;
    while (*pp) {
        if (*pp == user) {
            rcu_assign_pointer(*pp, user->next_by_uid);
            break;
        }
        pp = &(*pp)->next_by_uid;
    }

    call_rcu(&user->rcu, chimera_vfs_user_cache_rcu_free);
} // chimera_vfs_user_cache_remove_locked

static void *
chimera_vfs_user_cache_expiry_thread(void *arg)
{
    struct chimera_vfs_user_cache *cache = arg;
    struct chimera_vfs_user       *user, *next;
    struct timespec                ts;
    int                            i;
    unsigned int                   uid_idx;

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
            pthread_mutex_lock(&cache->name_buckets[i].lock);

            user = cache->name_buckets[i].head;
            while (user) {
                next = user->next_by_name;
                if (!user->pinned &&
                    (ts.tv_sec > user->expiration.tv_sec ||
                     (ts.tv_sec == user->expiration.tv_sec &&
                      ts.tv_nsec >= user->expiration.tv_nsec))) {

                    uid_idx = chimera_vfs_user_cache_hash_uid(
                        user->uid, cache->num_buckets);

                    if (uid_idx != (unsigned int) i) {
                        pthread_mutex_lock(
                            &cache->uid_buckets[uid_idx].lock);
                    }

                    chimera_vfs_user_cache_remove_locked(cache, user);

                    if (uid_idx != (unsigned int) i) {
                        pthread_mutex_unlock(
                            &cache->uid_buckets[uid_idx].lock);
                    }
                }
                user = next;
            }

            pthread_mutex_unlock(&cache->name_buckets[i].lock);
        }
    }

    pthread_mutex_unlock(&cache->expiry_lock);

    urcu_memb_unregister_thread();

    return NULL;
} // chimera_vfs_user_cache_expiry_thread

static inline struct chimera_vfs_user_cache *
chimera_vfs_user_cache_create(
    int num_buckets,
    int ttl)
{
    struct chimera_vfs_user_cache *cache;
    int                            i;

    cache              = calloc(1, sizeof(*cache));
    cache->num_buckets = num_buckets;
    cache->ttl         = ttl;
    cache->shutdown    = 0;

    cache->name_buckets = calloc(num_buckets,
                                 sizeof(struct chimera_vfs_user_cache_bucket));
    cache->uid_buckets = calloc(num_buckets,
                                sizeof(struct chimera_vfs_user_cache_bucket));

    cache->builtin_users = NULL;
    pthread_mutex_init(&cache->builtin_lock, NULL);

    for (i = 0; i < num_buckets; i++) {
        pthread_mutex_init(&cache->name_buckets[i].lock, NULL);
        pthread_mutex_init(&cache->uid_buckets[i].lock, NULL);
    }

    pthread_mutex_init(&cache->expiry_lock, NULL);
    pthread_cond_init(&cache->expiry_cond, NULL);

    pthread_create(&cache->expiry_thread, NULL,
                   chimera_vfs_user_cache_expiry_thread, cache);

    return cache;
} // chimera_vfs_user_cache_create // chimera_vfs_user_cache_create

static inline void
chimera_vfs_user_cache_destroy(struct chimera_vfs_user_cache *cache)
{
    struct chimera_vfs_user *user, *next;
    int                      i;

    pthread_mutex_lock(&cache->expiry_lock);
    cache->shutdown = 1;
    pthread_cond_signal(&cache->expiry_cond);
    pthread_mutex_unlock(&cache->expiry_lock);

    pthread_join(cache->expiry_thread, NULL);

    urcu_memb_barrier();

    for (i = 0; i < cache->num_buckets; i++) {
        user = cache->name_buckets[i].head;
        while (user) {
            next = user->next_by_name;
            free(user);
            user = next;
        }
        pthread_mutex_destroy(&cache->name_buckets[i].lock);
        pthread_mutex_destroy(&cache->uid_buckets[i].lock);
    }

    free(cache->name_buckets);
    free(cache->uid_buckets);

    pthread_mutex_destroy(&cache->builtin_lock);
    pthread_mutex_destroy(&cache->expiry_lock);
    pthread_cond_destroy(&cache->expiry_cond);

    free(cache);
} // chimera_vfs_user_cache_destroy // chimera_vfs_user_cache_destroy

static inline int
chimera_vfs_user_cache_add(
    struct chimera_vfs_user_cache *cache,
    const char                    *username,
    const char                    *password,
    const char                    *smbpasswd,
    const char                    *sid,
    uint32_t                       uid,
    uint32_t                       gid,
    uint32_t                       ngids,
    const uint32_t                *gids,
    int                            pinned)
{
    struct chimera_vfs_user *user, *existing, **pp;
    unsigned int             name_idx, uid_idx;
    struct timespec          now;
    int                      username_len;
    int                      existing_pinned = 0;

    username_len = strlen(username);
    name_idx     = chimera_vfs_user_cache_hash_name(username, username_len,
                                                    cache->num_buckets);
    uid_idx = chimera_vfs_user_cache_hash_uid(uid, cache->num_buckets);

    user = calloc(1, sizeof(*user));
    strncpy(user->username, username, sizeof(user->username) - 1);
    user->username_len = username_len;
    if (password) {
        strncpy(user->password, password, sizeof(user->password) - 1);
    }
    if (smbpasswd) {
        strncpy(user->smbpasswd, smbpasswd, sizeof(user->smbpasswd) - 1);
    }
    if (sid) {
        strncpy(user->sid, sid, sizeof(user->sid) - 1);
    }
    user->uid    = uid;
    user->gid    = gid;
    user->pinned = pinned;

    if (ngids > CHIMERA_VFS_CRED_MAX_GIDS) {
        ngids = CHIMERA_VFS_CRED_MAX_GIDS;
    }
    user->ngids = ngids;
    if (ngids > 0 && gids) {
        memcpy(user->gids, gids, ngids * sizeof(uint32_t));
    }

    if (!pinned) {
        clock_gettime(CLOCK_REALTIME, &now);
        user->expiration.tv_sec  = now.tv_sec + cache->ttl;
        user->expiration.tv_nsec = now.tv_nsec;
    }

    /* Lock name bucket first, then uid bucket (consistent order) */
    pthread_mutex_lock(&cache->name_buckets[name_idx].lock);

    if (name_idx != uid_idx) {
        pthread_mutex_lock(&cache->uid_buckets[uid_idx].lock);
    }

    /* Check for existing entry with same username and remove it */
    existing = cache->name_buckets[name_idx].head;
    while (existing) {
        if (strcmp(existing->username, username) == 0) {
            existing_pinned = existing->pinned;
            chimera_vfs_user_cache_remove_locked(cache, existing);
            break;
        }
        existing = existing->next_by_name;
    }

    /* Insert into name chain */
    user->next_by_name = cache->name_buckets[name_idx].head;
    rcu_assign_pointer(cache->name_buckets[name_idx].head, user);

    /* Insert into uid chain */
    user->next_by_uid = cache->uid_buckets[uid_idx].head;
    rcu_assign_pointer(cache->uid_buckets[uid_idx].head, user);

    if (name_idx != uid_idx) {
        pthread_mutex_unlock(&cache->uid_buckets[uid_idx].lock);
    }

    pthread_mutex_unlock(&cache->name_buckets[name_idx].lock);

    /* Maintain builtin list for pinned users */
    if (pinned || existing_pinned) {
        pthread_mutex_lock(&cache->builtin_lock);

        /* If old user was pinned, remove it from builtin list */
        if (existing_pinned && existing) {
            pp = &cache->builtin_users;
            while (*pp) {
                if (*pp == existing) {
                    *pp = existing->next_builtin;
                    break;
                }
                pp = &(*pp)->next_builtin;
            }
        }

        /* If new user is pinned, add to builtin list */
        if (pinned) {
            user->next_builtin   = cache->builtin_users;
            cache->builtin_users = user;
        }

        pthread_mutex_unlock(&cache->builtin_lock);
    }

    return 0;
} // chimera_vfs_user_cache_add // chimera_vfs_user_cache_add

static inline int
chimera_vfs_user_cache_remove(
    struct chimera_vfs_user_cache *cache,
    const char                    *username)
{
    unsigned int              name_idx, uid_idx;
    struct chimera_vfs_user  *user;
    struct chimera_vfs_user **pp;
    int                       username_len;
    int                       was_pinned = 0;

    username_len = strlen(username);
    name_idx     = chimera_vfs_user_cache_hash_name(username, username_len,
                                                    cache->num_buckets);

    pthread_mutex_lock(&cache->name_buckets[name_idx].lock);

    user = cache->name_buckets[name_idx].head;
    while (user) {
        if (strcmp(user->username, username) == 0) {
            uid_idx = chimera_vfs_user_cache_hash_uid(user->uid,
                                                      cache->num_buckets);
            was_pinned = user->pinned;

            if (uid_idx != name_idx) {
                pthread_mutex_lock(&cache->uid_buckets[uid_idx].lock);
            }

            chimera_vfs_user_cache_remove_locked(cache, user);

            if (uid_idx != name_idx) {
                pthread_mutex_unlock(&cache->uid_buckets[uid_idx].lock);
            }

            pthread_mutex_unlock(&cache->name_buckets[name_idx].lock);

            /* Remove from builtin list if pinned */
            if (was_pinned) {
                pthread_mutex_lock(&cache->builtin_lock);
                pp = &cache->builtin_users;
                while (*pp) {
                    if (*pp == user) {
                        *pp = user->next_builtin;
                        break;
                    }
                    pp = &(*pp)->next_builtin;
                }
                pthread_mutex_unlock(&cache->builtin_lock);
            }

            return 0;
        }
        user = user->next_by_name;
    }

    pthread_mutex_unlock(&cache->name_buckets[name_idx].lock);
    return -1;
} // chimera_vfs_user_cache_remove // chimera_vfs_user_cache_remove

static inline const struct chimera_vfs_user *
chimera_vfs_user_cache_lookup_by_name(
    struct chimera_vfs_user_cache *cache,
    const char                    *username)
{
    unsigned int             name_idx;
    struct chimera_vfs_user *user;
    int                      username_len;

    username_len = strlen(username);
    name_idx     = chimera_vfs_user_cache_hash_name(username, username_len,
                                                    cache->num_buckets);

    user = rcu_dereference(cache->name_buckets[name_idx].head);
    while (user) {
        if (strcmp(user->username, username) == 0) {
            return user;
        }
        user = rcu_dereference(user->next_by_name);
    }

    return NULL;
} // chimera_vfs_user_cache_lookup_by_name

static inline const struct chimera_vfs_user *
chimera_vfs_user_cache_lookup_by_uid(
    struct chimera_vfs_user_cache *cache,
    uint32_t                       uid)
{
    unsigned int             uid_idx;
    struct chimera_vfs_user *user;

    uid_idx = chimera_vfs_user_cache_hash_uid(uid, cache->num_buckets);

    user = rcu_dereference(cache->uid_buckets[uid_idx].head);
    while (user) {
        if (user->uid == uid) {
            return user;
        }
        user = rcu_dereference(user->next_by_uid);
    }

    return NULL;
} // chimera_vfs_user_cache_lookup_by_uid

static inline int
chimera_vfs_user_cache_lookup_by_gid(
    struct chimera_vfs_user_cache  *cache,
    uint32_t                        gid,
    const struct chimera_vfs_user **results,
    int                             max_results)
{
    int                      count = 0;
    int                      i;
    unsigned int             j;
    struct chimera_vfs_user *user;

    for (i = 0; i < cache->num_buckets && count < max_results; i++) {
        user = rcu_dereference(cache->name_buckets[i].head);
        while (user && count < max_results) {
            if (user->gid == gid) {
                results[count++] = user;
            } else {
                for (j = 0; j < user->ngids; j++) {
                    if (user->gids[j] == gid) {
                        results[count++] = user;
                        break;
                    }
                }
            }
            user = rcu_dereference(user->next_by_name);
        }
    }

    return count;
} // chimera_vfs_user_cache_lookup_by_gid

static inline int
chimera_vfs_user_cache_is_member(
    struct chimera_vfs_user_cache *cache,
    uint32_t                       uid,
    uint32_t                       gid)
{
    const struct chimera_vfs_user *user;
    unsigned int                   i;

    user = chimera_vfs_user_cache_lookup_by_uid(cache, uid);
    if (!user) {
        return 0;
    }

    if (user->gid == gid) {
        return 1;
    }

    for (i = 0; i < user->ngids; i++) {
        if (user->gids[i] == gid) {
            return 1;
        }
    }

    return 0;
} // chimera_vfs_user_cache_is_member


typedef int (*chimera_vfs_user_iterate_cb)(
    const struct chimera_vfs_user *user,
    void                          *data);

static inline void
chimera_vfs_user_cache_iterate_builtin(
    struct chimera_vfs_user_cache *cache,
    chimera_vfs_user_iterate_cb    callback,
    void                          *data)
{
    struct chimera_vfs_user *user;

    pthread_mutex_lock(&cache->builtin_lock);

    user = cache->builtin_users;
    while (user) {
        if (callback(user, data) != 0) {
            break;
        }
        user = user->next_builtin;
    }

    pthread_mutex_unlock(&cache->builtin_lock);
} // chimera_vfs_user_cache_iterate_builtin
