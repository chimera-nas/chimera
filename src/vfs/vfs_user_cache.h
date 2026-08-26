// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <urcu/urcu-qsbr.h>

#include "sdk/vfs_cred.h"
#include "vfs_internal.h"

// Maximum length for a Windows SID string (S-1-5-21-xxx-xxx-xxx-rid format)
#define CHIMERA_VFS_SID_MAX_LEN 80

/*
 * In-memory identity store: maps a known principal between its UNIX identity
 * (uid/gid/supplementary groups), its name, and its Windows SID.  This is the
 * single authority the protocol servers, the idmap formatter, and SMB auth all
 * consult.  Entries are either pinned (configured local users) or cached with a
 * TTL (e.g. AD users learned at auth, or resolved on demand by the identity
 * resolver).
 *
 * Concurrency: lookups are lock-free RCU reads (callers hold the urcu read
 * lock); all mutations (add / remove / TTL expiry) are serialized by a single
 * cache write_lock.  Mutations are rare (auth, config load, the 60s expiry
 * sweep) so a single writer lock keeps the three indices (name, uid, sid)
 * trivially consistent without the cross-index lock-ordering hazards a
 * per-bucket scheme would create.
 */
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
    struct chimera_vfs_user *next_by_sid;
    struct chimera_vfs_user *next_builtin;
    uint32_t                 gids[CHIMERA_VFS_CRED_MAX_GIDS];
    char                     username[256];
    char                     password[256];
    char                     smbpasswd[256];
    char                     sid[CHIMERA_VFS_SID_MAX_LEN];
};

/*
 * A group record.  Deliberately a distinct type from chimera_vfs_user rather
 * than a flag on it: a group is not a user, and sharing the record would put
 * group entries in the name/uid/sid chains where lookup_by_uid, sid_to_uid and
 * the username dedup in _add would return or clobber them.  Groups live in
 * their own two chains below, sharing this cache's write_lock and expiry
 * thread.
 */
struct chimera_vfs_group {
    uint32_t                  gid;
    int                       groupname_len;
    struct timespec           expiration;
    int                       pinned;
    struct rcu_head           rcu;
    struct chimera_vfs_group *next_by_gid;
    struct chimera_vfs_group *next_by_sid;
    char                      groupname[256];
    char                      sid[CHIMERA_VFS_SID_MAX_LEN];
};

struct chimera_vfs_user_cache_bucket {
    struct chimera_vfs_user *head;
};

struct chimera_vfs_group_cache_bucket {
    struct chimera_vfs_group *head;
};

struct chimera_vfs_user_cache {
    int                                    num_buckets;
    int                                    ttl;
    struct chimera_vfs_user_cache_bucket  *name_buckets;
    struct chimera_vfs_user_cache_bucket  *uid_buckets;
    struct chimera_vfs_user_cache_bucket  *sid_buckets;
    struct chimera_vfs_group_cache_bucket *group_gid_buckets;
    struct chimera_vfs_group_cache_bucket *group_sid_buckets;
    struct chimera_vfs_user               *builtin_users;
    pthread_mutex_t                        write_lock;
    pthread_t                              expiry_thread;
    pthread_mutex_t                        expiry_lock;
    pthread_cond_t                         expiry_cond;
    int                                    shutdown;
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

static inline unsigned int
chimera_vfs_user_cache_hash_sid(
    const char *sid,
    int         num_buckets)
{
    return chimera_vfs_hash(sid, strlen(sid)) % num_buckets;
} // chimera_vfs_user_cache_hash_sid

static inline unsigned int
chimera_vfs_group_cache_hash_gid(
    uint32_t gid,
    int      num_buckets)
{
    return gid % num_buckets;
} // chimera_vfs_group_cache_hash_gid

static void
chimera_vfs_user_cache_rcu_free(struct rcu_head *head)
{
    struct chimera_vfs_user *user = caa_container_of(
        head, struct chimera_vfs_user, rcu);

    free(user);
} // chimera_vfs_user_cache_rcu_free

static void
chimera_vfs_group_cache_rcu_free(struct rcu_head *head)
{
    struct chimera_vfs_group *group = caa_container_of(
        head, struct chimera_vfs_group, rcu);

    free(group);
} // chimera_vfs_group_cache_rcu_free

/*
 * Unlink `group` from the gid and sid chains and schedule it for RCU-deferred
 * free.  Caller must hold cache->write_lock.
 */
static inline void
chimera_vfs_group_cache_remove_locked(
    struct chimera_vfs_user_cache *cache,
    struct chimera_vfs_group      *group)
{
    unsigned int               gid_idx, sid_idx;
    struct chimera_vfs_group **pp;

    gid_idx = chimera_vfs_group_cache_hash_gid(group->gid, cache->num_buckets);

    pp = &cache->group_gid_buckets[gid_idx].head;
    while (*pp) {
        if (*pp == group) {
            rcu_assign_pointer(*pp, group->next_by_gid);
            break;
        }
        pp = &(*pp)->next_by_gid;
    }

    /* Only indexed by SID when one is known (NSS supplies none). */
    if (group->sid[0]) {
        sid_idx = chimera_vfs_user_cache_hash_sid(group->sid,
                                                  cache->num_buckets);
        pp = &cache->group_sid_buckets[sid_idx].head;
        while (*pp) {
            if (*pp == group) {
                rcu_assign_pointer(*pp, group->next_by_sid);
                break;
            }
            pp = &(*pp)->next_by_sid;
        }
    }

    call_rcu(&group->rcu, chimera_vfs_group_cache_rcu_free);
} // chimera_vfs_group_cache_remove_locked

/*
 * Unlink `user` from all three index chains and schedule it for RCU-deferred
 * free.  Caller must hold cache->write_lock.
 */
static inline void
chimera_vfs_user_cache_remove_locked(
    struct chimera_vfs_user_cache *cache,
    struct chimera_vfs_user       *user)
{
    unsigned int              name_idx, uid_idx, sid_idx;
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

    /* Remove from sid chain (only indexed when a SID is present) */
    if (user->sid[0]) {
        sid_idx = chimera_vfs_user_cache_hash_sid(user->sid,
                                                  cache->num_buckets);
        pp = &cache->sid_buckets[sid_idx].head;
        while (*pp) {
            if (*pp == user) {
                rcu_assign_pointer(*pp, user->next_by_sid);
                break;
            }
            pp = &(*pp)->next_by_sid;
        }
    }

    call_rcu(&user->rcu, chimera_vfs_user_cache_rcu_free);
} // chimera_vfs_user_cache_remove_locked

static void *
chimera_vfs_user_cache_expiry_thread(void *arg)
{
    struct chimera_vfs_user_cache *cache = arg;
    struct chimera_vfs_user       *user, *next;
    struct chimera_vfs_group      *group, *group_next;
    struct timespec                ts;
    int                            i;

    /* Pure writer: sweeps expired entries under the write lock (rcu_assign +
     * call_rcu), never takes an RCU read lock.  Not registered as a QSBR
     * reader -- a thread parked in cond_timedwait must not sit in the
     * grace-period quorum. */
    pthread_mutex_lock(&cache->expiry_lock);

    while (!cache->shutdown) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 60;

        pthread_cond_timedwait(&cache->expiry_cond, &cache->expiry_lock, &ts);

        if (cache->shutdown) {
            break;
        }

        /* Drop the expiry lock during the sweep and serialize the actual
         * removals on the cache write_lock like every other mutation. */
        pthread_mutex_unlock(&cache->expiry_lock);

        clock_gettime(CLOCK_REALTIME, &ts);

        pthread_mutex_lock(&cache->write_lock);

        for (i = 0; i < cache->num_buckets; i++) {
            user = cache->name_buckets[i].head;
            while (user) {
                next = user->next_by_name;
                if (!user->pinned &&
                    (ts.tv_sec > user->expiration.tv_sec ||
                     (ts.tv_sec == user->expiration.tv_sec &&
                      ts.tv_nsec >= user->expiration.tv_nsec))) {
                    chimera_vfs_user_cache_remove_locked(cache, user);
                }
                user = next;
            }
        }

        /* Same sweep, same lock, for the group chains. */
        for (i = 0; i < cache->num_buckets; i++) {
            group = cache->group_gid_buckets[i].head;
            while (group) {
                group_next = group->next_by_gid;
                if (!group->pinned &&
                    (ts.tv_sec > group->expiration.tv_sec ||
                     (ts.tv_sec == group->expiration.tv_sec &&
                      ts.tv_nsec >= group->expiration.tv_nsec))) {
                    chimera_vfs_group_cache_remove_locked(cache, group);
                }
                group = group_next;
            }
        }

        pthread_mutex_unlock(&cache->write_lock);

        pthread_mutex_lock(&cache->expiry_lock);
    }

    pthread_mutex_unlock(&cache->expiry_lock);

    return NULL;
} // chimera_vfs_user_cache_expiry_thread

static inline struct chimera_vfs_user_cache *
chimera_vfs_user_cache_create(
    int num_buckets,
    int ttl)
{
    struct chimera_vfs_user_cache *cache;

    cache              = calloc(1, sizeof(*cache));
    cache->num_buckets = num_buckets;
    cache->ttl         = ttl;
    cache->shutdown    = 0;

    cache->name_buckets = calloc(num_buckets,
                                 sizeof(struct chimera_vfs_user_cache_bucket));
    cache->uid_buckets = calloc(num_buckets,
                                sizeof(struct chimera_vfs_user_cache_bucket));
    cache->sid_buckets = calloc(num_buckets,
                                sizeof(struct chimera_vfs_user_cache_bucket));

    cache->group_gid_buckets = calloc(num_buckets,
                                      sizeof(struct chimera_vfs_group_cache_bucket));
    cache->group_sid_buckets = calloc(num_buckets,
                                      sizeof(struct chimera_vfs_group_cache_bucket));

    cache->builtin_users = NULL;

    pthread_mutex_init(&cache->write_lock, NULL);
    pthread_mutex_init(&cache->expiry_lock, NULL);
    pthread_cond_init(&cache->expiry_cond, NULL);

    pthread_create(&cache->expiry_thread, NULL,
                   chimera_vfs_user_cache_expiry_thread, cache);

    return cache;
} // chimera_vfs_user_cache_create

static inline void
chimera_vfs_user_cache_destroy(struct chimera_vfs_user_cache *cache)
{
    struct chimera_vfs_user  *user, *next;
    struct chimera_vfs_group *group, *group_next;
    int                       i;

    pthread_mutex_lock(&cache->expiry_lock);
    cache->shutdown = 1;
    pthread_cond_signal(&cache->expiry_cond);
    pthread_mutex_unlock(&cache->expiry_lock);

    pthread_join(cache->expiry_thread, NULL);

    urcu_qsbr_barrier();

    for (i = 0; i < cache->num_buckets; i++) {
        user = cache->name_buckets[i].head;
        while (user) {
            next = user->next_by_name;
            free(user);
            user = next;
        }
    }

    for (i = 0; i < cache->num_buckets; i++) {
        group = cache->group_gid_buckets[i].head;
        while (group) {
            group_next = group->next_by_gid;
            free(group);
            group = group_next;
        }
    }

    free(cache->name_buckets);
    free(cache->uid_buckets);
    free(cache->sid_buckets);
    free(cache->group_gid_buckets);
    free(cache->group_sid_buckets);

    pthread_mutex_destroy(&cache->write_lock);
    pthread_mutex_destroy(&cache->expiry_lock);
    pthread_cond_destroy(&cache->expiry_cond);

    free(cache);
} // chimera_vfs_user_cache_destroy

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
    unsigned int             name_idx, uid_idx, sid_idx;
    struct timespec          now;
    int                      username_len;
    int                      existing_pinned = 0;

    username_len = strlen(username);

    user = calloc(1, sizeof(*user));
    /* snprintf (not strncpy): always NUL-terminates and avoids the
     * stringop-truncation warning when a caller passes a fixed-size buffer. */
    snprintf(user->username, sizeof(user->username), "%s", username);
    user->username_len = username_len;
    if (password) {
        snprintf(user->password, sizeof(user->password), "%s", password);
    }
    if (smbpasswd) {
        snprintf(user->smbpasswd, sizeof(user->smbpasswd), "%s", smbpasswd);
    }
    if (sid) {
        snprintf(user->sid, sizeof(user->sid), "%s", sid);
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

    name_idx = chimera_vfs_user_cache_hash_name(username, username_len,
                                                cache->num_buckets);
    uid_idx = chimera_vfs_user_cache_hash_uid(uid, cache->num_buckets);

    pthread_mutex_lock(&cache->write_lock);

    /* Check for an existing entry with the same username and remove it */
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

    /* Insert into sid chain (only when a SID is present) */
    if (user->sid[0]) {
        sid_idx = chimera_vfs_user_cache_hash_sid(user->sid,
                                                  cache->num_buckets);
        user->next_by_sid = cache->sid_buckets[sid_idx].head;
        rcu_assign_pointer(cache->sid_buckets[sid_idx].head, user);
    }

    /* Maintain the builtin list for pinned users */
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

    if (pinned) {
        user->next_builtin   = cache->builtin_users;
        cache->builtin_users = user;
    }

    pthread_mutex_unlock(&cache->write_lock);

    return 0;
} // chimera_vfs_user_cache_add

static inline int
chimera_vfs_user_cache_remove(
    struct chimera_vfs_user_cache *cache,
    const char                    *username)
{
    unsigned int              name_idx;
    struct chimera_vfs_user  *user;
    struct chimera_vfs_user **pp;
    int                       username_len;
    int                       found = -1;

    username_len = strlen(username);
    name_idx     = chimera_vfs_user_cache_hash_name(username, username_len,
                                                    cache->num_buckets);

    pthread_mutex_lock(&cache->write_lock);

    user = cache->name_buckets[name_idx].head;
    while (user) {
        if (strcmp(user->username, username) == 0) {
            if (user->pinned) {
                pp = &cache->builtin_users;
                while (*pp) {
                    if (*pp == user) {
                        *pp = user->next_builtin;
                        break;
                    }
                    pp = &(*pp)->next_builtin;
                }
            }
            chimera_vfs_user_cache_remove_locked(cache, user);
            found = 0;
            break;
        }
        user = user->next_by_name;
    }

    pthread_mutex_unlock(&cache->write_lock);
    return found;
} // chimera_vfs_user_cache_remove

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

/*
 * Resolve a Windows SID string to its cached user (RCU read-side).  Only users
 * added with a non-empty SID are indexed here.
 */
static inline const struct chimera_vfs_user *
chimera_vfs_user_cache_lookup_by_sid(
    struct chimera_vfs_user_cache *cache,
    const char                    *sid)
{
    unsigned int             sid_idx;
    struct chimera_vfs_user *user;

    if (!sid || !sid[0]) {
        return NULL;
    }

    sid_idx = chimera_vfs_user_cache_hash_sid(sid, cache->num_buckets);

    user = rcu_dereference(cache->sid_buckets[sid_idx].head);
    while (user) {
        if (strcmp(user->sid, sid) == 0) {
            return user;
        }
        user = rcu_dereference(user->next_by_sid);
    }

    return NULL;
} // chimera_vfs_user_cache_lookup_by_sid

/*
 * Add (or refresh) a group record.  Dedup is by gid, not by name: unlike
 * usernames there is no unique-name guarantee across domains, and gid is the
 * primary lookup key.  `sid` may be NULL (NSS resolves a gid to a name but
 * supplies no SID), in which case the record is not indexed by SID and
 * gid_to_sid will keep missing -- callers then fall back to the algorithmic
 * idmap SID.
 */
static inline int
chimera_vfs_group_cache_add(
    struct chimera_vfs_user_cache *cache,
    const char                    *groupname,
    const char                    *sid,
    uint32_t                       gid,
    int                            pinned)
{
    struct chimera_vfs_group *group, *existing;
    unsigned int              gid_idx, sid_idx;
    struct timespec           now;

    group = calloc(1, sizeof(*group));
    snprintf(group->groupname, sizeof(group->groupname), "%s",
             groupname ? groupname : "");
    group->groupname_len = (int) strlen(group->groupname);
    if (sid) {
        snprintf(group->sid, sizeof(group->sid), "%s", sid);
    }
    group->gid    = gid;
    group->pinned = pinned;

    if (!pinned) {
        clock_gettime(CLOCK_REALTIME, &now);
        group->expiration.tv_sec  = now.tv_sec + cache->ttl;
        group->expiration.tv_nsec = now.tv_nsec;
    }

    gid_idx = chimera_vfs_group_cache_hash_gid(gid, cache->num_buckets);

    pthread_mutex_lock(&cache->write_lock);

    /* Replace any existing record for this gid. */
    existing = cache->group_gid_buckets[gid_idx].head;
    while (existing) {
        if (existing->gid == gid) {
            chimera_vfs_group_cache_remove_locked(cache, existing);
            break;
        }
        existing = existing->next_by_gid;
    }

    group->next_by_gid = cache->group_gid_buckets[gid_idx].head;
    rcu_assign_pointer(cache->group_gid_buckets[gid_idx].head, group);

    if (group->sid[0]) {
        sid_idx = chimera_vfs_user_cache_hash_sid(group->sid,
                                                  cache->num_buckets);
        group->next_by_sid = cache->group_sid_buckets[sid_idx].head;
        rcu_assign_pointer(cache->group_sid_buckets[sid_idx].head, group);
    }

    pthread_mutex_unlock(&cache->write_lock);

    return 0;
} // chimera_vfs_group_cache_add

/*
 * Resolve a gid to its cached group record (RCU read-side).
 *
 * Note this is NOT chimera_vfs_user_cache_lookup_by_gid below, which despite
 * the similar name answers a different question: which *users* are members of
 * a gid.
 */
static inline const struct chimera_vfs_group *
chimera_vfs_group_cache_lookup_by_gid(
    struct chimera_vfs_user_cache *cache,
    uint32_t                       gid)
{
    unsigned int              gid_idx;
    struct chimera_vfs_group *group;

    gid_idx = chimera_vfs_group_cache_hash_gid(gid, cache->num_buckets);

    group = rcu_dereference(cache->group_gid_buckets[gid_idx].head);
    while (group) {
        if (group->gid == gid) {
            return group;
        }
        group = rcu_dereference(group->next_by_gid);
    }

    return NULL;
} // chimera_vfs_group_cache_lookup_by_gid

/*
 * Resolve a Windows group SID string to its cached group (RCU read-side).
 * Only groups added with a non-empty SID are indexed here.
 */
static inline const struct chimera_vfs_group *
chimera_vfs_group_cache_lookup_by_sid(
    struct chimera_vfs_user_cache *cache,
    const char                    *sid)
{
    unsigned int              sid_idx;
    struct chimera_vfs_group *group;

    if (!sid || !sid[0]) {
        return NULL;
    }

    sid_idx = chimera_vfs_user_cache_hash_sid(sid, cache->num_buckets);

    group = rcu_dereference(cache->group_sid_buckets[sid_idx].head);
    while (group) {
        if (strcmp(group->sid, sid) == 0) {
            return group;
        }
        group = rcu_dereference(group->next_by_sid);
    }

    return NULL;
} // chimera_vfs_group_cache_lookup_by_sid

/*
 * Which *users* are members of `gid` (primary or supplementary).  Unrelated to
 * chimera_vfs_group_cache_lookup_by_gid above, which resolves the gid itself to
 * a group record.
 */
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

    pthread_mutex_lock(&cache->write_lock);

    user = cache->builtin_users;
    while (user) {
        if (callback(user, data) != 0) {
            break;
        }
        user = user->next_builtin;
    }

    pthread_mutex_unlock(&cache->write_lock);
} // chimera_vfs_user_cache_iterate_builtin
