// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>
#include <unistd.h>
#include <urcu/urcu-qsbr.h>

#include "vfs/vfs_user_cache.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

static void
test_empty_lookups(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    cache = chimera_vfs_user_cache_create(64, 600);

    urcu_qsbr_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "nonexistent");
    assert(user == NULL);

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 9999);
    assert(user == NULL);

    assert(chimera_vfs_user_cache_is_member(cache, 9999, 9999) == 0);

    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("empty lookups return NULL");
} /* test_empty_lookups */

static void
test_add_and_lookup(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;
    uint32_t                       gids[] = { 100, 27 };

    cache = chimera_vfs_user_cache_create(64, 600);

    chimera_vfs_user_cache_add(cache, "alice", "$6$salt$hash",
                               "cleartext", NULL, 1000, 1000, 2, gids, 1);

    urcu_qsbr_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "alice");
    assert(user != NULL);
    assert(strcmp(user->username, "alice") == 0);
    assert(user->uid == 1000);
    assert(user->gid == 1000);
    assert(user->ngids == 2);
    assert(user->gids[0] == 100);
    assert(user->gids[1] == 27);
    assert(strcmp(user->password, "$6$salt$hash") == 0);
    assert(strcmp(user->smbpasswd, "cleartext") == 0);

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 1000);
    assert(user != NULL);
    assert(strcmp(user->username, "alice") == 0);

    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("add and lookup by name/uid");
} /* test_add_and_lookup */

static void
test_gid_lookup(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *results[16];
    int                            count;
    uint32_t                       alice_gids[] = { 100, 27 };
    uint32_t                       bob_gids[]   = { 100, 44 };

    cache = chimera_vfs_user_cache_create(64, 600);

    chimera_vfs_user_cache_add(cache, "alice", NULL, NULL, NULL,
                               1000, 1000, 2, alice_gids, 1);
    chimera_vfs_user_cache_add(cache, "bob", NULL, NULL, NULL,
                               1001, 1001, 2, bob_gids, 1);

    urcu_qsbr_read_lock();

    /* Both alice and bob are in group 100 */
    count = chimera_vfs_user_cache_lookup_by_gid(cache, 100, results, 16);
    assert(count == 2);

    /* Only alice is in group 27 */
    count = chimera_vfs_user_cache_lookup_by_gid(cache, 27, results, 16);
    assert(count == 1);
    assert(strcmp(results[0]->username, "alice") == 0);

    /* Only bob is in group 44 */
    count = chimera_vfs_user_cache_lookup_by_gid(cache, 44, results, 16);
    assert(count == 1);
    assert(strcmp(results[0]->username, "bob") == 0);

    /* alice has primary gid 1000 */
    count = chimera_vfs_user_cache_lookup_by_gid(cache, 1000, results, 16);
    assert(count == 1);
    assert(strcmp(results[0]->username, "alice") == 0);

    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("gid lookup with multiple users");
} /* test_gid_lookup */

static void
test_remove(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;
    int                            rc;

    cache = chimera_vfs_user_cache_create(64, 600);

    chimera_vfs_user_cache_add(cache, "alice", NULL, NULL, NULL,
                               1000, 1000, 0, NULL, 1);

    rc = chimera_vfs_user_cache_remove(cache, "alice");
    assert(rc == 0);

    /* Wait for RCU grace period */
    urcu_qsbr_synchronize_rcu();

    urcu_qsbr_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "alice");
    assert(user == NULL);

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 1000);
    assert(user == NULL);

    urcu_qsbr_read_unlock();

    /* Removing non-existent user should return -1 */
    rc = chimera_vfs_user_cache_remove(cache, "alice");
    assert(rc == -1);

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("remove user by username");
} /* test_remove */

static void
test_ttl_expiration(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    /* Create cache with 1 second TTL */
    cache = chimera_vfs_user_cache_create(64, 1);

    chimera_vfs_user_cache_add(cache, "temp_user", NULL, NULL, NULL,
                               2000, 2000, 0, NULL, 0);

    urcu_qsbr_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "temp_user");
    assert(user != NULL);
    urcu_qsbr_read_unlock();

    /* Sleep long enough for TTL to expire and expiry thread to run.
     * The expiry thread sleeps up to 60s, so we manually trigger
     * expiration by destroying and checking. Instead, we just verify
     * the expiration field is set correctly. */
#ifndef __clang_analyzer__
    /* Suppress: clang analyzer falsely thinks urcu read lock is held */
    sleep(2);
#endif /* ifndef __clang_analyzer__ */

    /* Signal the expiry thread to wake up and do a sweep */
    pthread_mutex_lock(&cache->expiry_lock);
    pthread_cond_signal(&cache->expiry_cond);
    pthread_mutex_unlock(&cache->expiry_lock);

    /* Give expiry thread time to process */
    usleep(100000);

    /* Wait for RCU grace period */
    urcu_qsbr_synchronize_rcu();

    urcu_qsbr_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "temp_user");
    assert(user == NULL);
    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("TTL expiration removes non-pinned users");
} /* test_ttl_expiration */

static void
test_pinned_no_expire(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    /* Create cache with 1 second TTL */
    cache = chimera_vfs_user_cache_create(64, 1);

    /* Add pinned user */
    chimera_vfs_user_cache_add(cache, "pinned_user", NULL, NULL, NULL,
                               3000, 3000, 0, NULL, 1);

#ifndef __clang_analyzer__
    /* Suppress: clang analyzer falsely thinks urcu read lock is held */
    sleep(2);
#endif /* ifndef __clang_analyzer__ */

    /* Signal the expiry thread */
    pthread_mutex_lock(&cache->expiry_lock);
    pthread_cond_signal(&cache->expiry_cond);
    pthread_mutex_unlock(&cache->expiry_lock);

    usleep(100000);

    urcu_qsbr_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "pinned_user");
    assert(user != NULL);
    assert(user->pinned == 1);
    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("pinned users do not expire");
} /* test_pinned_no_expire */

static void
test_is_member(void)
{
    struct chimera_vfs_user_cache *cache;
    uint32_t                       gids[] = { 100, 27, 44 };

    cache = chimera_vfs_user_cache_create(64, 600);

    chimera_vfs_user_cache_add(cache, "alice", NULL, NULL, NULL,
                               1000, 1000, 3, gids, 1);

    urcu_qsbr_read_lock();

    /* Primary group */
    assert(chimera_vfs_user_cache_is_member(cache, 1000, 1000) == 1);

    /* Secondary groups */
    assert(chimera_vfs_user_cache_is_member(cache, 1000, 100) == 1);
    assert(chimera_vfs_user_cache_is_member(cache, 1000, 27) == 1);
    assert(chimera_vfs_user_cache_is_member(cache, 1000, 44) == 1);

    /* Not a member */
    assert(chimera_vfs_user_cache_is_member(cache, 1000, 9999) == 0);

    /* Non-existent user */
    assert(chimera_vfs_user_cache_is_member(cache, 8888, 1000) == 0);

    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("is_member checks primary and secondary gids");
} /* test_is_member */

/* The SID index: a real (e.g. AD) SID round-trips to its uid, and the uid's
 * cached entry carries that same SID back. */
static void
test_sid_index(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    cache = chimera_vfs_user_cache_create(64, 600);

    chimera_vfs_user_cache_add(cache, "alice", NULL, NULL,
                               "S-1-5-21-111-222-333-1105",
                               1000, 1000, 0, NULL, 1);
    /* A user with no SID must not be reachable by SID lookup. */
    chimera_vfs_user_cache_add(cache, "bob", NULL, NULL, NULL,
                               1001, 1001, 0, NULL, 1);

    urcu_qsbr_read_lock();

    user = chimera_vfs_user_cache_lookup_by_sid(cache, "S-1-5-21-111-222-333-1105");
    assert(user != NULL);
    assert(user->uid == 1000);

    /* uid lookup returns the same real SID (the round-trip direction). */
    user = chimera_vfs_user_cache_lookup_by_uid(cache, 1000);
    assert(user != NULL);
    assert(strcmp(user->sid, "S-1-5-21-111-222-333-1105") == 0);

    /* Unknown / SID-less lookups miss. */
    assert(chimera_vfs_user_cache_lookup_by_sid(cache, "S-1-5-21-9-9-9-9") == NULL);
    assert(chimera_vfs_user_cache_lookup_by_sid(cache, "") == NULL);

    urcu_qsbr_read_unlock();

    /* Removing the user also unindexes its SID. */
    chimera_vfs_user_cache_remove(cache, "alice");
    urcu_qsbr_synchronize_rcu();
    urcu_qsbr_read_lock();
    assert(chimera_vfs_user_cache_lookup_by_sid(cache,
                                                "S-1-5-21-111-222-333-1105") == NULL);
    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("SID index round-trips uid<->real SID and unindexes on remove");
} /* test_sid_index */

/*
 * Group records: gid<->SID round trip, dedup by gid, TTL expiry.
 */
static void
test_group_index(void)
{
    struct chimera_vfs_user_cache  *cache;
    const struct chimera_vfs_group *group;

    cache = chimera_vfs_user_cache_create(64, 600);

    chimera_vfs_group_cache_add(cache, "Domain Users",
                                "S-1-5-21-111-222-333-513", 513, 0);

    urcu_qsbr_read_lock();

    group = chimera_vfs_group_cache_lookup_by_gid(cache, 513);
    assert(group != NULL);
    assert(group->gid == 513);
    assert(strcmp(group->groupname, "Domain Users") == 0);
    assert(strcmp(group->sid, "S-1-5-21-111-222-333-513") == 0);

    group = chimera_vfs_group_cache_lookup_by_sid(cache,
                                                  "S-1-5-21-111-222-333-513");
    assert(group != NULL);
    assert(group->gid == 513);

    /* Unknown / SID-less lookups miss. */
    assert(chimera_vfs_group_cache_lookup_by_gid(cache, 9999) == NULL);
    assert(chimera_vfs_group_cache_lookup_by_sid(cache, "S-1-5-21-9-9-9-9") == NULL);
    assert(chimera_vfs_group_cache_lookup_by_sid(cache, "") == NULL);
    assert(chimera_vfs_group_cache_lookup_by_sid(cache, NULL) == NULL);

    urcu_qsbr_read_unlock();

    /* A group with no SID resolves by gid but is not SID-indexed. */
    chimera_vfs_group_cache_add(cache, "localgrp", NULL, 27, 0);
    urcu_qsbr_read_lock();
    group = chimera_vfs_group_cache_lookup_by_gid(cache, 27);
    assert(group != NULL && group->sid[0] == '\0');
    urcu_qsbr_read_unlock();

    /* Re-adding the same gid replaces the record and unindexes the old SID. */
    chimera_vfs_group_cache_add(cache, "Domain Users",
                                "S-1-5-21-999-888-777-513", 513, 0);
    urcu_qsbr_synchronize_rcu();
    urcu_qsbr_read_lock();
    assert(chimera_vfs_group_cache_lookup_by_sid(cache,
                                                 "S-1-5-21-111-222-333-513") == NULL);
    group = chimera_vfs_group_cache_lookup_by_sid(cache,
                                                  "S-1-5-21-999-888-777-513");
    assert(group != NULL && group->gid == 513);
    /* Exactly one record per gid. */
    group = chimera_vfs_group_cache_lookup_by_gid(cache, 513);
    assert(group != NULL);
    assert(strcmp(group->sid, "S-1-5-21-999-888-777-513") == 0);
    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("group index round-trips gid<->real SID and dedups by gid");
} /* test_group_index */

/*
 * The reason groups are a distinct record type: a group must be invisible to
 * every user-side lookup, and must not disturb a user that shares its name.
 */
static void
test_group_user_isolation(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    cache = chimera_vfs_user_cache_create(64, 600);

    chimera_vfs_user_cache_add(cache, "shared", NULL, NULL,
                               "S-1-5-21-111-222-333-1105",
                               1000, 1000, 0, NULL, 0);

    /* Same name, and a gid numerically equal to the user's uid -- the two
     * collisions that a single shared record would get wrong. */
    chimera_vfs_group_cache_add(cache, "shared",
                                "S-1-5-21-111-222-333-513", 1000, 0);

    urcu_qsbr_read_lock();

    /* The user survives intact: no eviction by the group's name, no clobber. */
    user = chimera_vfs_user_cache_lookup_by_name(cache, "shared");
    assert(user != NULL);
    assert(user->uid == 1000);
    assert(strcmp(user->sid, "S-1-5-21-111-222-333-1105") == 0);

    /* uid 1000 still resolves to the user, not the gid-1000 group. */
    user = chimera_vfs_user_cache_lookup_by_uid(cache, 1000);
    assert(user != NULL);
    assert(strcmp(user->sid, "S-1-5-21-111-222-333-1105") == 0);

    /* The group SID is not reachable through the user SID index (this is what
     * used to make sid_to_uid hand back a meaningless uid). */
    assert(chimera_vfs_user_cache_lookup_by_sid(cache,
                                                "S-1-5-21-111-222-333-513") == NULL);

    /* ...and the user SID is not reachable through the group SID index. */
    assert(chimera_vfs_group_cache_lookup_by_sid(cache,
                                                 "S-1-5-21-111-222-333-1105") == NULL);

    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("group records stay out of every user index");
} /* test_group_user_isolation */

static void
test_group_ttl_expiration(void)
{
    struct chimera_vfs_user_cache *cache;

    /* ttl 0: the entry is already expired when the sweeper next runs. */
    cache = chimera_vfs_user_cache_create(64, 0);

    chimera_vfs_group_cache_add(cache, "ephemeral",
                                "S-1-5-21-111-222-333-514", 514, 0);
    chimera_vfs_group_cache_add(cache, "permanent",
                                "S-1-5-21-111-222-333-515", 515, 1);

    urcu_qsbr_read_lock();
    assert(chimera_vfs_group_cache_lookup_by_gid(cache, 514) != NULL);
    urcu_qsbr_read_unlock();

    /* Drive the sweep directly rather than waiting out its 60s period. */
    pthread_mutex_lock(&cache->write_lock);
    {
        struct chimera_vfs_group *group, *next;
        struct timespec           ts;
        int                       i;

        clock_gettime(CLOCK_REALTIME, &ts);
        for (i = 0; i < cache->num_buckets; i++) {
            group = cache->group_gid_buckets[i].head;
            while (group) {
                next = group->next_by_gid;
                if (!group->pinned &&
                    (ts.tv_sec > group->expiration.tv_sec ||
                     (ts.tv_sec == group->expiration.tv_sec &&
                      ts.tv_nsec >= group->expiration.tv_nsec))) {
                    chimera_vfs_group_cache_remove_locked(cache, group);
                }
                group = next;
            }
        }
    }
    pthread_mutex_unlock(&cache->write_lock);

    urcu_qsbr_synchronize_rcu();
    urcu_qsbr_read_lock();
    assert(chimera_vfs_group_cache_lookup_by_gid(cache, 514) == NULL);
    assert(chimera_vfs_group_cache_lookup_by_sid(cache,
                                                 "S-1-5-21-111-222-333-514") == NULL);
    /* Pinned groups survive the sweep. */
    assert(chimera_vfs_group_cache_lookup_by_gid(cache, 515) != NULL);
    urcu_qsbr_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("expired groups are swept, pinned groups survive");
} /* test_group_ttl_expiration */

int
main(void)
{
    urcu_qsbr_register_thread();

    fprintf(stderr, "Running vfs_user_cache tests:\n");

    test_empty_lookups();
    test_add_and_lookup();
    test_gid_lookup();
    test_remove();
    test_ttl_expiration();
    test_pinned_no_expire();
    test_is_member();
    test_sid_index();
    test_group_index();
    test_group_user_isolation();
    test_group_ttl_expiration();

    fprintf(stderr, "All tests passed.\n");

    urcu_qsbr_unregister_thread();

    return 0;
} /* main */
