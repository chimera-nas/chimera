// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>
#include <unistd.h>
#include <urcu.h>
#include <urcu/urcu-memb.h>

#include "vfs/vfs_user_cache.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

static void
test_empty_lookups(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    cache = chimera_vfs_user_cache_create(64, 600);

    urcu_memb_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "nonexistent");
    assert(user == NULL);

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 9999);
    assert(user == NULL);

    assert(chimera_vfs_user_cache_is_member(cache, 9999, 9999) == 0);

    urcu_memb_read_unlock();

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

    urcu_memb_read_lock();

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

    urcu_memb_read_unlock();

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

    urcu_memb_read_lock();

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

    urcu_memb_read_unlock();

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
    urcu_memb_synchronize_rcu();

    urcu_memb_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "alice");
    assert(user == NULL);

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 1000);
    assert(user == NULL);

    urcu_memb_read_unlock();

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

    urcu_memb_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "temp_user");
    assert(user != NULL);
    urcu_memb_read_unlock();

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
    urcu_memb_synchronize_rcu();

    urcu_memb_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "temp_user");
    assert(user == NULL);
    urcu_memb_read_unlock();

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

    urcu_memb_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "pinned_user");
    assert(user != NULL);
    assert(user->pinned == 1);
    urcu_memb_read_unlock();

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

    urcu_memb_read_lock();

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

    urcu_memb_read_unlock();

    chimera_vfs_user_cache_destroy(cache);

    TEST_PASS("is_member checks primary and secondary gids");
} /* test_is_member */

int
main(void)
{
    urcu_memb_register_thread();

    fprintf(stderr, "Running vfs_user_cache tests:\n");

    test_empty_lookups();
    test_add_and_lookup();
    test_gid_lookup();
    test_remove();
    test_ttl_expiration();
    test_pinned_no_expire();
    test_is_member();

    fprintf(stderr, "All tests passed.\n");

    urcu_memb_unregister_thread();

    return 0;
} /* main */
