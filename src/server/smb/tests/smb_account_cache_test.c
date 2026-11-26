// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit test for the SMB account cache: lookups by SID and by name, case
 * insensitive names, refresh, FIFO eviction past the cap, expiry, and use
 * from several threads at once.
 */

#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <pthread.h>
#include <unistd.h>
#endif /* ifdef _WIN32 */

#include "smb_account_cache.h"

static int failures;

static void
report(
    const char *name,
    int         ok)
{
    if (ok) {
        printf("PASS: %s\n", name);
    } else {
        printf("FAIL: %s\n", name);
        failures++;
    }
} /* report */

static void
make(
    struct chimera_vfs_identity_account *a,
    const char                          *sid,
    const char                          *name,
    uint32_t                             type)
{
    memset(a, 0, sizeof(*a));
    snprintf(a->sid, sizeof(a->sid), "%s", sid);
    snprintf(a->domain, sizeof(a->domain), "%s", "WBSTUB");
    snprintf(a->name, sizeof(a->name), "%s", name);
    a->sid_type = type;
} /* make */

static int
same(
    const struct chimera_vfs_identity_account *a,
    const struct chimera_vfs_identity_account *b)
{
    return memcmp(a, b, sizeof(*a)) == 0;
} /* same */

#ifndef _WIN32
/* The cache is filled from resolver answers and read by lookups on every SMB
 * thread.  Several threads put and look up the same few accounts in a cache
 * too small to hold them, so entries are evicted and replaced under the
 * readers all the time: a hit must still be the whole account asked for. */
#define STRESS_THREADS  4
#define STRESS_ACCOUNTS 32
#define STRESS_ROUNDS   20000

struct stress {
    struct chimera_smb_account_cache *cache;
    int                               id;
    int                               hits;
    int                               bad;
};

static void *
stress_main(void *arg)
{
    struct stress                      *st = arg;
    struct chimera_vfs_identity_account a, out;
    char                                sid[64], name[32], key[64];
    int                                 round, i, hit;

    for (round = 0; round < STRESS_ROUNDS; round++) {
        i = (round * 7 + st->id * 13) % STRESS_ACCOUNTS;
        snprintf(sid, sizeof(sid), "S-1-5-21-1-2-3-%d", 3000 + i);
        snprintf(name, sizeof(name), "stress%d", i);
        snprintf(key, sizeof(key), "wbstub\\STRESS%d", i);
        make(&a, sid, name, 1);

        if (round % 3 == 0) {
            chimera_smb_account_cache_put(st->cache, &a, NULL);
            continue;
        }
        hit = round % 3 == 1 ?
            chimera_smb_account_cache_get_sid(st->cache, sid, &out) :
            chimera_smb_account_cache_get_name(st->cache, key, &out);
        if (hit) {
            st->hits++;
            if (!same(&a, &out)) {
                st->bad++;
            }
        }
    }
    return NULL;
} /* stress_main */

static void
test_threads(void)
{
    struct stress                     st[STRESS_THREADS];
    pthread_t                         tid[STRESS_THREADS];
    struct chimera_smb_account_cache *cache;
    int                               i, hits = 0, bad = 0;

    cache = chimera_smb_account_cache_create(8, 600);
    for (i = 0; i < STRESS_THREADS; i++) {
        st[i] = (struct stress) { .cache = cache, .id = i };
        pthread_create(&tid[i], NULL, stress_main, &st[i]);
    }
    for (i = 0; i < STRESS_THREADS; i++) {
        pthread_join(tid[i], NULL);
        hits += st[i].hits;
        bad  += st[i].bad;
    }
    chimera_smb_account_cache_destroy(cache);

    printf("#   %d hit(s) across %d threads, %d wrong\n", hits,
           STRESS_THREADS, bad);
    report("threads sharing a small cache only ever see whole accounts",
           hits > 0 && bad == 0);
} /* test_threads */
#endif /* ifndef _WIN32 */

int
main(void)
{
    struct chimera_vfs_identity_account U, G, A, B, C, out, r;
    struct chimera_smb_account_cache   *cache;
    int                                 ok;

    make(&U, "S-1-5-21-1-2-3-1002", "user1002", 1);
    make(&G, "S-1-5-21-1-2-3-513", "Domain Users", 2);
    make(&A, "S-1-5-21-1-2-3-2001", "a", 1);
    make(&B, "S-1-5-21-1-2-3-2002", "b", 1);
    make(&C, "S-1-5-21-1-2-3-2003", "c", 1);

    cache = chimera_smb_account_cache_create(16, 600);
    chimera_smb_account_cache_put(cache, &U, NULL);
    memset(&out, 0, sizeof(out));
    ok = chimera_smb_account_cache_get_sid(cache, U.sid, &out) == 1 && same(&out, &U);
    memset(&out, 0, sizeof(out));
    ok = ok && chimera_smb_account_cache_get_name(cache, "WBSTUB\\user1002", &out) == 1 && same(&out, &U);
    ok = ok && chimera_smb_account_cache_get_name(cache, "user1002", &out) == 0;
    report("hit by sid and by domain-qualified name", ok);

    chimera_smb_account_cache_put(cache, &G, NULL);
    ok = chimera_smb_account_cache_get_name(cache, "wbstub\\domain users", &out) == 1 &&
        chimera_smb_account_cache_get_name(cache, "WBSTUB\\DOMAIN USERS", &out) == 1;
    report("names compare case-insensitively", ok);

    chimera_smb_account_cache_put(cache, &U, "User1002");
    ok = chimera_smb_account_cache_get_name(cache, "user1002", &out) == 1 &&
        chimera_smb_account_cache_get_name(cache, "USER1002", &out) == 1;
    report("the name as sent is indexed too", ok);

    r = U;
    snprintf(r.name, sizeof(r.name), "%s", "renamed");
    chimera_smb_account_cache_put(cache, &r, NULL);
    ok = chimera_smb_account_cache_get_sid(cache, U.sid, &out) == 1 &&
        strcmp(out.name, "renamed") == 0;
    report("a refresh replaces the record", ok);
    chimera_smb_account_cache_destroy(cache);

    cache = chimera_smb_account_cache_create(2, 600);
    chimera_smb_account_cache_put(cache, &A, NULL);
    chimera_smb_account_cache_put(cache, &B, NULL);
    chimera_smb_account_cache_put(cache, &C, NULL);
    ok = chimera_smb_account_cache_get_sid(cache, A.sid, &out) == 0 &&
        chimera_smb_account_cache_get_sid(cache, B.sid, &out) == 1 &&
        chimera_smb_account_cache_get_sid(cache, C.sid, &out) == 1;
    report("oldest entries are evicted past the cap", ok);
    chimera_smb_account_cache_destroy(cache);

    cache = chimera_smb_account_cache_create(16, 1);
    chimera_smb_account_cache_put(cache, &U, NULL);
    ok = chimera_smb_account_cache_get_sid(cache, U.sid, &out) == 1;
    sleep(2);
    ok = ok && chimera_smb_account_cache_get_sid(cache, U.sid, &out) == 0 &&
        chimera_smb_account_cache_get_name(cache, "WBSTUB\\user1002", &out) == 0;
    report("entries expire after the ttl", ok);
    chimera_smb_account_cache_destroy(cache);

#ifndef _WIN32
    test_threads();
#endif /* ifndef _WIN32 */

    return failures ? 1 : 0;
} /* main */
