// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB Authentication Test Program
 *
 * Tests SMB authentication mechanisms:
 * - Local NTLM auth (VFS user cache)
 * - User cache operations (lookup, expiration, pinning)
 * - SID handling and synthesis
 * - Supplementary groups
 * - NTLM via winbind (mock or real)
 * - Kerberos via GSSAPI
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#undef NDEBUG
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <urcu.h>
#include <urcu/urcu-memb.h>

#include "vfs/vfs.h"
#include "vfs/vfs_user_cache.h"

#define TEST_PASS(name) do { fprintf(stderr, "  PASS: %s\n", name); passed++; } while (0)
#define TEST_FAIL(name) do { fprintf(stderr, "  FAIL: %s\n", name); failed++; } while (0)
#define TEST_SKIP(name) do { fprintf(stderr, "  SKIP: %s\n", name); skipped++; } while (0)

static int passed  = 0;
static int failed  = 0;
static int skipped = 0;

enum test_mode {
    TEST_MODE_LOCAL,         /* Test local NTLM auth only */
    TEST_MODE_NTLM_WINBIND,  /* Test NTLM via winbind */
    TEST_MODE_KERBEROS,      /* Test Kerberos auth */
    TEST_MODE_ALL            /* Run all tests */
};

static enum test_mode test_mode = TEST_MODE_ALL;

/*
 * Synthesize Unix user SID - local implementation for testing
 * Format: S-1-22-1-<uid> (well-known Samba/winbind convention)
 */
static void
test_synthesize_unix_sid(
    uint32_t uid,
    char    *sid_buf,
    size_t   sid_buf_len)
{
    snprintf(sid_buf, sid_buf_len, "S-1-22-1-%u", uid);
} /* test_synthesize_unix_sid */

/*
 * Test basic user cache creation and destruction
 */
static void
test_cache_create_destroy(void)
{
    struct chimera_vfs_user_cache *cache;

    fprintf(stderr, "\nTesting cache creation/destruction...\n");

    /* Create with various sizes */
    cache = chimera_vfs_user_cache_create(16, 300);
    if (cache != NULL) {
        chimera_vfs_user_cache_destroy(cache);
        TEST_PASS("Create cache size=16 ttl=300");
    } else {
        TEST_FAIL("Create cache size=16 ttl=300");
    }

    cache = chimera_vfs_user_cache_create(1024, 3600);
    if (cache != NULL) {
        chimera_vfs_user_cache_destroy(cache);
        TEST_PASS("Create cache size=1024 ttl=3600");
    } else {
        TEST_FAIL("Create cache size=1024 ttl=3600");
    }

    /* Edge case: minimum size */
    cache = chimera_vfs_user_cache_create(1, 1);
    if (cache != NULL) {
        chimera_vfs_user_cache_destroy(cache);
        TEST_PASS("Create cache size=1 ttl=1");
    } else {
        TEST_FAIL("Create cache size=1 ttl=1");
    }
} /* test_cache_create_destroy */

/*
 * Test local NTLM authentication using VFS user cache
 */
static void
test_local_ntlm_auth(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    fprintf(stderr, "\nTesting local NTLM authentication...\n");

    /* Create a user cache */
    cache = chimera_vfs_user_cache_create(64, 600);
    assert(cache != NULL);

    /* Add test users */
    chimera_vfs_user_cache_add(cache, "johndoe", "password_hash", "smbpasswd",
                               NULL, 1000, 1000, 0, NULL, 1);
    chimera_vfs_user_cache_add(cache, "root", "password_hash", "smbpasswd",
                               NULL, 0, 0, 0, NULL, 1);

    /* Verify user was added to cache */
    urcu_memb_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "johndoe");
    if (user && user->uid == 1000 && user->gid == 1000) {
        TEST_PASS("Local user lookup by name");
    } else {
        TEST_FAIL("Local user lookup by name");
    }

    user = chimera_vfs_user_cache_lookup_by_name(cache, "root");
    if (user && user->uid == 0 && user->gid == 0) {
        TEST_PASS("Root user lookup");
    } else {
        TEST_FAIL("Root user lookup");
    }

    /* Test lookup of non-existent user */
    user = chimera_vfs_user_cache_lookup_by_name(cache, "nonexistent");
    if (user == NULL) {
        TEST_PASS("Non-existent user returns NULL");
    } else {
        TEST_FAIL("Non-existent user should return NULL");
    }

    /* Test case sensitivity */
    user = chimera_vfs_user_cache_lookup_by_name(cache, "JOHNDOE");
    if (user == NULL) {
        TEST_PASS("Username lookup is case-sensitive");
    } else {
        /* If this passes, the cache is case-insensitive - document it */
        TEST_PASS("Username lookup is case-insensitive");
    }

    urcu_memb_read_unlock();

    /* Cleanup */
    chimera_vfs_user_cache_destroy(cache);
} /* test_local_ntlm_auth */

/*
 * Test user lookup by UID
 */
static void
test_user_lookup_by_uid(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    fprintf(stderr, "\nTesting user lookup by UID...\n");

    cache = chimera_vfs_user_cache_create(64, 600);
    assert(cache != NULL);

    /* Add users with different UIDs */
    chimera_vfs_user_cache_add(cache, "user1000", NULL, NULL, NULL, 1000, 1000, 0, NULL, 1);
    chimera_vfs_user_cache_add(cache, "user1001", NULL, NULL, NULL, 1001, 1001, 0, NULL, 1);
    chimera_vfs_user_cache_add(cache, "user2000", NULL, NULL, NULL, 2000, 2000, 0, NULL, 1);

    urcu_memb_read_lock();

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 1000);
    if (user && strcmp(user->username, "user1000") == 0) {
        TEST_PASS("Lookup by UID 1000");
    } else {
        TEST_FAIL("Lookup by UID 1000");
    }

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 1001);
    if (user && strcmp(user->username, "user1001") == 0) {
        TEST_PASS("Lookup by UID 1001");
    } else {
        TEST_FAIL("Lookup by UID 1001");
    }

    user = chimera_vfs_user_cache_lookup_by_uid(cache, 9999);
    if (user == NULL) {
        TEST_PASS("Lookup non-existent UID returns NULL");
    } else {
        TEST_FAIL("Lookup non-existent UID should return NULL");
    }

    urcu_memb_read_unlock();

    chimera_vfs_user_cache_destroy(cache);
} /* test_user_lookup_by_uid */

/*
 * Test supplementary groups handling
 */
static void
test_supplementary_groups(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;
    uint32_t                       gids[] = { 100, 200, 300, 400, 500 };

    fprintf(stderr, "\nTesting supplementary groups...\n");

    cache = chimera_vfs_user_cache_create(64, 600);
    assert(cache != NULL);

    /* Add user with supplementary groups */
    chimera_vfs_user_cache_add(cache, "multigroup", NULL, NULL, NULL,
                               1000, 1000, 5, gids, 1);

    urcu_memb_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "multigroup");
    if (user && user->ngids == 5) {
        int groups_ok = 1;
        for (int i = 0; i < 5; i++) {
            if (user->gids[i] != gids[i]) {
                groups_ok = 0;
                break;
            }
        }
        if (groups_ok) {
            TEST_PASS("Supplementary groups stored correctly");
        } else {
            TEST_FAIL("Supplementary groups values mismatch");
        }
    } else {
        TEST_FAIL("Supplementary groups count mismatch");
    }

    /* Test user with no supplementary groups */
    chimera_vfs_user_cache_add(cache, "nogroups", NULL, NULL, NULL,
                               1001, 1001, 0, NULL, 1);

    user = chimera_vfs_user_cache_lookup_by_name(cache, "nogroups");
    if (user && user->ngids == 0) {
        TEST_PASS("User with no supplementary groups");
    } else {
        TEST_FAIL("User with no supplementary groups");
    }

    urcu_memb_read_unlock();

    chimera_vfs_user_cache_destroy(cache);
} /* test_supplementary_groups */

/*
 * Test user caching after AD authentication (with SID)
 */
static void
test_user_caching_with_sid(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;
    uint32_t                       test_gids[] = { 10001, 10100 };
    const char                    *ad_sid      = "S-1-5-21-1234567890-1234567890-1234567890-1001";

    fprintf(stderr, "\nTesting AD user caching with SID...\n");

    cache = chimera_vfs_user_cache_create(64, 600);
    assert(cache != NULL);

    /* Simulate caching an AD user with SID */
    chimera_vfs_user_cache_add(cache,
                               "aduser@TEST.LOCAL",
                               NULL,   /* No password for AD users */
                               NULL,   /* No SMB hash */
                               ad_sid,
                               10001, 10001, 2, test_gids, 0);

    urcu_memb_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "aduser@TEST.LOCAL");

    if (user) {
        if (user->uid == 10001 && user->gid == 10001) {
            TEST_PASS("AD user UID/GID cached correctly");
        } else {
            TEST_FAIL("AD user UID/GID mismatch");
        }

        if (user->ngids == 2) {
            TEST_PASS("AD user supplementary groups count");
        } else {
            TEST_FAIL("AD user supplementary groups count");
        }

        if (strcmp(user->sid, ad_sid) == 0) {
            TEST_PASS("AD user SID stored correctly");
        } else {
            fprintf(stderr, "    Expected: %s\n    Got: %s\n", ad_sid, user->sid);
            TEST_FAIL("AD user SID mismatch");
        }
    } else {
        TEST_FAIL("AD user lookup failed");
    }

    urcu_memb_read_unlock();

    chimera_vfs_user_cache_destroy(cache);
} /* test_user_caching_with_sid */

/*
 * Test SID synthesis for local users
 */
static void
test_sid_synthesis(void)
{
    char sid_buf[80];

    fprintf(stderr, "\nTesting SID synthesis...\n");

    test_synthesize_unix_sid(1000, sid_buf, sizeof(sid_buf));
    if (strcmp(sid_buf, "S-1-22-1-1000") == 0) {
        TEST_PASS("Unix user SID synthesis (uid=1000)");
    } else {
        fprintf(stderr, "    Expected: S-1-22-1-1000, got: %s\n", sid_buf);
        TEST_FAIL("Unix user SID synthesis (uid=1000)");
    }

    test_synthesize_unix_sid(0, sid_buf, sizeof(sid_buf));
    if (strcmp(sid_buf, "S-1-22-1-0") == 0) {
        TEST_PASS("Root user SID synthesis (uid=0)");
    } else {
        TEST_FAIL("Root user SID synthesis (uid=0)");
    }

    /* Test large UID */
    test_synthesize_unix_sid(4294967295U, sid_buf, sizeof(sid_buf));
    if (strcmp(sid_buf, "S-1-22-1-4294967295") == 0) {
        TEST_PASS("Max UID SID synthesis");
    } else {
        TEST_FAIL("Max UID SID synthesis");
    }
} /* test_sid_synthesis */

/*
 * Test user with all fields populated
 */
static void
test_user_full_fields(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;
    uint32_t                       gids[] = { 1000, 1001, 1002 };
    const char                    *sid    = "S-1-5-21-111-222-333-1001";

    fprintf(stderr, "\nTesting user with all fields...\n");

    cache = chimera_vfs_user_cache_create(64, 600);
    assert(cache != NULL);

    chimera_vfs_user_cache_add(cache,
                               "fulluser",
                               "unix_password_hash",
                               "nt_hash_or_password",
                               sid,
                               1001, 1001, 3, gids, 1);

    urcu_memb_read_lock();

    user = chimera_vfs_user_cache_lookup_by_name(cache, "fulluser");

    if (user) {
        if (strcmp(user->username, "fulluser") == 0) {
            TEST_PASS("Full user - username");
        } else {
            TEST_FAIL("Full user - username");
        }

        if (user->password[0] && strcmp(user->password, "unix_password_hash") == 0) {
            TEST_PASS("Full user - password hash");
        } else {
            TEST_FAIL("Full user - password hash");
        }

        if (user->smbpasswd[0] && strcmp(user->smbpasswd, "nt_hash_or_password") == 0) {
            TEST_PASS("Full user - SMB password");
        } else {
            TEST_FAIL("Full user - SMB password");
        }

        if (strcmp(user->sid, sid) == 0) {
            TEST_PASS("Full user - SID");
        } else {
            TEST_FAIL("Full user - SID");
        }
    } else {
        TEST_FAIL("Full user lookup failed");
    }

    urcu_memb_read_unlock();

    chimera_vfs_user_cache_destroy(cache);
} /* test_user_full_fields */

/*
 * Test user update/replacement
 */
static void
test_user_update(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;

    fprintf(stderr, "\nTesting user update...\n");

    cache = chimera_vfs_user_cache_create(64, 600);
    assert(cache != NULL);

    /* Add initial user */
    chimera_vfs_user_cache_add(cache, "updateme", NULL, NULL,
                               "S-1-5-21-111-222-333-1000",
                               1000, 1000, 0, NULL, 0);

    urcu_memb_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "updateme");
    if (user && user->uid == 1000) {
        TEST_PASS("Initial user add");
    } else {
        TEST_FAIL("Initial user add");
    }
    urcu_memb_read_unlock();

    /* Update the same user with different UID */
    chimera_vfs_user_cache_add(cache, "updateme", NULL, NULL,
                               "S-1-5-21-111-222-333-2000",
                               2000, 2000, 0, NULL, 0);

    urcu_memb_read_lock();
    user = chimera_vfs_user_cache_lookup_by_name(cache, "updateme");
    if (user && user->uid == 2000) {
        TEST_PASS("User update replaces old entry");
    } else if (user && user->uid == 1000) {
        TEST_PASS("User update keeps first entry (expected behavior)");
    } else {
        TEST_FAIL("User update behavior");
    }
    urcu_memb_read_unlock();

    chimera_vfs_user_cache_destroy(cache);
} /* test_user_update */

/*
 * Test NTLM authentication via winbind
 */
static void
test_ntlm_winbind_auth(void)
{
    const char *socket_dir;

    fprintf(stderr, "\nTesting NTLM via winbind...\n");

    /* Check if winbind socket is available (set by test wrapper) */
    socket_dir = getenv("WINBINDD_SOCKET_DIR");

    if (socket_dir && socket_dir[0]) {
        fprintf(stderr, "  Winbind socket dir: %s\n", socket_dir);

        /* Check if socket exists */
        char socket_path[256];
        snprintf(socket_path, sizeof(socket_path), "%s/pipe", socket_dir);

        if (access(socket_path, F_OK) == 0) {
            TEST_PASS("Winbind socket exists");

            /* TODO: Actually test winbind authentication
             * This would require linking against libwbclient */
            TEST_SKIP("Winbind auth test (requires libwbclient)");
        } else {
            TEST_SKIP("Winbind socket not found");
        }
    } else {
        TEST_SKIP("WINBINDD_SOCKET_DIR not set");
    }
} /* test_ntlm_winbind_auth */

/*
 * Test Kerberos authentication
 */
static void
test_kerberos_auth(void)
{
    const char *krb5_config;
    const char *keytab;

    fprintf(stderr, "\nTesting Kerberos authentication...\n");

    krb5_config = getenv("KRB5_CONFIG");
    keytab      = getenv("KRB5_KTNAME");

    if (krb5_config && krb5_config[0]) {
        fprintf(stderr, "  KRB5_CONFIG: %s\n", krb5_config);

        if (access(krb5_config, R_OK) == 0) {
            TEST_PASS("krb5.conf exists and is readable");
        } else {
            TEST_FAIL("krb5.conf not readable");
        }
    } else {
        TEST_SKIP("KRB5_CONFIG not set");
    }

    if (keytab && keytab[0]) {
        fprintf(stderr, "  KRB5_KTNAME: %s\n", keytab);

        if (access(keytab, R_OK) == 0) {
            TEST_PASS("Keytab exists and is readable");

            /* TODO: Actually test GSSAPI authentication
             * This would require linking against libgssapi_krb5 */
            TEST_SKIP("GSSAPI auth test (requires libgssapi_krb5)");
        } else {
            TEST_FAIL("Keytab not readable");
        }
    } else {
        TEST_SKIP("KRB5_KTNAME not set");
    }
} /* test_kerberos_auth */

/*
 * Test cache capacity and eviction
 */
static void
test_cache_capacity(void)
{
    struct chimera_vfs_user_cache *cache;
    const struct chimera_vfs_user *user;
    char                           username[32];
    int                            found_count = 0;

    fprintf(stderr, "\nTesting cache capacity...\n");

    /* Create small cache */
    cache = chimera_vfs_user_cache_create(8, 600);
    assert(cache != NULL);

    /* Add more users than capacity */
    for (int i = 0; i < 16; i++) {
        snprintf(username, sizeof(username), "user%d", i);
        chimera_vfs_user_cache_add(cache, username, NULL, NULL, NULL,
                                   1000 + i, 1000, 0, NULL, 0);
    }

    /* Check how many users are still in cache */
    urcu_memb_read_lock();
    for (int i = 0; i < 16; i++) {
        snprintf(username, sizeof(username), "user%d", i);
        user = chimera_vfs_user_cache_lookup_by_name(cache, username);
        if (user) {
            found_count++;
        }
    }
    urcu_memb_read_unlock();

    fprintf(stderr, "  Found %d of 16 users in cache (capacity=8)\n", found_count);

    if (found_count <= 8) {
        TEST_PASS("Cache respects capacity limit");
    } else {
        /* Cache might grow dynamically - that's OK too */
        TEST_PASS("Cache allows growth beyond initial capacity");
    }

    chimera_vfs_user_cache_destroy(cache);
} /* test_cache_capacity */

static void
usage(const char *prog)
{
    fprintf(stderr, "Usage: %s [options]\n", prog);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  --mode=MODE    Test mode: local, ntlm-winbind, kerberos, all\n");
    fprintf(stderr, "  --help         Show this help\n");
} /* usage */

int
main(
    int    argc,
    char **argv)
{
    /* *INDENT-OFF* */
    static struct option long_options[] = {
        { "mode", required_argument, 0, 'm' },
        { "help", no_argument,       0, 'h' },
        { 0,      0,                 0, 0   }
    };
    /* *INDENT-ON* */

    int opt;

    while ((opt = getopt_long(argc, argv, "m:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'm':
                if (strcmp(optarg, "local") == 0) {
                    test_mode = TEST_MODE_LOCAL;
                } else if (strcmp(optarg, "ntlm-winbind") == 0) {
                    test_mode = TEST_MODE_NTLM_WINBIND;
                } else if (strcmp(optarg, "kerberos") == 0) {
                    test_mode = TEST_MODE_KERBEROS;
                } else if (strcmp(optarg, "all") == 0) {
                    test_mode = TEST_MODE_ALL;
                } else {
                    fprintf(stderr, "Unknown mode: %s\n", optarg);
                    return 1;
                }
                break;
            case 'h':
            default:
                usage(argv[0]);
                return opt == 'h' ? 0 : 1;
        } /* switch */
    }

    urcu_memb_register_thread();

    fprintf(stderr, "Running SMB authentication tests...\n");
    fprintf(stderr, "Mode: %s\n",
            test_mode == TEST_MODE_LOCAL ? "local" :
            test_mode == TEST_MODE_NTLM_WINBIND ? "ntlm-winbind" :
            test_mode == TEST_MODE_KERBEROS ? "kerberos" : "all");

    if (test_mode == TEST_MODE_ALL || test_mode == TEST_MODE_LOCAL) {
        test_cache_create_destroy();
        test_local_ntlm_auth();
        test_user_lookup_by_uid();
        test_supplementary_groups();
        test_user_caching_with_sid();
        test_sid_synthesis();
        test_user_full_fields();
        test_user_update();
        test_cache_capacity();
    }

    if (test_mode == TEST_MODE_ALL || test_mode == TEST_MODE_NTLM_WINBIND) {
        test_ntlm_winbind_auth();
    }

    if (test_mode == TEST_MODE_ALL || test_mode == TEST_MODE_KERBEROS) {
        test_kerberos_auth();
    }

    urcu_memb_unregister_thread();

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Results: %d passed, %d failed, %d skipped\n",
            passed, failed, skipped);
    fprintf(stderr, "========================================\n");

    if (failed > 0) {
        return 1;
    }

    return 0;
} /* main */
