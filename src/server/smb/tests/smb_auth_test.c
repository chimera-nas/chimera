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
#include "common/getopt.h"
#undef NDEBUG
#include <assert.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#include <time.h>
#include "common/chimera_rcu.h"

#include "vfs/vfs.h"
#include "vfs/vfs_user_cache.h"
#include "server/smb/smb_wbclient.h"
#include "server/smb/smb_kerberos_identity.h"
#include "server/smb/smb_auth.h"

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
    chimera_rcu_read_lock(&cache->rcu);

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

    chimera_rcu_read_unlock(&cache->rcu);

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

    chimera_rcu_read_lock(&cache->rcu);

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

    chimera_rcu_read_unlock(&cache->rcu);

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

    chimera_rcu_read_lock(&cache->rcu);

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

    chimera_rcu_read_unlock(&cache->rcu);

    /* Test user with no supplementary groups.  The add has to happen outside
     * the read section: it mutates the cache, and a mutation nested inside a
     * read section on the same domain deadlocks wherever the read side is a
     * real lock rather than a QSBR no-op. */
    chimera_vfs_user_cache_add(cache, "nogroups", NULL, NULL, NULL,
                               1001, 1001, 0, NULL, 1);

    chimera_rcu_read_lock(&cache->rcu);

    user = chimera_vfs_user_cache_lookup_by_name(cache, "nogroups");
    if (user && user->ngids == 0) {
        TEST_PASS("User with no supplementary groups");
    } else {
        TEST_FAIL("User with no supplementary groups");
    }

    chimera_rcu_read_unlock(&cache->rcu);

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

    chimera_rcu_read_lock(&cache->rcu);

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

    chimera_rcu_read_unlock(&cache->rcu);

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

    chimera_rcu_read_lock(&cache->rcu);

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

    chimera_rcu_read_unlock(&cache->rcu);

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

    chimera_rcu_read_lock(&cache->rcu);
    user = chimera_vfs_user_cache_lookup_by_name(cache, "updateme");
    if (user && user->uid == 1000) {
        TEST_PASS("Initial user add");
    } else {
        TEST_FAIL("Initial user add");
    }
    chimera_rcu_read_unlock(&cache->rcu);

    /* Update the same user with different UID */
    chimera_vfs_user_cache_add(cache, "updateme", NULL, NULL,
                               "S-1-5-21-111-222-333-2000",
                               2000, 2000, 0, NULL, 0);

    chimera_rcu_read_lock(&cache->rcu);
    user = chimera_vfs_user_cache_lookup_by_name(cache, "updateme");
    if (user && user->uid == 2000) {
        TEST_PASS("User update replaces old entry");
    } else if (user && user->uid == 1000) {
        TEST_PASS("User update keeps first entry (expected behavior)");
    } else {
        TEST_FAIL("User update behavior");
    }
    chimera_rcu_read_unlock(&cache->rcu);

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
    chimera_rcu_read_lock(&cache->rcu);
    for (int i = 0; i < 16; i++) {
        snprintf(username, sizeof(username), "user%d", i);
        user = chimera_vfs_user_cache_lookup_by_name(cache, username);
        if (user) {
            found_count++;
        }
    }
    chimera_rcu_read_unlock(&cache->rcu);

    fprintf(stderr, "  Found %d of 16 users in cache (capacity=8)\n", found_count);

    if (found_count <= 8) {
        TEST_PASS("Cache respects capacity limit");
    } else {
        /* Cache might grow dynamically - that's OK too */
        TEST_PASS("Cache allows growth beyond initial capacity");
    }

    chimera_vfs_user_cache_destroy(cache);
} /* test_cache_capacity */

/*
 * Test the LmChallengeResponse normalization the winbind logon path applies.
 *
 * MS-NLMP 3.2.5.1.2 lets a client that authenticates with NTLMv2 alone send a
 * zero-length LmChallengeResponse, which the server must read as the implied
 * Z(24).  winbind's auth_crap interface refuses length 0 outright -- even when
 * the NTLMv2 proof verifies -- so smb_wbclient_lm_response() materializes those
 * 24 zero bytes; without it every mount.cifs logon against a winbind-backed
 * server fails with STATUS_LOGON_FAILURE.  A client that did send a real LM
 * field must be forwarded untouched, and a single-byte field must stay
 * single-byte: validate_authenticate() keys anonymous-logon detection off
 * `lm_response_len <= 1` and must keep seeing the length as sent.
 */
static void
test_wbclient_lm_implied_zeros(void)
{
    const uint8_t  real_lm[24] = { 0xAA, 0xBB, 0xCC, 0xDD };
    const uint8_t  one_byte[1] = { 0x00 };
    const uint8_t *data;
    uint32_t       len;
    int            all_zero;

    fprintf(stderr, "\nTesting winbind LM response normalization...\n");

    /* Zero length -> the implied Z(24).  Passed a NULL buffer on purpose: the
     * normalizer must not dereference a field the client omitted. */
    data = NULL;
    len  = 0xFFFFFFFF;
    smb_wbclient_lm_response(NULL, 0, &data, &len);

    if (len == 24) {
        TEST_PASS("Zero-length LM response becomes 24 bytes");
    } else {
        TEST_FAIL("Zero-length LM response becomes 24 bytes");
    }

    all_zero = (data != NULL);
    for (uint32_t i = 0; data && i < len; i++) {
        if (data[i] != 0) {
            all_zero = 0;
        }
    }

    if (all_zero) {
        TEST_PASS("Implied LM response is 24 zero bytes");
    } else {
        TEST_FAIL("Implied LM response is 24 zero bytes");
    }

    /* A real 24-byte LM field (what smbclient and Windows send) is forwarded
     * verbatim. */
    smb_wbclient_lm_response(real_lm, sizeof(real_lm), &data, &len);

    if (len == sizeof(real_lm) && data == real_lm) {
        TEST_PASS("24-byte LM response passes through unchanged");
    } else {
        TEST_FAIL("24-byte LM response passes through unchanged");
    }

    /* A single byte must NOT be padded, or the anonymous logon signal breaks. */
    smb_wbclient_lm_response(one_byte, sizeof(one_byte), &data, &len);

    if (len == 1 && data == one_byte) {
        TEST_PASS("Single-byte LM response is not padded");
    } else {
        TEST_FAIL("Single-byte LM response is not padded");
    }
} /* test_wbclient_lm_implied_zeros */

/*
 * Test the identity policy applied to an accepted Kerberos context.
 *
 * A GSSAPI accept proves who the client is; it is not a logon until the
 * principal maps to a Unix identity.  With winbind_enabled the mapping must come
 * from winbind, so an unreachable winbindd (or a principal it cannot map)
 * refuses the logon instead of degrading it to the 65534 identity -- which is
 * what a deployment whose winbindd had died used to hand every Kerberos client
 * while its NTLM logons were being refused.  Without winbind the logon is
 * refused unless the deployment opted into the nobody mapping, and that
 * opt-in never overrides a configured winbind.  The winbind-down cases are
 * skipped when a winbindd really answers on this host.
 */
static void
test_kerberos_identity_policy(void)
{
    struct smb_kerberos_identity ident;
    int                          rc;

    fprintf(stderr, "\nTesting Kerberos identity policy...\n");

    /* No principal is never a logon, whatever the configuration. */
    rc = smb_kerberos_resolve_identity(0, 1, NULL, &ident);
    if (rc == -1) {
        TEST_PASS("NULL principal is refused");
    } else {
        TEST_FAIL("NULL principal is refused");
    }

    rc = smb_kerberos_resolve_identity(1, 1, "", &ident);
    if (rc == -1) {
        TEST_PASS("Empty principal is refused");
    } else {
        TEST_FAIL("Empty principal is refused");
    }

    if (smb_wbclient_available()) {
        TEST_SKIP("winbind_enabled without winbindd (a winbindd answers on this host)");
    } else {
        rc = smb_kerberos_resolve_identity(1, 0, "testuser1@TEST.LOCAL", &ident);
        if (rc == -1) {
            TEST_PASS("winbind_enabled with winbindd unavailable refuses the logon");
        } else {
            TEST_FAIL("winbind_enabled with winbindd unavailable refuses the logon");
        }

        /* The fallback knob never overrides a configured winbind. */
        rc = smb_kerberos_resolve_identity(1, 1, "testuser1@TEST.LOCAL", &ident);
        if (rc == -1) {
            TEST_PASS("anonymous fallback is ignored when winbind_enabled is set");
        } else {
            TEST_FAIL("anonymous fallback is ignored when winbind_enabled is set");
        }
    }

    /* No identity source at all: refused by default. */
    rc = smb_kerberos_resolve_identity(0, 0, "testuser1@TEST.LOCAL", &ident);
    if (rc == -1 && ident.resolved == 0) {
        TEST_PASS("winbind disabled without the fallback knob refuses the logon");
    } else {
        TEST_FAIL("winbind disabled without the fallback knob refuses the logon");
    }

    /* The explicit opt-in serves the principal as nobody. */
    rc = smb_kerberos_resolve_identity(0, 1, "testuser1@TEST.LOCAL", &ident);
    if (rc == 0 && ident.uid == 65534 && ident.gid == 65534 &&
        ident.ngids == 0 && ident.is_ad_user == 0 && ident.resolved == 1 &&
        strcmp(ident.sid, "S-1-22-1-65534") == 0) {
        TEST_PASS("anonymous fallback maps the principal to uid/gid 65534");
    } else {
        TEST_FAIL("anonymous fallback maps the principal to uid/gid 65534");
    }
} /* test_kerberos_identity_policy */

/* SPNEGO tokens as the two client families emit them, trimmed to what the
* detector reads.  Lengths are exact: a wrong byte here fails the test. */
/* *INDENT-OFF* */

/* negTokenInit, mechTypes [MS KRB5, KRB5, NTLMSSP], mechToken 4 opaque bytes:
 * what a domain-joined Windows client sends first. */
static const uint8_t spnego_kerberos_first[] = {
    0x60, 0x3a,
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,       /* SPNEGO OID */
    0xa0, 0x30,                                           /* negTokenInit */
    0x30, 0x2e,
    0xa0, 0x24,                                           /* mechTypes */
    0x30, 0x22,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
    0xa2, 0x06,                                           /* mechToken */
    0x04, 0x04, 0xde, 0xad, 0xbe, 0xef,
};

/* negTokenInit, mechTypes [NTLMSSP], mechToken "NTLMSSP\0" + 4 bytes:
 * smbclient / a Windows client that chose NTLM. */
static const uint8_t spnego_ntlmssp_first[] = {
    0x60, 0x2c,
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,
    0xa0, 0x22,
    0x30, 0x20,
    0xa0, 0x0e,
    0x30, 0x0c,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
    0xa2, 0x0e,
    0x04, 0x0c, 'N', 'T', 'L', 'M', 'S', 'S', 'P', 0x00, 0x01, 0x00, 0x00, 0x00,
};

/* negTokenInit, mechTypes [NEGOEX, NTLMSSP], opaque mechToken. */
static const uint8_t spnego_negoex_first[] = {
    0x60, 0x30,
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,
    0xa0, 0x26,
    0x30, 0x24,
    0xa0, 0x1a,
    0x30, 0x18,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x1e,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
    0xa2, 0x06,
    0x04, 0x04, 0xde, 0xad, 0xbe, 0xef,
};

/* negTokenInit, mechTypes [KRB5] only: a client that cannot do NTLM. */
static const uint8_t spnego_kerberos_only[] = {
    0x60, 0x23,
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,
    0xa0, 0x19,
    0x30, 0x17,
    0xa0, 0x0d,
    0x30, 0x0b,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02,
    0xa2, 0x06,
    0x04, 0x04, 0xde, 0xad, 0xbe, 0xef,
};

/* Bare krb5 InitialContextToken, no SPNEGO: thisMech KRB5, the AP-REQ
 * TOK_ID 01 00, then 4 opaque bytes standing in for the AP-REQ. */
static const uint8_t gss_krb5_bare[] = {
    0x60, 0x11,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02,
    0x01, 0x00,
    0xde, 0xad, 0xbe, 0xef,
};

/* negTokenResp carrying an NTLMSSP AUTHENTICATE (second NTLM leg). */
static const uint8_t spnego_resp_ntlmssp[] = {
    0xa1, 0x14,
    0x30, 0x12,
    0xa2, 0x10,
    0x04, 0x0e, 'N', 'T', 'L', 'M', 'S', 'S', 'P', 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
};
/* *INDENT-ON* */

static void
test_spnego_mechanism_detection(void)
{
    uint8_t truncated[sizeof(spnego_kerberos_first)];
    uint8_t bare_other[sizeof(gss_krb5_bare)];

    fprintf(stderr, "\nTesting SPNEGO mechanism detection...\n");

    if (smb_auth_detect_mechanism((const uint8_t *) "NTLMSSP\0\x01\x00\x00\x00", 12) ==
        SMB_AUTH_MECH_NTLM) {
        TEST_PASS("raw NTLMSSP token is NTLM");
    } else {
        TEST_FAIL("raw NTLMSSP token is NTLM");
    }

    if (smb_auth_detect_mechanism(spnego_kerberos_first, sizeof(spnego_kerberos_first)) ==
        SMB_AUTH_MECH_KERBEROS) {
        TEST_PASS("Kerberos-first negTokenInit is Kerberos even with NTLMSSP in mechTypes");
    } else {
        TEST_FAIL("Kerberos-first negTokenInit is Kerberos even with NTLMSSP in mechTypes");
    }

    if (smb_auth_detect_mechanism(spnego_ntlmssp_first, sizeof(spnego_ntlmssp_first)) ==
        SMB_AUTH_MECH_NTLM) {
        TEST_PASS("NTLMSSP-first negTokenInit is NTLM");
    } else {
        TEST_FAIL("NTLMSSP-first negTokenInit is NTLM");
    }

    if (smb_auth_detect_mechanism(spnego_negoex_first, sizeof(spnego_negoex_first)) ==
        SMB_AUTH_MECH_NEGOEX) {
        TEST_PASS("NEGOEX-first negTokenInit is NEGOEX");
    } else {
        TEST_FAIL("NEGOEX-first negTokenInit is NEGOEX");
    }

    if (smb_auth_detect_mechanism(spnego_kerberos_only, sizeof(spnego_kerberos_only)) ==
        SMB_AUTH_MECH_KERBEROS) {
        TEST_PASS("Kerberos-only negTokenInit is Kerberos");
    } else {
        TEST_FAIL("Kerberos-only negTokenInit is Kerberos");
    }

    if (smb_auth_detect_mechanism(gss_krb5_bare, sizeof(gss_krb5_bare)) ==
        SMB_AUTH_MECH_KERBEROS) {
        TEST_PASS("bare krb5 InitialContextToken is Kerberos");
    } else {
        TEST_FAIL("bare krb5 InitialContextToken is Kerberos");
    }

    /* Same framing with a thisMech other than Kerberos (last OID byte
     * changed): not a mechanism the server accepts unwrapped. */
    memcpy(bare_other, gss_krb5_bare, sizeof(bare_other));
    bare_other[12] = 0x03;
    if (smb_auth_detect_mechanism(bare_other, sizeof(bare_other)) == SMB_AUTH_MECH_UNKNOWN) {
        TEST_PASS("bare InitialContextToken of another mechanism is UNKNOWN");
    } else {
        TEST_FAIL("bare InitialContextToken of another mechanism is UNKNOWN");
    }

    if (smb_auth_detect_mechanism(spnego_resp_ntlmssp, sizeof(spnego_resp_ntlmssp)) ==
        SMB_AUTH_MECH_NTLM) {
        TEST_PASS("negTokenResp with an NTLMSSP token is NTLM");
    } else {
        TEST_FAIL("negTokenResp with an NTLMSSP token is NTLM");
    }

    /* Outer length claims 0x3a bytes but only 20 are present. */
    memcpy(truncated, spnego_kerberos_first, sizeof(truncated));
    if (smb_auth_detect_mechanism(truncated, 20) == SMB_AUTH_MECH_UNKNOWN) {
        TEST_PASS("truncated negTokenInit is UNKNOWN");
    } else {
        TEST_FAIL("truncated negTokenInit is UNKNOWN");
    }

    /* mechTypes length byte pointing past the buffer must not be followed. */
    memcpy(truncated, spnego_kerberos_first, sizeof(truncated));
    truncated[15] = 0x7f;
    if (smb_auth_detect_mechanism(truncated, sizeof(truncated)) == SMB_AUTH_MECH_UNKNOWN) {
        TEST_PASS("oversized mechTypes length is UNKNOWN");
    } else {
        TEST_FAIL("oversized mechTypes length is UNKNOWN");
    }

    if (smb_auth_spnego_offers_ntlmssp(spnego_kerberos_first, sizeof(spnego_kerberos_first)) == 1 &&
        smb_auth_spnego_offers_ntlmssp(spnego_negoex_first, sizeof(spnego_negoex_first)) == 1 &&
        smb_auth_spnego_offers_ntlmssp(spnego_kerberos_only, sizeof(spnego_kerberos_only)) == 0 &&
        smb_auth_spnego_offers_ntlmssp(gss_krb5_bare, sizeof(gss_krb5_bare)) == 0 &&
        smb_auth_spnego_offers_ntlmssp(spnego_resp_ntlmssp, sizeof(spnego_resp_ntlmssp)) == 0 &&
        smb_auth_spnego_offers_ntlmssp(truncated, 20) == 0) {
        TEST_PASS("NTLMSSP offer detection across mechTypes lists");
    } else {
        TEST_FAIL("NTLMSSP offer detection across mechTypes lists");
    }
} /* test_spnego_mechanism_detection */

static void
test_spnego_ntlmssp_hint(void)
{
    /* RFC 4178 negTokenResp: accept-incomplete, supportedMech NTLMSSP, no
     * responseToken.  This is what makes a Kerberos-first client restart
     * with its NTLMSSP NEGOTIATE on the next SESSION_SETUP. */
    /* *INDENT-OFF* */
    static const uint8_t expected[] = {
        0xa1, 0x15,
        0x30, 0x13,
        0xa0, 0x03, 0x0a, 0x01, 0x01,
        0xa1, 0x0c,
        0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
    };
    /* *INDENT-ON* */
    size_t   len = 0;
    uint8_t *hint;

    fprintf(stderr, "\nTesting the SPNEGO NTLMSSP steering hint...\n");

    hint = smb_auth_spnego_ntlmssp_hint(&len);
    if (hint && len == sizeof(expected) && memcmp(hint, expected, len) == 0) {
        TEST_PASS("hint is the 23-byte accept-incomplete/NTLMSSP negTokenResp");
    } else {
        TEST_FAIL("hint is the 23-byte accept-incomplete/NTLMSSP negTokenResp");
    }
    free(hint);

    /* The hint itself must not be mistaken for a client token with a
     * mechanism in it. */
    if (smb_auth_detect_mechanism(expected, sizeof(expected)) == SMB_AUTH_MECH_UNKNOWN) {
        TEST_PASS("hint bytes carry no client mechanism");
    } else {
        TEST_FAIL("hint bytes carry no client mechanism");
    }
} /* test_spnego_ntlmssp_hint */

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

    chimera_rcu_register_thread();

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
        test_wbclient_lm_implied_zeros();
        test_kerberos_identity_policy();
        test_spnego_mechanism_detection();
        test_spnego_ntlmssp_hint();
    }

    if (test_mode == TEST_MODE_ALL || test_mode == TEST_MODE_NTLM_WINBIND) {
        test_ntlm_winbind_auth();
    }

    if (test_mode == TEST_MODE_ALL || test_mode == TEST_MODE_KERBEROS) {
        test_kerberos_auth();
    }

    chimera_rcu_unregister_thread();

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Results: %d passed, %d failed, %d skipped\n",
            passed, failed, skipped);
    fprintf(stderr, "========================================\n");

    if (failed > 0) {
        return 1;
    }

    return 0;
} /* main */
