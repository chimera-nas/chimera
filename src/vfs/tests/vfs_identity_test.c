// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Identity resolver: a cache hit resolves synchronously (inline); a miss is
 * resolved off the event loop by a worker (default NSS module) and delivered
 * back on the caller's evpl thread via the doorbell, populating the cache so a
 * subsequent lookup is synchronous; an unresolvable key completes with NULL.
 */

#include <stdio.h>
#include <string.h>
#ifndef _WIN32
#include <pwd.h>
#include <grp.h>
#endif /* ifndef _WIN32 */
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_identity.h"
#include "vfs/vfs_user_cache.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

struct probe {
    int      done;
    int      inline_fired;
    int      found;
    int      is_group;
    uint32_t uid;
    uint32_t gid;
    char     name[256];
    char     sid[CHIMERA_VFS_SID_MAX_LEN];
};

static void
resolve_cb(
    const struct chimera_vfs_identity_result *result,
    void                                     *private_data)
{
    struct probe *p = private_data;

    p->found = (result != NULL);
    if (result) {
        p->is_group = result->is_group;
        if (result->is_group) {
            p->gid = result->group.gid;
            snprintf(p->name, sizeof(p->name), "%s", result->group.groupname);
            snprintf(p->sid, sizeof(p->sid), "%s", result->group.sid);
        } else {
            p->uid = result->user.uid;
            snprintf(p->name, sizeof(p->name), "%s", result->user.username);
            snprintf(p->sid, sizeof(p->sid), "%s", result->user.sid);
        }
    }
    p->done = 1;
} /* resolve_cb */

/*
 * Stands in for the winbind module: registered AFTER the built-in NSS one and,
 * unlike NSS, able to supply a real SID for a numeric key.  Its configuration
 * names the one gid it claims to know, which the test picks to be a gid NSS
 * can ALSO resolve -- that overlap is the whole point, since first-wins would
 * hand the answer to NSS and lose the SID.
 */
#define TEST_WB_GID_SID "S-1-5-21-777-888-999-513"

static uint32_t sid_bearing_target;

static void *
sid_bearing_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
    assert(sscanf(cfgdata, "%u", &sid_bearing_target) == 1);
    return &sid_bearing_target;
} /* sid_bearing_init */

static enum chimera_vfs_identity_status
sid_bearing_lookup(
    void                               *private_data,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    uint32_t target = *(const uint32_t *) private_data;

    (void) name;

    if (key == CHIMERA_VFS_IDENTITY_BY_GID && id == target) {
        out->is_group  = 1;
        out->group.gid = id;
        snprintf(out->group.groupname, sizeof(out->group.groupname), "wbgroup");
        out->group.groupname_len = (int) strlen(out->group.groupname);
        snprintf(out->group.sid, sizeof(out->group.sid), TEST_WB_GID_SID);
        return CHIMERA_VFS_IDENTITY_OK;
    }

    return CHIMERA_VFS_IDENTITY_NOT_MINE;
} /* sid_bearing_lookup */

static struct chimera_vfs_identity_module sid_bearing_module = {
    .sdk_version  = CHIMERA_VFS_IDENTITY_SDK_VERSION,
    .name         = "sidbearing",
    .capabilities = CHIMERA_VFS_IDENTITY_CAP_LOOKUP,
    .init         = sid_bearing_init,
    .lookup       = sid_bearing_lookup,
};

/* A module without CAP_LOOKUP must be skipped by the walk, never called. */
static enum chimera_vfs_identity_status
no_lookup_domain_info(
    void                                    *private_data,
    struct chimera_vfs_identity_domain_info *out)
{
    (void) private_data;
    snprintf(out->netbios_domain, sizeof(out->netbios_domain), "TESTDOM");
    return CHIMERA_VFS_IDENTITY_OK;
} /* no_lookup_domain_info */

static struct chimera_vfs_identity_module domain_only_module = {
    .sdk_version  = CHIMERA_VFS_IDENTITY_SDK_VERSION,
    .name         = "domainonly",
    .capabilities = CHIMERA_VFS_IDENTITY_CAP_DOMAIN_INFO,
    .domain_info  = no_lookup_domain_info,
};

/*
 * Stands in for winbind's principal mapping.  Records the name the engine
 * handed it, so the test can see the normalization, and knows one account.
 * DOWN\x reports the backend unavailable.
 */
#define TEST_PRINCIPAL_UID       5001
#define TEST_PRINCIPAL_SID       "S-1-5-21-111-222-333-5001"
#define TEST_PRINCIPAL_GROUP_SID "S-1-5-21-111-222-333-513"

static char principal_seen[512];
static int  principal_calls;

static enum chimera_vfs_identity_status
principal_lookup(
    void                               *private_data,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    (void) private_data;
    (void) id;

    if (key != CHIMERA_VFS_IDENTITY_BY_PRINCIPAL) {
        return CHIMERA_VFS_IDENTITY_NOT_MINE;
    }

    snprintf(principal_seen, sizeof(principal_seen), "%s", name);
    principal_calls++;

    if (strncmp(name, "DOWN\\", 5) == 0) {
        return CHIMERA_VFS_IDENTITY_UNAVAILABLE;
    }
    if (strcmp(name, "AD\\bob") != 0) {
        return CHIMERA_VFS_IDENTITY_NOT_MINE;
    }

    out->user.uid = TEST_PRINCIPAL_UID;
    out->user.gid = 513;
    snprintf(out->user.username, sizeof(out->user.username), "AD\\bob");
    out->user.username_len = (int) strlen(out->user.username);
    snprintf(out->user.sid, sizeof(out->user.sid), TEST_PRINCIPAL_SID);
    snprintf(out->user.group_sid, sizeof(out->user.group_sid), TEST_PRINCIPAL_GROUP_SID);
    return CHIMERA_VFS_IDENTITY_OK;
} /* principal_lookup */

static struct chimera_vfs_identity_module principal_module = {
    .sdk_version  = CHIMERA_VFS_IDENTITY_SDK_VERSION,
    .name         = "principal",
    .capabilities = CHIMERA_VFS_IDENTITY_CAP_LOOKUP |
        CHIMERA_VFS_IDENTITY_CAP_PRINCIPAL,
    .lookup       = principal_lookup,
};

/* A second principal mapper, for another domain, to route between. */
#define TEST_OTHER_UID 6001

static int                                other_calls;

static enum chimera_vfs_identity_status
other_principal_lookup(
    void                               *private_data,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    (void) private_data;
    (void) id;

    if (key != CHIMERA_VFS_IDENTITY_BY_PRINCIPAL) {
        return CHIMERA_VFS_IDENTITY_NOT_MINE;
    }

    other_calls++;

    if (strcmp(name, "OTHER\\bob") != 0) {
        return CHIMERA_VFS_IDENTITY_NOT_MINE;
    }

    out->user.uid = TEST_OTHER_UID;
    out->user.gid = TEST_OTHER_UID;
    return CHIMERA_VFS_IDENTITY_OK;
} /* other_principal_lookup */

static struct chimera_vfs_identity_module other_principal_module = {
    .sdk_version  = CHIMERA_VFS_IDENTITY_SDK_VERSION,
    .name         = "otherprincipal",
    .capabilities = CHIMERA_VFS_IDENTITY_CAP_LOOKUP |
        CHIMERA_VFS_IDENTITY_CAP_PRINCIPAL,
    .lookup       = other_principal_lookup,
};

#ifdef _WIN32
/* Windows has no NSS provider. Supply deterministic worker results so the
 * same async completion, caching, and SID-precedence checks still run. */
static enum chimera_vfs_identity_status
fixture_identity_lookup(
    void                               *private_data,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    (void) private_data;
    if (key == CHIMERA_VFS_IDENTITY_BY_NAME && !strcmp(name, "root")) {
        out->user.uid = 0;
        out->user.gid = 0;
        snprintf(out->user.username, sizeof(out->user.username), "root");
        out->user.username_len = 4;
        return CHIMERA_VFS_IDENTITY_OK;
    }
    if (key == CHIMERA_VFS_IDENTITY_BY_GID && id <= 1) {
        out->is_group  = 1;
        out->group.gid = id;
        snprintf(out->group.groupname, sizeof(out->group.groupname), "group%u", id);
        out->group.groupname_len = (int) strlen(out->group.groupname);
        return CHIMERA_VFS_IDENTITY_OK;
    }
    return CHIMERA_VFS_IDENTITY_NOT_MINE;
} /* fixture_identity_lookup */

static struct chimera_vfs_identity_module fixture_module = {
    .sdk_version  = CHIMERA_VFS_IDENTITY_SDK_VERSION,
    .name         = "fixture",
    .capabilities = CHIMERA_VFS_IDENTITY_CAP_LOOKUP,
    .lookup       = fixture_identity_lookup,
};
#endif /* ifdef _WIN32 */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_vfs           *vfs;
    struct chimera_vfs_thread    *thread;
    struct evpl                  *evpl;
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    struct probe                  p;

#ifndef _WIN32
    struct passwd                *root_pw;
    struct group                 *root_gr;
    uint32_t                      g;
#endif /* ifndef _WIN32 */
    uint32_t                      root_uid;
    const char                   *root_group_name;
    char                          sidbuf[CHIMERA_VFS_SID_MAX_LEN];
    uint32_t                      scratch_id;
    uint32_t                      wb_gid;

    chimera_log_init();

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs",
            sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv",
            sizeof(module_cfgs[1].module_name) - 1);

    evpl = evpl_create(NULL);
    assert(evpl != NULL);

    vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(vfs != NULL);

    thread = chimera_vfs_thread_init(evpl, vfs);
    assert(thread != NULL);

#ifdef _WIN32
    chimera_vfs_identity_register_module(vfs, &fixture_module, "");
#endif /* ifdef _WIN32 */

    /* A module that only knows domain membership sits in the walk without
     * being asked to resolve anything. */
    chimera_vfs_identity_register_module(vfs, &domain_only_module, "");
    assert(chimera_vfs_identity_has_capability(vfs, CHIMERA_VFS_IDENTITY_CAP_DOMAIN_INFO));

    /* --- 1. cache hit resolves inline (synchronously) --- */
    chimera_vfs_add_user(vfs, "alice", NULL, NULL,
                         "S-1-5-21-111-222-333-1105",
                         4000, 4000, 0, NULL, 1);

    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_UID, 4000,
                                 NULL, resolve_cb, &p);
    /* No evpl pump: a hit must have fired the callback already. */
    assert(p.done == 1);
    assert(p.found == 1);
    assert(p.uid == 4000);
    assert(strcmp(p.sid, "S-1-5-21-111-222-333-1105") == 0);
    TEST_PASS("cache hit resolves synchronously");

    /* Same identity reachable by its real SID (Stage-1 index + resolver). */
    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_SID, 0,
                                 "S-1-5-21-111-222-333-1105", resolve_cb, &p);
    assert(p.done == 1 && p.found == 1 && p.uid == 4000);
    TEST_PASS("cache hit by SID resolves synchronously");

    /* --- 2. miss resolved off-loop by the NSS handler, then cached --- */
#ifdef _WIN32
    root_uid = 0;
#else  /* ifdef _WIN32 */
    root_pw = getpwnam("root");
    assert(root_pw != NULL); /* present on any host/container */
    root_uid = root_pw->pw_uid;
#endif /* ifdef _WIN32 */

    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_NAME, 0,
                                 "root", resolve_cb, &p);
    /* A miss must NOT have completed inline; it is parked on a worker. */
    assert(p.done == 0);

    while (!p.done) {
        evpl_continue(evpl);
    }
    assert(p.found == 1);
    assert(p.uid == root_uid);
    TEST_PASS("miss resolved asynchronously via identity worker");

    /* The worker populated the cache: the same identity is now a sync hit. */
    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_UID,
                                 root_uid, NULL, resolve_cb, &p);
    assert(p.done == 1 && p.found == 1 && p.uid == root_uid);
    TEST_PASS("resolved identity is cached for synchronous reuse");

    /* --- 3. BY_GID resolves to a GROUP record, not a user --- */
#ifdef _WIN32
    root_group_name = "group0";
#else  /* ifdef _WIN32 */
    root_gr = getgrgid(0);
    assert(root_gr != NULL); /* gid 0 exists on any host/container */
    root_group_name = root_gr->gr_name;
#endif /* ifdef _WIN32 */

    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_GID, 0,
                                 NULL, resolve_cb, &p);
    /* A gid was never resolvable before, so this must park on a worker. */
    assert(p.done == 0);
    while (!p.done) {
        evpl_continue(evpl);
    }
    assert(p.found == 1);
    assert(p.is_group == 1);
    assert(p.gid == 0);
    assert(strcmp(p.name, root_group_name) == 0);
    /* NSS supplies no SID, so the caller falls back to the algorithmic idmap. */
    assert(p.sid[0] == '\0');
    TEST_PASS("gid resolves asynchronously to a group record via provider");

    /* The worker populated the GROUP chains: now a synchronous hit. */
    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_GID, 0,
                                 NULL, resolve_cb, &p);
    assert(p.done == 1 && p.found == 1 && p.is_group == 1 && p.gid == 0);
    TEST_PASS("resolved group is cached for synchronous reuse");

    /* A cached group must not be reachable as a user, and vice versa. */
    assert(chimera_vfs_identity_cached(vfs, CHIMERA_VFS_IDENTITY_BY_GID,
                                       0, NULL) == 1);
    assert(chimera_vfs_identity_gid_to_sid(vfs, 4000, sidbuf,
                                           sizeof(sidbuf)) < 0);
    assert(chimera_vfs_identity_sid_to_gid(vfs, "S-1-5-21-111-222-333-1105",
                                           &scratch_id) < 0);
    TEST_PASS("group and user chains stay disjoint");

    /* --- 4. a SID-bearing handler beats a SID-less one on a numeric key ---
     *
     * The field regression: NSS is registered first and, where
     * nsswitch routes group through winbind, it resolves the gid itself and
     * returns a name with no SID.  First-wins would stop there, cache the group
     * SID-less, and -- because the cache hit suppresses any further resolve --
     * pin the marshaller to the algorithmic S-1-5-88 SID permanently.  Pick a
     * gid NSS definitely CAN resolve, so the two handlers genuinely overlap. */
#ifdef _WIN32
    wb_gid = 1;
#else  /* ifdef _WIN32 */
    wb_gid = 0;
    for (g = 1; g < 100; g++) {
        if (getgrgid(g)) {
            wb_gid = g;
            break;
        }
    }
    assert(wb_gid != 0); /* some low non-root group exists on any host */
    assert(getgrgid(wb_gid) != NULL);
#endif /* ifdef _WIN32 */

    {
        char wb_cfg[16];

        snprintf(wb_cfg, sizeof(wb_cfg), "%u", wb_gid);
        chimera_vfs_identity_register_module(vfs, &sid_bearing_module, wb_cfg);
    }

    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_GID, wb_gid,
                                 NULL, resolve_cb, &p);
    assert(p.done == 0);
    while (!p.done) {
        evpl_continue(evpl);
    }
    assert(p.found == 1);
    assert(p.is_group == 1);
    assert(p.gid == wb_gid);
    /* NSS answered too, but without a SID -- the SID-bearing answer must win. */
    assert(strcmp(p.sid, TEST_WB_GID_SID) == 0);
    TEST_PASS("a SID-bearing handler wins over an earlier SID-less one");

    /* And the SID actually reached the cache, so marshalling can find it. */
    assert(chimera_vfs_identity_gid_to_sid(vfs, wb_gid, sidbuf,
                                           sizeof(sidbuf)) > 0);
    assert(strcmp(sidbuf, TEST_WB_GID_SID) == 0);
    assert(chimera_vfs_identity_sid_to_gid(vfs, TEST_WB_GID_SID,
                                           &scratch_id) == 0);
    assert(scratch_id == wb_gid);
    TEST_PASS("the winning SID is what gid_to_sid returns");

    /* --- 5. unresolvable keys complete (async) with no identity --- */
    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_SID, 0,
                                 "S-1-5-21-9-9-9-9", resolve_cb, &p);
    assert(p.done == 0); /* parked (no winbind handler registered) */
    while (!p.done) {
        evpl_continue(evpl);
    }
    assert(p.found == 0);
    TEST_PASS("unresolvable SID completes with no user");

    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_GID,
                                 0x7ffffffe, NULL, resolve_cb, &p);
    assert(p.done == 0);
    while (!p.done) {
        evpl_continue(evpl);
    }
    assert(p.found == 0);
    TEST_PASS("unresolvable gid completes with no group");

    /* --- 6. domain membership comes from the first module that has it --- */
    {
        struct chimera_vfs_identity_domain_info join;

        assert(chimera_vfs_identity_domain_info(vfs, &join) == CHIMERA_VFS_IDENTITY_OK);
        assert(strcmp(join.netbios_domain, "TESTDOM") == 0);
        assert(join.netbios_name[0] == '\0');
        TEST_PASS("domain_info answered by the module that has the capability");
    }

    /* --- 7. principals go only to modules that opted in --- */
    {
        struct chimera_vfs_identity_result r;
        const uint32_t                     principal_caps =
            CHIMERA_VFS_IDENTITY_CAP_LOOKUP | CHIMERA_VFS_IDENTITY_CAP_PRINCIPAL;

        /* NSS knows "root", but a principal is never handed to it: with no
         * principal mapper nothing answers. */
        assert(!chimera_vfs_identity_has_capability(vfs, principal_caps));
        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "root", &r) == CHIMERA_VFS_IDENTITY_NOT_MINE);
        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "root@EXAMPLE.COM", &r) ==
               CHIMERA_VFS_IDENTITY_NOT_MINE);
        TEST_PASS("a principal is never resolved through NSS");

        chimera_vfs_identity_register_module(vfs, &principal_module, "");
        assert(chimera_vfs_identity_has_capability(vfs, principal_caps));

        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "bob@AD", &r) == CHIMERA_VFS_IDENTITY_OK);
        assert(strcmp(principal_seen, "AD\\bob") == 0);
        assert(!r.is_group && r.user.uid == TEST_PRINCIPAL_UID);
        assert(strcmp(r.user.sid, TEST_PRINCIPAL_SID) == 0);
        assert(strcmp(r.user.group_sid, TEST_PRINCIPAL_GROUP_SID) == 0);
        TEST_PASS("user@REALM reaches the module as REALM\\user");

        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "AD\\bob", &r) == CHIMERA_VFS_IDENTITY_OK);
        assert(strcmp(principal_seen, "AD\\bob") == 0);
        TEST_PASS("DOMAIN\\user passes through unchanged");

        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "carol@AD", &r) == CHIMERA_VFS_IDENTITY_NOT_MINE);
        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "bob@DOWN", &r) == CHIMERA_VFS_IDENTITY_UNAVAILABLE);
        TEST_PASS("an unknown principal and a down backend are told apart");

        /* The blocking lookup bypasses the cache: nothing was added. */
        assert(!chimera_vfs_identity_cached(vfs, CHIMERA_VFS_IDENTITY_BY_UID,
                                            TEST_PRINCIPAL_UID, NULL));
        TEST_PASS("the blocking lookup leaves the cache alone");
    }

    /* --- 8. principals are routed by domain, after the realm map --- */
    {
        struct chimera_vfs_identity_result r;

        chimera_vfs_identity_register_module(vfs, &other_principal_module, "");
        chimera_vfs_identity_add_domain(vfs, "principal", "AD");
        chimera_vfs_identity_add_domain(vfs, "principal", "DOWN");
        chimera_vfs_identity_add_domain(vfs, "otherprincipal", "OTHER");
        chimera_vfs_identity_add_realm(vfs, "AD.EXAMPLE.COM", "AD");

        principal_calls = other_calls = 0;
        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "bob@ad.example.com", &r) == CHIMERA_VFS_IDENTITY_OK);
        assert(strcmp(principal_seen, "AD\\bob") == 0);
        assert(r.user.uid == TEST_PRINCIPAL_UID);
        assert(principal_calls == 1 && other_calls == 0);
        TEST_PASS("a mapped realm reaches its domain's module as DOMAIN\\user");

        principal_calls = other_calls = 0;
        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "bob@OTHER", &r) == CHIMERA_VFS_IDENTITY_OK);
        assert(r.user.uid == TEST_OTHER_UID);
        assert(principal_calls == 0 && other_calls == 1);
        TEST_PASS("a principal skips the modules that serve other domains");

        /* The first module would answer AD\bob if it were asked; it is not
         * asked about OTHER\bob at all, so the same name in two domains maps to
         * two accounts. */
        principal_calls = other_calls = 0;
        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "bob@UNROUTED", &r) == CHIMERA_VFS_IDENTITY_NOT_MINE);
        assert(chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                           "bob", &r) == CHIMERA_VFS_IDENTITY_NOT_MINE);
        assert(principal_calls == 0 && other_calls == 0);
        TEST_PASS("a domain no module serves, or none at all, is nobody's");
    }

    chimera_vfs_thread_destroy(thread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All identity resolver tests passed!\n");
    return 0;
} /* main */
