// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Identity resolver: a cache hit resolves synchronously (inline); a miss is
 * resolved off the event loop by a worker (default NSS handler) and delivered
 * back on the caller's evpl thread via the doorbell, populating the cache so a
 * subsequent lookup is synchronous; an unresolvable key completes with NULL.
 */

#include <stdio.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>
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
 * Stands in for the winbind handler: registered AFTER the built-in NSS one and,
 * unlike NSS, able to supply a real SID for a numeric key.  `private_data`
 * points at the one gid it claims to know, which the test picks to be a gid NSS
 * can ALSO resolve -- that overlap is the whole point, since first-wins would
 * hand the answer to NSS and lose the SID.
 */
#define TEST_WB_GID_SID "S-1-5-21-777-888-999-513"

static int
sid_bearing_handler(
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out,
    void                               *private_data)
{
    uint32_t target = *(const uint32_t *) private_data;

    (void) name;

    if (key == CHIMERA_VFS_IDENTITY_BY_GID && id == target) {
        out->is_group  = 1;
        out->group.gid = id;
        snprintf(out->group.groupname, sizeof(out->group.groupname), "wbgroup");
        out->group.groupname_len = (int) strlen(out->group.groupname);
        snprintf(out->group.sid, sizeof(out->group.sid), TEST_WB_GID_SID);
        return 0;
    }

    return -1;
} /* sid_bearing_handler */

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
    struct passwd                *root_pw;
    struct group                 *root_gr;
    char                          sidbuf[CHIMERA_VFS_SID_MAX_LEN];
    uint32_t                      scratch_id;
    uint32_t                      wb_gid;
    uint32_t                      g;

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
    root_pw = getpwnam("root");
    assert(root_pw != NULL); /* present on any host/container */

    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_NAME, 0,
                                 "root", resolve_cb, &p);
    /* A miss must NOT have completed inline; it is parked on a worker. */
    assert(p.done == 0);

    while (!p.done) {
        evpl_continue(evpl);
    }
    assert(p.found == 1);
    assert(p.uid == root_pw->pw_uid);
    TEST_PASS("miss resolved asynchronously via NSS worker");

    /* The worker populated the cache: the same identity is now a sync hit. */
    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_UID,
                                 root_pw->pw_uid, NULL, resolve_cb, &p);
    assert(p.done == 1 && p.found == 1 && p.uid == root_pw->pw_uid);
    TEST_PASS("resolved identity is cached for synchronous reuse");

    /* --- 3. BY_GID resolves to a GROUP record, not a user --- */
    root_gr = getgrgid(0);
    assert(root_gr != NULL); /* gid 0 exists on any host/container */

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
    assert(strcmp(p.name, root_gr->gr_name) == 0);
    /* NSS supplies no SID, so the caller falls back to the algorithmic idmap. */
    assert(p.sid[0] == '\0');
    TEST_PASS("gid resolves asynchronously to a group record via NSS");

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
    wb_gid = 0;
    for (g = 1; g < 100; g++) {
        if (getgrgid(g)) {
            wb_gid = g;
            break;
        }
    }
    assert(wb_gid != 0); /* some low non-root group exists on any host */
    assert(getgrgid(wb_gid) != NULL);

    chimera_vfs_identity_register_handler(vfs, sid_bearing_handler, &wb_gid);

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

    chimera_vfs_thread_destroy(thread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All identity resolver tests passed!\n");
    return 0;
} /* main */
