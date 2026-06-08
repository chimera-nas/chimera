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
    uint32_t uid;
    char     sid[CHIMERA_VFS_SID_MAX_LEN];
};

static void
resolve_cb(
    const struct chimera_vfs_user *user,
    void                          *private_data)
{
    struct probe *p = private_data;

    p->found = (user != NULL);
    if (user) {
        p->uid = user->uid;
        snprintf(p->sid, sizeof(p->sid), "%s", user->sid);
    }
    p->done = 1;
} /* resolve_cb */

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

    vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 0, metrics);
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

    /* --- 3. unresolvable key completes (async) with no user --- */
    memset(&p, 0, sizeof(p));
    chimera_vfs_identity_resolve(thread, CHIMERA_VFS_IDENTITY_BY_SID, 0,
                                 "S-1-5-21-9-9-9-9", resolve_cb, &p);
    assert(p.done == 0); /* parked (no winbind handler registered) */
    while (!p.done) {
        evpl_continue(evpl);
    }
    assert(p.found == 0);
    TEST_PASS("unresolvable SID completes with no user");

    chimera_vfs_thread_destroy(thread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All identity resolver tests passed!\n");
    return 0;
} /* main */
