// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Fleet-wide client recovery records against a real (memkv) KV store shared
 * by several "nodes": each node reloads only the records it owns, migrates its
 * legacy per-node band, deletes only what it still owns, and imports a peer's
 * records only once that peer has stopped heartbeating.
 */

#include "common/thread.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "common/logging.h"
#include "prometheus-c.h"
#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "nfs_common.h"
#include "nfs4_session.h"
#include "nfs4_recovery.h"
#include "nfs_kv_keys.h"

#define CHECK(cond) do { \
            if (!(cond)) { \
                fprintf(stderr, "%s:%d: CHECK failed: %s\n", \
                        __FILE__, __LINE__, #cond); \
                abort(); \
            } \
} while (0)

/* Referenced from nfs4_callback.c (linked in); unused here. */
void
chimera_nfs4_open_resume_after_probe(struct nfs_request *req)
{
    (void) req;
} /* chimera_nfs4_open_resume_after_probe */

/* ------------------------------------------------------------------ *
*  synchronous KV helpers over a private loop                        *
* ------------------------------------------------------------------ */

struct kv_env {
    struct prometheus_metrics *metrics;
    struct chimera_vfs        *vfs;
    struct evpl               *evpl;
    struct chimera_vfs_thread *vfs_thread;
};

struct kv_wait {
    bool     done;
    int      error;
    uint8_t  value[2048];
    uint32_t value_len;
};

static void
kv_env_init(struct kv_env *env)
{
    struct chimera_vfs_module_cfg cfg;

    env->metrics = prometheus_metrics_create(NULL, NULL, 0);
    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.module_name, "memkv", sizeof(cfg.module_name) - 1);
    env->vfs = chimera_vfs_init(1, 0, &cfg, 1, "memkv", 60, 1, 1, 0,
                                env->metrics);
    CHECK(env->vfs != NULL);
    env->evpl       = evpl_create(NULL);
    env->vfs_thread = chimera_vfs_thread_init(env->evpl, env->vfs);
} /* kv_env_init */

static void
kv_env_destroy(struct kv_env *env)
{
    chimera_vfs_thread_destroy(env->vfs_thread);
    evpl_destroy(env->evpl);
    chimera_vfs_destroy(env->vfs);
    prometheus_metrics_destroy(env->metrics);
} /* kv_env_destroy */

static void
kv_wait_loop(
    struct kv_env  *env,
    struct kv_wait *w)
{
    while (!w->done) {
        evpl_continue(env->evpl);
    }
} /* kv_wait_loop */

static void
kv_op_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct kv_wait *w = private_data;

    w->error = error_code;
    w->done  = true;
} /* kv_op_done */

static void
kv_get_done(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct kv_wait *w = private_data;

    w->error     = error_code;
    w->value_len = 0;
    if (error_code == CHIMERA_VFS_OK && value && value_len <= sizeof(w->value)) {
        memcpy(w->value, value, value_len);
        w->value_len = value_len;
    }
    w->done = true;
} /* kv_get_done */

static void
kv_put(
    struct kv_env *env,
    const void    *key,
    uint32_t       key_len,
    const void    *value,
    uint32_t       value_len)
{
    struct kv_wait w = { 0 };

    chimera_vfs_put_key(env->vfs_thread, key, key_len, value, value_len,
                        kv_op_done, &w);
    kv_wait_loop(env, &w);
    CHECK(w.error == CHIMERA_VFS_OK);
} /* kv_put */

/* Returns the record's owning node, or -1 when there is no record. */
static int
kv_client_owner(
    struct kv_env *env,
    const char    *owner)
{
    struct nfs_recovery_record r;
    struct kv_wait             w = { 0 };
    uint8_t                    key[CHIMERA_KV_NFS_KEY_MAX];
    uint32_t                   key_len;

    key_len = nfs_kv_client_key(key, owner, strlen(owner));
    chimera_vfs_get_key(env->vfs_thread, key, key_len, kv_get_done, &w);
    kv_wait_loop(env, &w);
    if (w.error != CHIMERA_VFS_OK || w.value_len == 0) {
        return -1;
    }
    CHECK(nfs_recovery_deserialize(w.value, w.value_len, &r, 0) == 0);
    return r.owner_node;
} /* kv_client_owner */

/* Fire-and-forget deletes have no completion to wait on: poll, in KV
 * round-trips, until `owner`'s record is gone, then give any delete issued
 * alongside it a few more round-trips to land. */
static void
kv_wait_gone(
    struct kv_env *env,
    const char    *owner)
{
    int i;

    for (i = 0; i < 1000 && kv_client_owner(env, owner) != -1; i++) {
    }
    CHECK(i < 1000);
    for (i = 0; i < 10; i++) {
        (void) kv_client_owner(env, owner);
    }
} /* kv_wait_gone */

static bool
kv_legacy_exists(
    struct kv_env *env,
    uint16_t       node,
    const char    *owner)
{
    struct kv_wait w = { 0 };
    uint8_t        key[CHIMERA_KV_NFS_KEY_MAX];
    uint32_t       key_len;

    key_len = nfs_kv_recovery_key(key, node, (const uint8_t *) owner,
                                  strlen(owner));
    chimera_vfs_get_key(env->vfs_thread, key, key_len, kv_get_done, &w);
    kv_wait_loop(env, &w);
    return w.error == CHIMERA_VFS_OK && w.value_len > 0;
} /* kv_legacy_exists */

static void
seed_client(
    struct kv_env *env,
    const char    *owner,
    uint16_t       owner_node)
{
    struct nfs_client c;
    uint8_t           key[CHIMERA_KV_NFS_KEY_MAX];
    uint8_t           value[2048];
    uint32_t          key_len, value_len;

    memset(&c, 0, sizeof(c));
    c.client_id     = 0x100 + owner_node;
    c.verifier      = 0x77;
    c.owner_len     = strlen(owner);
    c.last_touch_ns = 0;
    memcpy(c.owner_string, owner, c.owner_len);

    key_len   = nfs_kv_client_key(key, owner, c.owner_len);
    value_len = nfs_recovery_serialize(value, sizeof(value), &c, owner_node);
    kv_put(env, key, key_len, value, value_len);
} /* seed_client */

/* An NFR2 value under the legacy per-node key, as an older build wrote it. */
static void
seed_legacy(
    struct kv_env *env,
    uint16_t       node,
    const char    *owner)
{
    uint8_t  key[CHIMERA_KV_NFS_KEY_MAX];
    uint8_t  value[256];
    uint32_t key_len, p = 0;
    uint16_t owner_len = strlen(owner);

    nfs_kv_put_le32(value, &p, 0x3252464Eu); /* "NFR2" */
    nfs_kv_put_le64(value, &p, 0x11);
    nfs_kv_put_le64(value, &p, 0x22);
    nfs_kv_put_le64(value, &p, 0x33);
    nfs_kv_put_le64(value, &p, 0);           /* unstamped: never stale */
    value[p++] = owner_len & 0xff;
    value[p++] = owner_len >> 8;
    memcpy(value + p, owner, owner_len);
    p += owner_len;

    key_len = nfs_kv_recovery_key(key, node, (const uint8_t *) owner, owner_len);
    kv_put(env, key, key_len, value, p);
} /* seed_legacy */

/* Write `node`'s epoch record with a heartbeat `age_s` seconds old. */
static void
seed_epoch_boot(
    struct kv_env *env,
    uint16_t       node,
    uint64_t       age_s,
    uint64_t       boot_id)
{
    struct timespec ts;
    uint8_t         key[CHIMERA_KV_PREFIX_LEN];
    uint8_t         value[32];
    uint32_t        key_len, p = 0;
    uint64_t        now;

    clock_gettime(CLOCK_REALTIME, &ts);
    now = (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;

    nfs_kv_put_le32(value, &p, 0x31504645u); /* "EPF1" */
    nfs_kv_put_le64(value, &p, boot_id);
    nfs_kv_put_le64(value, &p, now - age_s * 1000000000ULL);
    key_len = nfs_kv_epoch_key(key, node);
    kv_put(env, key, key_len, value, p);
} /* seed_epoch_boot */

static void
seed_epoch(
    struct kv_env *env,
    uint16_t       node,
    uint64_t       age_s)
{
    seed_epoch_boot(env, node, age_s, 5);
} /* seed_epoch */

static bool
in_reclaim_set(
    struct nfs_recovery *rec,
    const char          *owner,
    int                 *owner_node)
{
    struct nfs_recovery_record *r;

    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, owner, strlen(owner), r);
    if (r && owner_node) {
        *owner_node = r->owner_node;
    }
    evpl_mutex_unlock(&rec->lock);
    return r != NULL;
} /* in_reclaim_set */

/* A node backed by the shared store (memkv is forced persistent). */
static void
node_start(
    struct nfs_recovery *rec,
    struct kv_env       *env,
    uint16_t             node)
{
    CHECK(nfs_recovery_load(rec, env->vfs, node, 90, 180, true) == 0);
    rec->persistence_disabled = false;
    nfs_recovery_cold_start(rec);
    CHECK(!nfs_recovery_loading(rec));
} /* node_start */

/* ------------------------------------------------------------------ *
*  tests                                                             *
* ------------------------------------------------------------------ */

/* Each node reloads exactly its own clients out of the shared band, and moves
 * its legacy per-node records into it. */
static void
test_cold_start_filters_and_migrates(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;
    int                 owner_node = -1;

    kv_env_init(&env);
    seed_client(&env, "own_client", 1);
    seed_client(&env, "peer_client", 2);
    seed_legacy(&env, 1, "legacy_client");
    seed_legacy(&env, 2, "peer_legacy_client");

    node_start(&rec, &env, 1);

    CHECK(in_reclaim_set(&rec, "own_client", &owner_node) && owner_node == 1);
    CHECK(in_reclaim_set(&rec, "legacy_client", &owner_node) && owner_node == 1);
    CHECK(!in_reclaim_set(&rec, "peer_client", NULL));
    CHECK(!in_reclaim_set(&rec, "peer_legacy_client", NULL));
    CHECK(rec.pending_reclaim == 2);
    CHECK(rec.in_grace);

    /* Migrated: the fleet record names node 1, the legacy key is gone.  The
     * peer's legacy band is not this node's to touch. */
    CHECK(kv_client_owner(&env, "legacy_client") == 1);
    CHECK(!kv_legacy_exists(&env, 1, "legacy_client"));
    CHECK(kv_legacy_exists(&env, 2, "peer_legacy_client"));

    nfs_recovery_free(&rec);

    /* The next boot finds the migrated record on the fleet band alone. */
    node_start(&rec, &env, 1);
    CHECK(in_reclaim_set(&rec, "legacy_client", NULL));
    CHECK(rec.pending_reclaim == 2);
    nfs_recovery_free(&rec);

    kv_env_destroy(&env);
    printf("ok: cold_start_filters_and_migrates\n");
} /* test_cold_start_filters_and_migrates */

/* A node deletes a client's record only while it still owns it. */
static void
test_forget_only_owned(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;

    kv_env_init(&env);
    seed_client(&env, "mine", 1);
    seed_client(&env, "moved_away", 2);

    node_start(&rec, &env, 1);
    nfs_recovery_forget(env.vfs_thread, &rec, "moved_away", 10);
    nfs_recovery_forget(env.vfs_thread, &rec, "mine", 4);

    kv_wait_gone(&env, "mine");
    CHECK(kv_client_owner(&env, "moved_away") == 2);
    CHECK(!in_reclaim_set(&rec, "mine", NULL));

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: forget_only_owned\n");
} /* test_forget_only_owned */

struct import_wait {
    bool     done;
    enum nfs_recovery_failover result;
    uint64_t observed_ns;
};

static void
import_done(
    enum nfs_recovery_failover result,
    uint64_t                   observed_ns,
    void                      *arg)
{
    struct import_wait *w = arg;

    w->result      = result;
    w->observed_ns = observed_ns;
    w->done        = true;
} /* import_done */

/* *seen is the heartbeat observed by the previous call; updated on return. */
static enum nfs_recovery_failover
import_seen(
    struct kv_env       *env,
    struct nfs_recovery *rec,
    uint16_t             peer,
    const char          *owner,
    uint64_t            *seen)
{
    struct import_wait w = { 0 };

    nfs_recovery_import_peer(env->vfs_thread, rec, peer, owner, strlen(owner),
                             *seen, import_done, &w);
    while (!w.done) {
        evpl_continue(env->evpl);
    }
    if (!*seen) {
        *seen = w.observed_ns;
    }
    return w.result;
} /* import_seen */

static enum nfs_recovery_failover
import(
    struct kv_env       *env,
    struct nfs_recovery *rec,
    uint16_t             peer,
    const char          *owner)
{
    uint64_t seen = 0;

    return import_seen(env, rec, peer, owner, &seen);
} /* import */

struct lookup_wait {
    bool     done;
    enum nfs_recovery_owner result;
    uint16_t node;
};

static void
lookup_done(
    enum nfs_recovery_owner result,
    uint16_t                node,
    void                   *arg)
{
    struct lookup_wait *w = arg;

    w->result = result;
    w->node   = node;
    w->done   = true;
} /* lookup_done */

static struct lookup_wait
lookup(
    struct kv_env       *env,
    struct nfs_recovery *rec,
    const char          *owner)
{
    struct lookup_wait w = { 0 };

    nfs_recovery_lookup_owner(env->vfs_thread, rec, owner, strlen(owner),
                              lookup_done, &w);
    while (!w.done) {
        evpl_continue(env->evpl);
    }
    return w;
} /* lookup */

/* A dead peer's clients are imported (once) and opened a window; a live
 * peer's are left alone. */
static void
test_import_only_dead_peer(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;
    struct lookup_wait  lw;
    int                 owner_node = -1;
    uint64_t            seen       = 0;

    kv_env_init(&env);
    seed_client(&env, "dead_a", 2);
    seed_client(&env, "dead_b", 2);
    seed_client(&env, "dead_d", 2);
    seed_client(&env, "live_c", 3);
    seed_client(&env, "unknown_e", 5);
    seed_epoch(&env, 2, 3600); /* node 2 last beat an hour ago */
    seed_epoch(&env, 3, 1);    /* node 3 is alive */
    seed_epoch(&env, 6, 3600); /* node 6 is dead and owns no clients */
    {
        /* node 5's epoch record and one client record do not decode */
        uint8_t  key[CHIMERA_KV_NFS_KEY_MAX];
        uint32_t key_len = nfs_kv_epoch_key(key, 5);

        kv_put(&env, key, key_len, "garbage", 7);
        key_len = nfs_kv_client_key(key, "corrupt_f", 9);
        kv_put(&env, key, key_len, "garbage", 7);
    }

    node_start(&rec, &env, 1);
    CHECK(!rec.in_grace);      /* nothing of its own to reclaim */

    /* An unreadable record is an error, not an absent one. */
    lw = lookup(&env, &rec, "dead_a");
    CHECK(lw.result == NFS_RECOVERY_OWNER_FOUND && lw.node == 2);
    CHECK(lookup(&env, &rec, "nobody").result == NFS_RECOVERY_OWNER_ABSENT);
    CHECK(lookup(&env, &rec, "corrupt_f").result == NFS_RECOVERY_OWNER_ERROR);

    /* Fresh heartbeat: pending (a node that has just died looks the same)
     * until it is seen to move.  Nothing imported, no window. */
    CHECK(import_seen(&env, &rec, 3, "live_c", &seen) ==
          NFS_RECOVERY_LIVENESS_PENDING);
    CHECK(seen != 0);
    CHECK(import_seen(&env, &rec, 3, "live_c", &seen) ==
          NFS_RECOVERY_LIVENESS_PENDING);
    seed_epoch(&env, 3, 0);
    CHECK(import_seen(&env, &rec, 3, "live_c", &seen) ==
          NFS_RECOVERY_NOT_FAILOVER);
    CHECK(!in_reclaim_set(&rec, "live_c", NULL));
    CHECK(!rec.in_grace);

    /* Unreadable liveness is not proof of death: nothing imported, and the
     * peer is not cached as imported, so a later attempt checks again. */
    CHECK(import(&env, &rec, 5, "unknown_e") == NFS_RECOVERY_STORE_ERROR);
    CHECK(!in_reclaim_set(&rec, "unknown_e", NULL));
    CHECK(!rec.in_grace);
    CHECK(import(&env, &rec, 5, "unknown_e") == NFS_RECOVERY_STORE_ERROR);

    /* A dead peer with nothing to import: the window opened for the scan is
     * closed again once it settles. */
    CHECK(import(&env, &rec, 6, "nobody") == NFS_RECOVERY_NOT_FAILOVER);
    CHECK(rec.imports_in_flight == 0);
    CHECK(!rec.in_grace);

    /* Dead peer: all of its clients come in, still owned by it. */
    CHECK(import(&env, &rec, 2, "dead_a") == NFS_RECOVERY_FAILOVER);
    CHECK(in_reclaim_set(&rec, "dead_a", &owner_node) && owner_node == 2);
    CHECK(in_reclaim_set(&rec, "dead_b", NULL));
    CHECK(rec.failover_imported == 3);
    CHECK(rec.pending_reclaim == 3);
    CHECK(rec.in_grace);
    CHECK(nfs_recovery_open_check(&rec, NULL, false) == NFS4ERR_GRACE);

    /* The second client of that peer skips the scan. */
    CHECK(import(&env, &rec, 2, "dead_b") == NFS_RECOVERY_FAILOVER);
    CHECK(rec.failover_imported == 3);

    /* Self is never a failover. */
    CHECK(import(&env, &rec, 1, "dead_a") == NFS_RECOVERY_NOT_FAILOVER);

    /* Once a client has reclaimed it is no longer owed one. */
    {
        struct nfs_client c;

        memset(&c, 0, sizeof(c));
        c.owner_len = 6;
        memcpy(c.owner_string, "dead_a", 6);
        nfs_recovery_reclaim_complete(&rec, &c);
    }
    CHECK(import(&env, &rec, 2, "dead_a") == NFS_RECOVERY_NOT_FAILOVER);

    /* Once the peer is back, its clients are its own again, imported or not:
     * a fresh heartbeat is pending until it is seen to move. */
    seed_epoch(&env, 2, 1);
    seen = 0;
    CHECK(import_seen(&env, &rec, 2, "dead_d", &seen) ==
          NFS_RECOVERY_LIVENESS_PENDING);
    seed_epoch(&env, 2, 0);
    CHECK(import_seen(&env, &rec, 2, "dead_d", &seen) ==
          NFS_RECOVERY_NOT_FAILOVER);
    CHECK(in_reclaim_set(&rec, "dead_d", NULL));

    /* The deadline purge deletes the records of the clients that never came
     * back (dead_d), but only while the dead peer still owns them: dead_b has
     * since moved to node 4. */
    seed_client(&env, "dead_b", 4);
    evpl_mutex_lock(&rec.lock);
    rec.grace_end_ns = 0;
    evpl_mutex_unlock(&rec.lock);
    nfs_recovery_sweep_once(&rec, env.vfs_thread);
    CHECK(!rec.in_grace);
    kv_wait_gone(&env, "dead_d");
    CHECK(kv_client_owner(&env, "dead_b") == 4);
    CHECK(kv_client_owner(&env, "dead_a") == 2);

    /* The peer restarts, serves a new client, and dies again: its new boot is
     * scanned, not skipped as already imported. */
    seed_client(&env, "dead_g", 2);
    seed_epoch_boot(&env, 2, 3600, 6);
    CHECK(import(&env, &rec, 2, "dead_g") == NFS_RECOVERY_FAILOVER);
    CHECK(in_reclaim_set(&rec, "dead_g", NULL));
    CHECK(rec.in_grace);

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: import_only_dead_peer\n");
} /* test_import_only_dead_peer */

int
main(void)
{
    chimera_log_init();

    test_cold_start_filters_and_migrates();
    test_forget_only_owned();
    test_import_only_dead_peer();
    return 0;
} /* main */
