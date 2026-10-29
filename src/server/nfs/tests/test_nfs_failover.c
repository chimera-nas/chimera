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
#include "nfs4_lease.h"
#include "nfs4_recovery.h"
#include "nfs4_drc.h"
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
seed_client_boot(
    struct kv_env *env,
    const char    *owner,
    uint16_t       owner_node,
    uint64_t       boot_id)
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
    value_len = nfs_recovery_serialize(value, sizeof(value), &c, owner_node,
                                       boot_id);
    kv_put(env, key, key_len, value, value_len);
} /* seed_client_boot */

/* Written by the boot seed_epoch() reports. */
static void
seed_client(
    struct kv_env *env,
    const char    *owner,
    uint16_t       owner_node)
{
    seed_client_boot(env, owner, owner_node, 5);
} /* seed_client */

/* An NFR3 record whose client last renewed `renew_age_s` seconds ago. */
static void
seed_client_renewed(
    struct kv_env *env,
    const char    *owner,
    uint16_t       owner_node,
    uint64_t       boot_id,
    uint64_t       renew_age_s)
{
    struct timespec ts;
    uint8_t         key[CHIMERA_KV_NFS_KEY_MAX];
    uint8_t         value[256];
    uint32_t        key_len, p = 0;
    uint16_t        owner_len = strlen(owner);
    uint64_t        now;

    clock_gettime(CLOCK_REALTIME, &ts);
    now = (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;

    nfs_kv_put_le32(value, &p, 0x3352464Eu); /* "NFR3" */
    nfs_kv_put_le64(value, &p, boot_id);
    nfs_kv_put_le64(value, &p, 0x77);
    nfs_kv_put_le64(value, &p, 0x100 + owner_node);
    nfs_kv_put_le64(value, &p, now - renew_age_s * 1000000000ULL);
    value[p++] = owner_node & 0xff;
    value[p++] = owner_node >> 8;
    value[p++] = owner_len & 0xff;
    value[p++] = owner_len >> 8;
    memcpy(value + p, owner, owner_len);
    p += owner_len;

    key_len = nfs_kv_client_key(key, owner, owner_len);
    kv_put(env, key, key_len, value, p);
} /* seed_client_renewed */

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

/* Write `node`'s epoch record with the given heartbeat. */
static void
seed_epoch_written(
    struct kv_env *env,
    uint16_t       node,
    uint64_t       boot_id,
    uint64_t       written_at)
{
    uint8_t  key[CHIMERA_KV_PREFIX_LEN];
    uint8_t  value[32];
    uint32_t key_len, p = 0;

    nfs_kv_put_le32(value, &p, 0x31504645u); /* "EPF1" */
    nfs_kv_put_le64(value, &p, boot_id);
    nfs_kv_put_le64(value, &p, written_at);
    key_len = nfs_kv_epoch_key(key, node);
    kv_put(env, key, key_len, value, p);
} /* seed_epoch_written */

/* Write `node`'s epoch record with a heartbeat `age_s` seconds old. */
static void
seed_epoch_boot(
    struct kv_env *env,
    uint16_t       node,
    uint64_t       age_s,
    uint64_t       boot_id)
{
    struct timespec ts;
    uint64_t        now;

    clock_gettime(CLOCK_REALTIME, &ts);
    now = (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
    seed_epoch_written(env, node, boot_id, now - age_s * 1000000000ULL);
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

/* A node backed by the shared store (memkv is forced persistent).  The 2 s
 * lease makes 1.5 leases 3 s, so age_watch never needs more monotonic
 * history than a freshly booted host has. */
static void
node_start(
    struct nfs_recovery *rec,
    struct kv_env       *env,
    uint16_t             node)
{
    CHECK(nfs_recovery_load(rec, env->vfs, node, 2, 180, true) == 0);
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

/* A legacy record whose client a peer has since taken over (it moved while
 * this node was down or still on an older build) is not migrated over the
 * peer's fleet record, and is not owed a reclaim here. */
static void
test_migration_keeps_peer_record(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;

    kv_env_init(&env);
    seed_legacy(&env, 1, "moved_x");
    seed_client(&env, "moved_x", 2);
    seed_legacy(&env, 1, "stays_y");

    node_start(&rec, &env, 1);
    CHECK(!in_reclaim_set(&rec, "moved_x", NULL));
    CHECK(in_reclaim_set(&rec, "stays_y", NULL));
    CHECK(rec.pending_reclaim == 1);
    CHECK(kv_client_owner(&env, "moved_x") == 2);
    CHECK(!kv_legacy_exists(&env, 1, "moved_x"));
    CHECK(kv_client_owner(&env, "stays_y") == 1);
    CHECK(!kv_legacy_exists(&env, 1, "stays_y"));

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: migration_keeps_peer_record\n");
} /* test_migration_keeps_peer_record */

/* A courtesy client's record is not refreshed: its stamp would not move, and
 * the rewrite would take the record back from a node the client moved to. */
static void
test_courtesy_not_refreshed(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;
    struct nfs_client   c;
    uint64_t            now = 1000ULL * 1000000000ULL;

    kv_env_init(&env);
    node_start(&rec, &env, 1);

    memset(&c, 0, sizeof(c));
    c.confirmed = 1;
    CHECK(nfs_recovery_persist_due(&rec, &c, now));
    atomic_store(&c.courtesy, 1);
    CHECK(!nfs_recovery_persist_due(&rec, &c, now));

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: courtesy_not_refreshed\n");
} /* test_courtesy_not_refreshed */

struct import_wait {
    bool done;
    enum nfs_recovery_failover result;
};

static void
import_done(
    enum nfs_recovery_failover result,
    void                      *arg)
{
    struct import_wait *w = arg;

    w->result = result;
    w->done   = true;
} /* import_done */

/* `record_boot` is the boot that wrote owner's record (0 = unknown); the
 * decision began at `since` (0: before any read of the peer). */
static enum nfs_recovery_failover
import_since(
    struct kv_env       *env,
    struct nfs_recovery *rec,
    uint16_t             peer,
    const char          *owner,
    uint64_t             record_boot,
    uint64_t             since)
{
    struct import_wait w = { 0 };

    nfs_recovery_import_peer(env->vfs_thread, rec, peer, owner, strlen(owner),
                             record_boot, since, import_done, &w);
    while (!w.done) {
        evpl_continue(env->evpl);
    }
    return w.result;
} /* import_since */

static enum nfs_recovery_failover
import_boot(
    struct kv_env       *env,
    struct nfs_recovery *rec,
    uint16_t             peer,
    const char          *owner,
    uint64_t             record_boot)
{
    return import_since(env, rec, peer, owner, record_boot, 0);
} /* import_boot */

static enum nfs_recovery_failover
import(
    struct kv_env       *env,
    struct nfs_recovery *rec,
    uint16_t             peer,
    const char          *owner)
{
    return import_boot(env, rec, peer, owner, 0);
} /* import */

/* Backdate this node's first read of `node`'s current heartbeat by 1.5
 * leases, as if it had not changed for that long. */
static void
age_watch(
    struct nfs_recovery *rec,
    uint16_t             node)
{
    struct nfs_recovery_peer_watch *w;
    uint64_t                        bound =
        3ULL * rec->lease_time_s * 1000000000ULL / 2;

    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->peer_watch, &node, sizeof(node), w);
    CHECK(w != NULL);
    CHECK(w->since_ns > bound);
    w->since_ns -= bound;
    evpl_mutex_unlock(&rec->lock);
} /* age_watch */

/* `node` has written a new heartbeat and then stopped: read it once (no
 * client of its is named, so nothing is imported), then age that read. */
static void
heartbeat_stopped(
    struct kv_env       *env,
    struct nfs_recovery *rec,
    uint16_t             node)
{
    CHECK(import(env, rec, node, "nobody") != NFS_RECOVERY_FAILOVER);
    age_watch(rec, node);
} /* heartbeat_stopped */

/* Run the sweeper's owner re-check now, and let its reads land. */
static void
owner_check(
    struct kv_env       *env,
    struct nfs_recovery *rec)
{
    atomic_store(&rec->next_owner_check_ns, 0);
    nfs_recovery_sweep_once(rec, env->vfs_thread);
    for (int i = 0; i < 10; i++) {
        (void) kv_client_owner(env, "nobody");
    }
} /* owner_check */

struct lookup_wait {
    bool     done;
    enum nfs_recovery_owner result;
    uint16_t node;
    uint64_t boot_id;
};

static void
lookup_done(
    enum nfs_recovery_owner result,
    uint16_t                node,
    uint64_t                boot_id,
    void                   *arg)
{
    struct lookup_wait *w = arg;

    w->result  = result;
    w->node    = node;
    w->boot_id = boot_id;
    w->done    = true;
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
 * peer's are left alone.  Liveness is judged by this node's clock: the
 * heartbeats' own timestamps (an hour old for nodes 2 and 6) are only
 * compared for change. */
static void
test_import_only_dead_peer(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;
    struct lookup_wait  lw;
    int                 owner_node = -1;

    kv_env_init(&env);
    seed_client(&env, "dead_a", 2);
    seed_client(&env, "dead_b", 2);
    seed_client(&env, "dead_d", 2);
    seed_client(&env, "live_c", 3);
    seed_client(&env, "unknown_e", 5);
    seed_epoch(&env, 2, 3600);
    seed_epoch(&env, 3, 1);
    seed_epoch(&env, 6, 3600); /* node 6 owns no clients */
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
    CHECK(lw.boot_id == 5);
    CHECK(lookup(&env, &rec, "nobody").result == NFS_RECOVERY_OWNER_ABSENT);
    CHECK(lookup(&env, &rec, "corrupt_f").result == NFS_RECOVERY_OWNER_ERROR);

    /* First read: pending, as a node that has just died looks the same.  Still
     * pending while unchanged; alive once it moves.  Nothing imported, no
     * window. */
    CHECK(import(&env, &rec, 3, "live_c") == NFS_RECOVERY_LIVENESS_PENDING);
    CHECK(import(&env, &rec, 3, "live_c") == NFS_RECOVERY_LIVENESS_PENDING);
    seed_epoch(&env, 3, 0);
    CHECK(import(&env, &rec, 3, "live_c") == NFS_RECOVERY_NOT_FAILOVER);
    CHECK(!in_reclaim_set(&rec, "live_c", NULL));
    CHECK(!rec.in_grace);

    /* An hour-old heartbeat proves nothing on its own (the clocks may differ):
     * node 2 is pending until it has been seen unchanged for 1.5 leases. */
    CHECK(import(&env, &rec, 2, "dead_a") == NFS_RECOVERY_LIVENESS_PENDING);
    CHECK(!in_reclaim_set(&rec, "dead_a", NULL));

    /* Unreadable liveness is not proof of death: nothing imported, and the
     * peer is not cached as imported, so a later attempt checks again. */
    CHECK(import(&env, &rec, 5, "unknown_e") == NFS_RECOVERY_STORE_ERROR);
    CHECK(!in_reclaim_set(&rec, "unknown_e", NULL));
    CHECK(!rec.in_grace);
    CHECK(import(&env, &rec, 5, "unknown_e") == NFS_RECOVERY_STORE_ERROR);

    /* A dead peer with nothing to import: the window opened for the scan is
     * closed again once it settles. */
    heartbeat_stopped(&env, &rec, 6);
    CHECK(import(&env, &rec, 6, "nobody") == NFS_RECOVERY_NOT_FAILOVER);
    CHECK(rec.imports_in_flight == 0);
    CHECK(!rec.in_grace);

    /* Dead peer: all of its clients come in, still owned by it. */
    age_watch(&rec, 2);
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
     * this node has been reading its heartbeat, so a change proves it alive. */
    seed_epoch(&env, 2, 1);
    CHECK(import(&env, &rec, 2, "dead_d") == NFS_RECOVERY_NOT_FAILOVER);
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
    heartbeat_stopped(&env, &rec, 2);
    CHECK(import(&env, &rec, 2, "dead_g") == NFS_RECOVERY_FAILOVER);
    CHECK(in_reclaim_set(&rec, "dead_g", NULL));
    CHECK(rec.in_grace);

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: import_only_dead_peer\n");
} /* test_import_only_dead_peer */

/* A peer that restarted lost the state its earlier boot held: those clients
 * fail over, while the ones its running boot wrote are still its own.  The
 * earlier boot's client last renewed an hour ago, long before the restart;
 * it is still owed a reclaim, since that boot's last heartbeat is unknown. */
static void
test_restarted_peer(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;
    struct nfs_client   c;

    kv_env_init(&env);
    seed_client_renewed(&env, "old_boot_a", 2, 5, 3600);
    seed_client_boot(&env, "new_boot_b", 2, 6);
    seed_client_boot(&env, "new_boot_c", 2, 6);
    seed_epoch_boot(&env, 2, 1, 6); /* node 2 is up again, on boot 6 */
    node_start(&rec, &env, 1);

    CHECK(import_boot(&env, &rec, 2, "new_boot_b", 6) ==
          NFS_RECOVERY_LIVENESS_PENDING);
    CHECK(import_boot(&env, &rec, 2, "old_boot_a", 5) ==
          NFS_RECOVERY_FAILOVER);
    CHECK(in_reclaim_set(&rec, "old_boot_a", NULL));
    CHECK(!in_reclaim_set(&rec, "new_boot_b", NULL));
    CHECK(rec.in_grace);

    /* Boot 6 then dies as well, its last beat left in place: its own clients
     * are still scanned. */
    heartbeat_stopped(&env, &rec, 2);
    CHECK(import_boot(&env, &rec, 2, "new_boot_b", 6) ==
          NFS_RECOVERY_FAILOVER);
    CHECK(in_reclaim_set(&rec, "new_boot_b", NULL));
    CHECK(in_reclaim_set(&rec, "new_boot_c", NULL));

    /* old_boot_a reclaims here, goes back to node 2's next boot, and that
    * boot dies: the newer record is a new obligation, not a duplicate. */
    memset(&c, 0, sizeof(c));
    c.owner_len = 10;
    memcpy(c.owner_string, "old_boot_a", 10);
    nfs_recovery_reclaim_complete(&rec, &c);
    seed_client_boot(&env, "old_boot_a", 2, 7);
    seed_epoch_boot(&env, 2, 3600, 7);
    heartbeat_stopped(&env, &rec, 2);
    CHECK(import_boot(&env, &rec, 2, "old_boot_a", 7) ==
          NFS_RECOVERY_FAILOVER);

    /* new_boot_b went back to node 2 as well (boot 8 rewrote its record):
     * closing the window purges new_boot_c, never reclaimed and unchanged, but
     * not the record a later boot wrote. */
    seed_client_boot(&env, "new_boot_b", 2, 8);
    evpl_mutex_lock(&rec.lock);
    rec.grace_end_ns = 0;
    evpl_mutex_unlock(&rec.lock);
    nfs_recovery_sweep_once(&rec, env.vfs_thread);
    CHECK(!rec.in_grace);
    kv_wait_gone(&env, "new_boot_c");
    CHECK(kv_client_owner(&env, "new_boot_b") == 2);

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: restarted_peer\n");
} /* test_restarted_peer */

/* The peer took far longer than 1.5 leases to restart, then died before its
 * earlier boot's client came back: that client is still owed a reclaim. */
static void
test_slow_restart_then_dead(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;

    kv_env_init(&env);
    seed_client_renewed(&env, "before_restart", 2, 5, 3600);
    seed_client_boot(&env, "after_restart", 2, 6);
    seed_epoch_boot(&env, 2, 1, 6);
    node_start(&rec, &env, 1);

    heartbeat_stopped(&env, &rec, 2);
    CHECK(import_boot(&env, &rec, 2, "after_restart", 6) ==
          NFS_RECOVERY_FAILOVER);
    CHECK(in_reclaim_set(&rec, "after_restart", NULL));
    CHECK(in_reclaim_set(&rec, "before_restart", NULL));

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: slow_restart_then_dead\n");
} /* test_slow_restart_then_dead */

/* The read a heartbeat change is measured from belongs to every decision, so
 * one made before a decision began cannot prove the peer alive for it: the
 * peer may have written its final beat in between. */
static void
test_first_read_cannot_prove_alive(void)
{
    struct kv_env                   env;
    struct nfs_recovery             rec;
    struct nfs_recovery_peer_watch *w;
    uint16_t                        peer = 2;
    uint64_t                        since_b, last_beat;

    kv_env_init(&env);
    seed_client(&env, "client_b", 2);
    seed_epoch(&env, 2, 1);
    node_start(&rec, &env, 1);

    /* Another client's decision reads node 2 while it is alive. */
    CHECK(import_since(&env, &rec, 2, "client_a", 5, nfs_lease_now_ns()) ==
          NFS_RECOVERY_LIVENESS_PENDING);

    /* Node 2 writes one more beat and crashes; client_b's decision begins. */
    seed_epoch(&env, 2, 0);
    since_b = nfs_lease_now_ns();
    CHECK(import_since(&env, &rec, 2, "client_b", 5, since_b) ==
          NFS_RECOVERY_LIVENESS_PENDING);
    CHECK(!in_reclaim_set(&rec, "client_b", NULL));

    /* A read that completes out of order does not rewind the watch. */
    HASH_FIND(hh, rec.peer_watch, &peer, sizeof(peer), w);
    CHECK(w != NULL);
    last_beat = w->written_at;
    seed_epoch(&env, 2, 5);
    CHECK(import_since(&env, &rec, 2, "client_b", 5, since_b) ==
          NFS_RECOVERY_LIVENESS_PENDING);
    CHECK(w->written_at == last_beat);
    seed_epoch_written(&env, 2, 5, last_beat);

    /* The final beat then stays unchanged for 1.5 leases. */
    age_watch(&rec, 2);
    CHECK(import_since(&env, &rec, 2, "client_b", 5, since_b) ==
          NFS_RECOVERY_FAILOVER);
    CHECK(in_reclaim_set(&rec, "client_b", NULL));

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: first_read_cannot_prove_alive\n");
} /* test_first_read_cannot_prove_alive */

/* With several survivors, a dead peer's clients split between them.  One
 * that reclaims on another survivor is owed nothing here: the sweeper sees
 * its record change hands, and the window closes before its deadline. */
static void
test_import_retires_moved_clients(void)
{
    struct kv_env       env;
    struct nfs_recovery rec;

    kv_env_init(&env);
    seed_client(&env, "split_a", 2);
    seed_client(&env, "split_b", 2);
    seed_epoch(&env, 2, 3600);
    node_start(&rec, &env, 1);

    heartbeat_stopped(&env, &rec, 2);
    CHECK(import(&env, &rec, 2, "split_a") == NFS_RECOVERY_FAILOVER);
    CHECK(rec.pending_reclaim == 2);
    CHECK(rec.in_grace);

    /* Still the dead peer's: owed here. */
    owner_check(&env, &rec);
    CHECK(rec.pending_reclaim == 2);

    /* split_a reclaims on node 3; split_b comes back to this node. */
    seed_client(&env, "split_a", 3);
    seed_client(&env, "split_b", 1);
    owner_check(&env, &rec);
    CHECK(rec.pending_reclaim == 1);
    CHECK(rec.in_grace);

    /* split_b then moves on to node 4 as well: nobody is owed a reclaim. */
    seed_client(&env, "split_b", 4);
    owner_check(&env, &rec);
    CHECK(rec.pending_reclaim == 0);
    CHECK(!rec.in_grace);
    CHECK(kv_client_owner(&env, "split_a") == 3);
    CHECK(kv_client_owner(&env, "split_b") == 4);

    nfs_recovery_free(&rec);
    kv_env_destroy(&env);
    printf("ok: import_retires_moved_clients\n");
} /* test_import_retires_moved_clients */

struct rc_wait {
    bool done;
    bool ok;
};

static void
rc_done(
    bool  ok,
    void *arg)
{
    struct rc_wait *w = arg;

    w->ok   = ok;
    w->done = true;
} /* rc_done */

static bool
persist_rc(
    struct kv_env       *env,
    struct nfs4_session *session)
{
    struct rc_wait w = { 0 };

    nfs4_drc_persist_reclaim_complete(env->vfs_thread, session, rc_done, &w);
    while (!w.done) {
        evpl_continue(env->evpl);
    }
    return w.ok;
} /* persist_rc */

/* The RECLAIM_COMPLETE bit is written to the session record; no record (or a
 * corrupt one) leaves nothing to mark. */
static void
test_reclaim_complete_persisted(void)
{
    struct kv_env                  env;
    struct nfs4_drc_session_record srec, out;
    struct kv_wait                 w = { 0 };
    struct nfs4_session           *session;
    uint8_t                        key[CHIMERA_KV_SESSION_KEY_LEN];
    uint8_t                        val[512];
    uint32_t                       key_len, val_len;

    kv_env_init(&env);
    session = calloc(1, sizeof(*session));
    CHECK(session);
    atomic_init(&session->destroyed, false);
    memset(session->nfs4_session_id, 0x3C, NFS4_SESSIONID_SIZE);
    memset(&srec, 0, sizeof(srec));
    srec.clientid  = 42;
    srec.owner_len = 5;
    memcpy(srec.owner, "owner", 5);
    key_len = nfs_kv_session_key(key, session->nfs4_session_id);
    val_len = nfs4_drc_session_serialize(val, sizeof(val), &srec);
    kv_put(&env, key, key_len, val, val_len);

    CHECK(persist_rc(&env, session));
    chimera_vfs_get_key(env.vfs_thread, key, key_len, kv_get_done, &w);
    kv_wait_loop(&env, &w);
    CHECK(nfs4_drc_session_deserialize(w.value, w.value_len, &out) == 0);
    CHECK(out.flags & NFS4_DRC_SESSION_RECLAIM_COMPLETE);
    CHECK(out.clientid == 42);

    /* Already set, absent, or corrupt: nothing to write, still ok. */
    CHECK(persist_rc(&env, session));
    memset(session->nfs4_session_id, 0x3D, NFS4_SESSIONID_SIZE);
    CHECK(persist_rc(&env, session));
    key_len = nfs_kv_session_key(key, session->nfs4_session_id);
    kv_put(&env, key, key_len, "garbage", 7);
    CHECK(persist_rc(&env, session));

    /* Destroyed while the bit was being written: the record is not left. */
    memset(session->nfs4_session_id, 0x3E, NFS4_SESSIONID_SIZE);
    key_len = nfs_kv_session_key(key, session->nfs4_session_id);
    kv_put(&env, key, key_len, val, val_len);
    atomic_store(&session->destroyed, true);
    CHECK(persist_rc(&env, session));
    memset(&w, 0, sizeof(w));
    chimera_vfs_get_key(env.vfs_thread, key, key_len, kv_get_done, &w);
    kv_wait_loop(&env, &w);
    CHECK(w.error == CHIMERA_VFS_ENOENT || w.value_len == 0);

    free(session);
    kv_env_destroy(&env);
    printf("ok: reclaim_complete_persisted\n");
} /* test_reclaim_complete_persisted */

int
main(void)
{
    chimera_log_init();

    test_cold_start_filters_and_migrates();
    test_forget_only_owned();
    test_migration_keeps_peer_record();
    test_courtesy_not_refreshed();
    test_import_only_dead_peer();
    test_restarted_peer();
    test_slow_restart_then_dead();
    test_first_read_cannot_prove_alive();
    test_import_retires_moved_clients();
    test_reclaim_complete_persisted();
    return 0;
} /* main */
