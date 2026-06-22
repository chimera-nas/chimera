// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Exercises the sync delegation thread pool with a BLOCKING backend (cairn).
 * Verifies:
 *   - With the pool enabled, BLOCKING module dispatch is routed to a sync
 *     delegation thread (off the caller thread) with FH-hash affinity.
 *   - With the pool disabled (count = 0), the BLOCKING capability is ignored
 *     and dispatch falls back to inline execution on the caller thread.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define NUM_SYNC_THREADS  4
#define NUM_DISTINCT_KEYS 32

struct test_ctx {
    int                        done;
    enum chimera_vfs_error status;
    int                        search_hits;
    pthread_t                  dispatch_tid;
    struct chimera_vfs        *vfs;
    struct chimera_vfs_thread *vfs_thread;
    struct evpl               *evpl;
};

static void
put_key_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* put_key_callback */

static void
delete_key_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* delete_key_callback */

/*
 * The per-entry callback runs inside the module's dispatch routine, which
 * executes on the thread that picked the request up. With sync delegation
 * enabled, a BLOCKING module runs on a sync delegation thread; with it
 * disabled, the BLOCKING flag is ignored and dispatch runs on the caller.
 */
static int
search_keys_callback(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->dispatch_tid = pthread_self();
    ctx->search_hits++;
    return 0;
} /* search_keys_callback */

static void
search_keys_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* search_keys_complete */

static void
wait_for_completion(struct test_ctx *ctx)
{
    while (!ctx->done) {
        evpl_continue(ctx->evpl);
    }
    ctx->done = 0;
} /* wait_for_completion */

static void
put_key(
    struct test_ctx *ctx,
    const char      *key,
    const char      *value)
{
    ctx->search_hits = 0;
    chimera_vfs_put_key(
        ctx->vfs_thread,
        key,
        strlen(key),
        value,
        strlen(value),
        put_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* put_key */

/*
 * Runs a search over a range that matches a single inserted key and returns
 * the pthread_t that ran the per-entry callback.
 */
static pthread_t
search_for_single_key(
    struct test_ctx *ctx,
    const char      *key)
{
    ctx->search_hits  = 0;
    ctx->dispatch_tid = 0;

    /* Inclusive range [key, key]: cairn iterates the KV keyspace from the
     * start key and stops once a key sorts past the end key, so an end key
     * equal to the start key matches exactly the one stored entry. */
    chimera_vfs_search_keys(
        ctx->vfs_thread,
        key,
        strlen(key),
        key,
        strlen(key),
        0,
        search_keys_callback,
        search_keys_complete,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->search_hits == 1);
    assert(ctx->dispatch_tid != 0);

    return ctx->dispatch_tid;
} /* search_for_single_key */

static void
delete_keys(
    struct test_ctx *ctx,
    char             keys[][32],
    int              count)
{
    int i;

    for (i = 0; i < count; i++) {
        chimera_vfs_delete_key(
            ctx->vfs_thread,
            keys[i],
            strlen(keys[i]),
            delete_key_callback,
            ctx);
        wait_for_completion(ctx);
        assert(ctx->status == CHIMERA_VFS_OK);
    }
} /* delete_keys */

static void
test_sync_delegation_enabled(struct test_ctx *ctx)
{
    pthread_t main_tid = pthread_self();
    pthread_t tids[NUM_DISTINCT_KEYS];
    char      keys[NUM_DISTINCT_KEYS][32];
    char      values[NUM_DISTINCT_KEYS][32];
    int       i, j;
    int       distinct_threads = 0;
    int       saw_main_thread  = 0;

    for (i = 0; i < NUM_DISTINCT_KEYS; i++) {
        snprintf(keys[i], sizeof(keys[i]), "sync_test_%02d", i);
        snprintf(values[i], sizeof(values[i]), "v_%02d", i);
        put_key(ctx, keys[i], values[i]);
    }

    for (i = 0; i < NUM_DISTINCT_KEYS; i++) {
        tids[i] = search_for_single_key(ctx, keys[i]);
        if (pthread_equal(tids[i], main_tid)) {
            saw_main_thread = 1;
        }
    }

    /* A BLOCKING backend with the sync pool enabled must never dispatch on
     * the caller thread. */
    assert(saw_main_thread == 0);

    /* Affinity: re-running search for the same key lands on the same thread. */
    for (i = 0; i < NUM_DISTINCT_KEYS; i++) {
        pthread_t again = search_for_single_key(ctx, keys[i]);
        assert(pthread_equal(again, tids[i]));
    }

    /* Distribution: 4 threads, 32 independently hashed keys -> multiple used. */
    for (i = 0; i < NUM_DISTINCT_KEYS; i++) {
        int seen = 0;
        for (j = 0; j < i; j++) {
            if (pthread_equal(tids[i], tids[j])) {
                seen = 1;
                break;
            }
        }
        if (!seen) {
            distinct_threads++;
        }
    }
    assert(distinct_threads >= 2);

    delete_keys(ctx, keys, NUM_DISTINCT_KEYS);

    fprintf(stderr, "    %d distinct dispatch threads used across %d keys\n",
            distinct_threads, NUM_DISTINCT_KEYS);
    TEST_PASS("sync delegation routes BLOCKING dispatch off the caller thread with FH-hash affinity");
} /* test_sync_delegation_enabled */

static void
test_sync_delegation_disabled(struct test_ctx *ctx)
{
    pthread_t main_tid = pthread_self();
    pthread_t tid;
    char      keys[1][32];

    snprintf(keys[0], sizeof(keys[0]), "inline_key_a");
    put_key(ctx, keys[0], "v_a");

    tid = search_for_single_key(ctx, keys[0]);

    /* With the sync pool disabled, the BLOCKING flag is ignored and dispatch
     * must happen inline on the caller thread. */
    assert(pthread_equal(tid, main_tid));

    delete_keys(ctx, keys, 1);

    TEST_PASS("sync delegation disabled ignores BLOCKING and routes dispatch inline");
} /* test_sync_delegation_disabled */

static void
run_phase(
    int         num_sync_threads,
    const char *label,
    void (     *test_fn )(struct test_ctx *))
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[1];
    struct prometheus_metrics    *metrics;
    char                          tmpl[]    = "/tmp/chimera_sync_deleg_XXXXXX";
    char                         *cairn_dir = mkdtemp(tmpl);
    char                          cairn_cfg[256];
    char                          rmcmd[512];

    assert(cairn_dir != NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "cairn", sizeof(module_cfgs[0].module_name) - 1);
    snprintf(cairn_cfg, sizeof(cairn_cfg),
             "{\"initialize\":true,\"path\":\"%s\"}", cairn_dir);
    strncpy(module_cfgs[0].config_data, cairn_cfg, sizeof(module_cfgs[0].config_data) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(
        num_sync_threads,  /* num_sync_delegation_threads */
        0,                 /* num_async_delegation_threads */
        module_cfgs,
        1,
        "cairn",
        60,
        1,                 /* attr_cache_enabled */
        0,                 /* num_rcu_reclaim_threads: 0 = one per CPU */
        metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    fprintf(stderr, "Phase: %s (num_sync_delegation_threads=%d)\n",
            label, num_sync_threads);

    test_fn(&ctx);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    snprintf(rmcmd, sizeof(rmcmd), "rm -rf %s", cairn_dir);
    if (system(rmcmd) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", cairn_dir);
    }
} /* run_phase */

int
main(
    int    argc,
    char **argv)
{
    chimera_log_init();

    run_phase(NUM_SYNC_THREADS, "sync delegation enabled",
              test_sync_delegation_enabled);
    run_phase(0, "sync delegation disabled",
              test_sync_delegation_disabled);

    fprintf(stderr, "All sync delegation tests passed!\n");
    return 0;
} /* main */
