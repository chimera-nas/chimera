// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>
#include <unistd.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

struct test_ctx {
    int                        done;
    enum chimera_vfs_error     status;
    const void                *value;
    uint32_t                   value_len;
    int                        search_count;
    char                       search_keys[16][64];
    uint32_t                   search_key_lens[16];
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
get_key_callback(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status    = error_code;
    ctx->value     = value;
    ctx->value_len = value_len;
    ctx->done      = 1;
} /* get_key_callback */

static void
delete_key_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* delete_key_callback */

static int
search_keys_callback(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct test_ctx *ctx = private_data;

    if (ctx->search_count < 16 && key_len < sizeof(ctx->search_keys[0])) {
        memcpy(ctx->search_keys[ctx->search_count], key, key_len);
        ctx->search_key_lens[ctx->search_count] = key_len;
    }
    ctx->search_count++;
    return 0; /* continue searching */
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
test_put_get_delete(struct test_ctx *ctx)
{
    const char *key       = "test_key";
    uint32_t    key_len   = strlen(key);
    const char *value     = "test_value";
    uint32_t    value_len = strlen(value);

    /* Put a key-value pair */
    chimera_vfs_put_key(
        ctx->vfs_thread,
        key,
        key_len,
        value,
        value_len,
        put_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    /* Get the value back */
    chimera_vfs_get_key(
        ctx->vfs_thread,
        key,
        key_len,
        get_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->value_len == value_len);
    assert(memcmp(ctx->value, value, value_len) == 0);

    /* Delete the key */
    chimera_vfs_delete_key(
        ctx->vfs_thread,
        key,
        key_len,
        delete_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    /* Verify key is gone */
    chimera_vfs_get_key(
        ctx->vfs_thread,
        key,
        key_len,
        get_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_ENOENT);

    TEST_PASS("put/get/delete key operations");
} /* test_put_get_delete */

static void
test_update_value(struct test_ctx *ctx)
{
    const char *key        = "update_key";
    uint32_t    key_len    = strlen(key);
    const char *value1     = "first_value";
    uint32_t    value1_len = strlen(value1);
    const char *value2     = "second_value_longer";
    uint32_t    value2_len = strlen(value2);

    /* Put initial value */
    chimera_vfs_put_key(
        ctx->vfs_thread,
        key,
        key_len,
        value1,
        value1_len,
        put_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    /* Update with new value */
    chimera_vfs_put_key(
        ctx->vfs_thread,
        key,
        key_len,
        value2,
        value2_len,
        put_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    /* Get should return updated value */
    chimera_vfs_get_key(
        ctx->vfs_thread,
        key,
        key_len,
        get_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->value_len == value2_len);
    assert(memcmp(ctx->value, value2, value2_len) == 0);

    /* Cleanup */
    chimera_vfs_delete_key(
        ctx->vfs_thread,
        key,
        key_len,
        delete_key_callback,
        ctx);

    wait_for_completion(ctx);

    TEST_PASS("update value for existing key");
} /* test_update_value */

static void
test_search_keys(struct test_ctx *ctx)
{
    /* Insert several keys for search test */
    const char *keys[]   = { "search_aaa", "search_bbb", "search_ccc", "search_ddd" };
    const char *values[] = { "val_aaa", "val_bbb", "val_ccc", "val_ddd" };
    int         num_keys = sizeof(keys) / sizeof(keys[0]);

    for (int i = 0; i < num_keys; i++) {
        chimera_vfs_put_key(
            ctx->vfs_thread,
            keys[i],
            strlen(keys[i]),
            values[i],
            strlen(values[i]),
            put_key_callback,
            ctx);

        wait_for_completion(ctx);
        assert(ctx->status == CHIMERA_VFS_OK);
    }

    /* Search the whole range; results must come back in sorted key order. */
    ctx->search_count = 0;
    chimera_vfs_search_keys(
        ctx->vfs_thread,
        "search_aaa",
        strlen("search_aaa"),
        "search_zzz",
        strlen("search_zzz"),
        0,
        search_keys_callback,
        search_keys_complete,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->search_count == num_keys);

    for (int i = 0; i < num_keys; i++) {
        assert(ctx->search_key_lens[i] == strlen(keys[i]));
        assert(memcmp(ctx->search_keys[i], keys[i], ctx->search_key_lens[i]) == 0);
    }

    /* Inclusive end: a range ending exactly on "search_ccc" returns it. */
    ctx->search_count = 0;
    chimera_vfs_search_keys(
        ctx->vfs_thread,
        "search_aaa",
        strlen("search_aaa"),
        "search_ccc",
        strlen("search_ccc"),
        0,
        search_keys_callback,
        search_keys_complete,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->search_count == 3); /* aaa, bbb, ccc */

    /* Exclusive end: the same range drops the key equal to the end bound. */
    ctx->search_count = 0;
    chimera_vfs_search_keys(
        ctx->vfs_thread,
        "search_aaa",
        strlen("search_aaa"),
        "search_ccc",
        strlen("search_ccc"),
        CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE,
        search_keys_callback,
        search_keys_complete,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->search_count == 2); /* aaa, bbb */
    assert(ctx->search_key_lens[1] == strlen("search_bbb"));
    assert(memcmp(ctx->search_keys[1], "search_bbb", strlen("search_bbb")) == 0);

    /* Variable-length keys that share a prefix and are LONGER than a short end
     * bound.  A prefix scan must return all of them: a stored key longer than
     * the end key is past the range only if its shared prefix already sorts at
     * or after the end (regression for a cairn end-key check that treated any
     * longer key as past-end, dropping every match). */
    const char *pkeys[]   = { "len_a", "len_ab", "len_abc" };
    int         num_pkeys = sizeof(pkeys) / sizeof(pkeys[0]);

    for (int i = 0; i < num_pkeys; i++) {
        chimera_vfs_put_key(ctx->vfs_thread, pkeys[i], strlen(pkeys[i]),
                            "v", 1, put_key_callback, ctx);
        wait_for_completion(ctx);
        assert(ctx->status == CHIMERA_VFS_OK);
    }
    /* A key just past the "len_" band (so the [ "len_", "len`" ) bound must
     * exclude it while keeping the longer in-band keys). */
    chimera_vfs_put_key(ctx->vfs_thread, "lenz", 4, "v", 1, put_key_callback, ctx);
    wait_for_completion(ctx);

    ctx->search_count = 0;
    chimera_vfs_search_keys(
        ctx->vfs_thread,
        "len_", 4,
        "len`", 4,               /* 0x60 = '_'(0x5f) + 1: bounds the "len_" band */
        CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE,
        search_keys_callback,
        search_keys_complete,
        ctx);
    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->search_count == num_pkeys); /* all three "len_*" keys, not "lenz" */
    for (int i = 0; i < num_pkeys; i++) {
        assert(ctx->search_key_lens[i] == strlen(pkeys[i]));
        assert(memcmp(ctx->search_keys[i], pkeys[i], ctx->search_key_lens[i]) == 0);
    }

    for (int i = 0; i < num_pkeys; i++) {
        chimera_vfs_delete_key(ctx->vfs_thread, pkeys[i], strlen(pkeys[i]),
                               delete_key_callback, ctx);
        wait_for_completion(ctx);
    }
    chimera_vfs_delete_key(ctx->vfs_thread, "lenz", 4, delete_key_callback, ctx);
    wait_for_completion(ctx);

    /* Cleanup */
    for (int i = 0; i < num_keys; i++) {
        chimera_vfs_delete_key(
            ctx->vfs_thread,
            keys[i],
            strlen(keys[i]),
            delete_key_callback,
            ctx);

        wait_for_completion(ctx);
    }

    TEST_PASS("search keys in range");
} /* test_search_keys */

static void
test_binary_keys_values(struct test_ctx *ctx)
{
    /* Test with binary data containing null bytes */
    uint8_t  key[]     = { 0x00, 0x01, 0x02, 0x03, 0x00, 0x05 };
    uint32_t key_len   = sizeof(key);
    uint8_t  value[]   = { 0xFF, 0x00, 0xAB, 0xCD, 0x00, 0x00, 0xEF };
    uint32_t value_len = sizeof(value);

    chimera_vfs_put_key(
        ctx->vfs_thread,
        key,
        key_len,
        value,
        value_len,
        put_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_get_key(
        ctx->vfs_thread,
        key,
        key_len,
        get_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->value_len == value_len);
    assert(memcmp(ctx->value, value, value_len) == 0);

    chimera_vfs_delete_key(
        ctx->vfs_thread,
        key,
        key_len,
        delete_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    TEST_PASS("binary keys and values with null bytes");
} /* test_binary_keys_values */

static void
test_nonexistent_key(struct test_ctx *ctx)
{
    const char *key     = "nonexistent_key_12345";
    uint32_t    key_len = strlen(key);

    chimera_vfs_get_key(
        ctx->vfs_thread,
        key,
        key_len,
        get_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_ENOENT);

    /* Deleting a nonexistent key is idempotent: backends that track presence
     * (memkv, sqlite) report ENOENT, while those whose delete is a blind
     * tombstone (cairn/RocksDB) report OK.  Either is acceptable. */
    chimera_vfs_delete_key(
        ctx->vfs_thread,
        key,
        key_len,
        delete_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_ENOENT || ctx->status == CHIMERA_VFS_OK);

    TEST_PASS("operations on nonexistent key");
} /* test_nonexistent_key */

/* Run the full KV test suite against a single KV-only backend. */
static void
run_suite(
    const char                *kv_name,
    const char                *kv_config,
    struct prometheus_metrics *metrics)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[1];

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, kv_name, sizeof(module_cfgs[0].module_name) - 1);
    if (kv_config) {
        strncpy(module_cfgs[0].config_data, kv_config, sizeof(module_cfgs[0].config_data) - 1);
    }

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    /* 4 sync delegation threads so a CAP_BLOCKING backend (sqlite) is exercised
     * through the delegation pool and completion bounce-back. */
    ctx.vfs = chimera_vfs_init(
        4,              /* num_sync_delegation_threads */
        0,              /* num_async_delegation_threads */
        module_cfgs,
        1,              /* num_modules */
        kv_name,        /* kv_module_name */
        60,             /* cache_ttl */
        0,              /* num_rcu_reclaim_threads: 0 = one per CPU */
        metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    fprintf(stderr, "Running KV API tests with %s backend...\n", kv_name);

    test_put_get_delete(&ctx);
    test_update_value(&ctx);
    test_search_keys(&ctx);
    test_binary_keys_values(&ctx);
    test_nonexistent_key(&ctx);

    fprintf(stderr, "All KV tests passed for %s!\n", kv_name);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
} /* run_suite */

int
main(
    int    argc,
    char **argv)
{
    struct prometheus_metrics *metrics;
    char                       tmpl[] = "/tmp/chimera_sqlite_kv_test_XXXXXX";
    char                      *db_dir;
    char                       sqlite_cfg[256];
    char                       rmcmd[320];

    chimera_log_init();

    /* Create minimal metrics object (required for VFS init) */
    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    /* In-memory KV-only backend. */
    run_suite("memkv", NULL, metrics);

#ifdef CHIMERA_KV_TEST_SQLITE
    /* Persistent sqlite KV-only backend (WAL, single table). */
    db_dir = mkdtemp(tmpl);
    assert(db_dir != NULL);
    snprintf(sqlite_cfg, sizeof(sqlite_cfg), "{\"path\":\"%s/kv.db\"}", db_dir);

    run_suite("sqlite", sqlite_cfg, metrics);

    snprintf(rmcmd, sizeof(rmcmd), "rm -rf %s", db_dir);
    if (system(rmcmd) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", db_dir);
    }
#else  /* ifdef CHIMERA_KV_TEST_SQLITE */
    (void) db_dir;
    (void) tmpl;
    (void) sqlite_cfg;
    (void) rmcmd;
#endif /* ifdef CHIMERA_KV_TEST_SQLITE */

#ifdef CHIMERA_KV_TEST_CAIRN
    /* Persistent cairn (RocksDB) KV backend -- exercises the end-key range
     * comparison the search test relies on. */
    {
        char  ctmpl[] = "/tmp/chimera_cairn_kv_test_XXXXXX";
        char *cdir    = mkdtemp(ctmpl);
        char  cairn_cfg[256];
        char  crm[320];

        assert(cdir != NULL);
        snprintf(cairn_cfg, sizeof(cairn_cfg),
                 "{\"initialize\":true,\"path\":\"%s\"}", cdir);

        run_suite("cairn", cairn_cfg, metrics);

        snprintf(crm, sizeof(crm), "rm -rf %s", cdir);
        if (system(crm) != 0) {
            fprintf(stderr, "warning: failed to remove %s\n", cdir);
        }
    }
#endif /* ifdef CHIMERA_KV_TEST_CAIRN */

    fprintf(stderr, "All KV tests passed!\n");

    prometheus_metrics_destroy(metrics);

    return 0;
} /* main */
