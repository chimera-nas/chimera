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
    enum chimera_vfs_error status;
    const void                *value;
    uint32_t                   value_len;
    int                        search_count;
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

    /* Search for all keys in range */
    ctx->search_count = 0;
    chimera_vfs_search_keys(
        ctx->vfs_thread,
        "search_aaa",
        strlen("search_aaa"),
        "search_zzz",
        strlen("search_zzz"),
        search_keys_callback,
        search_keys_complete,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->search_count == num_keys);

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

    /* Delete should also return ENOENT for nonexistent key */
    chimera_vfs_delete_key(
        ctx->vfs_thread,
        key,
        key_len,
        delete_key_callback,
        ctx);

    wait_for_completion(ctx);
    assert(ctx->status == CHIMERA_VFS_ENOENT);

    TEST_PASS("operations on nonexistent key return ENOENT");
} /* test_nonexistent_key */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;

    chimera_log_init();

    /* Create minimal metrics object (required for VFS init) */
    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    /* Initialize memfs module config */
    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);

    /* Create event loop */
    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    /* Initialize VFS with memfs as KV module (default) */
    ctx.vfs = chimera_vfs_init(
        4,              /* num_delegation_threads */
        module_cfgs,
        1,              /* num_modules */
        "",             /* kv_module_name - empty means use default (memfs) */
        60,             /* cache_ttl */
        metrics);

    assert(ctx.vfs != NULL);

    /* Initialize VFS thread */
    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    fprintf(stderr, "Running KV API tests with memfs backend...\n");

    /* Run tests */
    test_put_get_delete(&ctx);
    test_update_value(&ctx);
    test_search_keys(&ctx);
    test_binary_keys_values(&ctx);
    test_nonexistent_key(&ctx);

    fprintf(stderr, "All KV tests passed!\n");

    /* Cleanup */
    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    return 0;
} /* main */
