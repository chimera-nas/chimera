// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Inode-scoped remove (chimera_vfs_remove_at_match_fh): the unlink must only
 * take effect while the name STILL resolves to the caller-supplied child FH.
 * This guards an asynchronous delete-on-close against a file that was removed
 * and re-created with the SAME name by another opener in the meantime -- the
 * replacement must NOT be destroyed.  Drives memfs directly.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_attrs.h"
#include "vfs/vfs_cred.h"
#include "vfs/vfs_error.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    struct chimera_vfs_open_handle *handle;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
};

static void
wait_done(struct test_ctx *ctx)
{
    while (!ctx->done) {
        evpl_continue(ctx->evpl);
    }
    ctx->done = 0;
} /* wait_done */

static void
mount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mount_cb */

static void
lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* lookup_cb */

static void
openfh_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    ctx->done   = 1;
} /* openfh_cb */

static void
openat_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre,
    struct chimera_vfs_attrs       *dir_post,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh, oh->fh, oh->fh_len);
        ctx->fh_len = oh->fh_len;
    }
    ctx->done = 1;
} /* openat_cb */

static void
remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* remove_cb */

/* Create `name` under `dir`; capture the new file's FH in ctx->fh and return
 * (and keep) the open handle. */
static struct chimera_vfs_open_handle *
create_file(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    chimera_vfs_open_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* create_file */

static struct chimera_vfs_open_handle *
open_fh(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    chimera_vfs_open_fh(ctx->vfs_thread, cred, fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* open_fh */

/* Look up `name` under `dir`; return the status (and the FH in ctx->fh on OK). */
static enum chimera_vfs_error
do_lookup(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    chimera_vfs_lookup(ctx->vfs_thread, cred, dir->fh, dir->fh_len,
                       name, strlen(name),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_lookup */

static enum chimera_vfs_error
do_remove(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    chimera_vfs_remove_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                          NULL, 0, 0, 0, NULL, remove_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_remove */

static enum chimera_vfs_error
do_remove_match(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    const uint8_t                  *child_fh,
    uint32_t                        child_fh_len)
{
    chimera_vfs_remove_at_match_fh(ctx->vfs_thread, cred, dir, name, strlen(name),
                                   child_fh, child_fh_len, 0, 0, NULL,
                                   remove_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_remove_match */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx                 ctx = { 0 };
    struct chimera_vfs_module_cfg   module_cfgs[2];
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         cred;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
    uint8_t                         fh1[CHIMERA_VFS_FH_SIZE], fh2[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh1_len, fh2_len;
    struct chimera_vfs_open_handle *root_handle, *h1, *h2;

    chimera_log_init();
    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "/", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &cred, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(root_fh, ctx.fh, ctx.fh_len);
    root_fh_len = ctx.fh_len;

    root_handle = open_fh(&ctx, &cred, root_fh, root_fh_len);

    /* Create "foo" -> inode #1, capture its FH, then unlink the name. */
    h1      = create_file(&ctx, &cred, root_handle, "foo");
    fh1_len = ctx.fh_len;
    memcpy(fh1, ctx.fh, fh1_len);
    assert(do_remove(&ctx, &cred, root_handle, "foo") == CHIMERA_VFS_OK);

    /* Re-create "foo" -> inode #2 (same name, different object). */
    h2      = create_file(&ctx, &cred, root_handle, "foo");
    fh2_len = ctx.fh_len;
    memcpy(fh2, ctx.fh, fh2_len);
    assert(!(fh1_len == fh2_len && memcmp(fh1, fh2, fh1_len) == 0));
    TEST_PASS("recreate yields a distinct FH");

    /* Inode-scoped remove targeting the STALE inode #1 must be a no-op: the
     * name now resolves to inode #2, which must survive. */
    assert(do_remove_match(&ctx, &cred, root_handle, "foo", fh1, fh1_len) ==
           CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, &cred, root_handle, "foo") == CHIMERA_VFS_OK);
    assert(ctx.fh_len == fh2_len && memcmp(ctx.fh, fh2, fh2_len) == 0);
    TEST_PASS("match_fh against a stale FH leaves the recreated file intact");

    /* Inode-scoped remove targeting the CURRENT inode #2 unlinks it. */
    assert(do_remove_match(&ctx, &cred, root_handle, "foo", fh2, fh2_len) ==
           CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, &cred, root_handle, "foo") == CHIMERA_VFS_ENOENT);
    TEST_PASS("match_fh against the current FH unlinks the file");

    /* Release the open handles (both inodes are now unlinked) and tear the
     * module down so the allocator does not flag leaked references. */
    chimera_vfs_release(ctx.vfs_thread, h1);
    chimera_vfs_release(ctx.vfs_thread, h2);
    chimera_vfs_release(ctx.vfs_thread, root_handle);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "vfs_inode_scoped_remove_test: ALL PASS\n");
    return 0;
} /* main */
