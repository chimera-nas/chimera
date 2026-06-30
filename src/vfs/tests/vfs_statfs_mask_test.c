// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Regression test for the STATFS/FSID attribute-mask conflation.
 *
 * FSID is dual-purpose: it is part of the statfs response set, but it is also
 * an ordinary per-file attribute that ordinary stat-style requests carry (an
 * NFSv3 post-op attr request includes FSID).  Because FSID used to be the only
 * thing distinguishing CHIMERA_VFS_ATTR_MASK_STATFS from "a normal attr
 * request", backends that gated their (cold, lock-heavy) full statfs
 * computation on `va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS` ran that
 * computation on every LOOKUP/CREATE/WRITE reply -- turning a cold path hot.
 *
 * The fix introduces CHIMERA_VFS_ATTR_MASK_STATFS_VALUES (the statfs value
 * fields with FSID excluded) as the gate.  This test pins both halves:
 *
 *   1. The mask invariants, so nobody re-conflates FSID into the value gate.
 *   2. The observable behaviour on an engine-authoritative backend (memfs):
 *      an NFSv3-style request (stat fields + FSID) yields FSID but does NOT
 *      populate the statfs value fields, while a full statfs request does.
 */

#include <stdio.h>
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

/* The six statfs *value* bits, none of which is FSID. */
#define STATFS_VALUE_BITS ( \
            CHIMERA_VFS_ATTR_SPACE_AVAIL | \
            CHIMERA_VFS_ATTR_SPACE_FREE | \
            CHIMERA_VFS_ATTR_SPACE_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_FREE | \
            CHIMERA_VFS_ATTR_FILES_AVAIL)

/* An NFSv3 post-op attr request: the stat set plus FSID (this is exactly the
 * shape that used to spuriously trip the full-statfs gate). */
#define NFS3_LIKE_MASK    (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FSID)

/* (1) Compile-time mask invariants. */
_Static_assert((CHIMERA_VFS_ATTR_MASK_STATFS_VALUES & CHIMERA_VFS_ATTR_FSID) == 0,
               "MASK_STATFS_VALUES must NOT include FSID");
_Static_assert((CHIMERA_VFS_ATTR_MASK_STATFS & CHIMERA_VFS_ATTR_FSID) == CHIMERA_VFS_ATTR_FSID,
               "MASK_STATFS must still include FSID");
_Static_assert(CHIMERA_VFS_ATTR_MASK_STATFS_VALUES ==
               (CHIMERA_VFS_ATTR_MASK_STATFS & ~CHIMERA_VFS_ATTR_FSID),
               "MASK_STATFS_VALUES is MASK_STATFS with FSID cleared");
_Static_assert(CHIMERA_VFS_ATTR_MASK_STATFS_VALUES == STATFS_VALUE_BITS,
               "MASK_STATFS_VALUES is exactly the six value bits");

static void
test_mask_invariants(void)
{
    /* The bug: an NFSv3-style request DOES overlap the full statfs mask ... */
    assert((NFS3_LIKE_MASK & CHIMERA_VFS_ATTR_MASK_STATFS) != 0);
    /* ... but the fix means it does NOT overlap the value gate, so no backend
     * will do full statfs work for it. */
    assert((NFS3_LIKE_MASK & CHIMERA_VFS_ATTR_MASK_STATFS_VALUES) == 0);

    /* A real statfs request (any value bit) trips the value gate. */
    assert((CHIMERA_VFS_ATTR_MASK_STATFS & CHIMERA_VFS_ATTR_MASK_STATFS_VALUES) != 0);

    TEST_PASS("statfs/fsid mask invariants");
} /* test_mask_invariants */

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_attrs        attr;
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
getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        ctx->attr = *attr;
    }
    ctx->done = 1;
} /* getattr_cb */

/* getattr `handle` with `mask`, returning the resulting attrs in ctx->attr. */
static void
getattr(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    uint64_t                       mask)
{
    memset(&ctx->attr, 0, sizeof(ctx->attr));
    chimera_vfs_getattr(ctx->vfs_thread, cred, NULL, ctx->handle, mask,
                        getattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* getattr */

/* (2) Observable backend behaviour. */
static void
test_memfs_behaviour(void)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs           *vfs;
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    struct chimera_vfs_cred       cred;
    uint8_t                       root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      root_fh_len;

    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "/", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* Resolve the mounted share's root handle. */
    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &cred, NULL, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx.vfs_thread, &cred, NULL, ctx.fh, ctx.fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* NFSv3-style request: stat set + FSID.  FSID is satisfied, but the statfs
     * value fields are NOT computed (the hot-path win). */
    getattr(&ctx, &cred, NFS3_LIKE_MASK);
    assert(ctx.attr.va_set_mask & CHIMERA_VFS_ATTR_FSID);
    assert((ctx.attr.va_set_mask & STATFS_VALUE_BITS) == 0);
    TEST_PASS("NFSv3-style attrs (stat+FSID) populate FSID, not statfs values");

    /* FSID alone: same -- only va_fsid, no full statfs work. */
    getattr(&ctx, &cred, CHIMERA_VFS_ATTR_FSID);
    assert(ctx.attr.va_set_mask & CHIMERA_VFS_ATTR_FSID);
    assert((ctx.attr.va_set_mask & STATFS_VALUE_BITS) == 0);
    TEST_PASS("FSID alone populates only va_fsid");

    /* A genuine statfs request populates the value fields (and still FSID). */
    getattr(&ctx, &cred, CHIMERA_VFS_ATTR_MASK_STATFS);
    assert((ctx.attr.va_set_mask & STATFS_VALUE_BITS) == STATFS_VALUE_BITS);
    assert(ctx.attr.va_set_mask & CHIMERA_VFS_ATTR_FSID);
    TEST_PASS("full statfs request populates the statfs value set");

    chimera_vfs_release(ctx.vfs_thread, ctx.handle);
    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);
} /* test_memfs_behaviour */

int
main(
    int    argc,
    char **argv)
{
    chimera_log_init();

    test_mask_invariants();
    test_memfs_behaviour();

    fprintf(stderr, "All statfs-mask tests passed\n");
    return 0;
} /* main */
