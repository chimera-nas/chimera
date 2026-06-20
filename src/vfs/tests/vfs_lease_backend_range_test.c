// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * CAP_LEASE authoritative, range-faithful byte-range arbitration.
 *
 * The VFS core projects each protocol byte-range lock to the authoritative
 * backend via chimera_vfs_lease_acquire_backend(), passing the real
 * kind/offset/length/owner.  This is the cross-node enforcement point: in a
 * multi-node deployment each node calls it, and the backend (here memfs)
 * arbitrates overlap conflicts between owners.  We exercise that backend API
 * directly (with two distinct owners on one memfs file) to prove ranges are
 * honored -- conflicting overlaps from different owners are denied, disjoint
 * ranges coexist, and a release frees the range for a previously-blocked owner.
 *
 * (The per-node local conflict matrix is tested separately in vfs_state_test;
 * it would short-circuit two owners in a single VFS instance before the backend
 * is consulted, so this test drives the backend op directly.)
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_lease_types.h"
#include "common/logging.h"
#include "prometheus-c.h"

static int passed = 0, failed = 0;
#define CHECK(cond, name) do { if (cond) { fprintf(stderr, "  PASS: %s\n", name); passed++; } \
                               else { fprintf(stderr, "  FAIL: %s\n", name); failed++; } } while (0)

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
    /* lease-acquire result */
    enum chimera_vfs_lease_result lresult;
    struct chimera_vfs_lease_mode   lgranted;
    uint64_t                        ltoken;
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

    (void) thread;
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

    (void) set_attr; (void) attr; (void) dir_pre; (void) dir_post;
    ctx->status = error_code;
    ctx->handle = oh;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh, oh->fh, oh->fh_len);
        ctx->fh_len = oh->fh_len;
    }
    ctx->done = 1;
} /* openat_cb */

static void
acquire_cb(
    enum chimera_vfs_error        error_code,
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease_mode granted,
    uint64_t                      token,
    void                         *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status   = error_code;
    ctx->lresult  = result;
    ctx->lgranted = granted;
    ctx->ltoken   = token;
    ctx->done     = 1;
} /* acquire_cb */

static void
release_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* release_cb */

/* Acquire a byte-range W lock [off,len) for owner `olo` directly at the backend;
 * returns the lease result and (out) the granted token. */
static enum chimera_vfs_lease_result
backend_lock(
    struct test_ctx *ctx,
    uint64_t         olo,
    uint64_t         off,
    uint64_t         len,
    uint8_t          modebits,
    uint64_t        *out_token)
{
    struct chimera_vfs_lease_mode mode = { modebits, 0 };

    chimera_vfs_lease_acquire_backend(ctx->vfs_thread, NULL, ctx->fh, ctx->fh_len,
                                      CHIMERA_VFS_LEASE_RANGE, mode, off, len,
                                      CHIMERA_VFS_LEASE_PROTO_NLM, olo, 0,
                                      acquire_cb, ctx);
    wait_done(ctx);
    if (out_token) {
        *out_token = ctx->ltoken;
    }
    return ctx->lresult;
} /* backend_lock */

static void
backend_unlock(
    struct test_ctx *ctx,
    uint64_t         token)
{
    struct chimera_vfs_lease_mode none = { 0, 0 };

    chimera_vfs_lease_release_backend(ctx->vfs_thread, NULL, ctx->fh, ctx->fh_len,
                                      token, none, 0, 0, release_cb, ctx);
    wait_done(ctx);
} /* backend_unlock */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx                 ctx = { 0 };
    struct chimera_vfs_cred         owner;
    struct chimera_vfs_module_cfg   module_cfgs[2];
    struct prometheus_metrics      *metrics;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
    struct chimera_vfs_open_handle *root_handle;
    struct chimera_vfs_attrs        sattr;
    uint64_t                        tok_a = 0, tok_b = 0;

    (void) argc; (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;
    chimera_vfs_cred_init_unix(&owner, 0, 0, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl       = evpl_create(NULL);
    ctx.vfs        = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 0, metrics);
    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "/", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* Resolve the share root and create a file to lock. */
    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &owner, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(root_fh, ctx.fh, ctx.fh_len);
    root_fh_len = ctx.fh_len;

    chimera_vfs_open_fh(ctx.vfs_thread, &owner, root_fh, root_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    root_handle = ctx.handle;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;
    chimera_vfs_open_at(ctx.vfs_thread, &owner, root_handle, "f", 1,
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    /* ctx.fh/fh_len now hold the file handle used by backend_lock(). */
    chimera_vfs_release(ctx.vfs_thread, ctx.handle);
    chimera_vfs_release(ctx.vfs_thread, root_handle);

    fprintf(stderr, "\nbackend range arbitration\n");

    /* Owner A takes a write lock on [0,100). */
    CHECK(backend_lock(&ctx, 0xAAAA, 0, 100, CHIMERA_VFS_LEASE_MODE_W, &tok_a)
          == CHIMERA_VFS_LEASE_GRANTED && tok_a != 0,
          "owner A W[0,100) granted");

    /* Owner B's overlapping write [50,150) must be DENIED. */
    CHECK(backend_lock(&ctx, 0xBBBB, 50, 100, CHIMERA_VFS_LEASE_MODE_W, NULL)
          == CHIMERA_VFS_LEASE_DENIED,
          "owner B overlapping W[50,150) denied");

    /* Owner B's disjoint write [200,100) coexists. */
    CHECK(backend_lock(&ctx, 0xBBBB, 200, 100, CHIMERA_VFS_LEASE_MODE_W, &tok_b)
          == CHIMERA_VFS_LEASE_GRANTED && tok_b != 0,
          "owner B disjoint W[200,300) granted");

    /* Owner B's overlapping read [0,100) still conflicts with A's write. */
    CHECK(backend_lock(&ctx, 0xBBBB, 0, 100, CHIMERA_VFS_LEASE_MODE_R, NULL)
          == CHIMERA_VFS_LEASE_DENIED,
          "owner B overlapping R[0,100) denied by A's write");

    /* A releases [0,100); now B can take it. */
    backend_unlock(&ctx, tok_a);
    uint64_t tok_c = 0;
    CHECK(backend_lock(&ctx, 0xBBBB, 0, 100, CHIMERA_VFS_LEASE_MODE_W, &tok_c)
          == CHIMERA_VFS_LEASE_GRANTED,
          "owner B W[0,100) granted after A releases");

    /* Release the locks still held so teardown is leak-clean. */
    backend_unlock(&ctx, tok_b);
    backend_unlock(&ctx, tok_c);

    fprintf(stderr, "\nResults: %d passed, %d failed\n", passed, failed);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    return failed == 0 ? 0 : 1;
} /* main */
