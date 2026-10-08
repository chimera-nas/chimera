// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Per-op blocking: a module's blocking_ops selects which ops are posted to a
 * sync delegation thread; every other op is dispatched inline on the caller.
 * A shim module, "splitblk", is memfs in every respect -- same capabilities,
 * same fh_magic -- except that its dispatch records the thread each opcode
 * ran on before handing the request to memfs.  Each phase sets the shim's
 * blocking_ops before the VFS is initialized, then runs
 * write, read, getattr and commit against one file and checks where each
 * was dispatched.  The attr cache is disabled so getattr reaches the shim.
 */

#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#undef NDEBUG
#include <assert.h>
#include "common/thread.h"

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/sdk/vfs_fh_magic.h"
#include "vfs/sdk/vfs_module.h"
#include "vfs/sdk/vfs_request.h"
#include "common/logging.h"
#include "common/macros.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define NUM_SYNC_THREADS 4
#define IO_LEN           4096

/* Built up by OR-ing ops in; a backend with asynchronous I/O but blocking
 * metadata takes the complement from CHIMERA_VFS_BLOCKING_ALL instead. */
#define READ_WRITE_OPS   (CHIMERA_VFS_OP_BIT(CHIMERA_VFS_OP_READ) | \
                          CHIMERA_VFS_OP_BIT(CHIMERA_VFS_OP_WRITE))

extern struct chimera_vfs_module vfs_memfs;

/* ---- the splitblk shim ------------------------------------------------- */

/* Written by whichever thread runs the dispatch, read by the test thread
 * after the op's completion has been delivered back to it. */
static evpl_thread_id_t          dispatch_tid[CHIMERA_VFS_OP_NUM];

static void
splitblk_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    dispatch_tid[request->opcode] = evpl_current_thread();
    vfs_memfs.dispatch(request, private_data);
} /* splitblk_dispatch */

/* Completed from vfs_memfs in run_phase() before the VFS is initialized;
 * found by chimera_vfs_init via dlsym("vfs_splitblk"), which is why the test
 * binary is linked with ENABLE_EXPORTS. */
SYMBOL_EXPORT struct chimera_vfs_module vfs_splitblk = {
    .sdk_version = CHIMERA_VFS_SDK_VERSION,
    .name        = "splitblk",
    .fh_magic    = CHIMERA_VFS_FH_MAGIC_MEMFS,
    .dispatch    = splitblk_dispatch,
};

/* ---- harness ------------------------------------------------------------ */

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    struct chimera_vfs_cred         cred;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
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
write_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* write_cb */

static void
read_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        assert(count == IO_LEN);
        evpl_iovecs_release(ctx->evpl, iov, niov);
    }
    ctx->done = 1;
} /* read_cb */

static void
getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* getattr_cb */

static void
commit_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* commit_cb */

/* Run one op against the file and report whether its dispatch ran off the
 * caller thread. */
static int
run_op(
    struct test_ctx *ctx,
    uint32_t         opcode)
{
    struct evpl_iovec iov[8];
    int               niov;

    dispatch_tid[opcode] = 0;

    switch (opcode) {
        case CHIMERA_VFS_OP_WRITE:
            niov = evpl_iovec_alloc(ctx->evpl, IO_LEN, 4096, 8, 0, iov);
            assert(niov > 0);
            for (int i = 0; i < niov; i++) {
                memset(evpl_iovec_data(&iov[i]), 0x5a, evpl_iovec_length(&iov[i]));
            }
            chimera_vfs_write(ctx->vfs_thread, &ctx->cred, ctx->handle, 0, IO_LEN,
                              CHIMERA_VFS_WRITE_FILESYNC, 0, 0, iov, niov,
                              write_cb, ctx);
            wait_done(ctx);
            evpl_iovecs_release(ctx->evpl, iov, niov);
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_vfs_read(ctx->vfs_thread, &ctx->cred, ctx->handle, 0, IO_LEN,
                             iov, 8, 0, read_cb, ctx);
            wait_done(ctx);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_vfs_getattr(ctx->vfs_thread, &ctx->cred, ctx->handle,
                                CHIMERA_VFS_ATTR_MASK_STAT, getattr_cb, ctx);
            wait_done(ctx);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_vfs_commit(ctx->vfs_thread, &ctx->cred, ctx->handle, 0, IO_LEN,
                               0, 0, commit_cb, ctx);
            wait_done(ctx);
            break;
        default:
            assert(0);
    } /* switch */

    assert(ctx->status == CHIMERA_VFS_OK);

    /* The op must actually have reached the backend. */
    assert(dispatch_tid[opcode] != 0);

    return !evpl_thread_equal(dispatch_tid[opcode], evpl_current_thread());
} /* run_op */

/* Expected placement of each op in a phase: 1 = delegated, 0 = inline. */
struct expect {
    int write;
    int read;
    int getattr;
    int commit;
};

static void
run_phase(
    const char          *label,
    uint64_t             blocking_ops,
    int                  num_sync_threads,
    const struct expect *expect)
{
    struct test_ctx                 ctx = { 0 };
    struct chimera_vfs_module_cfg   module_cfgs[2];
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_open_handle *mnt;
    struct chimera_vfs_attrs        sattr;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;

    fprintf(stderr, "Phase: %s (num_sync_delegation_threads=%d)\n",
            label, num_sync_threads);

    chimera_vfs_cred_init_unix(&ctx.cred, 0, 0, 0, NULL);

    vfs_splitblk.capabilities   = vfs_memfs.capabilities;
    vfs_splitblk.blocking_ops   = blocking_ops;
    vfs_splitblk.init           = vfs_memfs.init;
    vfs_splitblk.destroy        = vfs_memfs.destroy;
    vfs_splitblk.thread_init    = vfs_memfs.thread_init;
    vfs_splitblk.thread_destroy = vfs_memfs.thread_destroy;

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "splitblk", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    /* attr_cache_enabled = 0, name_cache_enabled = 0: getattr must reach the
     * backend rather than be answered from cache. */
    ctx.vfs = chimera_vfs_init(num_sync_threads, 0, module_cfgs, 2, "memkv", 60,
                               0, 0, 0, metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "splitblk", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "splitblk", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(ctx.vfs_thread->vfs, root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &ctx.cred, root_fh, root_fh_len,
                       "test", 4, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx.vfs_thread, &ctx.cred, ctx.fh, ctx.fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    mnt = ctx.handle;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    chimera_vfs_open_at(ctx.vfs_thread, &ctx.cred, mnt, "f", 1,
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx.vfs_thread, mnt);

    assert(run_op(&ctx, CHIMERA_VFS_OP_WRITE) == expect->write);
    assert(run_op(&ctx, CHIMERA_VFS_OP_READ) == expect->read);
    assert(run_op(&ctx, CHIMERA_VFS_OP_GETATTR) == expect->getattr);
    assert(run_op(&ctx, CHIMERA_VFS_OP_COMMIT) == expect->commit);

    chimera_vfs_release(ctx.vfs_thread, ctx.handle);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_rmfs(ctx.vfs_thread, NULL, "splitblk", "fs0", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    TEST_PASS(label);
} /* run_phase */

int
main(
    int    argc,
    char **argv)
{
    const struct expect read_write  = { 1, 1, 0, 0 };
    const struct expect commit_only = { 0, 0, 0, 1 };
    const struct expect metadata    = { 0, 0, 1, 1 };
    const struct expect all         = { 1, 1, 1, 1 };
    const struct expect none        = { 0, 0, 0, 0 };

    chimera_log_init();

    run_phase("read/write blocking delegates only read and write",
              READ_WRITE_OPS, NUM_SYNC_THREADS, &read_write);
    run_phase("commit blocking delegates only commit",
              CHIMERA_VFS_OP_BIT(CHIMERA_VFS_OP_COMMIT), NUM_SYNC_THREADS,
              &commit_only);
    run_phase("all but read/write delegates everything else",
              CHIMERA_VFS_BLOCKING_ALL & ~READ_WRITE_OPS, NUM_SYNC_THREADS,
              &metadata);
    run_phase("BLOCKING_ALL delegates every op",
              CHIMERA_VFS_BLOCKING_ALL, NUM_SYNC_THREADS, &all);
    run_phase("no blocking ops dispatches inline",
              0, NUM_SYNC_THREADS, &none);
    run_phase("blocking ops run inline without a sync pool",
              READ_WRITE_OPS, 0, &none);

    fprintf(stderr, "All split blocking tests passed!\n");
    return 0;
} /* main */
