// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * memfs clone_range at 4 KiB (cluster) granularity.  memfs stores files in
 * larger internal blocks (64 KiB by default), so a clone that fully covers an
 * internal block is shared copy-on-write while partial / misaligned edges are
 * realised by read-modify-write.  This exercises both paths end-to-end and
 * byte-verifies the result, including that destination bytes outside the cloned
 * range are preserved.  Runs under Debug/ASan, the only coverage the path gets
 * outside the (Release-only) WPTS copy-offload cases that drive it over SMB.
 */

#include <stdio.h>
#include <stdlib.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
/* The pool lifecycle -- mkfs, mount, umount, rmfs -- is not a sequence and
 * is not expressible as one.  It comes from the core's per-op header, which
 * is where those four still live. */
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/tests/compound_test_util.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define BLOCK    (64 * 1024)
#define FILESIZE (96 * 1024)   /* 1.5 internal blocks */

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    struct chimera_vfs_open_handle *handle;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    const uint8_t                  *expect;     /* read verification */
    uint32_t                        expect_len;
    int                             verify_ok;
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

/* Create `name` under `dir` and return the open handle (kept open).  The
 * create handle carries the inode in vfs_private, exactly as the SMB create
 * path delivers to clone_range/copy_range. */
static struct chimera_vfs_open_handle *
create_file(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_open_handle       *oh;
    struct chimera_vfs_attrs              sattr;
    int                                   i_open;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_puthandle(cp, dir, CHIMERA_VFS_OPEN_INFERRED);
    i_open = chimera_vfs_compound_add_open(cp, name, (int) strlen(name),
                                           CHIMERA_VFS_OPEN_CREATE, 0, &sattr,
                                           CHIMERA_VFS_ATTR_FH, 0, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    assert(ctx->status == CHIMERA_VFS_OK);

    op = chimera_vfs_compound_op(cp, (uint32_t) i_open);
    assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
    memcpy(ctx->fh, op->attr.va_fh, op->attr.va_fh_len);
    ctx->fh_len = op->attr.va_fh_len;

    oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
    assert(oh != NULL);
    chimera_vfs_compound_free(cp);

    ctx->handle = oh;
    return oh;
} /* create_file */

static void
write_data(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *h,
    uint64_t                        offset,
    const uint8_t                  *buf,
    uint32_t                        len)
{
    struct chimera_vfs_compound *cp;
    struct evpl_iovec            iov;
    int                          niov;

    niov = evpl_iovec_alloc(ctx->evpl, len, 0, 1, 0, &iov);
    assert(niov == 1);
    memcpy(iov.data, buf, len);

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_write(cp, h, offset, len, 1, &iov, 1, 0, 0, NULL);

    ctx->status = compound_test_run(ctx->evpl, cp);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);

    /* memfs takes its own (SHARED) reference into the block buffers, so drop
     * the caller's reference on the staged write iovec. */
    evpl_iovec_release(ctx->evpl, &iov);
} /* write_data */

#define READ_MAX_IOV 64

static void
read_verify(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *h,
    uint32_t                        len,
    const uint8_t                  *expect)
{
    /* memfs read fills a caller-provided descriptor array with one zero-copy
     * iovec per block (so niov must cover every block the range spans). */
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    struct evpl_iovec                     iov[READ_MAX_IOV];
    struct evpl_iovec                    *got;
    uint32_t                              off = 0;
    int                                   i_read, ngot;

    cp     = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    i_read = chimera_vfs_compound_add_read(cp, h, 0, len, iov, READ_MAX_IOV, 0,
                                           NULL, NULL, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    assert(ctx->status == CHIMERA_VFS_OK);

    op             = chimera_vfs_compound_op(cp, (uint32_t) i_read);
    ctx->verify_ok = (op->read_len == len);

    for (int i = 0; i < op->niov; i++) {
        uint32_t n = op->iov[i].length;

        if (off + n > len) {
            n = len - off;
        }
        if (memcmp(op->iov[i].data, expect + off, n) != 0) {
            ctx->verify_ok = 0;
        }
        off += op->iov[i].length;
    }

    /* The read iovecs are the compound's until taken; take and release them. */
    chimera_vfs_compound_take_iov(cp, (uint32_t) i_read, &got, &ngot);
    if (ngot) {
        evpl_iovecs_release(ctx->evpl, got, ngot);
    }

    chimera_vfs_compound_free(cp);

    assert(ctx->verify_ok);
} /* read_verify */

/* CLONE_RANGE takes both objects from the caller and never addresses the
 * current one, so this is a sequence of exactly one op. */
static enum chimera_vfs_error
clone_range(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *src,
    uint64_t                        src_off,
    struct chimera_vfs_open_handle *dst,
    uint64_t                        dst_off,
    uint64_t                        len)
{
    struct chimera_vfs_compound *cp;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_clone_range(cp, src, src_off, dst, dst_off, len,
                                         0, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    chimera_vfs_compound_free(cp);

    return ctx->status;
} /* clone_range */

static void
clone(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *src,
    uint64_t                        src_off,
    struct chimera_vfs_open_handle *dst,
    uint64_t                        dst_off,
    uint64_t                        len)
{
    assert(clone_range(ctx, cred, src, src_off, dst, dst_off, len) ==
           CHIMERA_VFS_OK);
} /* clone */

/* Unlink `name` from `dir`; `child_fh` is the recall target remove_at would
 * otherwise resolve for itself. */
static void
remove_file(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    const uint8_t                  *child_fh,
    uint32_t                        child_fh_len)
{
    struct chimera_vfs_compound *cp;
    int                          i_remove;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_puthandle(cp, dir, CHIMERA_VFS_OPEN_INFERRED);
    i_remove = chimera_vfs_compound_add_remove(cp, name, (int) strlen(name),
                                               0, 0, 0);
    chimera_vfs_compound_op_set_remove_match(cp, (uint32_t) i_remove,
                                             child_fh, child_fh_len, 0, NULL);

    ctx->status = compound_test_run(ctx->evpl, cp);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
} /* remove_file */

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
    uint8_t                         src_fh[CHIMERA_VFS_FH_SIZE], dst_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        src_fh_len, dst_fh_len;
    struct chimera_vfs_open_handle *root_handle, *src_h, *dst_h;
    uint8_t                        *pat_a, *pat_b, *expect;

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

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "memfs", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    assert(compound_test_mount_root(ctx.vfs_thread, ctx.evpl, &cred, "test",
                                    root_fh, &root_fh_len) == CHIMERA_VFS_OK);

    assert(compound_test_open_fh(ctx.vfs_thread, ctx.evpl, &cred,
                                 root_fh, root_fh_len,
                                 CHIMERA_VFS_OPEN_INFERRED,
                                 &root_handle) == CHIMERA_VFS_OK);

    src_h = create_file(&ctx, &cred, root_handle, "src");
    memcpy(src_fh, ctx.fh, ctx.fh_len);
    src_fh_len = ctx.fh_len;
    dst_h      = create_file(&ctx, &cred, root_handle, "dst");
    memcpy(dst_fh, ctx.fh, ctx.fh_len);
    dst_fh_len = ctx.fh_len;

    /* Distinct byte patterns so a mis-copied byte is visible. */
    pat_a  = malloc(FILESIZE);
    pat_b  = malloc(FILESIZE);
    expect = malloc(FILESIZE);
    for (int i = 0; i < FILESIZE; i++) {
        pat_a[i] = (uint8_t) (i * 7 + 1);
        pat_b[i] = (uint8_t) (i * 3 + 200);
    }

    write_data(&ctx, &cred, src_h, 0, pat_a, FILESIZE);
    write_data(&ctx, &cred, dst_h, 0, pat_b, FILESIZE);

    /* Sanity: write+read roundtrip before any clone. */
    read_verify(&ctx, &cred, src_h, FILESIZE, pat_a);
    read_verify(&ctx, &cred, dst_h, FILESIZE, pat_b);
    TEST_PASS("write/read roundtrip");

    /* 1. Whole-block clone (CoW share fast path): src[0..64K) -> dst[0..64K). */
    clone(&ctx, &cred, src_h, 0, dst_h, 0, BLOCK);
    memcpy(expect, pat_a, BLOCK);              /* cloned */
    memcpy(expect + BLOCK, pat_b + BLOCK, FILESIZE - BLOCK); /* preserved */
    read_verify(&ctx, &cred, dst_h, FILESIZE, expect);
    TEST_PASS("whole internal-block clone shares CoW and preserves the tail");

    /* 2. Sub-block, non-zero offset clone (read-modify-write path):
     *    src[4K..12K) -> dst[68K..76K).  4 KiB aligned, well inside block 1. */
    clone(&ctx, &cred, src_h, 4 * 1024, dst_h, 68 * 1024, 8 * 1024);
    memcpy(expect + 68 * 1024, pat_a + 4 * 1024, 8 * 1024); /* cloned slice */
    read_verify(&ctx, &cred, dst_h, FILESIZE, expect);
    TEST_PASS("sub-block 4K-aligned clone RMWs and preserves both edges");

    /* 3. Clone straddling the internal-block boundary (partial both sides):
     *    src[60K..68K) -> dst[60K..68K). */
    clone(&ctx, &cred, src_h, 60 * 1024, dst_h, 60 * 1024, 8 * 1024);
    memcpy(expect + 60 * 1024, pat_a + 60 * 1024, 8 * 1024);
    read_verify(&ctx, &cred, dst_h, FILESIZE, expect);
    TEST_PASS("clone straddling the internal-block boundary RMWs correctly");

    /* 4. Misaligned offset/length must be rejected (POSIX FICLONERANGE). */
    assert(clone_range(&ctx, &cred, src_h, 100, dst_h, 0, 4096) ==
           CHIMERA_VFS_EINVAL);
    TEST_PASS("sub-cluster (non-4K-aligned) clone is rejected with EINVAL");

    chimera_vfs_release(ctx.vfs_thread, src_h);
    chimera_vfs_release(ctx.vfs_thread, dst_h);

    /* Unlink the files so their (and the CoW-shared) block buffers are freed
     * before the module is torn down -- keeps LeakSanitizer quiet. */
    remove_file(&ctx, &cred, root_handle, "src", src_fh, src_fh_len);
    remove_file(&ctx, &cred, root_handle, "dst", dst_fh, dst_fh_len);
    chimera_vfs_release(ctx.vfs_thread, root_handle);

    free(pat_a);
    free(pat_b);
    free(expect);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* umount does not return until every handle on the mount has been
     * closed and released, so the removal below succeeds immediately.  The
     * retry is kept as a backstop only. */
    for (int i = 0; i < 50; i++) {
        chimera_vfs_rmfs(ctx.vfs_thread, NULL, "memfs", "fs0", mount_cb, &ctx);
        wait_done(&ctx);
        if (ctx.status != CHIMERA_VFS_EBUSY) {
            break;
        }
        usleep(100000);
    }
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All memfs clone_range tests passed!\n");
    return 0;
} /* main */
