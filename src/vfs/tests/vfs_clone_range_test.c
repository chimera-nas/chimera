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
    uint32_t         off = 0;

    ctx->status    = error_code;
    ctx->verify_ok = 0;

    if (error_code == CHIMERA_VFS_OK) {
        /* memfs returns one zero-copy iovec per block; gather and compare. */
        ctx->verify_ok = (count == ctx->expect_len);
        for (int i = 0; i < niov; i++) {
            uint32_t n = iov[i].length;
            if (off + n > ctx->expect_len) {
                n = ctx->expect_len - off;
            }
            if (memcmp(iov[i].data, ctx->expect + off, n) != 0) {
                ctx->verify_ok = 0;
            }
            off += iov[i].length;
        }
        /* The read iovecs are caller-owned references; release them. */
        if (niov) {
            evpl_iovecs_release(ctx->evpl, iov, niov);
        }
    }
    ctx->done = 1;
} /* read_cb */

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

static void
clone_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* clone_cb */

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
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    chimera_vfs_open_at(ctx->vfs_thread, cred, NULL, dir, name, strlen(name),
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
    chimera_vfs_open_fh(ctx->vfs_thread, cred, NULL, fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* open_fh */

static void
write_data(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *h,
    uint64_t                        offset,
    const uint8_t                  *buf,
    uint32_t                        len)
{
    struct evpl_iovec iov;
    int               niov;

    niov = evpl_iovec_alloc(ctx->evpl, len, 0, 1, 0, &iov);
    assert(niov == 1);
    memcpy(iov.data, buf, len);

    chimera_vfs_write(ctx->vfs_thread, cred, NULL, h, offset, len, 1, 0, 0,
                      &iov, 1, write_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

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
    struct evpl_iovec iov[READ_MAX_IOV];

    ctx->expect     = expect;
    ctx->expect_len = len;

    chimera_vfs_read(ctx->vfs_thread, cred, NULL, h, 0, len, iov, READ_MAX_IOV, 0,
                     read_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->verify_ok);
} /* read_verify */

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
    chimera_vfs_clone_range(ctx->vfs_thread, cred, NULL, src, src_off, dst, dst_off,
                            len, 0, 0, clone_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* clone */

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

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "/", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &cred, NULL, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(root_fh, ctx.fh, ctx.fh_len);
    root_fh_len = ctx.fh_len;

    root_handle = open_fh(&ctx, &cred, root_fh, root_fh_len);

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
    chimera_vfs_clone_range(ctx.vfs_thread, &cred, NULL, src_h, 100, dst_h, 0, 4096,
                            0, 0, clone_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_EINVAL);
    TEST_PASS("sub-cluster (non-4K-aligned) clone is rejected with EINVAL");

    chimera_vfs_release(ctx.vfs_thread, src_h);
    chimera_vfs_release(ctx.vfs_thread, dst_h);

    /* Unlink the files so their (and the CoW-shared) block buffers are freed
     * before the module is torn down -- keeps LeakSanitizer quiet. */
    chimera_vfs_remove_at(ctx.vfs_thread, &cred, NULL, root_handle, "src", 3,
                          src_fh, src_fh_len, 0, 0, NULL, remove_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    chimera_vfs_remove_at(ctx.vfs_thread, &cred, NULL, root_handle, "dst", 3,
                          dst_fh, dst_fh_len, 0, 0, NULL, remove_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx.vfs_thread, root_handle);

    free(pat_a);
    free(pat_b);
    free(expect);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All memfs clone_range tests passed!\n");
    return 0;
} /* main */
