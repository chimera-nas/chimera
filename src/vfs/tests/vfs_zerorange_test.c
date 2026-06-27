// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Backend-parameterized reproducer / regression test: a VFS zero_range
 * implemented as DEALLOCATE(range) + ALLOCATE(range) must leave the range
 * reading as zeros without disturbing neighbouring data.
 *
 * The Ubuntu 26.04 NFSv4.2 client implements FALLOC_FL_ZERO_RANGE as exactly
 * that op pair (confirmed by op-logging the server); older kernels lacked NFS
 * ZERO_RANGE, which is why only the 26.04 diskfs+NFSv4.2 fsx run fails.  A
 * single punch+alloc does not trip it -- the bug needs an extent map that has
 * accumulated a mix of written / unwritten / hole extents at sub-block-
 * unaligned boundaries.
 *
 * This is a small deterministic fsx-style model checker driven straight at the
 * VFS layer (no NFS, no KVM) so it runs in the dev container.  It keeps a
 * reference image and, after each pseudo-random (fixed-seed) write /
 * zero_range / punch at a sub-block range, reads the whole file back and
 * compares.  Run it per backend:
 *
 *     vfs_diskfs_zerorange_test <backend>
 *
 * where <backend> is memfs (default), diskfs_io_uring, diskfs_aio, linux,
 * io_uring, or cairn.  memfs/linux/etc. confirm the invariant holds (and that
 * the checker itself is sound); diskfs reproduces the bug.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "evpl/evpl_memory.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_attrs.h"
#include "vfs/vfs_cred.h"
#include "vfs/vfs_error.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define DEV_SIZE_BYTES (1024ULL * 1024ULL * 1024ULL) /* 1 GiB, sparse */
#define FILE_LEN       0x40000U   /* 256 KiB, matches fsx -l 262144 */
#define MAX_OP_LEN     0x10000U   /* 64 KiB max per op */
#define NUM_OPS        600        /* fsx tripped near op ~150 */
#define READ_CHUNK     0x10000U   /* read back in 64 KiB chunks */
#define READ_NIOV      64
#define WRITE_NIOV     16

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;     /* generic op result handle */
    struct chimera_vfs_open_handle *fhandle;    /* persistent file handle */
    uint8_t                        *readbuf;
    uint32_t                        read_dst;
    uint32_t                        readlen;
};

static uint64_t g_rng = 0x12345678ULL;

static uint32_t
rng(void)
{
    g_rng = g_rng * 6364136223846793005ULL + 1442695040888963407ULL;
    return (uint32_t) (g_rng >> 33);
} /* rng */

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
allocate_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* allocate_cb */

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

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        for (int i = 0; i < niov && off < ctx->readlen; i++) {
            uint32_t len = evpl_iovec_length(&iov[i]);

            if (len > ctx->readlen - off) {
                len = ctx->readlen - off;
            }
            memcpy(ctx->readbuf + ctx->read_dst + off, evpl_iovec_data(&iov[i]),
                   len);
            off += len;
        }
        evpl_iovecs_release(ctx->evpl, iov, niov);
    }
    ctx->done = 1;
} /* read_cb */

static void
do_write(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    uint64_t                       off,
    uint64_t                       len,
    uint8_t                        byte)
{
    struct evpl_iovec iov[WRITE_NIOV];
    int               niov;

    niov = evpl_iovec_alloc(ctx->evpl, (unsigned int) len, 4096, WRITE_NIOV, 0,
                            iov);
    assert(niov > 0);
    for (int i = 0; i < niov; i++) {
        memset(evpl_iovec_data(&iov[i]), byte, evpl_iovec_length(&iov[i]));
    }

    chimera_vfs_write(ctx->vfs_thread, cred, NULL, ctx->fhandle, off, (uint32_t) len,
                      CHIMERA_VFS_WRITE_FILESYNC, 0, 0, iov, niov, write_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    evpl_iovecs_release(ctx->evpl, iov, niov);
} /* do_write */

/* Issue one allocate (flags=0) or deallocate (CHIMERA_VFS_ALLOCATE_DEALLOCATE)
 * and return its status without asserting (used to probe support). */
static enum chimera_vfs_error
do_allocate(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    uint64_t                       off,
    uint64_t                       len,
    uint32_t                       flags)
{
    chimera_vfs_allocate(ctx->vfs_thread, cred, ctx->fhandle, off, len, flags,
                         0, 0, allocate_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_allocate */

/* Read [0, FILE_LEN) into ctx->readbuf (holes / EOF read as zero). */
static void
read_all(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred)
{
    memset(ctx->readbuf, 0, FILE_LEN);

    for (uint64_t off = 0; off < FILE_LEN; off += READ_CHUNK) {
        struct evpl_iovec iov[READ_NIOV];
        uint32_t          chunk = READ_CHUNK;

        if (chunk > FILE_LEN - off) {
            chunk = (uint32_t) (FILE_LEN - off);
        }
        ctx->read_dst = (uint32_t) off;
        ctx->readlen  = chunk;

        chimera_vfs_read(ctx->vfs_thread, cred, NULL, ctx->fhandle, off, chunk, iov,
                         READ_NIOV, 0, read_cb, ctx);
        wait_done(ctx);
        assert(ctx->status == CHIMERA_VFS_OK);
    }
} /* read_all */

/* Per-backend wiring: module configs + mount module/path, plus any on-disk
 * scratch (a diskfs device file; a host directory for the passthrough /
 * persistent backends).  module_cfgs[] always ends with the memkv KV backend. */
struct backend_spec {
    int         ncfg;
    const char *mount_module;
    char        mount_path[300];
};

static void
backend_configure(
    const char                    *backend,
    const char                    *session_dir,
    struct chimera_vfs_module_cfg *cfgs,
    struct backend_spec           *spec)
{
    char dev_path[300];
    char cfg[512];

    memset(spec, 0, sizeof(*spec));

    if (strcmp(backend, "memfs") == 0) {
        strncpy(cfgs[0].module_name, "memfs", sizeof(cfgs[0].module_name) - 1);
        spec->mount_module = "memfs";
        snprintf(spec->mount_path, sizeof(spec->mount_path), "/");
    } else if (strcmp(backend, "linux") == 0 ||
               strcmp(backend, "io_uring") == 0) {
        strncpy(cfgs[0].module_name, backend, sizeof(cfgs[0].module_name) - 1);
        spec->mount_module = (strcmp(backend, "linux") == 0) ? "linux" : "io_uring";
        snprintf(spec->mount_path, sizeof(spec->mount_path), "%s", session_dir);
    } else if (strcmp(backend, "cairn") == 0) {
        strncpy(cfgs[0].module_name, "cairn", sizeof(cfgs[0].module_name) - 1);
        snprintf(cfg, sizeof(cfg),
                 "{\"initialize\":true,\"path\":\"%s\"}", session_dir);
        strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
        spec->mount_module = "cairn";
        snprintf(spec->mount_path, sizeof(spec->mount_path), "/");
    } else if (strcmp(backend, "diskfs_io_uring") == 0 ||
               strcmp(backend, "diskfs_aio") == 0) {
        const char *iotype = (strcmp(backend, "diskfs_aio") == 0) ? "libaio"
                                                                  : "io_uring";
        int         fd, rc;

        snprintf(dev_path, sizeof(dev_path), "%s/device-0.img", session_dir);
        fd = open(dev_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
        assert(fd >= 0);
        rc = ftruncate(fd, (off_t) DEV_SIZE_BYTES);
        assert(rc == 0);
        close(fd);

        /* Pin a small (64 MiB) intent log: the default journal size is large
         * enough to not fit in this 1 GiB scratch device's first allocation
         * group, and the test does not need a big log. */
        snprintf(cfg, sizeof(cfg),
                 "{\"initialize\":true,\"unsafe_async\":true,"
                 "\"intent_log_size\":67108864,"
                 "\"devices\":[{\"type\":\"%s\",\"size\":1,\"path\":\"%s\"}]}",
                 iotype, dev_path);
        strncpy(cfgs[0].module_name, "diskfs", sizeof(cfgs[0].module_name) - 1);
        strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
        spec->mount_module = "diskfs";
        snprintf(spec->mount_path, sizeof(spec->mount_path), "/");
    } else {
        fprintf(stderr, "unknown backend: %s\n", backend);
        exit(2);
    }

    strncpy(cfgs[1].module_name, "memkv", sizeof(cfgs[1].module_name) - 1);
    spec->ncfg = 2;
} /* backend_configure */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct backend_spec           spec;
    struct prometheus_metrics    *metrics;
    struct chimera_vfs_cred       cred;
    struct chimera_vfs_attrs      sattr;
    const char                   *backend = argc > 1 ? argv[1] : "memfs";
    char                          tmpl[]  = "/tmp/vfs_zr_XXXXXX";
    char                         *session_dir;
    uint8_t                       root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      root_fh_len;
    uint8_t                       file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      file_fh_len;
    uint8_t                      *model;
    uint64_t                      first_bad_op  = 0;
    int                           failed        = 0;
    int                           alloc_support = 1;

    chimera_log_init();
    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);

    session_dir = mkdtemp(tmpl);
    assert(session_dir != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    backend_configure(backend, session_dir, module_cfgs, &spec);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, spec.ncfg, "memkv", 60, 1, 1, 0,
                               metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    ctx.readbuf = malloc(FILE_LEN);
    model       = calloc(1, FILE_LEN);
    assert(ctx.readbuf && model);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", spec.mount_module,
                      spec.mount_path, NULL, mount_cb, &ctx);
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

    chimera_vfs_open_fh(ctx.vfs_thread, &cred, NULL, root_fh, root_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    {
        struct chimera_vfs_open_handle *root_handle = ctx.handle;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0644;

        chimera_vfs_open_at(ctx.vfs_thread, &cred, NULL, root_handle, "f", 1,
                            CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                            0, 0, openat_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        memcpy(file_fh, ctx.fh, ctx.fh_len);
        file_fh_len = ctx.fh_len;
        chimera_vfs_release(ctx.vfs_thread, ctx.handle);
        chimera_vfs_release(ctx.vfs_thread, root_handle);
    }

    chimera_vfs_open_fh(ctx.vfs_thread, &cred, NULL, file_fh, file_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    ctx.fhandle = ctx.handle;

    /* Seed the whole file with a non-zero pattern so zeroed ranges are
     * distinguishable from stale data. */
    do_write(&ctx, &cred, 0, FILE_LEN, 0xff);
    memset(model, 0xff, FILE_LEN);

    /* Probe whether this backend supports allocate/deallocate.  If not, the
     * run still checks write/read integrity (no zero_range/punch ops). */
    if (do_allocate(&ctx, &cred, 0, 4096, CHIMERA_VFS_ALLOCATE_DEALLOCATE) ==
        CHIMERA_VFS_OK) {
        (void) do_allocate(&ctx, &cred, 0, 4096, 0);
    } else {
        alloc_support = 0;
        fprintf(stderr,
                "NOTE: backend '%s' does not support ALLOCATE; "
                "verifying writes only\n", backend);
    }
    /* Re-seed: the probe perturbed [0,4096). */
    do_write(&ctx, &cred, 0, FILE_LEN, 0xff);
    memset(model, 0xff, FILE_LEN);

    for (int op = 0; op < NUM_OPS && !failed; op++) {
        uint64_t off  = rng() % FILE_LEN;
        uint64_t len  = (rng() % MAX_OP_LEN) + 1;
        uint32_t kind = alloc_support ? (rng() % 4) : (rng() % 2);
        uint8_t  byte = (uint8_t) ((op % 254) + 1); /* non-zero fill */

        if (off + len > FILE_LEN) {
            len = FILE_LEN - off;
        }

        switch (kind) {
            case 0:
            case 1: /* WRITE (bias 2:1 so the file keeps real data) */
                do_write(&ctx, &cred, off, len, byte);
                memset(model + off, byte, len);
                break;
            case 2: /* ZERO_RANGE = DEALLOCATE + ALLOCATE (the 26.04 sequence) */
                assert(do_allocate(&ctx, &cred, off, len,
                                   CHIMERA_VFS_ALLOCATE_DEALLOCATE) == CHIMERA_VFS_OK);
                assert(do_allocate(&ctx, &cred, off, len, 0) == CHIMERA_VFS_OK);
                memset(model + off, 0, len);
                break;
            case 3: /* PUNCH_HOLE = DEALLOCATE only */
                assert(do_allocate(&ctx, &cred, off, len,
                                   CHIMERA_VFS_ALLOCATE_DEALLOCATE) == CHIMERA_VFS_OK);
                memset(model + off, 0, len);
                break;
            default:
                break;
        } /* switch */

        read_all(&ctx, &cred);
        if (memcmp(model, ctx.readbuf, FILE_LEN) != 0) {
            uint64_t i;

            for (i = 0; i < FILE_LEN; i++) {
                if (model[i] != ctx.readbuf[i]) {
                    break;
                }
            }
            fprintf(stderr,
                    "MISMATCH on '%s' after op %d (kind=%u off=0x%lx len=0x%lx): "
                    "byte 0x%lx expected 0x%02x got 0x%02x\n",
                    backend, op, kind, (unsigned long) off, (unsigned long) len,
                    (unsigned long) i, model[i], ctx.readbuf[i]);
            first_bad_op = op;
            failed       = 1;
        }
    }

    chimera_vfs_release(ctx.vfs_thread, ctx.fhandle);
    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);
    free(ctx.readbuf);
    free(model);

    if (failed) {
        fprintf(stderr,
                "FAIL [%s]: zero_range/punch returned stale data (op %lu)\n",
                backend, (unsigned long) first_bad_op);
        return 1;
    }

    fprintf(stderr, "PASS [%s]: %d write/%s ops verified\n", backend, NUM_OPS,
            alloc_support ? "zero_range/punch" : "(no-allocate)");
    return 0;
} /* main */
