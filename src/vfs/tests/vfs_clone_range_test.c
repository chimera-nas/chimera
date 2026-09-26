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
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
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
    uint64_t                        copied;
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

static void                        (*copy_dispatch_original)(
    struct chimera_vfs_request *,
    void *);
static struct chimera_vfs_request *copy_delayed_read;
static void                       *copy_delayed_private;
static struct chimera_claim_actor  copy_src_actor, copy_dst_actor;
static int                         copy_expect_owners, copy_delay_read, copy_write_fault;
static unsigned                    copy_reads, copy_writes;
static int                         copy_synthetic;
static uintptr_t                   copy_stack_origin;

/* Exercise the real read/write ownership gates and buffers, with a controllable
 * asynchronous boundary and backend write completion. */
static void
copy_dispatch(
    struct chimera_vfs_request *req,
    void                       *private_data)
{
    if (req->opcode == CHIMERA_VFS_OP_READ || req->opcode == CHIMERA_VFS_OP_WRITE) {
        if (copy_synthetic) {
            /* ASan can place address-taken locals on its fake stack, whose
             * allocation positions change even when callback depth is flat.
             * Inspect the actual call frame rather than a local's address. */
            uintptr_t stack_here = (uintptr_t) __builtin_frame_address(0);
            if (!copy_stack_origin) {
                copy_stack_origin = stack_here;
            }
            uintptr_t distance = stack_here > copy_stack_origin ?
                stack_here - copy_stack_origin : copy_stack_origin - stack_here;
            assert(distance < 32768);
        }
        assert(req->io_owner_valid == copy_expect_owners);
        if (copy_expect_owners) {
            const struct chimera_claim_actor *want = req->opcode == CHIMERA_VFS_OP_READ ?
                &copy_src_actor : &copy_dst_actor;
            assert(!memcmp(&req->io_owner, want, sizeof(*want)));
        }
        if (req->opcode == CHIMERA_VFS_OP_READ) {
            copy_reads++;
            if (copy_synthetic) {
                assert(evpl_iovec_alloc(req->thread->evpl, req->read.length,
                                        0, 1, 0, req->read.iov) == 1);
                memset(req->read.iov[0].data, 0x51, req->read.length);
                req->read.r_length           = req->read.length;
                req->read.r_niov             = 1;
                req->read.r_eof              = 0;
                req->read.r_attr.va_set_mask = 0;
                req->status                  = CHIMERA_VFS_OK;
                req->complete(req);
                return;
            }
            if (copy_delay_read) {
                copy_delay_read      = 0;
                copy_delayed_read    = req;
                copy_delayed_private = private_data;
                return;
            }
        } else {
            copy_writes++;
            if (copy_write_fault || copy_synthetic) {
                struct chimera_acl *acl = NULL;
                req->write.r_length = copy_write_fault == 1 ? 0 :
                    req->write.length - (copy_write_fault == 2);
                req->write.r_sync                  = CHIMERA_VFS_WRITE_FILESYNC;
                req->write.r_pre_attr.va_set_mask  = 0;
                req->write.r_post_attr.va_set_mask = 0;
                if (copy_synthetic) {
                    acl = calloc(1, chimera_acl_size(1));
                    assert(acl);
                    acl->num_aces                      = 1;
                    acl->aces[0].access_mask           = CHIMERA_ACE_READ_DATA;
                    req->write.r_post_attr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
                    req->write.r_post_attr.va_acl      = acl;
                }
                req->status = CHIMERA_VFS_OK;
                req->complete(req);
                free(acl);
                return;
            }
        }
    }
    copy_dispatch_original(req, private_data);
} /* copy_dispatch */

static void
copy_cb(
    enum chimera_vfs_error    status,
    uint64_t                  copied,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    if (copy_synthetic) {
        assert(post && (post->va_set_mask & CHIMERA_VFS_ATTR_ACL));
        assert(post->va_acl && post->va_acl->num_aces == 1);
        assert(post->va_acl->aces[0].access_mask == CHIMERA_ACE_READ_DATA);
    }
    ctx->status = status;
    ctx->copied = copied;
    ctx->done   = 1;
} /* copy_cb */

static void
write_data(
    struct test_ctx *,
    const struct chimera_vfs_cred *,
    struct chimera_vfs_open_handle *,
    uint64_t,
    const uint8_t *,
    uint32_t);
static void
read_verify(
    struct test_ctx *,
    const struct chimera_vfs_cred *,
    struct chimera_vfs_open_handle *,
    uint32_t,
    const uint8_t *);

static void
test_copy_fallback(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *src,
    struct chimera_vfs_open_handle *dst)
{
    struct chimera_vfs_module *module       = src->vfs_module;
    uint64_t                   capabilities = module->capabilities;
    struct chimera_vfs_module  other_module = *module;
    struct chimera_claim_actor src_actor = { 0 }, dst_actor = { 0 };
    const uint32_t             length  = 384 * 1024;
    uint8_t                   *pattern = malloc(length);

    assert(pattern);
    for (uint32_t i = 0; i < length; i++) {
        pattern[i] = (uint8_t) (i * 11 + 3);
    }
    write_data(ctx, cred, src, 0, pattern, length);

    copy_dispatch_original     = module->dispatch;
    module->dispatch           = copy_dispatch;
    module->capabilities      &= ~CHIMERA_VFS_CAP_COPY_RANGE;
    src_actor.owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    src_actor.owner.client_key = 123;
    src_actor.owner.owner_lo   = 41;
    dst_actor.owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    dst_actor.owner.client_key = 456;
    dst_actor.owner.owner_lo   = 42;
    copy_src_actor             = src_actor;
    copy_dst_actor             = dst_actor;
    copy_expect_owners         = 1;
    copy_delay_read            = 1;
    copy_reads                 = copy_writes = 0;
    chimera_vfs_copy_range_owned(ctx->vfs_thread, cred, src, 0, dst, 0,
                                 length, 0, 0, 0, &src_actor, &dst_actor,
                                 copy_cb, ctx);
    assert(!ctx->done && copy_delayed_read);
    /* The fallback owns value copies while its caller is free to discard the
     * actors it supplied. The destination actor has not been used yet. */
    memset(&src_actor, 0, sizeof(src_actor));
    memset(&dst_actor, 0, sizeof(dst_actor));
    copy_dispatch_original(copy_delayed_read, copy_delayed_private);
    copy_delayed_read = NULL;
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK && ctx->copied == length);
    assert(copy_reads == 2 && copy_writes == 2);
    TEST_PASS("copy fallback retains both endpoint actors across asynchronous read");

    copy_expect_owners = 0;
    for (copy_write_fault = 1; copy_write_fault <= 2; copy_write_fault++) {
        copy_reads = copy_writes = 0;
        chimera_vfs_copy_range(ctx->vfs_thread, cred, src, 0, dst, 0,
                               16, 0, 0, 0, copy_cb, ctx);
        wait_done(ctx);
        assert(ctx->status == CHIMERA_VFS_EIO && ctx->copied == 0);
        assert(copy_reads == 1 && copy_writes == 1);
    }
    copy_write_fault = 0;
    TEST_PASS("copy fallback rejects zero and short writes without looping or success");

    /* Distinct module endpoints must use the generic fallback even when the
     * destination advertises native COPY_RANGE. Their ordinary I/O still uses
     * the real mounts, so this also exercises buffer ownership through VFS. */
    dst->vfs_module = &other_module;
    chimera_vfs_copy_range(ctx->vfs_thread, cred, src, 0, dst, 0,
                           16, 0, 0, 0, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK && ctx->copied == 16);
    dst->vfs_module = module;
    TEST_PASS("legacy copy wrapper supports cross-module fallback without actor attribution");

    chimera_vfs_copy_range(ctx->vfs_thread, cred, src, length, dst, 0,
                           16, 0, 0, 0, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK && ctx->copied == 0);

    /* Generate bounded buffers without storing a large file: hundreds of
     * synchronous backend completions must not grow the callback stack. */
    copy_synthetic    = 1;
    copy_stack_origin = 0;
    copy_reads        = copy_writes = 0;
    chimera_vfs_copy_range(ctx->vfs_thread, cred, src, 0, dst, 0,
                           64 * 1024 * 1024, 0, 0, CHIMERA_VFS_ATTR_ACL, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK && ctx->copied == 64 * 1024 * 1024);
    assert(copy_reads == 256 && copy_writes == 256);
    copy_synthetic = 0;
    TEST_PASS("copy fallback bounds callback stack and retains callback-scoped ACLs across 256 chunks");

    module->capabilities = capabilities;
    module->dispatch     = copy_dispatch_original;
    read_verify(ctx, cred, dst, length, pattern);
    free(pattern);
} /* test_copy_fallback */

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

    chimera_vfs_write(ctx->vfs_thread, cred, h, offset, len, 1, 0, 0,
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

    chimera_vfs_read(ctx->vfs_thread, cred, h, 0, len, iov, READ_MAX_IOV, 0,
                     read_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->verify_ok);
} /* read_verify */

static void
test_anonymous_admission_views(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *root)
{
    struct chimera_vfs_open_handle *src   = create_file(ctx, cred, root, "view-src");
    struct chimera_vfs_open_handle *dst   = create_file(ctx, cred, root, "view-dst");
    struct chimera_vfs_state       *state = ctx->vfs->vfs_state;
    struct chimera_vfs_file_state  *src_file, *dst_file;
    struct chimera_vfs_claim        src_closing, src_blocker, dst_closing, dst_blocker;
    struct chimera_claim_actor      setup = { 0 };
    struct chimera_claim_owner      owner = { .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
                                              .client_key = 501,                      .owner_lo = 1 };
    const struct chimera_vfs_claim *src_excluded[] = { &src_closing };
    const struct chimera_vfs_claim *dst_excluded[] = { &dst_closing };
    struct chimera_vfs_io_view      src_view       = { .excluded = src_excluded, .num_excluded = 1 };
    struct chimera_vfs_io_view      dst_view       = { .excluded = dst_excluded, .num_excluded = 1 };
    struct chimera_vfs_io_view      anonymous      = { 0 };
    struct evpl_iovec               bytes, read_iov[READ_MAX_IOV];
    const uint8_t                   expected[] = "admission view";

    assert(src->vfs_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE);
    assert(evpl_iovec_alloc(ctx->evpl, sizeof(expected), 0, 1, 0, &bytes) == 1);
    memcpy(bytes.data, expected, sizeof(expected));
    /* Seed the fixture before installing claims, without warming the shared
     * anonymous cache whose admission is under test. */
    setup.owner = owner;
    chimera_vfs_write_owned(ctx->vfs_thread, cred, src, 0, sizeof(expected), 1,
                            0, 0, &bytes, 1, &setup, write_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    src_file = chimera_vfs_state_get(state, src->fh, src->fh_len, src->fh_hash, true);
    dst_file = chimera_vfs_state_get(state, dst->fh, dst->fh_len, dst->fh_hash, true);
    chimera_vfs_claim_init_nfs4_open(&src_closing, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R, &owner);
    owner.owner_lo++;
    chimera_vfs_claim_init_nfs4_open(&dst_closing, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &owner);
    owner.proto = CHIMERA_CLAIM_PROTO_SMB2;
    owner.client_key++;
    chimera_vfs_claim_init_smb_open(&src_blocker, CHIMERA_CLAIM_W, CHIMERA_CLAIM_R, &owner);
    owner.owner_lo++;
    chimera_vfs_claim_init_smb_open(&dst_blocker, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W, &owner);
    assert(chimera_vfs_claim_try_acquire(state, src_file, &src_closing, NULL) == CHIMERA_CLAIM_GRANTED);
    assert(chimera_vfs_claim_try_acquire(state, src_file, &src_blocker, NULL) == CHIMERA_CLAIM_GRANTED);
    assert(chimera_vfs_claim_try_acquire(state, dst_file, &dst_closing, NULL) == CHIMERA_CLAIM_GRANTED);
    assert(chimera_vfs_claim_try_acquire(state, dst_file, &dst_blocker, NULL) == CHIMERA_CLAIM_GRANTED);

    ctx->expect     = expected;
    ctx->expect_len = sizeof(expected);
    chimera_vfs_read_view(ctx->vfs_thread, cred, src, 0, sizeof(expected), read_iov,
                          READ_MAX_IOV, 0, &src_view, read_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES); /* nonexcluded SMB deny-R */
    chimera_vfs_write_view(ctx->vfs_thread, cred, dst, 0, sizeof(expected), 1, 0, 0,
                           &bytes, 1, &dst_view, write_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES); /* nonexcluded SMB deny-W */
    TEST_PASS("anonymous views retain unrelated cross-protocol READ/WRITE denies");

    chimera_vfs_copy_range_view(ctx->vfs_thread, cred, src, 0, dst, 0, sizeof(expected),
                                0, 0, 0, &anonymous, &anonymous, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES && ctx->copied == 0);
    TEST_PASS("native-capable anonymous COPY with no exclusions still enforces claims");

    chimera_vfs_claim_release(state, src_file, &src_blocker);
    chimera_vfs_read_view(ctx->vfs_thread, cred, src, 0, sizeof(expected), read_iov,
                          READ_MAX_IOV, 0, &src_view, read_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK && ctx->verify_ok);
    chimera_vfs_read(ctx->vfs_thread, cred, src, 0, sizeof(expected), read_iov,
                     READ_MAX_IOV, 0, read_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES); /* no exemption leaked into cache */

    chimera_vfs_copy_range_view(ctx->vfs_thread, cred, src, 0, dst, 0, sizeof(expected),
                                0, 0, 0, &src_view, &dst_view, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES && ctx->copied == 0);
    chimera_vfs_claim_release(state, dst_file, &dst_blocker);
    chimera_vfs_copy_range_view(ctx->vfs_thread, cred, src, 0, dst, 0, sizeof(expected),
                                0, 0, 0, &anonymous, &dst_view, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES && ctx->copied == 0);
    chimera_vfs_copy_range_view(ctx->vfs_thread, cred, src, 0, dst, 0, sizeof(expected),
                                0, 0, 0, &src_view, &anonymous, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES && ctx->copied == 0);
    chimera_vfs_copy_range_view(ctx->vfs_thread, cred, src, 0, dst, 0, sizeof(expected),
                                0, 0, 0, &src_view, &dst_view, copy_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK && ctx->copied == sizeof(expected));
    TEST_PASS("native-capable COPY enforces independent anonymous source/destination views");

    chimera_vfs_write(ctx->vfs_thread, cred, dst, 0, sizeof(expected), 1, 0, 0,
                      &bytes, 1, write_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EACCES);
    /* A cached scoped W must not turn an ordinary R into a W request. */
    read_verify(ctx, cred, dst, sizeof(expected), expected);
    assert(src_file->implicit_claim.admit_excluded == NULL);
    assert(src_file->implicit_claim.admit_num_excluded == 0);
    assert(dst_file->implicit_claim.admit_excluded == NULL);
    assert(dst_file->implicit_claim.admit_num_excluded == 0);
    TEST_PASS("scoped cache permissions neither escape to later callers nor overdeny reads");

    chimera_vfs_claim_release(state, src_file, &src_closing);
    chimera_vfs_claim_release(state, dst_file, &dst_closing);
    chimera_vfs_state_put(state, src_file);
    chimera_vfs_state_put(state, dst_file);
    evpl_iovec_release(ctx->evpl, &bytes);
    chimera_vfs_remove_at(ctx->vfs_thread, cred, root, "view-src", 8,
                          src->fh, src->fh_len, 0, 0, 0, NULL, remove_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_remove_at(ctx->vfs_thread, cred, root, "view-dst", 8,
                          dst->fh, dst->fh_len, 0, 0, 0, NULL, remove_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, src);
    chimera_vfs_release(ctx->vfs_thread, dst);
} /* test_anonymous_admission_views */

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
    chimera_vfs_clone_range(ctx->vfs_thread, cred, src, src_off, dst, dst_off,
                            len, 0, 0, clone_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* clone */

static unsigned native_dispatches, native_breaks;
static void     (*native_original)(
    struct chimera_vfs_request *,
    void *);

static void
native_gate_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    if (request->opcode == CHIMERA_VFS_OP_COPY_RANGE || request->opcode == CHIMERA_VFS_OP_CLONE_RANGE ||
        request->opcode == CHIMERA_VFS_OP_ALLOCATE) {
        native_dispatches++;
        request->status = CHIMERA_VFS_OK;
        if (request->opcode == CHIMERA_VFS_OP_COPY_RANGE) {
            request->copy_range.r_length                = request->copy_range.length;
            request->copy_range.r_pre_attr.va_set_mask  = 0;
            request->copy_range.r_post_attr.va_set_mask = 0;
        } else if (request->opcode == CHIMERA_VFS_OP_CLONE_RANGE) {
            request->clone_range.r_pre_attr.va_set_mask  = 0;
            request->clone_range.r_post_attr.va_set_mask = 0;
        } else {
            request->allocate.r_pre_attr.va_set_mask  = 0;
            request->allocate.r_post_attr.va_set_mask = 0;
        }
        request->complete(request);
        return;
    }
    native_original(request, private_data);
} /* native_gate_dispatch */

static void
native_gate_break(
    struct chimera_vfs_claim *claim,
    uint8_t                   used,
    void                     *private_data)
{
    (void) claim; (void) private_data;
    assert(used == 0);
    native_breaks++;
} /* native_gate_break */

static void
test_owned_native_gate(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *root,
    struct chimera_vfs_open_handle *src)
{
    struct chimera_vfs_open_handle *dst   = create_file(ctx, cred, root, "native-gate");
    struct chimera_vfs_state       *state = ctx->vfs->vfs_state;
    struct chimera_vfs_file_state  *file  = chimera_vfs_state_get(state, dst->fh,
                                                                  dst->fh_len, dst->fh_hash, true);
    struct chimera_claim_owner      victim = { .proto = CHIMERA_CLAIM_PROTO_FUSE, .client_key = 123 };
    struct chimera_claim_actor      actor  = { .owner = { .proto = CHIMERA_CLAIM_PROTO_SMB2, .client_key = 456 } }
    ;
    struct chimera_vfs_module      *module       = dst->vfs_module;
    unsigned int                    capabilities = module->capabilities;

    native_original       = module->dispatch;
    module->dispatch      = native_gate_dispatch;
    module->capabilities |= CHIMERA_VFS_CAP_COPY_RANGE | CHIMERA_VFS_CAP_CLONE_RANGE;
    for (int kind = 0; kind < 3; kind++) {
        struct chimera_vfs_claim cache;
        chimera_vfs_claim_init_fuse_grant(&cache, &victim);
        cache.break_cb = native_gate_break;
        assert(chimera_vfs_claim_try_acquire(state, file, &cache, NULL) == CHIMERA_CLAIM_GRANTED);
        native_dispatches = native_breaks = 0;
        if (kind == 2) {
            chimera_vfs_allocate_owned(ctx->vfs_thread, cred, dst, 0,
                                       4096, 0, 0, 0, &actor, clone_cb, ctx);
        } else if (kind == 1) {
            chimera_vfs_clone_range_owned(ctx->vfs_thread, cred, src, 0, dst, 0,
                                          4096, 0, 0, &actor, &actor, clone_cb, ctx);
        } else {
            chimera_vfs_copy_range_owned(ctx->vfs_thread, cred, src, 0, dst, 0,
                                         4096, 0, 0, 0, &actor, &actor, copy_cb, ctx);
        }
        assert(native_breaks == 1 && !native_dispatches && !ctx->done);
        chimera_vfs_claim_ack(&cache, 0);
        wait_done(ctx);
        assert(ctx->status == CHIMERA_VFS_OK && native_dispatches == 1);
        chimera_vfs_claim_release(state, file, &cache);
    }
    module->dispatch     = native_original;
    module->capabilities = capabilities;
    chimera_vfs_state_put(state, file);
    chimera_vfs_remove_at(ctx->vfs_thread, cred, root, "native-gate", 11,
                          dst->fh, dst->fh_len, 0, 0, 0, NULL, remove_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, dst);
    TEST_PASS("owned native COPY/CLONE/ALLOCATE await cache invalidation before backend dispatch");
} /* test_owned_native_gate */

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

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &cred, root_fh, root_fh_len, "test", 4,
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
    chimera_vfs_clone_range(ctx.vfs_thread, &cred, src_h, 100, dst_h, 0, 4096,
                            0, 0, clone_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_EINVAL);
    TEST_PASS("sub-cluster (non-4K-aligned) clone is rejected with EINVAL");

    test_anonymous_admission_views(&ctx, &cred, root_handle);
    test_owned_native_gate(&ctx, &cred, root_handle, src_h);
    test_copy_fallback(&ctx, &cred, src_h, dst_h);

    chimera_vfs_release(ctx.vfs_thread, src_h);
    chimera_vfs_release(ctx.vfs_thread, dst_h);

    /* Unlink the files so their (and the CoW-shared) block buffers are freed
     * before the module is torn down -- keeps LeakSanitizer quiet. */
    chimera_vfs_remove_at(ctx.vfs_thread, &cred, root_handle, "src", 3,
                          src_fh, src_fh_len, 0, 0, 0, NULL, remove_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    chimera_vfs_remove_at(ctx.vfs_thread, &cred, root_handle, "dst", 3,
                          dst_fh, dst_fh_len, 0, 0, 0, NULL, remove_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
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
