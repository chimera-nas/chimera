// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Contract-semantics unit test for explicit VFS compounds
 * (CHIMERA_VFS_CAP_COMPOUND): begin/bind/eject/end lifecycle, the
 * enlisted/ejected/inflight counters, and end-callback synchronicity.
 *
 * One process, two mounts: cairn (the only CAP_COMPOUND backend) at /cairn
 * and memfs (non-capable) at /mem, so cross-mount ejection is exercised
 * for real.  Scenarios:
 *
 *   a. begin with no hint  -> non-NULL, unbound; end(COMMIT) and
 *      end(ABORT) are synchronous OK callbacks.
 *   b. begin with a memfs fh hint -> unbound; an op on memfs succeeds
 *      standalone (ejected_ops == 1, still unbound); end is sync OK.
 *   c. begin unbound; first op on the cairn mount lazily binds it
 *      (enlisted_ops == 1); a memfs op under the same compound ejects
 *      (ejected_ops == 1) but succeeds; end(COMMIT) OK; both effects
 *      visible afterwards (best-effort grouping, never all-or-nothing).
 *   d. begin with a cairn fh hint -> eagerly bound; two sequential ops
 *      -> enlisted_ops == 2 with inflight_ops back to 0 after each
 *      completion; end(COMMIT_DURABLE) OK and the mutations are visible
 *      via plain lookup/getattr afterwards.
 *   e. chimera_vfs_compound_loose(): per-thread singleton; every op
 *      ejects (never binds); end is a synchronous OK and does not
 *      invalidate it (same pointer comes back, still usable).
 *   f. a LOOSE-flagged begin behaves like (e) but is a distinct object
 *      from the singleton.
 *   g. a RETRYABLE compound that suffered an ejection but no engine
 *      conflict still commits OK (the EXHAUSTED rewrite only applies to
 *      a conflict raised by the engine).
 *   h. mutating vs read-only ejection accounting: bound to cairn, a memfs
 *      getattr ejects read-only (bumps ejected_ops only) and a memfs
 *      mkdir ejects mutating (bumps ejected_ops AND ejected_mutating_ops,
 *      the counter the conflict-replay veto keys on); the binding is
 *      unaffected and end(COMMIT) is OK.
 *   i. data read-your-writes on cairn: a file created under a compound,
 *      an enlisted WRITE then -- strictly after its completion -- an
 *      enlisted READ of the same range through the same compound returns
 *      the just-written bytes BEFORE the compound ends; after
 *      end(COMMIT_DURABLE) a plain standalone read returns them too.
 *   j. widened procs: set_xattr/get_xattr take the compound parameter
 *      (after cred); a pair on a cairn file inside one compound enlists
 *      (enlisted_ops += 2) and round-trips; the same pair with a NULL
 *      compound still works standalone.
 *   k. single-op fold liveness: begin cairn-hinted, one mkdir, then
 *      end(COMMIT_DURABLE) -- and again with end(COMMIT).  The end
 *      callback must fire (the harness pumps until it does) and the dir
 *      must persist: the regression canary for the backend's
 *      deferred-completion (cycle-commit) machinery -- a hang here means
 *      the cycle commit never scheduled.
 *
 * Deterministic: single VFS thread, no delegation pools, completions are
 * pumped with evpl_continue() -- no wall-clock waits.  The conflict path
 * (CHIMERA_VFS_ECOMPOUND_CONFLICT -> RETRYABLE replay, and its rewrite to
 * CHIMERA_VFS_ECOMPOUND_EXHAUSTED once ejected_ops != 0) requires
 * cross-thread contention on the cairn engine and is intentionally NOT
 * covered here.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
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
    uint64_t                        last_mode;
    const uint8_t                  *expect;      /* read_cb comparison buffer */
    uint32_t                        expect_len;
    int                             verify_ok;
    uint32_t                        xattr_len;   /* get_xattr_cb value length */
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
    if (error_code == CHIMERA_VFS_OK && attr) {
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
mkdir_at_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* mkdir_at_cb */

static void
getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK && attr) {
        ctx->last_mode = attr->va_mode;
    }
    ctx->done = 1;
} /* getattr_cb */

static void
end_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* end_cb */

static void
openat_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
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
        /* The backend may return one zero-copy iovec per block; gather and
         * compare against the expected pattern. */
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
set_xattr_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* set_xattr_cb */

static void
get_xattr_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status    = error_code;
    ctx->xattr_len = value_len;
    ctx->done      = 1;
} /* get_xattr_cb */

/* Resolve `name` under the fh in parent_fh into ctx->fh. */
static void
lookup_child(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *parent_fh,
    uint32_t                       parent_fh_len,
    const char                    *name)
{
    chimera_vfs_lookup(ctx->vfs_thread, cred, NULL, parent_fh, parent_fh_len,
                       name, (int) strlen(name),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* lookup_child */

/* Standalone (NULL-compound) resolve of `name` under parent_fh; returns 1 if
 * it exists, 0 on NOENT.  Unlike lookup_child it does not assert OK, so it can
 * probe whether a mid-compound mutation is already visible to an outside
 * observer (the Pillai non-atomic scenario). */
static int
lookup_present(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *parent_fh,
    uint32_t                       parent_fh_len,
    const char                    *name)
{
    chimera_vfs_lookup(ctx->vfs_thread, cred, NULL, parent_fh, parent_fh_len,
                       name, (int) strlen(name),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK || ctx->status == CHIMERA_VFS_ENOENT);
    return ctx->status == CHIMERA_VFS_OK;
} /* lookup_present */

static struct chimera_vfs_open_handle *
open_handle(
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
} /* open_handle */

/* One mkdir under `parent`, optionally enlisted in `compound`; asserts OK. */
static void
mkdir_under(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *parent,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;

    chimera_vfs_mkdir_at(ctx->vfs_thread, cred, compound, parent,
                         name, (int) strlen(name), &sattr,
                         CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                         0, 0, mkdir_at_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* mkdir_under */

/* Create `name` under `dir` (a regular file), optionally enlisted in
 * `compound`; returns the open handle (kept open) and leaves the new fh in
 * ctx->fh. */
static struct chimera_vfs_open_handle *
create_file(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    chimera_vfs_open_at(ctx->vfs_thread, cred, compound, dir,
                        name, (int) strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sattr,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* create_file */

/* One awaited write of buf[0..len) at `offset`, optionally enlisted in
 * `compound`; asserts OK.  Unstable (sync == 0): durability is the
 * compound end's business, which is exactly what scenario (i) exercises. */
static void
write_data(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_compound    *compound,
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

    chimera_vfs_write(ctx->vfs_thread, cred, compound, h, offset, len,
                      0, 0, 0, &iov, 1, write_cb, ctx);
    wait_done(ctx);
    if (ctx->status != CHIMERA_VFS_OK) {
        fprintf(stderr, "write_data: status %d\n", (int) ctx->status);
    }
    assert(ctx->status == CHIMERA_VFS_OK);

    /* The backend takes its own reference into the data buffers; drop the
     * caller's reference on the staged write iovec. */
    evpl_iovec_release(ctx->evpl, &iov);
} /* write_data */

#define TEST_READ_MAX_IOV 8

/* One awaited read of len bytes at `offset`, optionally enlisted in
* `compound`; asserts OK and that the bytes equal expect[0..len). */
static void
read_verify(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *h,
    uint64_t                        offset,
    const uint8_t                  *expect,
    uint32_t                        len)
{
    struct evpl_iovec iov[TEST_READ_MAX_IOV];

    ctx->expect     = expect;
    ctx->expect_len = len;

    chimera_vfs_read(ctx->vfs_thread, cred, compound, h, offset, len,
                     iov, TEST_READ_MAX_IOV, 0, read_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->verify_ok);
} /* read_verify */

/* set_xattr(name=value) then get_xattr(name), both optionally enlisted in
 * `compound` (the widened signatures: compound right after cred, NULL
 * permitted); asserts the value round-trips. */
static void
xattr_roundtrip(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *h,
    const char                     *name,
    const char                     *value)
{
    char     buf[64];
    uint32_t namelen   = (uint32_t) strlen(name);
    uint32_t value_len = (uint32_t) strlen(value);

    assert(value_len <= sizeof(buf));

    chimera_vfs_set_xattr(ctx->vfs_thread, cred, compound, h,
                          CHIMERA_VFS_XATTR_EITHER, name, namelen,
                          value, value_len, set_xattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    memset(buf, 0, sizeof(buf));
    ctx->xattr_len = 0;
    chimera_vfs_get_xattr(ctx->vfs_thread, cred, compound, h,
                          name, namelen, buf, (uint32_t) sizeof(buf),
                          get_xattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->xattr_len == value_len);
    assert(memcmp(buf, value, value_len) == 0);
} /* xattr_roundtrip */

/* End a compound that the contract promises to complete SYNCHRONOUSLY
 * (unbound / LOOSE): the callback must have run before end returns, with
 * no event-loop pump. */
static void
end_sync_ok(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_compound   *compound,
    enum chimera_vfs_compound_end  end_flag)
{
    ctx->done = 0;
    chimera_vfs_compound_end(ctx->vfs_thread, cred, compound, end_flag,
                             end_cb, ctx);
    assert(ctx->done == 1);     /* synchronous completion, nothing pumped */
    assert(ctx->status == CHIMERA_VFS_OK);
    ctx->done = 0;
} /* end_sync_ok */

/* End a BOUND compound: the commit/abort is dispatched, so pump until the
 * callback lands; asserts OK. */
static void
end_wait_ok(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_compound   *compound,
    enum chimera_vfs_compound_end  end_flag)
{
    chimera_vfs_compound_end(ctx->vfs_thread, cred, compound, end_flag,
                             end_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* end_wait_ok */

/* (a) begin with no hint: non-NULL, unbound, zeroed counters; end is a
 * synchronous OK for both COMMIT and ABORT. */
static void
test_begin_no_hint(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred, NULL, 0,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module == NULL);           /* unbound */
    assert(!(compound->flags & CHIMERA_VFS_COMPOUND_LOOSE));
    assert(compound->enlisted_ops == 0);
    assert(compound->ejected_ops == 0);
    assert(compound->ejected_mutating_ops == 0);
    assert(compound->inflight_ops == 0);

    end_sync_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    /* ABORT on a never-bound compound is equally synchronous. */
    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred, NULL, 0,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL && compound->module == NULL);
    end_sync_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_ABORT);

    TEST_PASS("begin with no hint is non-NULL/unbound; end is a synchronous OK");
} /* test_begin_no_hint */

/* (b) begin hinted at a non-capable mount (memfs): unbound; an op on that
* mount succeeds standalone and only bumps ejected_ops; end is sync OK. */
static void
test_non_capable_hint(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *mem_fh,
    uint32_t                        mem_fh_len,
    struct chimera_vfs_open_handle *mem_root)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          mem_fh, mem_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module == NULL);            /* memfs cannot bind it */

    mkdir_under(ctx, cred, compound, mem_root, "b1");

    assert(compound->module == NULL);            /* still unbound */
    assert(compound->enlisted_ops == 0);
    assert(compound->ejected_ops == 1);
    assert(compound->ejected_mutating_ops == 1); /* mkdir mutates */
    assert(compound->inflight_ops == 0);

    end_sync_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    /* The ejected op committed standalone: its effect is visible. */
    lookup_child(ctx, cred, mem_fh, mem_fh_len, "b1");

    TEST_PASS("memfs-hinted begin stays unbound; op ejects (ejected_ops == 1) and commits standalone");
} /* test_non_capable_hint */

/* (c) lazy bind + cross-mount ejection: an unbound compound binds at its
 * first op on the cairn mount; a memfs op under the same compound ejects
 * but still succeeds -- the best-effort grouping contract. */
static void
test_lazy_bind_and_eject(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root,
    const uint8_t                  *mem_fh,
    uint32_t                        mem_fh_len,
    struct chimera_vfs_open_handle *mem_root)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred, NULL, 0,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module == NULL);

    /* First op against the CAP_COMPOUND mount binds the compound. */
    mkdir_under(ctx, cred, compound, cairn_root, "c1");

    assert(compound->module != NULL);
    assert(strcmp(compound->module->name, "cairn") == 0);
    assert(compound->enlisted_ops == 1);
    assert(compound->ejected_ops == 0);
    assert(compound->ejected_mutating_ops == 0);
    assert(compound->inflight_ops == 0);

    /* Same compound, other mount: the op ejects and runs standalone. */
    mkdir_under(ctx, cred, compound, mem_root, "c2");

    assert(strcmp(compound->module->name, "cairn") == 0);   /* ownership fixed */
    assert(compound->enlisted_ops == 1);
    assert(compound->ejected_ops == 1);
    assert(compound->ejected_mutating_ops == 1);        /* mkdir mutates */
    assert(compound->inflight_ops == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    /* Both effects visible: the enlisted op committed with the compound,
     * the ejected op committed independently. */
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "c1");
    lookup_child(ctx, cred, mem_fh, mem_fh_len, "c2");

    TEST_PASS("first cairn op lazily binds (enlisted_ops == 1); cross-mount op ejects yet succeeds");
} /* test_lazy_bind_and_eject */

/* (d) eager bind from a cairn hint; two sequential enlisted ops; durable
 * commit; effects visible to plain (non-compound) lookup/getattr. */
static void
test_eager_bind_durable(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root)
{
    struct chimera_vfs_compound    *compound;
    struct chimera_vfs_open_handle *dh;
    uint8_t                         d1_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        d1_fh_len;

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module != NULL);           /* eagerly bound */
    assert(strcmp(compound->module->name, "cairn") == 0);
    assert(compound->enlisted_ops == 0);
    assert(compound->ejected_ops == 0);
    assert(compound->inflight_ops == 0);

    /* Sequential issue-after-completion chaining, per the ordering
     * contract: the second op goes out only after the first completed. */
    mkdir_under(ctx, cred, compound, cairn_root, "d1");
    assert(compound->enlisted_ops == 1);
    assert(compound->inflight_ops == 0);

    mkdir_under(ctx, cred, compound, cairn_root, "d2");
    assert(compound->enlisted_ops == 2);
    assert(compound->inflight_ops == 0);
    assert(compound->ejected_ops == 0);
    assert(compound->ejected_mutating_ops == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT_DURABLE);

    /* The mutations are visible through plain reads afterwards. */
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "d1");
    memcpy(d1_fh, ctx->fh, ctx->fh_len);
    d1_fh_len = ctx->fh_len;
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "d2");

    dh             = open_handle(ctx, cred, d1_fh, d1_fh_len);
    ctx->last_mode = 0;
    chimera_vfs_getattr(ctx->vfs_thread, cred, NULL, dh,
                        CHIMERA_VFS_ATTR_MASK_STAT, getattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert((ctx->last_mode & S_IFMT) == S_IFDIR);
    chimera_vfs_release(ctx->vfs_thread, dh);

    TEST_PASS("cairn-hinted begin binds eagerly; enlisted_ops == 2, inflight drains; COMMIT_DURABLE visible");
} /* test_eager_bind_durable */

/* (e) the per-thread LOOSE singleton: never binds, every op ejects, end is
 * a synchronous OK that does not invalidate it. */
static void
test_loose_singleton(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *cairn_root)
{
    struct chimera_vfs_compound *loose;
    uint32_t                     ejected_before;

    loose = chimera_vfs_compound_loose(ctx->vfs_thread);
    assert(loose != NULL);
    assert(loose->flags & CHIMERA_VFS_COMPOUND_LOOSE);
    assert(loose->module == NULL);

    ejected_before = loose->ejected_ops;

    /* Two ops on the CAP_COMPOUND mount: LOOSE means they must eject
     * rather than bind. */
    mkdir_under(ctx, cred, loose, cairn_root, "e1");
    mkdir_under(ctx, cred, loose, cairn_root, "e2");

    assert(loose->module == NULL);              /* never bound */
    assert(loose->enlisted_ops == 0);
    assert(loose->ejected_ops == ejected_before + 2);
    assert(loose->inflight_ops == 0);

    end_sync_ok(ctx, cred, loose, CHIMERA_VFS_COMPOUND_COMMIT);

    /* The singleton survives end: same object, still usable. */
    assert(chimera_vfs_compound_loose(ctx->vfs_thread) == loose);

    mkdir_under(ctx, cred, loose, cairn_root, "e3");
    assert(loose->module == NULL);
    assert(loose->ejected_ops == ejected_before + 3);

    end_sync_ok(ctx, cred, loose, CHIMERA_VFS_COMPOUND_COMMIT);
    assert(chimera_vfs_compound_loose(ctx->vfs_thread) == loose);

    TEST_PASS("loose singleton ejects everything, never binds, and survives end");
} /* test_loose_singleton */

/* (f) a LOOSE-flagged begin: same semantics as the singleton -- unbound
 * despite a cairn hint, every op ejects -- but a distinct object. */
static void
test_loose_flagged_begin(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          CHIMERA_VFS_COMPOUND_LOOSE);
    assert(compound != NULL);
    assert(compound != chimera_vfs_compound_loose(ctx->vfs_thread));
    assert(compound->flags & CHIMERA_VFS_COMPOUND_LOOSE);
    assert(compound->module == NULL);           /* LOOSE beats the cairn hint */

    mkdir_under(ctx, cred, compound, cairn_root, "f1");

    assert(compound->module == NULL);
    assert(compound->enlisted_ops == 0);
    assert(compound->ejected_ops == 1);
    assert(compound->inflight_ops == 0);

    end_sync_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "f1");

    TEST_PASS("LOOSE-flagged begin is a distinct unbound object; its ops eject");
} /* test_loose_flagged_begin */

/* (g) The RETRYABLE surface reachable without cross-thread contention: an
 * ejection alone never turns a conflict-free COMMIT into an error.
 * ECOMPOUND_CONFLICT (and its ECOMPOUND_EXHAUSTED rewrite once
 * ejected_ops != 0) only originate from an engine conflict, which needs a
 * competing thread and is out of scope for this test. */
static void
test_retryable_eject_commit_ok(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root,
    const uint8_t                  *mem_fh,
    uint32_t                        mem_fh_len,
    struct chimera_vfs_open_handle *mem_root)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          CHIMERA_VFS_COMPOUND_RETRYABLE);
    assert(compound != NULL);
    assert(compound->flags & CHIMERA_VFS_COMPOUND_RETRYABLE);
    assert(compound->module != NULL);            /* eagerly bound */

    mkdir_under(ctx, cred, compound, cairn_root, "g1");
    assert(compound->enlisted_ops == 1);

    mkdir_under(ctx, cred, compound, mem_root, "g2");
    assert(compound->ejected_ops == 1);
    assert(compound->ejected_mutating_ops == 1); /* forfeits replay */
    assert(compound->inflight_ops == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "g1");
    lookup_child(ctx, cred, mem_fh, mem_fh_len, "g2");

    TEST_PASS("RETRYABLE compound with an ejection still commits OK absent an engine conflict");
} /* test_retryable_eject_commit_ok */

/* (h) mutating vs read-only ejection accounting: on a cairn-bound compound,
 * a getattr on the memfs mount is a read-only ejection (ejected_ops only)
 * and a mkdir on memfs is a mutating ejection (ejected_ops AND
 * ejected_mutating_ops -- the counter the conflict-replay veto keys on).
 * Neither disturbs the binding. */
static void
test_mutating_vs_readonly_eject(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    const uint8_t                  *mem_fh,
    uint32_t                        mem_fh_len,
    struct chimera_vfs_open_handle *mem_root)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module != NULL);           /* eagerly bound */
    assert(strcmp(compound->module->name, "cairn") == 0);
    assert(compound->ejected_ops == 0);
    assert(compound->ejected_mutating_ops == 0);

    /* Read-only cross-mount op: ejects, but does not forfeit replay. */
    ctx->last_mode = 0;
    chimera_vfs_getattr(ctx->vfs_thread, cred, compound, mem_root,
                        CHIMERA_VFS_ATTR_MASK_STAT, getattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert((ctx->last_mode & S_IFMT) == S_IFDIR);

    assert(compound->enlisted_ops == 0);
    assert(compound->ejected_ops == 1);
    assert(compound->ejected_mutating_ops == 0);        /* read-only eject */
    assert(compound->inflight_ops == 0);

    /* Mutating cross-mount op: ejects and bumps both counters. */
    mkdir_under(ctx, cred, compound, mem_root, "h1");

    assert(compound->enlisted_ops == 0);
    assert(compound->ejected_ops == 2);
    assert(compound->ejected_mutating_ops == 1);
    assert(compound->inflight_ops == 0);
    assert(compound->module != NULL);           /* still bound to cairn */
    assert(strcmp(compound->module->name, "cairn") == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    /* The mutating ejection committed standalone: its effect is visible. */
    lookup_child(ctx, cred, mem_fh, mem_fh_len, "h1");

    TEST_PASS("read-only eject bumps ejected_ops only; mutating eject bumps ejected_mutating_ops too");
} /* test_mutating_vs_readonly_eject */

/* (i) data read-your-writes on cairn: a file created under the compound, an
 * enlisted WRITE, then -- strictly after its completion -- an enlisted READ
 * of the same range through the same compound returns the just-written
 * bytes BEFORE the compound ends.  After end(COMMIT_DURABLE) a plain
 * standalone read (fresh handle, NULL compound) returns them too. */
#define RYW_LEN 64

static void
test_data_read_your_writes(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root)
{
    struct chimera_vfs_compound    *compound;
    struct chimera_vfs_open_handle *h;
    struct chimera_vfs_open_handle *h2;
    uint8_t                         pattern[RYW_LEN];
    uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        file_fh_len;
    uint32_t                        base_enlisted;
    uint32_t                        base_ejected;

    for (int i = 0; i < RYW_LEN; i++) {
        pattern[i] = (uint8_t) (0xA5 ^ (i * 7));
    }

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module != NULL);           /* eagerly bound */
    assert(strcmp(compound->module->name, "cairn") == 0);

    h = create_file(ctx, cred, compound, cairn_root, "i_file");

    base_enlisted = compound->enlisted_ops;
    base_ejected  = compound->ejected_ops;

    /* Enlisted write; the read is issued only after its completion. */
    write_data(ctx, cred, compound, h, 0, pattern, RYW_LEN);
    assert(compound->enlisted_ops == base_enlisted + 1);
    assert(compound->ejected_ops == base_ejected);
    assert(compound->inflight_ops == 0);

    /* Read-your-writes through the same compound, before it ends. */
    read_verify(ctx, cred, compound, h, 0, pattern, RYW_LEN);
    assert(compound->enlisted_ops == base_enlisted + 2);
    assert(compound->ejected_ops == base_ejected);
    assert(compound->inflight_ops == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT_DURABLE);

    /* Persistence: a plain standalone lookup + read on a fresh handle sees
     * the committed bytes. */
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "i_file");
    memcpy(file_fh, ctx->fh, ctx->fh_len);
    file_fh_len = ctx->fh_len;

    h2 = open_handle(ctx, cred, file_fh, file_fh_len);
    read_verify(ctx, cred, NULL, h2, 0, pattern, RYW_LEN);
    chimera_vfs_release(ctx->vfs_thread, h2);

    chimera_vfs_release(ctx->vfs_thread, h);

    TEST_PASS("enlisted write then enlisted read returns the bytes in-compound; durable after end");
} /* test_data_read_your_writes */

/* (j) widened procs: set_xattr/get_xattr now take the compound parameter
 * (right after cred).  Enlisted on a cairn file inside one compound the
 * pair round-trips and advances enlisted_ops by 2; with a NULL compound
 * the same pair still works standalone. */
static void
test_widened_xattr_enlist(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root)
{
    struct chimera_vfs_compound    *compound;
    struct chimera_vfs_open_handle *h;

    /* The file itself is created standalone; scenario (j) is about the
     * widened proc signatures, not about creation under a compound. */
    h = create_file(ctx, cred, NULL, cairn_root, "j_file");

    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module != NULL);           /* eagerly bound */
    assert(strcmp(compound->module->name, "cairn") == 0);
    assert(compound->enlisted_ops == 0);

    xattr_roundtrip(ctx, cred, compound, h, "user.j_enlisted", "quux-compound");

    assert(compound->enlisted_ops == 2);        /* set + get both enlisted */
    assert(compound->ejected_ops == 0);
    assert(compound->ejected_mutating_ops == 0);
    assert(compound->inflight_ops == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    /* The widened signature's NULL path: the same pair standalone. */
    xattr_roundtrip(ctx, cred, NULL, h, "user.j_standalone", "quux-null");

    /* And the enlisted value is still there after the commit. */
    xattr_roundtrip(ctx, cred, NULL, h, "user.j_enlisted", "quux-compound");

    chimera_vfs_release(ctx->vfs_thread, h);

    TEST_PASS("set_xattr/get_xattr enlist under a compound (enlisted_ops += 2) and work with NULL");
} /* test_widened_xattr_enlist */

/* (k) single-op fold liveness: a cairn-hinted compound with exactly one
 * mkdir must complete its end for both COMMIT_DURABLE and COMMIT -- the
 * harness pumps until the end callback fires, so a hang here means the
 * backend's deferred-completion (cycle-commit) machinery never scheduled.
 * The dir must persist afterwards.  No timing assertions. */
static void
test_single_op_fold_liveness(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root)
{
    struct chimera_vfs_compound *compound;

    /* COMMIT_DURABLE leg */
    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module != NULL);           /* eagerly bound */

    mkdir_under(ctx, cred, compound, cairn_root, "k1");
    assert(compound->enlisted_ops == 1);
    assert(compound->inflight_ops == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT_DURABLE);
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "k1");

    /* COMMIT leg */
    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module != NULL);

    mkdir_under(ctx, cred, compound, cairn_root, "k2");
    assert(compound->enlisted_ops == 1);
    assert(compound->inflight_ops == 0);

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "k2");

    TEST_PASS("single-op compound ends fire for COMMIT_DURABLE and COMMIT; effects persist");
} /* test_single_op_fold_liveness */

/* (l) Pillai non-atomic mode: a multi-op GROUPING (non-RETRYABLE) WRITE
 * compound -- mkdir, mkdir, create+write, set_xattr, with a read-your-writes
 * read spliced in -- must yield the correct FINAL state after COMMIT whether
 * cairn applied it atomically (default) or op-by-op (CHIMERA_CAIRN_NONATOMIC).
 * Since atomicity is permitted-but-never-promised, both are conformant and the
 * final-state assertions hold either way; that is the property this guards.
 *
 * The scenario also proves the knob is actually doing something: after the
 * first enlisted mkdir completes, a STANDALONE (outside-the-compound) lookup
 * of that name probes the base DB.  With the knob ON the op was committed
 * early, so the prefix is already visible mid-compound; with it OFF the
 * mutation is still staged and invisible until END.  The env is read exactly
 * as the backend reads it, so this stays a single self-checking scenario. */
static void
test_nonatomic_grouping_final_state(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    const uint8_t                  *cairn_fh,
    uint32_t                        cairn_fh_len,
    struct chimera_vfs_open_handle *cairn_root)
{
    struct chimera_vfs_compound    *compound;
    struct chimera_vfs_open_handle *file_h;
    struct chimera_vfs_open_handle *reopen_h;
    const char                     *env       = getenv("CHIMERA_CAIRN_NONATOMIC");
    int                             nonatomic = (env && *env && *env != '0');
    uint8_t                         pattern[RYW_LEN];
    uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        file_fh_len;
    int                             mid_visible;

    for (int i = 0; i < RYW_LEN; i++) {
        pattern[i] = (uint8_t) (0x3C ^ (i * 11));
    }

    /* GROUPING lane: no RETRYABLE flag.  Eagerly bound to cairn. */
    compound = chimera_vfs_compound_begin(ctx->vfs_thread, cred,
                                          cairn_fh, cairn_fh_len,
                                          CHIMERA_VFS_COMPOUND_WRITE,
                                          chimera_vfs_compound_alloc_ts(ctx->vfs_thread),
                                          0);
    assert(compound != NULL);
    assert(compound->module != NULL);
    assert(strcmp(compound->module->name, "cairn") == 0);
    assert(!(compound->flags & CHIMERA_VFS_COMPOUND_RETRYABLE));

    /* Op 1: an enlisted mkdir. */
    mkdir_under(ctx, cred, compound, cairn_root, "l_dir1");

    /* Probe mid-compound visibility of the just-completed op through a
     * standalone lookup (base DB).  This asserts the knob's whole point. */
    mid_visible = lookup_present(ctx, cred, cairn_fh, cairn_fh_len, "l_dir1");
    if (nonatomic) {
        assert(mid_visible);        /* committed early: prefix already visible */
    } else {
        assert(!mid_visible);       /* staged: invisible until END */
    }

    /* Ops 2-5: more mutations, plus an in-compound read-your-writes check
     * that must hold in BOTH modes (early-committed to base, or staged). */
    mkdir_under(ctx, cred, compound, cairn_root, "l_dir2");

    file_h = create_file(ctx, cred, compound, cairn_root, "l_file");
    write_data(ctx, cred, compound, file_h, 0, pattern, RYW_LEN);
    read_verify(ctx, cred, compound, file_h, 0, pattern, RYW_LEN);
    xattr_roundtrip(ctx, cred, compound, file_h, "user.l_key", "pillai");

    end_wait_ok(ctx, cred, compound, CHIMERA_VFS_COMPOUND_COMMIT);

    /* Final state must be correct regardless of atomicity: every mutation is
     * durably present and the file's bytes and xattr round-trip. */
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "l_dir1");
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "l_dir2");
    lookup_child(ctx, cred, cairn_fh, cairn_fh_len, "l_file");
    memcpy(file_fh, ctx->fh, ctx->fh_len);
    file_fh_len = ctx->fh_len;

    reopen_h = open_handle(ctx, cred, file_fh, file_fh_len);
    read_verify(ctx, cred, NULL, reopen_h, 0, pattern, RYW_LEN);
    xattr_roundtrip(ctx, cred, NULL, reopen_h, "user.l_key", "pillai");
    chimera_vfs_release(ctx->vfs_thread, reopen_h);

    chimera_vfs_release(ctx->vfs_thread, file_h);

    if (nonatomic) {
        TEST_PASS("non-atomic grouping compound: prefix visible mid-compound; "
                  "final state correct after COMMIT");
    } else {
        TEST_PASS("atomic grouping compound: staged prefix invisible mid-compound; "
                  "final state correct after COMMIT");
    }
} /* test_nonatomic_grouping_final_state */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx                 ctx = { 0 };
    struct chimera_vfs_module_cfg   module_cfgs[3];
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         cred;
    struct chimera_vfs_open_handle *mem_root;
    struct chimera_vfs_open_handle *cairn_root;
    char                            tmpl[]    = "/tmp/chimera_compound_XXXXXX";
    char                           *cairn_dir = mkdtemp(tmpl);
    char                            cairn_cfg[256];
    char                            rmcmd[512];
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
    uint8_t                         mem_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        mem_fh_len;
    uint8_t                         cairn_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        cairn_fh_len;

    chimera_log_init();
    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);

    assert(cairn_dir != NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs",
            sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "cairn",
            sizeof(module_cfgs[1].module_name) - 1);
    snprintf(cairn_cfg, sizeof(cairn_cfg),
             "{\"initialize\":true,\"path\":\"%s\"}", cairn_dir);
    strncpy(module_cfgs[1].config_data, cairn_cfg,
            sizeof(module_cfgs[1].config_data) - 1);
    strncpy(module_cfgs[2].module_name, "memkv",
            sizeof(module_cfgs[2].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    /* No delegation pools (dispatch stays on this thread) and no attr/name
     * caches, so the post-commit visibility checks read the backend rather
     * than a cache. */
    ctx.vfs = chimera_vfs_init(
        0,                 /* num_sync_delegation_threads */
        0,                 /* num_async_delegation_threads */
        module_cfgs,
        3,
        "memkv",
        60,
        0,                 /* attr_cache_enabled */
        0,                 /* name_cache_enabled */
        0,                 /* num_rcu_reclaim_threads: 0 = one per CPU */
        metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "memfs", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "cairn", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/mem", "memfs", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/cairn", "cairn", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    lookup_child(&ctx, &cred, root_fh, root_fh_len, "mem");
    memcpy(mem_fh, ctx.fh, ctx.fh_len);
    mem_fh_len = ctx.fh_len;

    lookup_child(&ctx, &cred, root_fh, root_fh_len, "cairn");
    memcpy(cairn_fh, ctx.fh, ctx.fh_len);
    cairn_fh_len = ctx.fh_len;

    mem_root   = open_handle(&ctx, &cred, mem_fh, mem_fh_len);
    cairn_root = open_handle(&ctx, &cred, cairn_fh, cairn_fh_len);

    test_begin_no_hint(&ctx, &cred);
    test_non_capable_hint(&ctx, &cred, mem_fh, mem_fh_len, mem_root);
    test_lazy_bind_and_eject(&ctx, &cred, cairn_fh, cairn_fh_len, cairn_root,
                             mem_fh, mem_fh_len, mem_root);
    test_eager_bind_durable(&ctx, &cred, cairn_fh, cairn_fh_len, cairn_root);
    test_loose_singleton(&ctx, &cred, cairn_root);
    test_loose_flagged_begin(&ctx, &cred, cairn_fh, cairn_fh_len, cairn_root);
    test_retryable_eject_commit_ok(&ctx, &cred, cairn_fh, cairn_fh_len,
                                   cairn_root, mem_fh, mem_fh_len, mem_root);
    test_mutating_vs_readonly_eject(&ctx, &cred, cairn_fh, cairn_fh_len,
                                    mem_fh, mem_fh_len, mem_root);
    test_data_read_your_writes(&ctx, &cred, cairn_fh, cairn_fh_len,
                               cairn_root);
    test_widened_xattr_enlist(&ctx, &cred, cairn_fh, cairn_fh_len,
                              cairn_root);
    test_single_op_fold_liveness(&ctx, &cred, cairn_fh, cairn_fh_len,
                                 cairn_root);
    test_nonatomic_grouping_final_state(&ctx, &cred, cairn_fh, cairn_fh_len,
                                        cairn_root);

    chimera_vfs_release(ctx.vfs_thread, mem_root);
    chimera_vfs_release(ctx.vfs_thread, cairn_root);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/mem", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/cairn", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_rmfs(ctx.vfs_thread, NULL, "memfs", "fs0", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_rmfs(ctx.vfs_thread, NULL, "cairn", "fs0", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    snprintf(rmcmd, sizeof(rmcmd), "rm -rf %s", cairn_dir);
    if (system(rmcmd) != 0) {
        fprintf(stderr, "warning: failed to remove %s\n", cairn_dir);
    }

    fprintf(stderr, "All compound contract tests passed!\n");
    return 0;
} /* main */
