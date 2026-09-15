// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The VFS compound executor: submit a sequence, get one callback.
 *
 * What this pins down is the executor's contract rather than any filesystem
 * behaviour -- the ops themselves are the ordinary per-op path and are already
 * covered elsewhere.  What is new, and what breaks if the executor is wrong:
 *
 *   - chaining: an op addresses whatever the previous one resolved, and the
 *     caller never sees the intermediate file handle;
 *   - one callback: it fires exactly once, however many ops ran;
 *   - stop-at-first-failure: the ops after a failure do not run, and are
 *     distinguishable from ops that ran and succeeded;
 *   - per-op results survive to the callback, indexed as the caller appended
 *     them.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

struct test_ctx {
    int                        done;
    int                        callbacks;
    enum chimera_vfs_error     status;
    struct chimera_vfs        *vfs;
    struct chimera_vfs_thread *vfs_thread;
    struct evpl               *evpl;
    uint8_t                    fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                   fh_len;
};

/* What NFSv3 asks for in a wcc_data pre_op_attr (nfs_common/nfs3_attr.h's
 * CHIMERA_NFS3_ATTR_WCC_MASK), named here so this test does not depend on the
 * NFS server's headers to say what a protocol actually wants. */
#define CHIMERA_NFS3_LIKE_WCC_MASK \
    (CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME | CHIMERA_VFS_ATTR_CTIME)

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
mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
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
} /* mkdir_cb */

static void
compound_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->callbacks++;
    ctx->done = 1;
} /* compound_cb */

static void
open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    if (oh) {
        chimera_vfs_release_handle(ctx->vfs_thread, oh);
    }
    ctx->done = 1;
} /* open_cb */

static void
mkdir_under(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *parent_fh,
    uint32_t                       parent_fh_len,
    const char                    *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;

    chimera_vfs_mkdir(ctx->vfs_thread, cred, parent_fh, (int) parent_fh_len,
                      name, (int) strlen(name), &sattr,
                      CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                      mkdir_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* mkdir_under */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx                       ctx = { 0 };
    struct chimera_vfs_module_cfg         module_cfgs[2];
    struct prometheus_metrics            *metrics;
    struct chimera_vfs_cred               cred;
    struct chimera_vfs_compound          *cp;
    uint8_t                               root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                              root_fh_len;
    uint8_t                               a_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                              a_fh_len;
    int                                   i_put, i_look_a, i_look_b, i_getattr;
    int                                   i_getfh, i_access;
    const struct chimera_vfs_compound_op *op;

    (void) argc;
    (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strcpy(module_cfgs[0].module_name, "memfs");
    strcpy(module_cfgs[1].module_name, "memkv");

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    evpl_init(NULL);
    ctx.evpl = evpl_create(NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0,
                               metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);

    chimera_vfs_mkfs(ctx.vfs_thread, &cred, "memfs", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, &cred, "/mem", "memfs", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &cred, root_fh, (int) root_fh_len,
                       "mem", 3, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                       0, lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(root_fh, ctx.fh, ctx.fh_len);
    root_fh_len = ctx.fh_len;

    /* A two-level tree to chain through: /mem/a/b */
    mkdir_under(&ctx, &cred, root_fh, root_fh_len, "a");
    memcpy(a_fh, ctx.fh, ctx.fh_len);
    a_fh_len = ctx.fh_len;
    mkdir_under(&ctx, &cred, a_fh, a_fh_len, "b");

    /* ---- chaining: PUTFH(mem); LOOKUP a; LOOKUP b; GETATTR; GETFH ----
     * Every op after the PUTFH addresses whatever the previous one resolved.
     * The caller supplies exactly one file handle and reads one back. */
    cp       = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
    i_put    = chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
    i_look_a = chimera_vfs_compound_add_lookup(cp, "a", 1,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);
    i_look_b = chimera_vfs_compound_add_lookup(cp, "b", 1,
                                               CHIMERA_VFS_ATTR_MASK_STAT,
                                               CHIMERA_VFS_ATTR_MASK_STAT);
    i_getattr = chimera_vfs_compound_add_getattr(cp,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);
    i_getfh = chimera_vfs_compound_add_getfh(cp);

    assert(i_put == 0 && i_look_a == 1 && i_look_b == 2 &&
           i_getattr == 3 && i_getfh == 4);

    ctx.callbacks = 0;
    chimera_vfs_compound_submit(cp, compound_cb, &ctx);
    wait_done(&ctx);

    assert(ctx.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_num_completed(cp) == 5);

    op = chimera_vfs_compound_op(cp, i_look_b);
    assert(op->status == CHIMERA_VFS_OK);
    assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
    /* Both objects a LOOKUP names: the child in attr, the directory it was
     * found in in dir_post_attr.  NFSv3's LOOKUP3resok carries the pair. */
    assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
    assert(S_ISDIR(op->dir_post_attr.va_mode));

    op = chimera_vfs_compound_op(cp, i_getattr);
    assert(op->status == CHIMERA_VFS_OK);
    assert(S_ISDIR(op->attr.va_mode));

    /* GETFH returns the object the last LOOKUP resolved -- b, not a. */
    op = chimera_vfs_compound_op(cp, i_getfh);
    assert(op->status == CHIMERA_VFS_OK);
    assert(op->fh_len > 0);
    {
        const struct chimera_vfs_compound_op *lb =
            chimera_vfs_compound_op(cp, i_look_b);

        assert(op->fh_len == lb->attr.va_fh_len);
        assert(memcmp(op->fh, lb->attr.va_fh, op->fh_len) == 0);
    }

    chimera_vfs_compound_free(cp);
    TEST_PASS("a sequence chains through what each op resolved; one callback");

    /* ---- stop at the first failure ---- */
    cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
    chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
    chimera_vfs_compound_add_lookup(cp, "nonexistent", 11,
                                    CHIMERA_VFS_ATTR_MASK_STAT, 0);
    i_getattr = chimera_vfs_compound_add_getattr(cp,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);

    ctx.callbacks = 0;
    chimera_vfs_compound_submit(cp, compound_cb, &ctx);
    wait_done(&ctx);

    assert(ctx.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOENT);
    /* The PUTFH and the failing LOOKUP ran; the GETATTR did not. */
    assert(chimera_vfs_compound_num_completed(cp) == 2);
    assert(chimera_vfs_compound_op(cp, 0)->status == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, 1)->status == CHIMERA_VFS_ENOENT);
    assert(chimera_vfs_compound_op(cp, i_getattr)->status == CHIMERA_VFS_UNSET);

    chimera_vfs_compound_free(cp);
    TEST_PASS("execution stops at the first failure; later ops stay UNSET");

    /* ---- ACCESS is answered from the current object ---- */
    cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
    chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
    chimera_vfs_compound_add_lookup(cp, "a", 1, CHIMERA_VFS_ATTR_MASK_STAT, 0);
    i_access = chimera_vfs_compound_add_access(cp, CHIMERA_ACE_READ_DATA |
                                               CHIMERA_ACE_WRITE_DATA);

    ctx.callbacks = 0;
    chimera_vfs_compound_submit(cp, compound_cb, &ctx);
    wait_done(&ctx);

    assert(ctx.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    op = chimera_vfs_compound_op(cp, i_access);
    assert(op->status == CHIMERA_VFS_OK);
    /* root against a 0755 directory it owns */
    assert(op->granted & CHIMERA_ACE_READ_DATA);
    /* The decision was reached with the object's ACL in hand, not from its
     * mode bits alone -- an ACL-bearing object would otherwise be answered
     * from a mode that says nothing about who its ACL admits.  What survives to
     * here is the request, not the ACL itself: the result copy drops it
     * deliberately (see chimera_vfs_compound_store_attr), so the answer's own
     * masks cannot be what says the ACL was consulted. */
    assert(op->attr_mask & CHIMERA_VFS_ATTR_ACL);
    assert(!(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL));

    chimera_vfs_compound_free(cp);
    TEST_PASS("ACCESS is evaluated against the object the sequence resolved");

    /* ---- a LOOKUP through a non-directory ----
     * Resolving a name through a regular file is ENOTDIR.  The current object
     * is opened as a directory for the LOOKUP, which is what makes that answer
     * uniform on a backend whose open enforces it rather than left to however
     * that backend's lookup reports a non-directory parent.  The GETATTR before
     * it must still succeed: it needs no directory open, so the sequence's
     * shared handle is re-opened for the LOOKUP instead of the LOOKUP's
     * requirement being imposed on everything that came before. */
    {
        struct chimera_vfs_attrs sattr;
        uint8_t                  f_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 f_fh_len;
        int                      i_ga, i_lk;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0644;

        chimera_vfs_open(ctx.vfs_thread, &cred, root_fh, (int) root_fh_len,
                         "f", 1,
                         CHIMERA_VFS_OPEN_CREATE |
                         CHIMERA_VFS_OPEN_CREATE_REGULAR,
                         &sattr,
                         CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                         open_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        memcpy(f_fh, ctx.fh, ctx.fh_len);
        f_fh_len = ctx.fh_len;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, f_fh, (int) f_fh_len);
        i_ga = chimera_vfs_compound_add_getattr(cp,
                                                CHIMERA_VFS_ATTR_MASK_STAT);
        i_lk = chimera_vfs_compound_add_lookup(cp, "x", 1, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        assert(S_ISREG(chimera_vfs_compound_op(cp, i_ga)->attr.va_mode));
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOTDIR);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("a LOOKUP opens the current object as a directory");

    /* ---- a sequence that addresses an object before naming one ---- */
    cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
    chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);

    ctx.callbacks = 0;
    chimera_vfs_compound_submit(cp, compound_cb, &ctx);
    wait_done(&ctx);

    assert(ctx.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
    chimera_vfs_compound_free(cp);
    TEST_PASS("an op with no current object fails rather than guessing");

    /* ---- SAVEFH/RESTOREFH round-trip across a LOOKUP ----
     * Save the directory, walk away from it, put it back.  What this really
     * pins down is the handle lifetime: the sequence holds an open handle for
     * the current object, and the walk away and the walk back each change what
     * that is.  Under ASAN a handle released twice or leaked shows up here. */
    {
        int i_save, i_lk, i_restore, i_fh_after;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        /* A getattr on each side of the save, so the sequence is actually
         * holding an open handle when the slot is written and when it is
         * restored -- not just a file handle. */
        chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_save = chimera_vfs_compound_add_savefh(cp);
        i_lk   = chimera_vfs_compound_add_lookup(cp, "a", 1,
                                                 CHIMERA_VFS_ATTR_MASK_STAT, 0);
        chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_restore  = chimera_vfs_compound_add_restorefh(cp);
        i_fh_after = chimera_vfs_compound_add_getfh(cp);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        /* The SAVEFH addressed the directory; the LOOKUP moved off it. */
        op = chimera_vfs_compound_op(cp, i_save);
        assert(op->fh_len == root_fh_len);
        assert(memcmp(op->fh, root_fh, root_fh_len) == 0);

        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->fh_len == a_fh_len);
        assert(memcmp(op->fh, a_fh, a_fh_len) == 0);

        /* ...and the RESTOREFH put it back, for itself and for what follows. */
        op = chimera_vfs_compound_op(cp, i_restore);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == root_fh_len);
        assert(memcmp(op->fh, root_fh, root_fh_len) == 0);

        op = chimera_vfs_compound_op(cp, i_fh_after);
        assert(op->fh_len == root_fh_len);
        assert(memcmp(op->fh, root_fh, root_fh_len) == 0);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("SAVEFH/RESTOREFH round-trips the current object across a LOOKUP");

    /* ---- RESTOREFH with nothing saved ---- */
    cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
    chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
    chimera_vfs_compound_add_restorefh(cp);
    i_getfh = chimera_vfs_compound_add_getfh(cp);

    ctx.callbacks = 0;
    chimera_vfs_compound_submit(cp, compound_cb, &ctx);
    wait_done(&ctx);

    assert(ctx.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
    assert(chimera_vfs_compound_num_completed(cp) == 2);
    assert(chimera_vfs_compound_op(cp, i_getfh)->status == CHIMERA_VFS_UNSET);
    chimera_vfs_compound_free(cp);
    TEST_PASS("RESTOREFH with an empty saved slot fails and stops the sequence");

    /* ---- LOOKUPP walks back up ---- */
    {
        int i_lka, i_lkb, i_up;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lka = chimera_vfs_compound_add_lookup(cp, "a", 1, 0, 0);
        i_lkb = chimera_vfs_compound_add_lookup(cp, "b", 1, 0, 0);
        i_up  = chimera_vfs_compound_add_lookupp(cp,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        /* The parent of /mem/a/b is /mem/a, which is what the first LOOKUP
         * resolved. */
        op = chimera_vfs_compound_op(cp, i_up);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == chimera_vfs_compound_op(cp, i_lka)->fh_len);
        assert(memcmp(op->fh, chimera_vfs_compound_op(cp, i_lka)->fh,
                      op->fh_len) == 0);
        assert(chimera_vfs_compound_op(cp, i_lkb)->status == CHIMERA_VFS_OK);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("LOOKUPP makes the current object's parent current");

    /* ---- COMMIT of a regular file ----
     * The COMMIT needs a data open where the GETATTR before it needs a path
     * open.  Those are two different handles from two different caches, so the
     * sequence must re-open rather than hand the data op the path handle it is
     * already holding. */
    {
        struct chimera_vfs_attrs sattr;
        uint8_t                  c_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 c_fh_len;
        int                      i_ga, i_commit;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0644;

        chimera_vfs_open(ctx.vfs_thread, &cred, root_fh, (int) root_fh_len,
                         "c", 1,
                         CHIMERA_VFS_OPEN_CREATE |
                         CHIMERA_VFS_OPEN_CREATE_REGULAR,
                         &sattr,
                         CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
                         open_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        memcpy(c_fh, ctx.fh, ctx.fh_len);
        c_fh_len = ctx.fh_len;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, c_fh, (int) c_fh_len);
        i_ga     = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);
        i_commit = chimera_vfs_compound_add_commit(cp, 0, 0,
                                                   CHIMERA_VFS_ATTR_MODE,
                                                   CHIMERA_VFS_ATTR_MODE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_commit);
        assert(op->status == CHIMERA_VFS_OK);
        /* Both readings came back with the flush, each in its own slot.  The
         * pair is what NFSv3's COMMIT3resok.file_wcc reports, and a getattr
         * after the fact could not give it atomically. */
        assert(op->pre_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert(S_ISREG(op->pre_attr.va_mode));
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert(S_ISREG(op->attr.va_mode));

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("COMMIT re-opens the current object for data and reports its attrs");

    /* ---- READDIR of the current object ---- */
    {
        int      i_rd;
        uint32_t e;
        int      saw_b = 0;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, 0, 0);
        i_rd = chimera_vfs_compound_add_readdir(cp, 0, 0, 8192, 8192, 32,
                                                CHIMERA_VFS_ATTR_MASK_STAT,
                                                CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        /* The READDIR enumerated /mem/a, the object the LOOKUP resolved. */
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->eof);
        assert(op->num_entries >= 1);
        assert(op->num_entries <= CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES);

        /* The directory's own attributes, asked for alongside the entries.
         * NFSv3's READDIR3resok.dir_attributes is this, and getting it here
         * saves re-addressing the directory to stat it afterwards. */
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert(S_ISDIR(op->dir_post_attr.va_mode));

        for (e = 0; e < op->num_entries; e++) {
            if (op->entries[e].name_len == 1 &&
                op->entries[e].name[0] == 'b') {
                saw_b = 1;
                assert(S_ISDIR(op->entries[e].attr.va_mode));
                /* Per-entry attributes survive to the callback, and carry no
                 * ACL for the same reason no other attribute result does. */
                assert(!(op->entries[e].attr.va_set_mask &
                         CHIMERA_VFS_ATTR_ACL));
            }
        }
        assert(saw_b);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("READDIR pages the current object's entries with their attrs");

    /* ---- the xattr ops, against the object the sequence resolved ---- */
    {
        int i_set, i_list, i_get, i_remove, i_get2;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, 0, 0);
        i_set = chimera_vfs_compound_add_setxattr(cp, 0, "user.k", 6,
                                                  "value", 5);
        i_list = chimera_vfs_compound_add_listxattrs(cp, 0, 4096);
        i_get  = chimera_vfs_compound_add_getxattr(cp, "user.k", 6, 4096);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_set);
        assert(op->status == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_list);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->buffer_count >= 1);
        assert(strcmp((const char *) op->buffer, "user.k") == 0);

        op = chimera_vfs_compound_op(cp, i_get);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->buffer_len == 5);
        assert(memcmp(op->buffer, "value", 5) == 0);

        chimera_vfs_compound_free(cp);

        /* Removing it makes the next read of it fail, in the same sequence. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, 0, 0);
        i_remove = chimera_vfs_compound_add_removexattr(cp, "user.k", 6);
        i_get2   = chimera_vfs_compound_add_getxattr(cp, "user.k", 6, 4096);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_remove)->status ==
               CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_get2)->status !=
               CHIMERA_VFS_OK);
        /* The removal stands even though the sequence stopped at a failure:
         * nothing is rolled back (see the MUTATION note in vfs_compound.h). */
        assert(chimera_vfs_compound_status(cp) != CHIMERA_VFS_OK);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("the xattr ops address the current object; mutations are not undone");

    /* ---- OPEN creates, becomes current, and hands out its handle ----
     * The sequence resolves the parent, creates through it, and the created
     * object is what the ops after the OPEN address -- so a caller that wants
     * the new object's attributes and file handle asks for them here rather
     * than making a second round trip for what it just created. */
    {
        struct chimera_vfs_attrs sattr;
        int                      i_open, i_ga2, i_fh2;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0644;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "o1", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, &sattr,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        i_ga2 = chimera_vfs_compound_add_getattr(cp,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);
        i_fh2 = chimera_vfs_compound_add_getfh(cp);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->created);
        assert(!op->existed);
        assert(op->out_handle != NULL);
        assert(S_ISREG(op->attr.va_mode));
        /* The directory's change attribute either side of the create, which is
         * the whole of what a change_info reply needs. */
        assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);

        /* The ops after it addressed the object the OPEN produced. */
        assert(chimera_vfs_compound_op(cp, i_ga2)->status == CHIMERA_VFS_OK);
        assert(S_ISREG(chimera_vfs_compound_op(cp, i_ga2)->attr.va_mode));
        assert(chimera_vfs_compound_op(cp, i_fh2)->fh_len == op->fh_len);
        assert(memcmp(chimera_vfs_compound_op(cp, i_fh2)->fh, op->fh,
                      op->fh_len) == 0);

        /* Taking the handle is what makes it the caller's; the compound no
         * longer has it, and releasing it is now the caller's job. */
        {
            struct chimera_vfs_open_handle *taken;

            taken = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
            assert(taken != NULL);
            assert(chimera_vfs_compound_op(cp, i_open)->out_handle == NULL);
            assert(chimera_vfs_compound_take_handle(cp,
                                                    (uint32_t) i_open) == NULL);
            chimera_vfs_release(ctx.vfs_thread, taken);
        }

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("OPEN creates, becomes current, and hands out its handle");

    /* ---- REGULAR_ONLY refuses a non-regular object and says what it was ----
     * Without this the sequence would open the directory and leave the caller
     * to discover the type afterwards -- or, on a backend whose open of a FIFO
     * blocks, not leave it anything at all.  The status alone cannot carry the
     * answer a protocol wants (NFS4 distinguishes a symlink from a device from
     * a directory), so the mode comes back with it. */
    {
        int i_open;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "a", 1,
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY,
                                               NULL,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_EISDIR);
        assert(op->existed);
        assert(S_ISDIR(op->existing_mode));
        assert(op->out_handle == NULL);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EISDIR);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("OPEN with REGULAR_ONLY refuses a directory and reports its mode");

    /* ---- ATTRS_ON_CREATE_ONLY leaves an existing object alone ----
     * A create's attributes describe a creation.  Opening a name that is
     * already there must not restyle it, which is what NFS4 UNCHECKED4 and
     * NFS3 UNCHECKED both mean -- and what the caller previously had to
     * arrange by looking the name up itself and blanking the attributes
     * before it opened. */
    {
        struct chimera_vfs_attrs sattr;
        int                      i_open, i_ga3;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "o2", 2,
                                               CHIMERA_VFS_OPEN_CREATE,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_open)->created);
        chimera_vfs_compound_free(cp);

        /* Re-open the same name asking for 0777. */
        sattr.va_mode = S_IFREG | 0777;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(
            cp, "o2", 2,
            CHIMERA_VFS_OPEN_CREATE,
            CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        i_ga3 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_OK);
        assert(!op->created);
        assert(op->existed);
        /* The mode the object was created with, not the one this open asked
         * for. */
        assert((chimera_vfs_compound_op(cp, i_ga3)->attr.va_mode & 0777) ==
               0600);

        /* Deliberately NOT taken: free must release it.  An untaken handle is
         * the normal outcome of every path where the caller decided not to
         * keep the open, so it cannot be a leak. */
        assert(op->out_handle != NULL);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("OPEN with ATTRS_ON_CREATE_ONLY does not restyle an existing object");

    /* ---- an exclusive create that collides opens what is there ----
    * The collision is the answer the caller wants, not an error: NFS4's
    * EXCLUSIVE4 has to look at the object to tell its own earlier create from
    * somebody else's file.  The re-open applies none of the create's
    * attributes, so the object it finds is left exactly as it was. */
    {
        struct chimera_vfs_attrs sattr;
        int                      i_open, i_ga4;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0640;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "x1", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_EXCLUSIVE,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_open)->created);
        chimera_vfs_compound_free(cp);

        /* Without the option, a second exclusive create is refused. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "x1", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_EXCLUSIVE,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_open)->status ==
               CHIMERA_VFS_EEXIST);
        assert(chimera_vfs_compound_op(cp, i_open)->out_handle == NULL);
        chimera_vfs_compound_free(cp);

        /* With it, the same create opens the object instead, says it was
         * already there, and does not restyle it to the 0777 this open asked
         * for. */
        sattr.va_mode = S_IFREG | 0777;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(
            cp, "x1", 2,
            CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE,
            CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        i_ga4 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->existed);
        assert(!op->created);
        assert(op->out_handle != NULL);
        assert((chimera_vfs_compound_op(cp, i_ga4)->attr.va_mode & 0777) ==
               0640);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("an exclusive create that collides opens what is already there");

    /* ---- an OPEN with no name re-opens the current object ---- */
    {
        int i_open;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "o1", 2, 0, 0);
        i_open = chimera_vfs_compound_add_open(cp, NULL, 0,
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, NULL, 0, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->out_handle != NULL);
        assert(!op->created);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("an OPEN with no name re-opens the current object");

    /* ---- CREATE makes the object current; REMOVE leaves the parent ----
     * The two move the current object in opposite ways, and both report the
     * parent either side so a caller can say what changed.  A CREATE puts the
     * new object in hand, which is what lets the ops after it describe what was
     * just made; a REMOVE unlinks a name FROM the current object, so the
     * current object is still the directory afterwards. */
    {
        struct chimera_vfs_attrs sattr;
        int                      i_mkdir, i_fh, i_ln, i_rm, i_ga5, i_look;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0750;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_mkdir = chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "nd", 2, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        i_fh  = chimera_vfs_compound_add_getfh(cp);
        i_ga5 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_mkdir);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->created);
        assert(S_ISDIR(op->attr.va_mode));
        assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);

        /* The ops after it addressed the directory that was just made, not the
         * one it was made in. */
        assert(chimera_vfs_compound_op(cp, i_fh)->fh_len == op->fh_len);
        assert(memcmp(chimera_vfs_compound_op(cp, i_fh)->fh, op->fh,
                      op->fh_len) == 0);
        assert(S_ISDIR(chimera_vfs_compound_op(cp, i_ga5)->attr.va_mode));
        assert(memcmp(op->fh, root_fh, root_fh_len) != 0);

        chimera_vfs_compound_free(cp);

        /* A symlink, and then unlinking it again. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_ln = chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_SYMLINK, "sl", 2,
            "nd", 2, NULL, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(S_ISLNK(chimera_vfs_compound_op(cp, i_ln)->attr.va_mode));
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "sl", 2, 0,
                                               CHIMERA_NFS3_LIKE_WCC_MASK,
                                               CHIMERA_VFS_ATTR_MASK_STAT);
        /* Still the parent: a LOOKUP after the REMOVE resolves through it. */
        i_look = chimera_vfs_compound_add_lookup(cp, "nd", 2, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_rm);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        /* The caller's own directory masks are added to the executor's floor,
         * not swapped for it: CHANGE above is still there, and so is the
         * size/mtime/ctime triple NFSv3's wcc_data pre_op_attr is made of. */
        assert((op->dir_pre_attr.va_set_mask & CHIMERA_NFS3_LIKE_WCC_MASK) ==
               CHIMERA_NFS3_LIKE_WCC_MASK);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert(S_ISDIR(op->dir_post_attr.va_mode));
        assert(op->fh_len == root_fh_len);
        assert(memcmp(op->fh, root_fh, root_fh_len) == 0);
        assert(chimera_vfs_compound_op(cp, i_look)->status == CHIMERA_VFS_OK);

        chimera_vfs_compound_free(cp);

        /* The name is gone, and a second REMOVE says so rather than the
         * sequence swallowing it. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "sl", 2, 0, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_rm)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("CREATE makes the new object current; REMOVE keeps the parent");

    /* ---- SETATTR against the current object, and against a handle ----
     * The two are not the same operation: through a handle the change is
     * authorized by that open's grant (ftruncate), through the current object
     * by the object's own mode (truncate).  A caller that resolved a handle
     * for itself -- from an NFSv4 stateid, say -- needs the first. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *oh;
        int                             i_open, i_sa, i_ga6;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "sa", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        /* Through the current object. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0640;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "sa", 2, 0, 0);
        i_sa = chimera_vfs_compound_add_setattr(cp, NULL, &sattr,
                                                CHIMERA_VFS_ATTR_MASK_STAT,
                                                CHIMERA_VFS_ATTR_MASK_STAT);
        i_ga6 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_sa);
        assert(op->status == CHIMERA_VFS_OK);
        assert((chimera_vfs_compound_op(cp, i_ga6)->attr.va_mode & 0777) ==
               0640);

        /* The change straddled by its own two readings: 0600 before, 0640
         * after.  This is NFSv3's SETATTR3resok.obj_wcc, and the reason it
         * comes from the op rather than from a getattr on either side is that
         * only here are the two readings atomic with the change between
         * them. */
        assert(op->pre_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert((op->pre_attr.va_mode & 0777) == 0600);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert((op->attr.va_mode & 0777) == 0640);
        chimera_vfs_compound_free(cp);

        /* Through the borrowed handle, with no current object established at
         * all -- the op does not need one, which is the point. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        sattr.va_size     = 0;

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_sa = chimera_vfs_compound_add_setattr(cp, oh, &sattr, 0,
                                                CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_sa);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 0);

        /* Borrowed means borrowed: freeing the compound must not have
         * released it, so it is still ours to use and to release. */
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("SETATTR applies to the current object or to a borrowed handle");

    /* ---- READ, and the ownership of what it answers with ----
    * The data arrives as references to the backend's buffers, not a copy, so
    * it is owned exactly as an OPEN's handle is: the compound holds it until
    * the caller takes it, and releases what was never taken. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *oh;
        struct evpl_iovec              *riov;
        struct evpl_iovec               rdiov[16];
        int                             rniov, i_open, i_rd;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "rd", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        /* An empty file reads zero bytes at EOF rather than failing. */
        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 4096, rdiov, 16, 0, NULL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->read_len == 0);
        assert(op->eof_read);

        /* Taking it twice yields nothing the second time, so two callers
         * cannot both believe they hold the references. */
        chimera_vfs_compound_take_iov(cp, (uint32_t) i_rd, &riov, &rniov);
        {
            struct evpl_iovec *again;
            int                again_n;

            chimera_vfs_compound_take_iov(cp, (uint32_t) i_rd, &again,
                                          &again_n);
            assert(again_n == 0);
        }
        if (rniov) {
            evpl_iovecs_release(ctx.evpl, riov, rniov);
        }

        chimera_vfs_compound_free(cp);

        /* And a read whose data is never taken: free must release it. */
        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 4096, rdiov, 16, 0, NULL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_rd)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* A READ addressing the current object needs no handle at all. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rd", 2, 0, 0);
        i_rd = chimera_vfs_compound_add_read(cp, NULL, 0, 4096, rdiov, 16, 0, NULL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_rd)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_rd)->eof_read);
        chimera_vfs_compound_free(cp);

        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("READ answers with data the compound owns until it is taken");

    /* ---- WRITE, then READ it back in one sequence ----
     * The data going in is borrowed -- the caller allocated it and releases it
     * afterwards -- where the data coming out is owned.  The two directions are
     * deliberately not symmetric, and this is where that shows. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *oh;
        struct evpl_iovec               wiov;
        struct evpl_iovec               rdiov[16];
        int                             i_open, i_wr, i_rd;
        const char                     *payload = "compound";

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "wr", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        assert(evpl_iovec_alloc(ctx.evpl, 8, 0, 1, 0, &wiov) == 1);
        memcpy(evpl_iovec_data(&wiov), payload, 8);

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_wr = chimera_vfs_compound_add_write(cp, oh, 0, 8, 2, &wiov, 1,
                                              CHIMERA_VFS_ATTR_SIZE,
                                              CHIMERA_VFS_ATTR_SIZE, NULL);
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 8, rdiov, 16,
                                             CHIMERA_VFS_ATTR_MASK_STAT, NULL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_wr);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->written == 8);

        /* The write's own two readings bracket the write: a caller reporting
         * the change -- NFSv3 wcc_data, SMB2's sticky write time -- reads them
         * instead of racing a GETATTR against another writer. */
        assert(op->pre_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->pre_attr.va_size == 0);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 8);

        /* The READ in the same sequence saw what the WRITE in front of it
         * put there. */
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->read_len == 8);
        assert(op->niov >= 1);
        assert(memcmp(evpl_iovec_data(&op->iov[0]), payload, 8) == 0);

        /* The file's attributes rode back with the data, so a caller that has
         * to report them -- NFSv3's READ3resok.file_attributes -- does not
         * need a second op.  The size is the one the WRITE in front left. */
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 8);

        chimera_vfs_compound_free(cp);

        /* Borrowed: the compound did not release the payload, so it is still
         * ours. */
        evpl_iovec_release(ctx.evpl, &wiov);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("WRITE borrows its data; a READ behind it sees what it wrote");

    /* ---- RENAME and LINK read the SAVED slot, not just the current one ----
     * Every other name-changing op works inside one directory.  These two take
     * a source from the saved slot and a target from the current object, which
     * is how NFSv4 already spells them -- and it means a sequence can move a
     * name between two directories without leaving the submission to resolve
     * the second one. */
    {
        struct chimera_vfs_attrs sattr;
        int                      i_src, i_dst, i_ren, i_look, i_link, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0755;

        /* Two directories, and a file in the first. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_src = chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "rnsrc", 5, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_NODE, "f", 1, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_dst = chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "rndst", 5, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_src)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_dst)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* Move it: source directory saved, target current. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rnsrc", 5, 0, 0);
        chimera_vfs_compound_add_savefh(cp);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rndst", 5, 0, 0);
        i_ren = chimera_vfs_compound_add_rename(cp, "f", 1, "g", 1, 0, 0, 0);
        /* The current object is still the TARGET directory afterwards, so a
         * lookup behind the rename finds the name it just put there. */
        i_look = chimera_vfs_compound_add_lookup(cp, "g", 1,
                                                 CHIMERA_VFS_ATTR_MASK_STAT, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_ren);
        assert(op->status == CHIMERA_VFS_OK);
        /* Both directories changed, and both are reported: the saved one it
         * took the name from, and the current one it put the name in. */
        assert(op->from_dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->from_dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(chimera_vfs_compound_op(cp, i_look)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);
        TEST_PASS("RENAME moves a name from the saved directory to the current one");

        /* LINK: the saved slot is the OBJECT this time, not a directory. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rndst", 5, 0, 0);
        chimera_vfs_compound_add_lookup(cp, "g", 1, 0, 0);
        chimera_vfs_compound_add_savefh(cp);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rnsrc", 5, 0, 0);
        i_link = chimera_vfs_compound_add_link(cp, "h", 1, 0, 0, 0);
        i_ga   = chimera_vfs_compound_add_lookup(cp, "h", 1,
                                                 CHIMERA_VFS_ATTR_MASK_STAT, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_link);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        /* Two names for one object now. */
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->attr.va_nlink == 2);
        chimera_vfs_compound_free(cp);
        TEST_PASS("LINK gives the saved object a second name in the current directory");
    }

    /* ---- a RENAME with nothing saved is EINVAL, not a crash ---- */
    {
        int i_bad;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_bad         = chimera_vfs_compound_add_rename(cp, "a", 1, "b", 1, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        /* The adder cannot tell -- whether a SAVEFH ran is a property of the
         * sequence as it executes -- so the refusal lands on the op. */
        assert(chimera_vfs_compound_op(cp, i_bad)->status == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);
        TEST_PASS("a RENAME with an empty saved slot is EINVAL");
    }

    /* ---- an empty sequence completes ---- */
    cp            = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
    ctx.callbacks = 0;
    chimera_vfs_compound_submit(cp, compound_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.callbacks == 1);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_num_completed(cp) == 0);
    chimera_vfs_compound_free(cp);
    TEST_PASS("an empty sequence completes with one callback");

    chimera_vfs_umount(ctx.vfs_thread, &cred, "/mem", mount_cb, &ctx);
    wait_done(&ctx);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "vfs_compound_test: all checks passed\n");
    return 0;
} /* main */
