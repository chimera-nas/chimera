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
    struct test_ctx                ctx = { 0 };
    struct chimera_vfs_module_cfg  module_cfgs[2];
    struct prometheus_metrics     *metrics;
    struct chimera_vfs_cred        cred;
    struct chimera_vfs_compound   *cp;
    uint8_t                        root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                       root_fh_len;
    uint8_t                        a_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                       a_fh_len;
    int                            i_put, i_look_a, i_look_b, i_getattr;
    int                            i_getfh, i_access;
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
                                               CHIMERA_VFS_ATTR_MASK_STAT);
    i_look_b = chimera_vfs_compound_add_lookup(cp, "b", 1,
                                               CHIMERA_VFS_ATTR_MASK_STAT);
    i_getattr = chimera_vfs_compound_add_getattr(cp,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);
    i_getfh   = chimera_vfs_compound_add_getfh(cp);

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
                                    CHIMERA_VFS_ATTR_MASK_STAT);
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
    chimera_vfs_compound_add_lookup(cp, "a", 1, CHIMERA_VFS_ATTR_MASK_STAT);
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

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, f_fh, (int) f_fh_len);
        i_ga = chimera_vfs_compound_add_getattr(cp,
                                                CHIMERA_VFS_ATTR_MASK_STAT);
        i_lk = chimera_vfs_compound_add_lookup(cp, "x", 1, 0);

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

        cp     = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        /* A getattr on each side of the save, so the sequence is actually
         * holding an open handle when the slot is written and when it is
         * restored -- not just a file handle. */
        chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_save = chimera_vfs_compound_add_savefh(cp);
        i_lk   = chimera_vfs_compound_add_lookup(cp, "a", 1,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);
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

        cp    = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lka = chimera_vfs_compound_add_lookup(cp, "a", 1, 0);
        i_lkb = chimera_vfs_compound_add_lookup(cp, "b", 1, 0);
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

        cp       = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, c_fh, (int) c_fh_len);
        i_ga     = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);
        i_commit = chimera_vfs_compound_add_commit(cp, 0, 0,
                                                   CHIMERA_VFS_ATTR_MODE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_commit);
        assert(op->status == CHIMERA_VFS_OK);
        /* The pre-flush attributes the caller asked for came back with it. */
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

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, 0);
        i_rd = chimera_vfs_compound_add_readdir(cp, 0, 0, 8192, 8192, 32,
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

        cp    = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, 0);
        i_set = chimera_vfs_compound_add_setxattr(cp, 0, "user.k", 6,
                                                  "value", 5);
        i_list  = chimera_vfs_compound_add_listxattrs(cp, 0, 4096);
        i_get   = chimera_vfs_compound_add_getxattr(cp, "user.k", 6, 4096);

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
        cp       = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, 0);
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
