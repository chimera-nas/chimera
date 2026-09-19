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
#include <pthread.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"
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
    /* The thread the last completion ran on: the header promises the
     * submitting thread, and a parked LOCK is where that promise is tested. */
    pthread_t                  cb_thread;
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
    ctx->cb_thread = pthread_self();
    ctx->done      = 1;
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

/* A streaming READDIR's two callbacks, with a budget expressed the way a
 * marshalling caller expresses one: refuse the Nth entry.  `reset` counts
 * itself so the test can see it ran before every execution, and `append`
 * checks that the op already says which directory it is listing. */
struct stream_ctx {
    int      resets;
    int      appended;
    int      stop_at;        /* refuse this append (1-based); 0 never   */
    uint64_t refused_cookie;
    uint64_t last_cookie;    /* of the last entry TAKEN                 */
    uint8_t  dir_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t dir_fh_len;
};

static void
stream_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct stream_ctx *s = private_data;

    (void) compound;
    (void) index;

    s->resets++;
    s->appended       = 0;
    s->refused_cookie = 0;
    s->last_cookie    = 0;
} /* stream_reset */

static int
stream_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data)
{
    struct stream_ctx                    *s  = private_data;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);

    (void) inum;
    (void) name;
    (void) namelen;
    (void) attrs;

    /* Filled BEFORE the enumeration, because this is where it is needed. */
    assert(op->fh_len == s->dir_fh_len);
    assert(memcmp(op->fh, s->dir_fh, op->fh_len) == 0);

    if (s->stop_at && s->appended + 1 == s->stop_at) {
        s->refused_cookie = cookie;
        return -1;
    }

    s->appended++;
    s->last_cookie = cookie;

    return 0;
} /* stream_append */

/* A gate that records every op it is asked about and vetoes one of them. */
struct gate_ctx {
    int                    calls;
    uint32_t               veto_index;
    enum chimera_vfs_error veto;
    uint32_t               seen_index[CHIMERA_VFS_COMPOUND_MAX_OPS];
    enum chimera_vfs_error seen_status[CHIMERA_VFS_COMPOUND_MAX_OPS];
};

static void
veto_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct gate_ctx *g = private_data;

    (void) compound;

    g->seen_index[g->calls]  = index;
    g->seen_status[g->calls] = *status;
    g->calls++;

    if (index == g->veto_index) {
        *status = g->veto;
    }
} /* veto_gate */

/* A second VFS thread whose only job is to release a claim, so the grant it
 * pumps to a parked LOCK arrives on a thread that is not the one that
 * submitted the sequence.  It records itself so the test can prove the
 * completion did NOT run here. */
struct remote_release {
    struct chimera_vfs            *vfs;
    struct chimera_vfs_file_state *fs;
    struct chimera_vfs_claim      *claim;
    pthread_t                      self;
};

static void *
remote_release_main(void *arg)
{
    struct remote_release     *rr = arg;
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;

    evpl   = evpl_create(NULL);
    thread = chimera_vfs_thread_init(evpl, rr->vfs);

    rr->self = pthread_self();

    chimera_vfs_claim_release_ranged(thread, rr->vfs->vfs_state,
                                     rr->fs, rr->claim);

    chimera_vfs_thread_destroy(thread);
    evpl_destroy(evpl);

    return NULL;
} /* remote_release_main */

/* A KV range search that keeps the one value it went looking for, so a test
 * can see whether an OPEN's handle-state record reached the default KV. */
struct kv_probe {
    struct test_ctx *ctx;
    int              found;
    char             value[64];
    uint32_t         value_len;
};

static int
kv_probe_entry(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct kv_probe *p = private_data;

    (void) key;
    (void) key_len;

    if (value_len < sizeof(p->value)) {
        memcpy(p->value, value, value_len);
        p->value_len = value_len;
    }
    p->found++;

    return 0;
} /* kv_probe_entry */

static void
kv_probe_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct kv_probe *p = private_data;

    p->ctx->status = error_code;
    p->ctx->done   = 1;
} /* kv_probe_complete */

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
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 4096, rdiov, 16, 0, NULL, NULL, 0);

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
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 4096, rdiov, 16, 0, NULL, NULL, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_rd)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* A READ addressing the current object needs no handle at all. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rd", 2, 0, 0);
        i_rd = chimera_vfs_compound_add_read(cp, NULL, 0, 4096, rdiov, 16, 0, NULL, NULL, 0);

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
                                             CHIMERA_VFS_ATTR_MASK_STAT, NULL, NULL, 0);

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

    /* ---- LOCK_TEST probes, LOCK takes ----
     * Byte-range locks are the first sequence ops whose result is an object
     * the CALLER owns afterwards rather than a value copied out: the claim
     * core keeps pointers into the claim struct once it is inserted, so the
     * struct is the caller's and its address is its identity.  Releasing it is
     * out of band, exactly as releasing an open handle is. */
    {
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           claim_a, claim_b, probe;
        struct chimera_vfs_pending_acquire ticket_a, ticket_b;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_file_state     *fs;
        int                                i_open, i_probe, i_lock;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "lk", 2,
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

        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 1;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 2;

        /* Nothing holds the range yet, so the probe says so and the LOCK
         * behind it takes it -- both in one sequence, which is the point. */
        chimera_vfs_claim_init_range(&probe, true, false, 0, 16, &owner_a);
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh, CHIMERA_VFS_OPEN_INFERRED);
        i_probe = chimera_vfs_compound_add_lock_test(cp, &probe);
        i_lock  = chimera_vfs_compound_add_lock(cp, &claim_a, &ticket_a, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_probe);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);

        /* The lock is taken, and its file state came with it: the sequence
         * hands that over ON GRANTED ONLY, because the caller needs it to
         * release the lock later. */
        op = chimera_vfs_compound_op(cp, i_lock);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock);
        assert(fs != NULL);

        chimera_vfs_compound_free(cp);
        TEST_PASS("LOCK_TEST probes and LOCK takes, in one sequence");

        /* A second owner wanting the same range is refused.  The probe ANSWERS
         * -- that is all LOCKT and F_GETLK are -- so its op succeeds and the
         * sequence goes on; the acquire behind it is the one that stops. */
        chimera_vfs_claim_init_range(&probe, true, false, 0, 16, &owner_b);
        chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16, &owner_b);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh, CHIMERA_VFS_OPEN_INFERRED);
        i_probe = chimera_vfs_compound_add_lock_test(cp, &probe);
        i_lock  = chimera_vfs_compound_add_lock(cp, &claim_b, &ticket_b, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        op = chimera_vfs_compound_op(cp, i_probe);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result != CHIMERA_CLAIM_GRANTED);
        /* Who refused, in the caller's own terms. */
        assert(op->conflict.owner.owner_lo == owner_a.owner_lo);

        op = chimera_vfs_compound_op(cp, i_lock);
        assert(op->status != CHIMERA_VFS_OK);
        assert(op->claim_result != CHIMERA_CLAIM_GRANTED);
        /* Nothing was taken, so nothing is the caller's to put. */
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock) == NULL);

        chimera_vfs_compound_free(cp);
        TEST_PASS("a refused LOCK stops the sequence and names the holder");

        /* Out of band, exactly as a handle release is. */
        chimera_vfs_claim_release_ranged(ctx.vfs_thread,
                                         ctx.vfs->vfs_state, fs, &claim_a);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }

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

    /* ---- streaming READDIR: reset before the run, append per entry ----
     * Nothing is staged: the caller marshals each entry as it arrives, and
     * the entry that does not fit is refused on the spot.  What the executor
     * owes in return is that `reset` ran before the enumeration, that the op
     * already says which directory it is listing when `append` is called, and
     * that a refusal ends the page at THAT entry's cookie with eof clear -- so
     * the next page starts exactly where this one stopped. */
    {
        struct stream_ctx s;
        uint8_t           sd_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t          sd_fh_len;
        uint64_t          resume;
        int               i_rd, total;

        mkdir_under(&ctx, &cred, root_fh, root_fh_len, "sd");
        memcpy(sd_fh, ctx.fh, ctx.fh_len);
        sd_fh_len = ctx.fh_len;
        mkdir_under(&ctx, &cred, sd_fh, sd_fh_len, "e1");
        mkdir_under(&ctx, &cred, sd_fh, sd_fh_len, "e2");
        mkdir_under(&ctx, &cred, sd_fh, sd_fh_len, "e3");

        memset(&s, 0, sizeof(s));
        memcpy(s.dir_fh, sd_fh, sd_fh_len);
        s.dir_fh_len = sd_fh_len;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, sd_fh, (int) sd_fh_len);
        i_rd = chimera_vfs_compound_add_readdir_stream(
            cp, 0, 0, CHIMERA_VFS_ATTR_MASK_STAT, CHIMERA_VFS_ATTR_MASK_STAT,
            0, NULL, 0, stream_reset, stream_append, &s);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->eof);
        /* Streamed, not staged. */
        assert(op->entries == NULL);
        assert(op->num_entries == 0);
        assert(s.resets == 1);
        assert(s.appended >= 3);
        total = s.appended;
        /* The directory's own attributes still ride back. */
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert(S_ISDIR(op->dir_post_attr.va_mode));

        chimera_vfs_compound_free(cp);

        /* Refuse the second entry: the page ends there, eof clear. */
        memset(&s, 0, sizeof(s));
        memcpy(s.dir_fh, sd_fh, sd_fh_len);
        s.dir_fh_len = sd_fh_len;
        s.stop_at    = 2;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, sd_fh, (int) sd_fh_len);
        i_rd = chimera_vfs_compound_add_readdir_stream(
            cp, 0, 0, CHIMERA_VFS_ATTR_MASK_STAT, 0,
            0, NULL, 0, stream_reset, stream_append, &s);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(!op->eof);
        assert(s.resets == 1);
        assert(s.appended == 1);
        assert(s.refused_cookie != 0);
        assert(s.last_cookie != 0);
        /* r_cookie is where the backend stopped: the refused entry's own. */
        assert(op->r_cookie == s.refused_cookie);
        resume = s.last_cookie;

        chimera_vfs_compound_free(cp);

        /* ...and the next page, from the cookie of the last entry TAKEN, is
         * everything else -- the refused entry included. */
        s.stop_at = 0;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, sd_fh, (int) sd_fh_len);
        i_rd = chimera_vfs_compound_add_readdir_stream(
            cp, resume, 0, CHIMERA_VFS_ATTR_MASK_STAT, 0,
            0, NULL, 0, stream_reset, stream_append, &s);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->eof);
        assert(s.resets == 2);
        assert(s.appended == total - 1);

        chimera_vfs_compound_free(cp);

        /* Resuming from r_cookie instead is the trap the header warns about:
         * a cookie names its entry and a READDIR returns what follows it, so
         * the refused entry is skipped. */
        resume = s.refused_cookie;
        /* (refused_cookie survived: this run took every entry, refused none,
         * and reset cleared it -- so re-derive it from the first page.) */
        memset(&s, 0, sizeof(s));
        memcpy(s.dir_fh, sd_fh, sd_fh_len);
        s.dir_fh_len = sd_fh_len;
        s.stop_at    = 2;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, sd_fh, (int) sd_fh_len);
        chimera_vfs_compound_add_readdir_stream(
            cp, 0, 0, CHIMERA_VFS_ATTR_MASK_STAT, 0,
            0, NULL, 0, stream_reset, stream_append, &s);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        resume = s.refused_cookie;
        chimera_vfs_compound_free(cp);

        s.stop_at = 0;
        cp        = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, sd_fh, (int) sd_fh_len);
        i_rd = chimera_vfs_compound_add_readdir_stream(
            cp, resume, 0, CHIMERA_VFS_ATTR_MASK_STAT, 0,
            0, NULL, 0, stream_reset, stream_append, &s);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->eof);
        assert(s.appended == total - 2);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("a streaming READDIR resets, appends, and stops where append refuses");

    /* ---- the gate: a veto fails a successful op and stops the sequence ----
     * The caller is asked after every op, with the status the op is carrying,
     * and may replace it.  A veto on an op that succeeded turns it into a
     * failure exactly as a failing VFS op would be; and the gate is asked
     * about a failing op too, because "after every op" means every op. */
    {
        struct gate_ctx g;
        int             i_lk, i_ga;

        memset(&g, 0, sizeof(g));
        g.veto_index = 1;
        g.veto       = CHIMERA_VFS_EACCES;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_set_gate(cp, veto_gate, &g);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk = chimera_vfs_compound_add_lookup(cp, "a", 1,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EACCES);
        assert(chimera_vfs_compound_num_completed(cp) == 2);
        assert(g.calls == 2);
        assert(g.seen_index[0] == 0 && g.seen_status[0] == CHIMERA_VFS_OK);
        /* The gate saw the op's OWN status -- it had succeeded -- and its
         * results, which is what it judges from. */
        assert(g.seen_index[1] == 1 && g.seen_status[1] == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->status == CHIMERA_VFS_EACCES);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);

        chimera_vfs_compound_free(cp);

        /* A failing op is put to the gate as well. */
        memset(&g, 0, sizeof(g));
        g.veto_index = 99;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_set_gate(cp, veto_gate, &g);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk = chimera_vfs_compound_add_lookup(cp, "nonexistent", 11, 0, 0);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOENT);
        assert(g.calls == 2);
        assert(g.seen_index[1] == 1 && g.seen_status[1] == CHIMERA_VFS_ENOENT);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("the gate vetoes a successful op and is consulted for a failing one");

    /* ---- PUTHANDLE: what a lent handle serves, with its REAL flags ----
     * A caller lends the flags it opened with -- a data handle is READ_ONLY
     * and/or WRITE_ONLY, an opendir handle is PATH|DIRECTORY -- and neither
     * carries CHIMERA_VFS_OPEN_INFERRED, which is provenance and not a
     * capability.  So a data handle serves COMMIT, ALLOCATE, SEEK and GETATTR,
     * whose wants are spelled with the bit; a PATH|DIRECTORY handle serves a
     * COMMIT (fsyncdir is exactly that) but not a READ or WRITE, which an
     * O_PATH descriptor cannot do; and a data handle does not serve a LOOKUP,
     * which needs a directory.  A lent handle that does not serve fails the op
     * with EINVAL rather than being replaced: the caller's open bound rights to
     * it that a substitute would not carry. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *oh, *dh;
        struct evpl_iovec               wiov;
        struct evpl_iovec               rdiov[4];
        int                             i_open, i_ga, i_commit, i_seek, i_alloc;
        int                             i_lk, i_rd, i_wr, i_gh;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "ph", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        /* Some data, so a SEEK for data has something to find. */
        assert(evpl_iovec_alloc(ctx.evpl, 8, 0, 1, 0, &wiov) == 1);
        memcpy(evpl_iovec_data(&wiov), "puthandl", 8);

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_wr = chimera_vfs_compound_add_write(cp, oh, 0, 8, 0, &wiov, 1,
                                              0, 0, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_wr)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* A data handle, lent with its real flags, serves the four. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_ga     = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_commit = chimera_vfs_compound_add_commit(cp, 0, 0, 0, 0);
        i_seek   = chimera_vfs_compound_add_seek(cp, NULL, 0, 0);
        i_alloc  = chimera_vfs_compound_add_allocate(cp, NULL, 0, 16, 0, 0,
                                                     CHIMERA_VFS_ATTR_SIZE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_ga);
        assert(op->status == CHIMERA_VFS_OK);
        assert(S_ISREG(op->attr.va_mode));
        assert(chimera_vfs_compound_op(cp, i_commit)->status == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_seek);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->seek_offset == 0);
        op = chimera_vfs_compound_op(cp, i_alloc);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size >= 16);

        chimera_vfs_compound_free(cp);

        /* ...but not a LOOKUP, which needs a directory: EINVAL, and the
         * sequence stops rather than opening a directory of its own. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lk = chimera_vfs_compound_add_lookup(cp, "x", 1, 0, 0);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_num_completed(cp) == 2);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);

        chimera_vfs_compound_free(cp);

        /* An opendir handle, the shape FUSE's OPENDIR keeps. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        i_gh          = chimera_vfs_compound_add_gethandle(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        dh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_gh);
        assert(dh != NULL);
        chimera_vfs_compound_free(cp);

        /* It serves a COMMIT -- fsyncdir. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, dh,
                                           CHIMERA_VFS_OPEN_INFERRED |
                                           CHIMERA_VFS_OPEN_PATH |
                                           CHIMERA_VFS_OPEN_DIRECTORY);
        i_commit      = chimera_vfs_compound_add_commit(cp, 0, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_commit)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* ...and neither a READ nor a WRITE. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, dh,
                                           CHIMERA_VFS_OPEN_INFERRED |
                                           CHIMERA_VFS_OPEN_PATH |
                                           CHIMERA_VFS_OPEN_DIRECTORY);
        i_rd = chimera_vfs_compound_add_read(cp, NULL, 0, 4096, rdiov, 4,
                                             0, NULL, NULL, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_rd)->status == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, dh,
                                           CHIMERA_VFS_OPEN_INFERRED |
                                           CHIMERA_VFS_OPEN_PATH |
                                           CHIMERA_VFS_OPEN_DIRECTORY);
        i_wr = chimera_vfs_compound_add_write(cp, NULL, 0, 8, 0, &wiov, 1,
                                              0, 0, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_wr)->status == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        /* Lent means lent: both are still ours. */
        evpl_iovec_release(ctx.evpl, &wiov);
        chimera_vfs_release(ctx.vfs_thread, oh);
        chimera_vfs_release(ctx.vfs_thread, dh);
    }
    TEST_PASS("a lent handle serves by its real flags; a mismatch is EINVAL");

    /* ---- the cursor ops: OPEN_CURRENT, GETHANDLE, CLOSE, SAVE/RESTOREHANDLE
     * The current OPEN handle is a slot with one owner.  OPEN_CURRENT fills
     * it; GETHANDLE hands ownership to the caller without emptying it; CLOSE
     * ends the handle whatever its provenance and empties it; SAVEHANDLE and
     * RESTOREHANDLE MOVE it, so exactly one slot refers to it at any moment.
     * Every one of them with an empty slot is EINVAL. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *h;
        int                             i_gh, i_lk, i_fh, i_cl, i_sv, i_rs;
        int                             i_ga, i_open;

        /* OPEN_CURRENT then GETHANDLE: the caller owns it, the slot goes on
         * addressing it, and the LOOKUP behind them resolves through it
         * without releasing it. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        i_gh = chimera_vfs_compound_add_gethandle(cp);
        i_lk = chimera_vfs_compound_add_lookup(cp, "a", 1, 0, 0);
        i_fh = chimera_vfs_compound_add_getfh(cp);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_gh);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->out_handle != NULL);
        assert(op->out_handle->fh_len == root_fh_len);
        assert(memcmp(op->out_handle->fh, root_fh, root_fh_len) == 0);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_fh)->fh_len == a_fh_len);
        assert(memcmp(chimera_vfs_compound_op(cp, i_fh)->fh, a_fh,
                      a_fh_len) == 0);

        h = chimera_vfs_compound_take_handle(cp, (uint32_t) i_gh);
        assert(h != NULL);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, h);

        /* GETHANDLE with nothing open. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_gh          = chimera_vfs_compound_add_gethandle(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_gh)->status == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        /* CLOSE empties the slot: a GETHANDLE behind it has nothing. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        i_cl = chimera_vfs_compound_add_close(cp);
        i_gh = chimera_vfs_compound_add_gethandle(cp);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_cl)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_gh)->status == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_num_completed(cp) == 4);
        chimera_vfs_compound_free(cp);

        /* CLOSE with nothing open. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_cl          = chimera_vfs_compound_add_close(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_cl)->status == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        /* SAVEHANDLE parks the root's handle; the sequence then opens a
         * different object for itself; RESTOREHANDLE puts the root's back,
         * releasing the other -- and GETHANDLE shows which one is there. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        i_sv = chimera_vfs_compound_add_savehandle(cp);
        chimera_vfs_compound_add_putfh(cp, a_fh, (int) a_fh_len);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);
        i_rs = chimera_vfs_compound_add_restorehandle(cp);
        i_gh = chimera_vfs_compound_add_gethandle(cp);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_sv)->status == CHIMERA_VFS_OK);
        assert(S_ISDIR(chimera_vfs_compound_op(cp, i_ga)->attr.va_mode));
        assert(chimera_vfs_compound_op(cp, i_rs)->status == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_gh);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->out_handle != NULL);
        assert(op->out_handle->fh_len == root_fh_len);
        assert(memcmp(op->out_handle->fh, root_fh, root_fh_len) == 0);

        h = chimera_vfs_compound_take_handle(cp, (uint32_t) i_gh);
        assert(h != NULL);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, h);

        /* SAVEHANDLE and RESTOREHANDLE with nothing to move. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_sv          = chimera_vfs_compound_add_savehandle(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_sv)->status == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rs          = chimera_vfs_compound_add_restorehandle(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_rs)->status == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        /* CLOSE ends a LENT handle too: after this the caller must not
         * release it, and does not. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "cl", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        h = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(h != NULL);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, h, CHIMERA_VFS_OPEN_READ_ONLY);
        i_cl          = chimera_vfs_compound_add_close(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_cl)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("the open cursor ops fill, hand out, close, and move the slot");

    /* ---- PUTROOT makes the export root current ---- */
    {
        uint8_t  mroot_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t mroot_fh_len;
        int      i_fh, i_lk;

        chimera_vfs_get_root_fh(mroot_fh, &mroot_fh_len);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putroot(cp);
        i_fh = chimera_vfs_compound_add_getfh(cp);
        /* The mount is a name in that root, so a LOOKUP through it lands on
         * the same object the test resolved by path at the start. */
        i_lk = chimera_vfs_compound_add_lookup(cp, "mem", 3,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_fh);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == mroot_fh_len);
        assert(memcmp(op->fh, mroot_fh, mroot_fh_len) == 0);
        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == root_fh_len);
        assert(memcmp(op->fh, root_fh, root_fh_len) == 0);

        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("PUTROOT makes the export root the current file handle");

    /* ---- the path-addressed ops ----
     * Each resolves a whole path against the current FILE HANDLE, opens
     * nothing of its own, and -- when the caller asked for the file handle
     * among the attributes -- makes what it resolved current.  An OPEN_PATH's
     * handle is the current open handle afterwards and is reachable by the
     * next op through chimera_vfs_compound_op_use_handle. */
    {
        struct chimera_vfs_attrs sattr;
        struct evpl_iovec        wiov;
        int                      i_lp, i_cd, i_fh, i_cs, i_cn, i_op, i_wr;
        int                      i_ln, i_lp2, i_rn, i_rm, i_lp3;

        /* LOOKUP_PATH resolves and becomes current. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lp = chimera_vfs_compound_add_lookup_path(
            cp, "a", 1, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0);
        i_fh = chimera_vfs_compound_add_getfh(cp);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_lp);
        assert(op->status == CHIMERA_VFS_OK);
        assert(S_ISDIR(op->attr.va_mode));
        assert(op->fh_len == a_fh_len);
        assert(memcmp(op->fh, a_fh, a_fh_len) == 0);
        assert(chimera_vfs_compound_op(cp, i_fh)->fh_len == a_fh_len);
        assert(memcmp(chimera_vfs_compound_op(cp, i_fh)->fh, a_fh,
                      a_fh_len) == 0);
        chimera_vfs_compound_free(cp);

        /* CREATE_PATH, each of its three shapes.  The directory becomes
         * current, so the symlink is made inside it; the node is made back in
         * the root. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0755;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_cd = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "pd", 2, NULL, 0, &sattr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0);
        i_fh = chimera_vfs_compound_add_getfh(cp);
        i_cs = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_SYMLINK, "pl", 2, "..", 2, NULL,
            CHIMERA_VFS_ATTR_MASK_STAT, 0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        sattr.va_mode = S_IFIFO | 0600;
        i_cn          = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_NODE, "pn", 2, NULL, 0, &sattr,
            CHIMERA_VFS_ATTR_MASK_STAT, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(S_ISDIR(op->attr.va_mode));
        assert(chimera_vfs_compound_op(cp, i_fh)->fh_len == op->attr.va_fh_len);
        assert(memcmp(chimera_vfs_compound_op(cp, i_fh)->fh, op->attr.va_fh,
                      op->attr.va_fh_len) == 0);
        assert(S_ISLNK(chimera_vfs_compound_op(cp, i_cs)->attr.va_mode));
        assert(S_ISFIFO(chimera_vfs_compound_op(cp, i_cn)->attr.va_mode));
        chimera_vfs_compound_free(cp);

        /* OPEN_PATH creates and opens; the WRITE behind it addresses the
         * handle it produced. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        assert(evpl_iovec_alloc(ctx.evpl, 8, 0, 1, 0, &wiov) == 1);
        memcpy(evpl_iovec_data(&wiov), "pathopen", 8);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_op = chimera_vfs_compound_add_open_path(
            cp, "pf", 2,
            CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_CREATE_REGULAR |
            CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY,
            &sattr, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT);
        i_wr = chimera_vfs_compound_add_write(cp, NULL, 0, 8, 0, &wiov, 1,
                                              0, CHIMERA_VFS_ATTR_SIZE, NULL);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_wr, (uint32_t) i_op);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_op);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->created);
        assert(op->out_handle != NULL);
        assert(S_ISREG(op->attr.va_mode));
        op = chimera_vfs_compound_op(cp, i_wr);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->written == 8);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 8);
        /* The handle is deliberately left untaken: free releases it. */
        chimera_vfs_compound_free(cp);

        /* LINK_PATH, RENAME_PATH, REMOVE_PATH, and a LOOKUP_PATH of the name
         * that is now gone -- which is where the sequence stops.  A path op
         * that resolves an object makes it current (LINK_PATH resolves the
         * new link, LOOKUP_PATH what it looked up), and the next path is
         * resolved relative to THAT, so the sequence re-seeds the root in
         * front of each op that follows one of them. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_ln = chimera_vfs_compound_add_link_path(cp, "pf", 2, 0, "pf2", 3,
                                                  CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lp2 = chimera_vfs_compound_add_lookup_path(cp, "pf2", 3,
                                                     CHIMERA_VFS_ATTR_MASK_STAT,
                                                     0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rn  = chimera_vfs_compound_add_rename_path(cp, "pf2", 3, "pf3", 3);
        i_rm  = chimera_vfs_compound_add_remove_path(cp, "pf3", 3, 0);
        i_lp3 = chimera_vfs_compound_add_lookup_path(cp, "pf3", 3,
                                                     CHIMERA_VFS_ATTR_MASK_STAT,
                                                     0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOENT);
        assert(chimera_vfs_compound_num_completed(cp) == 8);
        assert(chimera_vfs_compound_op(cp, i_ln)->status == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_lp2);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->attr.va_nlink == 2);
        assert(chimera_vfs_compound_op(cp, i_rn)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_rm)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_lp3)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);

        evpl_iovec_release(ctx.evpl, &wiov);
    }
    TEST_PASS("the path ops resolve against the current file handle");

    /* ---- a LOCK granted in a sequence that then fails is released ----
     * The header's rule: a claim belongs to the sequence until the sequence
     * is over, and a sequence that finishes with a failure releases what it
     * inserted before the caller hears about it.  The op keeps its own
     * answer -- it ran, the arbiter said GRANTED -- but the file state is
     * gone from it, and the proof is that another owner can take the range
     * straight afterwards.  A veto from the gate on the LOCK itself is the
     * same case. */
    {
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           claim_a, claim_b, probe;
        struct chimera_vfs_pending_acquire ticket_a, ticket_b;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_file_state     *fs;
        struct gate_ctx                    g;
        int                                i_open, i_lock, i_lk, i_probe;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "la", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 11;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 12;

        /* GRANTED, then the LOOKUP behind it fails (a data handle cannot
         * serve one). */
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_lock(cp, &claim_a, &ticket_a, 0);
        i_lk   = chimera_vfs_compound_add_lookup(cp, "x", 1, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_EINVAL);
        op = chimera_vfs_compound_op(cp, i_lock);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        /* ...but the claim went with the failure. */
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock) == NULL);
        chimera_vfs_compound_free(cp);

        /* The range is free: another owner probes it and takes it. */
        chimera_vfs_claim_init_range(&probe, true, false, 0, 16, &owner_b);
        chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16, &owner_b);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_probe = chimera_vfs_compound_add_lock_test(cp, &probe);
        i_lock  = chimera_vfs_compound_add_lock(cp, &claim_b, &ticket_b, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_probe)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        assert(chimera_vfs_compound_op(cp, i_lock)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release_ranged(ctx.vfs_thread,
                                         ctx.vfs->vfs_state, fs, &claim_b);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs);

        /* The gate vetoing the LOCK itself: granted, then failed, then
         * released. */
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);
        memset(&g, 0, sizeof(g));
        g.veto_index = 1;
        g.veto       = CHIMERA_VFS_EPERM;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_set_gate(cp, veto_gate, &g);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_lock(cp, &claim_a, &ticket_a, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EPERM);
        op = chimera_vfs_compound_op(cp, i_lock);
        assert(op->status == CHIMERA_VFS_EPERM);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock) == NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_init_range(&probe, true, false, 0, 16, &owner_b);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_probe       = chimera_vfs_compound_add_lock_test(cp, &probe);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_probe)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        chimera_vfs_compound_free(cp);

        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("a LOCK in a sequence that then fails is released with it");

    /* ---- a parked LOCK granted from ANOTHER thread completes on this one --
     * A blocking lock waits on the file's pending queue, and the pump that
     * grants it runs on whatever thread released the blocker.  Here that is
     * a second VFS thread, and the promise under test is the header's: the
     * rest of the sequence, and the completion, run on the thread that
     * submitted -- never on the one the grant happened to arrive on. */
    {
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           claim_a, claim_b;
        struct chimera_vfs_pending_acquire ticket_a, ticket_b;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_file_state     *fs_a, *fs_b;
        struct remote_release              rr;
        pthread_t                          self = pthread_self();
        pthread_t                          tid;
        int                                i_open, i_lock, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "lp", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 21;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 22;

        /* A holds the range, granted on the spot. */
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_lock(cp, &claim_a, &ticket_a, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(pthread_equal(ctx.cb_thread, self));
        fs_a = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock);
        assert(fs_a != NULL);
        chimera_vfs_compound_free(cp);

        /* B blocks on it, with a GETATTR behind the LOCK so the sequence has
         * somewhere to go once the grant arrives. */
        chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16, &owner_b);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_lock(cp, &claim_b, &ticket_b,
                                               CHIMERA_VFS_COMPOUND_LOCK_WAIT |
                                               CHIMERA_VFS_COMPOUND_LOCK_WAIT_HARD);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);

        /* Parked: submit returned with nothing to report. */
        assert(ctx.callbacks == 0);
        assert(!ctx.done);

        /* A lets go, from a different VFS thread. */
        rr.vfs   = ctx.vfs;
        rr.fs    = fs_a;
        rr.claim = &claim_a;
        assert(pthread_create(&tid, NULL, remote_release_main, &rr) == 0);

        wait_done(&ctx);
        assert(pthread_join(tid, NULL) == 0);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_lock);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        /* The op behind the LOCK ran -- after the grant, and here. */
        op = chimera_vfs_compound_op(cp, i_ga);
        assert(op->status == CHIMERA_VFS_OK);
        assert(S_ISREG(op->attr.va_mode));
        /* On the submitting thread, not the releasing one. */
        assert(pthread_equal(ctx.cb_thread, self));
        assert(!pthread_equal(ctx.cb_thread, rr.self));

        fs_b = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock);
        assert(fs_b != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release_ranged(ctx.vfs_thread,
                                         ctx.vfs->vfs_state, fs_b, &claim_b);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs_b);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs_a);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("a LOCK granted from another thread completes on the submitting one");

    /* ---- REMOVE that matches its victim ----
     * The name-op setters ride behind the adder, so a caller that has no lease
     * to spare and no object to match pays nothing.  The match is the one
     * with a visible answer: a guarded REMOVE unlinks the name while it still
     * resolves to the object the caller had in hand, and leaves it alone once
     * something else has taken the name -- reporting OK either way, because
     * the caller's object is gone either way.  That OK is the whole contract:
     * an SMB delete-on-close firing late must not destroy the file another
     * opener has since created under the same name. */
    {
        struct chimera_vfs_attrs sattr;
        uint8_t                  v1_fh[CHIMERA_VFS_FH_SIZE], v2_fh[CHIMERA_VFS_FH_SIZE];
        uint8_t                  v2b_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 v1_fh_len, v2_fh_len, v2b_fh_len;
        uint8_t                  lease_key[16];
        int                      i_o1, i_o2, i_rm, i_lk, i;

        for (i = 0; i < 16; i++) {
            lease_key[i] = (uint8_t) (0xA0 + i);
        }

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_o1 = chimera_vfs_compound_add_open(cp, "rm1", 3,
                                             CHIMERA_VFS_OPEN_CREATE |
                                             CHIMERA_VFS_OPEN_WRITE_ONLY,
                                             0, &sattr, 0, 0, 0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_o2 = chimera_vfs_compound_add_open(cp, "rm2", 3,
                                             CHIMERA_VFS_OPEN_CREATE |
                                             CHIMERA_VFS_OPEN_WRITE_ONLY,
                                             0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        /* An OPEN's op->fh is the object it opened. */
        op = chimera_vfs_compound_op(cp, i_o1);
        memcpy(v1_fh, op->fh, op->fh_len);
        v1_fh_len = op->fh_len;
        op        = chimera_vfs_compound_op(cp, i_o2);
        memcpy(v2_fh, op->fh, op->fh_len);
        v2_fh_len = op->fh_len;
        chimera_vfs_compound_free(cp);

        /* The right fh: the name goes. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "rm1", 3, 0, 0,
                                               CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_set_remove_match(cp, (uint32_t) i_rm,
                                                 v1_fh, v1_fh_len, 1, NULL);
        i_lk = chimera_vfs_compound_add_lookup(cp, "rm1", 3, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        op = chimera_vfs_compound_op(cp, i_rm);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->child_fh_match);
        assert(!op->parent_lease_skip_valid);
        /* Still the parent, and it still reports the directory pair. */
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->fh_len == root_fh_len);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);

        /* rm2 is removed and re-created, so the fh the caller kept is now
         * stale: a different object answers to the name. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_remove(cp, "rm2", 3, 0, 0, 0);
        i_o2 = chimera_vfs_compound_add_open(cp, "rm2", 3,
                                             CHIMERA_VFS_OPEN_CREATE |
                                             CHIMERA_VFS_OPEN_WRITE_ONLY,
                                             0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_o2);
        memcpy(v2b_fh, op->fh, op->fh_len);
        v2b_fh_len = op->fh_len;
        assert(v2b_fh_len != v2_fh_len || memcmp(v2b_fh, v2_fh, v2_fh_len) != 0);
        chimera_vfs_compound_free(cp);

        /* The stale fh: the op reports OK -- what remove_at_match_fh says of a
         * mismatch, on every backend -- and the name is still there, still
         * resolving to the replacement.  The lease key rode along, copied. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "rm2", 3, 0, 0, 0);
        chimera_vfs_compound_op_set_remove_match(cp, (uint32_t) i_rm,
                                                 v2_fh, v2_fh_len, 1,
                                                 lease_key);
        i_lk = chimera_vfs_compound_add_lookup(cp, "rm2", 3,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rm);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->parent_lease_skip_valid);
        assert(memcmp(op->parent_lease_skip, lease_key, 16) == 0);
        assert(op->child_fh_len == v2_fh_len);
        assert(memcmp(op->child_fh, v2_fh, v2_fh_len) == 0);
        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == v2b_fh_len);
        assert(memcmp(op->fh, v2b_fh, v2b_fh_len) == 0);
        chimera_vfs_compound_free(cp);

        /* Without the match, the same stale fh is only the recall target and
         * the name goes -- which is what the plain remove_at does with one. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "rm2", 3, 0, 0, 0);
        chimera_vfs_compound_op_set_remove_match(cp, (uint32_t) i_rm,
                                                 v2_fh, v2_fh_len, 0, NULL);
        i_lk = chimera_vfs_compound_add_lookup(cp, "rm2", 3, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_rm)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);

        /* A match with nothing to match is a build failure, reported by
         * submit rather than run as an unconditional remove. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "rm2", 3, 0, 0, 0);
        chimera_vfs_compound_op_set_remove_match(cp, (uint32_t) i_rm,
                                                 NULL, 0, 1, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_num_completed(cp) == 0);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("a matched REMOVE unlinks its own object and spares a replacement");

    /* ---- LINK with replace ----
     * link(2) never clobbers, and neither does the op by default.  With the
     * setter's `replace` the destination name is taken over -- the S3 publish
     * of an unlinked object, and SMB's rename-via-link with ReplaceIfExists. */
    {
        struct chimera_vfs_attrs sattr;
        uint8_t                  la_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 la_fh_len;
        int                      i_oa, i_link, i_lk;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_oa = chimera_vfs_compound_add_open(cp, "lk_a", 4,
                                             CHIMERA_VFS_OPEN_CREATE |
                                             CHIMERA_VFS_OPEN_WRITE_ONLY,
                                             0, &sattr, 0, 0, 0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open(cp, "lk_b", 4,
                                      CHIMERA_VFS_OPEN_CREATE |
                                      CHIMERA_VFS_OPEN_WRITE_ONLY,
                                      0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_oa);
        memcpy(la_fh, op->fh, op->fh_len);
        la_fh_len = op->fh_len;
        chimera_vfs_compound_free(cp);

        /* Over an existing name, without replace: EEXIST. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, la_fh, (int) la_fh_len);
        chimera_vfs_compound_add_savefh(cp);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_link = chimera_vfs_compound_add_link(cp, "lk_b", 4, 0, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_link)->status == CHIMERA_VFS_EEXIST);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EEXIST);
        chimera_vfs_compound_free(cp);

        /* With it: the name now belongs to lk_a's object. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, la_fh, (int) la_fh_len);
        chimera_vfs_compound_add_savefh(cp);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_link = chimera_vfs_compound_add_link(cp, "lk_b", 4,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        chimera_vfs_compound_op_set_link_opts(cp, (uint32_t) i_link, 1, NULL, NULL);
        i_lk = chimera_vfs_compound_add_lookup(cp, "lk_b", 4,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_link);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->link_replace);
        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->attr.va_nlink == 2);
        assert(op->fh_len == la_fh_len);
        assert(memcmp(op->fh, la_fh, la_fh_len) == 0);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("LINK clobbers an existing name only with replace");

    /* ---- RENAME of a directory, saying so ----
     * SRC_IS_DIR is the caller's word on the renamed object's type, which the
     * notify filters want and only the open can supply; it rides in with the
     * setter beside the adder's remove-side flags, and a known target fh with
     * it.  Renaming onto an empty directory is the shape that exercises both. */
    {
        struct chimera_vfs_attrs sattr;
        uint8_t                  d1_fh[CHIMERA_VFS_FH_SIZE], d3_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 d1_fh_len, d3_fh_len;
        int                      i_d1, i_d3, i_ren, i_lk, i_lk2;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0755;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_d1 = chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "rd1", 3, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_d3 = chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "rd3", 3, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_d1);
        memcpy(d1_fh, op->fh, op->fh_len);
        d1_fh_len = op->fh_len;
        op        = chimera_vfs_compound_op(cp, i_d3);
        memcpy(d3_fh, op->fh, op->fh_len);
        d3_fh_len = op->fh_len;
        chimera_vfs_compound_free(cp);

        /* rd1 -> rd2, a directory, no target. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_savefh(cp);
        i_ren = chimera_vfs_compound_add_rename(cp, "rd1", 3, "rd2", 3,
                                                CHIMERA_VFS_REMOVE_ISDIR, 0, 0);
        chimera_vfs_compound_op_set_rename_opts(cp, (uint32_t) i_ren, NULL, 0,
                                                NULL, NULL,
                                                CHIMERA_VFS_RENAME_SRC_IS_DIR);
        i_lk = chimera_vfs_compound_add_lookup(cp, "rd2", 3,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_ren);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->rename_flags == CHIMERA_VFS_RENAME_SRC_IS_DIR);
        assert(op->remove_flags == CHIMERA_VFS_REMOVE_ISDIR);
        assert(op->target_fh_len == 0);
        assert(op->from_dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->status == CHIMERA_VFS_OK);
        assert(S_ISDIR(op->attr.va_mode));
        assert(op->fh_len == d1_fh_len);
        assert(memcmp(op->fh, d1_fh, d1_fh_len) == 0);
        chimera_vfs_compound_free(cp);

        /* rd2 -> rd3, onto the empty directory whose fh the caller knows. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_savefh(cp);
        i_ren = chimera_vfs_compound_add_rename(cp, "rd2", 3, "rd3", 3, 0, 0, 0);
        chimera_vfs_compound_op_set_rename_opts(cp, (uint32_t) i_ren,
                                                d3_fh, d3_fh_len, NULL, NULL,
                                                CHIMERA_VFS_RENAME_SRC_IS_DIR);
        i_lk = chimera_vfs_compound_add_lookup(cp, "rd3", 3,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk2         = chimera_vfs_compound_add_lookup(cp, "rd2", 3, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        op = chimera_vfs_compound_op(cp, i_ren);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->target_fh_len == d3_fh_len);
        assert(memcmp(op->target_fh, d3_fh, d3_fh_len) == 0);
        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == d1_fh_len);
        assert(memcmp(op->fh, d1_fh, d1_fh_len) == 0);
        assert(chimera_vfs_compound_op(cp, i_lk2)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("RENAME carries SRC_IS_DIR and a known target fh");

    /* ---- CREATE_PATH with intermediates: mkdir -p ----
     * The chain is made in one op, a second run of the same op is not an
     * error, and without the flag a missing parent is the ENOENT it always
     * was.  Only the DIR shape honours it. */
    {
        struct chimera_vfs_attrs sattr;
        uint8_t                  c_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 c_fh_len;
        int                      i_cp, i_fh, i_lp;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0755;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_cp = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "mp/a/b/c", 8, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 1);
        i_fh = chimera_vfs_compound_add_getfh(cp);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cp);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->path_intermediates);
        assert(S_ISDIR(op->attr.va_mode));
        /* The leaf became current, as for the single-level create. */
        assert(op->fh_len > 0);
        assert(chimera_vfs_compound_op(cp, i_fh)->fh_len == op->fh_len);
        memcpy(c_fh, op->fh, op->fh_len);
        c_fh_len = op->fh_len;
        chimera_vfs_compound_free(cp);

        /* Every component is there, and the leaf is the object reported. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lp = chimera_vfs_compound_add_lookup_path(
            cp, "mp/a/b/c", 8, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_lp);
        assert(S_ISDIR(op->attr.va_mode));
        assert(op->fh_len == c_fh_len);
        assert(memcmp(op->fh, c_fh, c_fh_len) == 0);
        chimera_vfs_compound_free(cp);

        /* Again: not an error, and the same object. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_cp = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "mp/a/b/c", 8, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 1);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cp);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == c_fh_len);
        assert(memcmp(op->fh, c_fh, c_fh_len) == 0);
        chimera_vfs_compound_free(cp);

        /* Without the flag, a missing parent is ENOENT. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_cp = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "mp2/x", 5, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_FH, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_cp)->status == CHIMERA_VFS_ENOENT);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);

        /* And so is a symlink asked for one: the flag is the DIR shape's. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_cp = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_SYMLINK, "mp3/l", 5, "..", 2,
            NULL, CHIMERA_VFS_ATTR_FH, 1);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        op = chimera_vfs_compound_op(cp, i_cp);
        assert(!op->path_intermediates);
        assert(op->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("CREATE_PATH with intermediates makes the chain; without, ENOENT");

    /* ---- handle state on an OPEN ----
     * memfs has no CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE, so this is the setter's
     * "no capability" arms: a named OPEN's record lands in the default KV
     * after the open, keyed by the new object's fh; an unnamed OPEN's record
     * goes to the backend, which ignores it; an OPEN_PATH refuses one. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_handle_state hs, hs2;
        struct kv_probe                 probe;
        uint8_t                         h_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        h_fh_len;
        int                             i_open, i_op;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        hs.key       = "durable";
        hs.key_len   = 7;
        hs.value     = "rec1";
        hs.value_len = 4;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "hs1", 3,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, &sattr,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        chimera_vfs_compound_op_set_handle_state(cp, (uint32_t) i_open, &hs);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->created);
        assert(op->handle_state == &hs);
        assert(op->out_handle != NULL);
        memcpy(h_fh, op->fh, op->fh_len);
        h_fh_len = op->fh_len;
        chimera_vfs_compound_free(cp);

        /* The record is in the default KV, on the same route put_key_at
         * took: search by the fh it was keyed under. */
        memset(&probe, 0, sizeof(probe));
        probe.ctx = &ctx;
        chimera_vfs_search_keys_at(ctx.vfs_thread, &cred, h_fh, (int) h_fh_len,
                                   "durable", 7, "durable\xff", 8, 0,
                                   kv_probe_entry, kv_probe_complete, &probe);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        assert(probe.found == 1);
        assert(probe.value_len == 4);
        assert(memcmp(probe.value, "rec1", 4) == 0);

        /* An unnamed OPEN: the op succeeds and, this backend lacking the
        * capability, nothing is stored -- open_fh has no KV fallback. */
        hs2         = hs;
        hs2.key     = "durable2";
        hs2.key_len = 8;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, h_fh, (int) h_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, NULL, 0,
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, NULL, 0, 0, 0);
        chimera_vfs_compound_op_set_handle_state(cp, (uint32_t) i_open, &hs2);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_open)->out_handle != NULL);
        chimera_vfs_compound_free(cp);

        memset(&probe, 0, sizeof(probe));
        probe.ctx = &ctx;
        chimera_vfs_search_keys_at(ctx.vfs_thread, &cred, h_fh, (int) h_fh_len,
                                   "durable2", 8, "durable2\xff", 9, 0,
                                   kv_probe_entry, kv_probe_complete, &probe);
        wait_done(&ctx);
        assert(probe.found == 0);

        /* An OPEN_PATH carrying a record is refused, and opens nothing. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_op = chimera_vfs_compound_add_open_path(
            cp, "hs2", 3,
            CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_CREATE_REGULAR |
            CHIMERA_VFS_OPEN_WRITE_ONLY,
            &sattr, CHIMERA_VFS_ATTR_FH);
        chimera_vfs_compound_op_set_handle_state(cp, (uint32_t) i_op, &hs);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        op = chimera_vfs_compound_op(cp, i_op);
        assert(op->status == CHIMERA_VFS_ENOTSUP);
        assert(op->out_handle == NULL);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTSUP);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_op          = chimera_vfs_compound_add_lookup(cp, "hs2", 3, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_op)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("handle state persists with a named OPEN; the other shapes say what they do");

    /* ---- CREATE_UNLINKED: an object with no name, written, then published ----
     * The directory stays the current file handle (an unlinked object has no
     * name to make current) while the new object's handle takes the open
     * cursor, so a WRITE lands in it and a GETFH still answers the directory.
     * The name comes later, by LINK, from the handle the caller took. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *oh;
        struct evpl_iovec               wiov;
        uint8_t                         obj_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        obj_fh_len;
        int                             i_cu, i_fh, i_wr, i_ln, i_lk;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0600;

        assert(evpl_iovec_alloc(ctx.evpl, 8, 0, 1, 0, &wiov) == 1);
        memcpy(evpl_iovec_data(&wiov), "unlinked", 8);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, (int) a_fh_len);
        i_cu = chimera_vfs_compound_add_create_unlinked(
            cp, CHIMERA_VFS_OPEN_WRITE_ONLY | CHIMERA_VFS_OPEN_READ_ONLY,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT);
        i_fh = chimera_vfs_compound_add_getfh(cp);
        i_wr = chimera_vfs_compound_add_write(cp, NULL, 0, 8, 2, &wiov, 1,
                                              0, CHIMERA_VFS_ATTR_SIZE, NULL);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_wr, (uint32_t) i_cu);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_cu);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->created);
        assert(op->out_handle != NULL);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert(S_ISREG(op->attr.va_mode));
        /* The op's own fh result is the DIRECTORY: the object is nameless. */
        assert(op->fh_len == a_fh_len);
        assert(memcmp(op->fh, a_fh, a_fh_len) == 0);
        assert(op->attr.va_fh_len != a_fh_len ||
               memcmp(op->attr.va_fh, a_fh, a_fh_len) != 0);
        memcpy(obj_fh, op->attr.va_fh, op->attr.va_fh_len);
        obj_fh_len = op->attr.va_fh_len;

        /* GETFH proves the file cursor did not move. */
        op = chimera_vfs_compound_op(cp, i_fh);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == a_fh_len);
        assert(memcmp(op->fh, a_fh, a_fh_len) == 0);

        op = chimera_vfs_compound_op(cp, i_wr);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->written == 8);
        assert(op->attr.va_size == 8);

        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_cu);
        assert(oh != NULL);
        assert(oh->fh_len == obj_fh_len);
        assert(memcmp(oh->fh, obj_fh, obj_fh_len) == 0);
        chimera_vfs_compound_free(cp);
        evpl_iovec_release(ctx.evpl, &wiov);

        /* Publish: the object (by its handle) is the saved fh, the directory
         * the current one, and LINK gives it its first name.  A LOOKUP in the
         * same sequence then finds it with the size the WRITE left. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_WRITE_ONLY |
                                           CHIMERA_VFS_OPEN_READ_ONLY);
        chimera_vfs_compound_add_savefh(cp);
        chimera_vfs_compound_add_putfh(cp, a_fh, (int) a_fh_len);
        i_ln = chimera_vfs_compound_add_link(cp, "published", 9,
                                             CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);
        i_lk = chimera_vfs_compound_add_lookup(cp, "published", 9,
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ln)->status == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_lk);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->fh_len == obj_fh_len);
        assert(memcmp(op->fh, obj_fh, obj_fh_len) == 0);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 8);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, oh);

        /* The two cursors name different objects, so the handle is held to
         * the lent rule: a READ that a WRITE_ONLY create does not serve is
         * EINVAL, not a re-open of the directory in the object's place.  A
         * LOOKUP, wanting the directory, re-opens it as usual and finds the
         * name given above. */
        {
            struct evpl_iovec rdiov[4];
            int               i_rd;

            cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            chimera_vfs_compound_add_putfh(cp, a_fh, (int) a_fh_len);
            i_cu = chimera_vfs_compound_add_create_unlinked(
                cp, CHIMERA_VFS_OPEN_WRITE_ONLY, &sattr, 0);
            i_lk = chimera_vfs_compound_add_lookup(cp, "published", 9,
                                                   CHIMERA_VFS_ATTR_MASK_STAT, 0);
            ctx.callbacks = 0;
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            wait_done(&ctx);
            assert(ctx.callbacks == 1);
            assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
            assert(chimera_vfs_compound_op(cp, i_cu)->status == CHIMERA_VFS_OK);
            op = chimera_vfs_compound_op(cp, i_lk);
            assert(op->status == CHIMERA_VFS_OK);
            assert(op->attr.va_size == 8);
            chimera_vfs_compound_free(cp);

            cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            chimera_vfs_compound_add_putfh(cp, a_fh, (int) a_fh_len);
            i_cu = chimera_vfs_compound_add_create_unlinked(
                cp, CHIMERA_VFS_OPEN_WRITE_ONLY, &sattr, 0);
            i_rd = chimera_vfs_compound_add_read(cp, NULL, 0, 8, rdiov, 4, 0,
                                                 NULL, NULL, 0);
            ctx.callbacks = 0;
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            wait_done(&ctx);
            assert(ctx.callbacks == 1);
            assert(chimera_vfs_compound_op(cp, i_cu)->status == CHIMERA_VFS_OK);
            op = chimera_vfs_compound_op(cp, i_rd);
            assert(op->status == CHIMERA_VFS_EINVAL);
            assert(op->niov == 0);
            assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
            chimera_vfs_compound_free(cp);
        }

        /* (Through a non-directory the answer is the backend's: memfs's PATH
         * open does not check the type and its create takes the fh only to
         * find the filesystem, so there is nothing for the executor to pin
         * there -- see the op's note.) */

        /* The export root is served by a module without the capability: the
         * op reports it rather than letting the per-op call abort. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putroot(cp);
        i_cu          = chimera_vfs_compound_add_create_unlinked(cp, 0, &sattr, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        op = chimera_vfs_compound_op(cp, i_cu);
        assert(op->status == CHIMERA_VFS_ENOTSUP);
        assert(op->out_handle == NULL);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("CREATE_UNLINKED makes a nameless object current-open; LINK names it");

    /* ---- named streams: OPEN_STREAM, LIST_STREAMS, REMOVE_STREAM ----
     * The fork is opened on the base the op addresses -- here the base OPEN's
     * handle by use_handle, the shape SMB issues -- and becomes current in
     * both cursors, so a WRITE and a GETATTR behind it see the fork.  The
     * list is a page of packed records with the unnamed fork first; removing
     * the stream takes it out of the next page. */
    {
        struct chimera_vfs_attrs               sattr;
        struct evpl_iovec                      wiov;
        const struct chimera_vfs_stream_entry *ent;
        const uint8_t                         *rec;
        uint8_t                                sf_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                               sf_fh_len, off;
        int                                    i_open, i_os, i_fh, i_wr, i_ga;
        int                                    i_ls, i_rs, i_ls2;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        assert(evpl_iovec_alloc(ctx.evpl, 8, 0, 1, 0, &wiov) == 1);
        memcpy(evpl_iovec_data(&wiov), "forkdata", 8);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "sf", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        i_os = chimera_vfs_compound_add_open_stream(
            cp, "s1", 2,
            CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY |
            CHIMERA_VFS_OPEN_READ_ONLY,
            NULL, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_os, (uint32_t) i_open);
        i_fh = chimera_vfs_compound_add_getfh(cp);
        i_wr = chimera_vfs_compound_add_write(cp, NULL, 0, 8, 2, &wiov, 1,
                                              0, CHIMERA_VFS_ATTR_SIZE, NULL);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_OK);
        memcpy(sf_fh, op->fh, op->fh_len);
        sf_fh_len = op->fh_len;

        op = chimera_vfs_compound_op(cp, i_os);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->created);
        assert(op->out_handle != NULL);
        /* The stream's attributes are the base's metadata with the fork's
         * size, and its own fh: a different object from the base. */
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE);
        assert((op->attr.va_mode & 0777) == 0600);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 0);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        assert(op->fh_len == op->attr.va_fh_len);
        assert(memcmp(op->fh, op->attr.va_fh, op->fh_len) == 0);
        assert(op->fh_len != sf_fh_len || memcmp(op->fh, sf_fh, sf_fh_len) != 0);

        /* Both cursors moved to the stream: GETFH says so, and the WRITE and
         * GETATTR behind it went to the fork. */
        op = chimera_vfs_compound_op(cp, i_fh);
        assert(op->fh_len == chimera_vfs_compound_op(cp, i_os)->fh_len);
        assert(memcmp(op->fh, chimera_vfs_compound_op(cp, i_os)->fh,
                      op->fh_len) == 0);
        op = chimera_vfs_compound_op(cp, i_wr);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->written == 8);
        op = chimera_vfs_compound_op(cp, i_ga);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->attr.va_size == 8);

        chimera_vfs_compound_free(cp);
        evpl_iovec_release(ctx.evpl, &wiov);

        /* The base's page, addressed on the cursor rules (a PATH open of the
         * current fh): the unnamed fork first, at the base's size of 0, then
         * s1 at 8 -- then REMOVE_STREAM, and the next page has only the
         * unnamed fork. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, sf_fh, (int) sf_fh_len);
        i_ls  = chimera_vfs_compound_add_list_streams(cp, 0, 4096, 1);
        i_rs  = chimera_vfs_compound_add_remove_stream(cp, "s1", 2);
        i_ls2 = chimera_vfs_compound_add_list_streams(cp, 0, 4096, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_ls);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->eof);
        assert(op->buffer_count == 2);
        assert(op->buffer_len > 0 && op->buffer_len <= 4096);

        rec = op->buffer;
        ent = (const struct chimera_vfs_stream_entry *) rec;
        assert(ent->name_len == 0);
        assert(ent->size == 0);
        /* want_fh: the unnamed fork carries the base's own fh. */
        assert(ent->fh_len == sf_fh_len);
        assert(memcmp(rec + sizeof(*ent), sf_fh, sf_fh_len) == 0);

        off = (uint32_t) (sizeof(*ent) + ent->name_len + ent->fh_len);
        off = (off + 7) & ~7u;
        ent = (const struct chimera_vfs_stream_entry *) (rec + off);
        assert(ent->name_len == 2);
        assert(memcmp(rec + off + sizeof(*ent), "s1", 2) == 0);
        assert(ent->size == 8);
        assert(ent->fh_len > 0);
        off += (uint32_t) (sizeof(*ent) + ent->name_len + ent->fh_len);
        off  = (off + 7) & ~7u;
        assert(off == op->buffer_len);

        assert(chimera_vfs_compound_op(cp, i_rs)->status == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_ls2);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->buffer_count == 1);
        ent = op->buffer;
        assert(ent->name_len == 0);
        assert(ent->fh_len == 0);
        assert(op->buffer_len == ((sizeof(*ent) + 7) & ~7u));
        chimera_vfs_compound_free(cp);

        /* The base stayed current across the list and the remove. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, sf_fh, (int) sf_fh_len);
        chimera_vfs_compound_add_remove_stream(cp, "s1", 2);
        i_fh          = chimera_vfs_compound_add_getfh(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        /* Already gone: the backend's own answer, and it stops the run. */
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOENT);
        assert(chimera_vfs_compound_op(cp, i_fh)->status == CHIMERA_VFS_UNSET);
        chimera_vfs_compound_free(cp);

        /* A backend without CAP_NAMED_STREAMS: the export root's module.
         * Each op is the per-op call's own ENOTSUP. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putroot(cp);
        i_os          = chimera_vfs_compound_add_open_stream(cp, "x", 1, 0, NULL, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        op = chimera_vfs_compound_op(cp, i_os);
        assert(op->status == CHIMERA_VFS_ENOTSUP);
        assert(op->out_handle == NULL);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putroot(cp);
        i_ls          = chimera_vfs_compound_add_list_streams(cp, 0, 4096, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_ls)->status == CHIMERA_VFS_ENOTSUP);
        chimera_vfs_compound_free(cp);

        /* A base the backend refuses streams on -- memfs allows them on
         * files and directories, not on a symlink -- comes back as the
         * backend's own status. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_create(cp, CHIMERA_VFS_COMPOUND_CREATE_SYMLINK,
                                        "slnk", 4, "sf", 2, NULL, 0, 0, 0);
        i_os = chimera_vfs_compound_add_open_stream(cp, "x", 1,
                                                    CHIMERA_VFS_OPEN_CREATE,
                                                    NULL, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        op = chimera_vfs_compound_op(cp, i_os);
        assert(op->status == CHIMERA_VFS_EINVAL);
        assert(op->out_handle == NULL);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("OPEN_STREAM makes the fork current; LIST/REMOVE_STREAM page and prune it");

    /* ---- READ into the caller's buffers ----
     * With dest_iov the data lands where the caller said, the op's iov is that
     * destination handed back, and the compound owns none of it: take_iov has
     * nothing to give and free releases nothing.  The file is the one the
     * WRITE test left holding "compound". */
    {
        struct evpl_iovec  dest;
        struct evpl_iovec  rdiov[4];
        struct evpl_iovec *tiov;
        int                tniov, i_rd;

        assert(evpl_iovec_alloc(ctx.evpl, 4096, 0, 1, 0, &dest) == 1);
        memset(evpl_iovec_data(&dest), 'x', 4096);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "wr", 2, 0, 0);
        i_rd = chimera_vfs_compound_add_read(cp, NULL, 0, 4096, rdiov, 4,
                                             CHIMERA_VFS_ATTR_MASK_STAT, NULL,
                                             &dest, 1);
        assert(i_rd >= 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->read_len == 8);
        assert(op->eof_read);
        assert(op->iov == &dest);
        assert(op->niov == 1);
        assert(memcmp(evpl_iovec_data(&dest), "compound", 8) == 0);
        /* Only read_len bytes were written; the rest is as the caller left it. */
        assert(((const char *) evpl_iovec_data(&dest))[8] == 'x');
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 8);

        chimera_vfs_compound_take_iov(cp, (uint32_t) i_rd, &tiov, &tniov);
        assert(tiov == NULL);
        assert(tniov == 0);
        /* Still readable after the take: the op never stopped describing the
         * caller's buffers. */
        assert(op->iov == &dest && op->niov == 1);
        chimera_vfs_compound_free(cp);

        /* Freeing the compound released nothing of the caller's: the buffer
         * is still whole and still ours to release. */
        assert(memcmp(evpl_iovec_data(&dest), "compound", 8) == 0);
        evpl_iovec_release(ctx.evpl, &dest);

        /* A destination with no count, or a destination plus an owner, is a
         * malformed op and the sequence does not build. */
        {
            struct chimera_claim_actor actor;

            memset(&actor, 0, sizeof(actor));
            assert(evpl_iovec_alloc(ctx.evpl, 64, 0, 1, 0, &dest) == 1);

            cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            assert(chimera_vfs_compound_add_read(cp, NULL, 0, 8, rdiov, 4, 0,
                                                 NULL, &dest, 0) == -1);
            assert(chimera_vfs_compound_add_read(cp, NULL, 0, 8, rdiov, 4, 0,
                                                 &actor, &dest, 1) == -1);
            ctx.callbacks = 0;
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            wait_done(&ctx);
            assert(ctx.callbacks == 1);
            assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
            chimera_vfs_compound_free(cp);
            evpl_iovec_release(ctx.evpl, &dest);
        }
    }
    TEST_PASS("READ with dest_iov lands in the caller's buffers, which it keeps");

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
