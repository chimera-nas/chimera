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

static const uint8_t compound_key[] = { 0, 'k', 255 };
static const uint8_t compound_value[] = { 11, 0, 22 };

static int
key_record_cb(const void *key, uint32_t key_len, const void *value,
              uint32_t value_len, void *private_data)
{
    struct test_ctx *ctx = private_data;
    assert(key_len == sizeof(compound_key) && !memcmp(key, compound_key, key_len));
    assert(value_len == sizeof(compound_value) && !memcmp(value, compound_value, value_len));
    ctx->callbacks++;
    return 0;
}

static void
key_status_cb(enum chimera_vfs_error status, void *private_data)
{
    struct test_ctx *ctx = private_data;
    ctx->status = status;
    ctx->done = 1;
}

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

struct decision_test {
    struct test_ctx *ctx;
    int              prepares;
    int              completes;
    int              resets;
    int              append_count;
};

static void
decision_reset(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct decision_test *d = private_data;

    (void) cp;
    d->resets++;
} /* decision_reset */

static void
decision_prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct decision_test           *d    = private_data;
    struct chimera_vfs_compound_op *args = chimera_vfs_compound_op_args(cp, index);

    assert(args != NULL);
    /* This binding must start from the submission value on both attempts. */
    assert(args->offset == 0);
    args->offset = 123;
    d->prepares++;
    *status = CHIMERA_VFS_EACCES;
} /* decision_prepare */

static void
decision_skip(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct decision_test *d = private_data;

    (void) status;
    d->prepares++;
    chimera_vfs_compound_op_skip(cp, index);
} /* decision_skip */

static void
decision_append(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct decision_test                 *d      = private_data;
    const struct chimera_vfs_compound_op *before = chimera_vfs_compound_op(cp, index);

    d->completes++;
    assert(*status == CHIMERA_VFS_OK);
    for (int i = 0; i < d->append_count; i++) {
        assert(chimera_vfs_compound_add_getfh(cp) >= 0);
    }
    assert(before == chimera_vfs_compound_op(cp, index));
} /* decision_append */

struct finish_test {
    struct test_ctx                *ctx;
    enum chimera_vfs_error execution_status;
    int                             finishes;
    int                             callbacks;
    int                             publications;
    int                             handle_index;
    struct chimera_vfs_open_handle *taken;
};

struct cursor_test {
    const uint8_t *expected_fh;
    uint32_t       expected_len;
    int            prepares;
};

static void
cursor_prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct cursor_test *test = private_data;
    uint32_t            len;
    const uint8_t      *fh = chimera_vfs_compound_current_fh(cp, &len);

    (void) index;
    assert(*status == CHIMERA_VFS_OK);
    assert(fh && len == test->expected_len);
    assert(memcmp(fh, test->expected_fh, len) == 0);
    test->prepares++;
} /* cursor_prepare */

static void
cursor_complete(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    int *completes = private_data;

    assert(*status == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_op(cp, index)->fh_len > 0);
    (*completes)++;
} /* cursor_complete */

static void
acl_attempt_complete(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op    = chimera_vfs_compound_op(cp, index);
    int                                  *calls = private_data;

    assert(*status == CHIMERA_VFS_OK);
    assert(op->set_attr.va_acl->num_aces == 1);
    assert(op->set_attr.va_acl->aces[0].access_mask == CHIMERA_ACE_MASK_ALL);
    assert(op->applied_acl != op->set_attr.va_acl);
    assert(op->applied_acl->num_aces == 1);
    assert(op->applied_acl->aces[0].access_mask == CHIMERA_ACE_MASK_ALL);
    /* Model a backend normalizing its writable attributes. The next attempt
     * must reconstruct them from the original input, not this edited copy. */
    op->applied_acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    op->applied_acl->num_aces            = 0;
    (*calls)++;
} /* acl_attempt_complete */

static void
reject_first_finish(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct finish_test *test = private_data;

    assert(chimera_vfs_compound_execution_status(cp) == test->execution_status);
    assert(test->publications == 0);
    test->finishes++;
    /* All operations may have succeeded, or execution may have stopped with
     * an ordinary error: transaction rejection dominates either outcome. */
    chimera_vfs_compound_finish_result(cp,
                                       test->finishes == 1 ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* reject_first_finish */

static void
finish_retry_complete(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct finish_test *test = private_data;

    test->callbacks++;
    if (chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN) {
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EAGAIN);
        assert(test->publications == 0);
        if (test->handle_index >= 0) {
            assert(chimera_vfs_compound_take_handle(cp, test->handle_index) == NULL);
        }
        assert(chimera_vfs_compound_retry(cp));
        return;
    }
    assert(chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_OK);
    assert(chimera_vfs_compound_status(cp) == test->execution_status);
    test->publications++;
    if (test->handle_index >= 0) {
        test->taken = chimera_vfs_compound_take_handle(cp, test->handle_index);
        assert(test->taken);
    }
    test->ctx->done = 1;
} /* finish_retry_complete */

static void
inherit_open_grant_prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) status;
    chimera_vfs_compound_op_args(cp, index)->inherited_grant_handle = private_data;
} /* inherit_open_grant_prepare */

static void
inherit_two_open_grants_prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_vfs_open_handle **sources = private_data;
    struct chimera_vfs_compound_op  *op      = chimera_vfs_compound_op_args(cp, index);

    (void) status;
    op->inherited_grant_handle  = sources[0];
    op->inherited_grant_handle2 = sources[1];
} /* inherit_two_open_grants_prepare */

struct reserve_handle_test {
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_claim       *original;
    struct chimera_vfs_claim       *proposed;
    int                             prepares;
    int                             ranged;
};

static void
reserve_handle_prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct reserve_handle_test *test = private_data;

    (void) status;
    assert(test->original->file);
    assert(!test->proposed->file);
    if (test->ranged) {
        assert(test->original->offset == 0 && test->original->length == 100 &&
               (test->original->used & CHIMERA_CLAIM_LW));
        chimera_vfs_claim_init_range(test->proposed, false, false, 20, 40, &test->original->owner);
    } else {
        assert(test->original->denied == CHIMERA_CLAIM_W);
        chimera_vfs_claim_init_nfs4_open(test->proposed, CHIMERA_CLAIM_R, 0, &test->original->owner);
    }
    chimera_vfs_compound_op_args(cp, index)->in_handle = test->handle;
    test->prepares++;
} /* reserve_handle_prepare */

struct coordination_test {
    struct finish_test           finish;
    struct chimera_vfs_compound *compound;
    const uint8_t               *first_fh, *second_fh;
    uint32_t                     first_len, second_len;
    uint64_t                     token;
    int                          starts, prepares, completes, suffixes, rejections, asynchronous;
    enum chimera_vfs_error coordination_status;
};

static void
coordination_start(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct coordination_test *test = private_data;
    uint32_t                  length;
    const uint8_t            *current = chimera_vfs_compound_current_fh(cp, &length);

    assert(cp == test->compound && index == 1);
    assert(length == fh_len && !memcmp(current, fh, length));
    assert(!test->finish.publications);
    test->starts++;
    test->token = token;
    if (!test->asynchronous) {
        assert(chimera_vfs_compound_coordinate_done(cp, token, test->coordination_status));
    }
} /* coordination_start */

static void
coordination_prepare(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct coordination_test *test = private_data;

    (void) cp;
    (void) index;
    assert(*status == CHIMERA_VFS_OK);
    test->prepares++;
} /* coordination_prepare */

static void
coordination_complete(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct coordination_test *test = private_data;

    (void) cp;
    (void) index;
    assert(*status == test->coordination_status);
    test->completes++;
} /* coordination_complete */

static void
coordination_suffix(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct coordination_test *test = private_data;

    (void) cp;
    (void) index;
    assert(*status == CHIMERA_VFS_OK);
    test->suffixes++;
} /* coordination_suffix */

static void
coordination_retarget(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct coordination_test       *test   = private_data;
    struct chimera_vfs_compound_op *op     = chimera_vfs_compound_op_args(cp, index);
    bool                            second = test->second_fh && test->finish.finishes == 1;

    (void) status;
    op->arg_fh_len = second ? test->second_len : test->first_len;
    memcpy(op->arg_fh, second ? test->second_fh : test->first_fh, op->arg_fh_len);
} /* coordination_retarget */

static void
coordination_finish(
    struct chimera_vfs_compound *cp,
    void                        *private_data)
{
    struct coordination_test *test = private_data;

    test->finish.finishes++;
    assert(chimera_vfs_compound_execution_status(cp) == test->finish.execution_status);
    chimera_vfs_compound_finish_result(cp, test->finish.finishes <= test->rejections ?
                                       CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* coordination_finish */

static void
coordination_dynamic_rejected(
    struct chimera_vfs_compound *cp,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) index;
    (void) status;
    assert(chimera_vfs_compound_add_coordinate(cp, coordination_start, private_data) == -1);
} /* coordination_dynamic_rejected */

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
                                               CHIMERA_VFS_ATTR_MASK_STAT);
    i_look_b = chimera_vfs_compound_add_lookup(cp, "b", 1,
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

    /* NFSv3 needs full before/after snapshots even for a failed mutation. */
    cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
    chimera_vfs_compound_add_putfh(cp, root_fh, root_fh_len);
    int missing_remove = chimera_vfs_compound_add_remove(cp, "missing-wcc", 11, 0);
    chimera_vfs_compound_set_result_masks(cp, missing_remove, 0,
                                          CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MTIME | CHIMERA_VFS_ATTR_CTIME,
                                          CHIMERA_VFS_ATTR_MASK_STAT);
    chimera_vfs_compound_submit(cp, compound_cb, &ctx);
    wait_done(&ctx);
    op = chimera_vfs_compound_op(cp, missing_remove);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOENT);
    assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
    assert((op->dir_post_attr.va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) == CHIMERA_VFS_ATTR_MASK_STAT);
    assert(!op->dir_pre_attr.va_acl && !op->dir_post_attr.va_acl);
    chimera_vfs_compound_free(cp);
    TEST_PASS("failed namespace operation retains requested NFSv3 WCC snapshots");

    /* OPEN-at preserves VFS name validation, including an empty component. */
    for (int bad = 0; bad < 2; bad++) {
        char name[257];
        memset(name, 'x', sizeof(name));
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, root_fh_len);
        int  opened = chimera_vfs_compound_add_open_at(cp, name, bad ? 256 : 0,
                                                       (bad ? CHIMERA_VFS_OPEN_CREATE : 0) | CHIMERA_VFS_OPEN_INFERRED,
                                                       NULL, 0);
        assert(opened >= 0);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) ==
               (bad ? CHIMERA_VFS_ENAMETOOLONG : CHIMERA_VFS_ENOENT));
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("OPEN-at keeps VFS empty-name and overlong-component validation");

    {
        char               name[256];
        memset(name, 'x', sizeof(name));
        struct finish_test rejected = { .ctx              = &ctx, .handle_index = -1,
                                        .execution_status = CHIMERA_VFS_ENAMETOOLONG };
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, root_fh_len);
        assert(chimera_vfs_compound_add_lookup(cp, name, sizeof(name), 0) < 0);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &rejected);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &rejected);
        wait_done(&ctx);
        assert(rejected.finishes == 2 && rejected.publications == 1);
        assert(chimera_vfs_compound_num_completed(cp) == 0);
        chimera_vfs_compound_free(cp);
        TEST_PASS("finish retry preserves construction errors without executing a shortened request");
    }

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
    /* Authorization attributes now retain an owned ACL through completion. */
    assert(op->attr_mask & CHIMERA_VFS_ATTR_ACL);
    assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
    assert(op->attr.va_acl != NULL);

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

    /* Preserve retained READ when a subsequent WRITE OPEN follows a chmod
     * that removes current read permission. The old RO and new RW descriptors
     * have distinct cache entries, so physical access alone is insufficient. */
    {
        struct chimera_vfs_cred         user;
        struct chimera_vfs_attrs        attrs = { 0 };
        struct chimera_vfs_open_handle *old;
        struct chimera_vfs_open_handle *new_write;
        struct chimera_vfs_open_handle *sources[2];
        struct chimera_vfs_open_handle  unbound;
        struct evpl_iovec               read_iov[1];
        struct finish_test              retry = { .ctx = &ctx, .handle_index = -1 };
        uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        file_fh_len;
        int                             opened, upgraded, read_index;

        chimera_vfs_cred_init_unix(&user, 1001, 1001, 0, NULL);
        attrs.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        attrs.va_mode     = 0444;
        cp                = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        opened = chimera_vfs_compound_add_open(cp, "grant-union", 11,
                                               CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &attrs, 0);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, opened);
        memcpy(file_fh, op->fh, op->fh_len);
        file_fh_len = op->fh_len;
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &user);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        opened = chimera_vfs_compound_add_open(cp, "grant-union", 11,
                                               CHIMERA_VFS_OPEN_READ_ONLY, 0, NULL, 0);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        old = chimera_vfs_compound_take_handle(cp, opened);
        assert(old && old->granted_bound && old->granted_valid);
        assert(old->granted_access & CHIMERA_ACE_READ_DATA);
        assert(!(old->granted_access & CHIMERA_ACE_WRITE_DATA));
        chimera_vfs_compound_free(cp);

        attrs.va_mode = 0222;
        cp            = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, file_fh, (int) file_fh_len);
        chimera_vfs_compound_add_setattr(cp, NULL, &attrs, 0);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* An unbound cached access calculation is not a retained capability. */
        unbound               = *old;
        unbound.granted_bound = 0;
        cp                    = chimera_vfs_compound_alloc(ctx.vfs_thread, &user);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open(cp, "grant-union", 11,
                                      CHIMERA_VFS_OPEN_WRITE_ONLY, 0, NULL, 0);
        upgraded = chimera_vfs_compound_add_open(cp, NULL, 0,
                                                 CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY,
                                                 0, NULL, 0);
        chimera_vfs_compound_set_op_prepare(cp, upgraded, inherit_open_grant_prepare, &unbound);
        read_index = chimera_vfs_compound_add_read(cp, NULL, 0, 1, read_iov, 1, NULL);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, upgraded)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, read_index)->status == CHIMERA_VFS_EACCES);
        new_write = chimera_vfs_compound_take_handle(cp, upgraded);
        assert(new_write && new_write->granted_bound);
        assert(new_write->granted_access & CHIMERA_ACE_WRITE_DATA);
        assert(!(new_write->granted_access & CHIMERA_ACE_READ_DATA));
        chimera_vfs_compound_free(cp);

        /* An independent result cache entry must obtain both sources. This
         * covers old WRITE plus newly authorized READ even when either cache
         * entry can serve the physical reopen: neither grant may be dropped. */
        {
            struct chimera_vfs_cred alternate;
            chimera_vfs_cred_init_unix(&alternate, 1002, 1002, 0, NULL);
            sources[0] = new_write;
            sources[1] = old;
            cp         = chimera_vfs_compound_alloc(ctx.vfs_thread, &alternate);
            chimera_vfs_compound_add_putfh(cp, file_fh, (int) file_fh_len);
            upgraded = chimera_vfs_compound_add_open(cp, NULL, 0,
                                                     CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY,
                                                     0, NULL, 0);
            chimera_vfs_compound_set_op_prepare(cp, upgraded, inherit_two_open_grants_prepare, sources);
            read_index = chimera_vfs_compound_add_read(cp, NULL, 0, 1, read_iov, 1, NULL);
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            wait_done(&ctx);
            assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
            op = chimera_vfs_compound_op(cp, upgraded);
            assert(op->out_handle != old && op->out_handle != new_write);
            assert(op->out_handle->granted_bound && op->out_handle->granted_valid);
            assert((op->out_handle->granted_access & (CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA)) ==
                   (CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA));
            assert(!(old->granted_access & CHIMERA_ACE_WRITE_DATA));
            assert(!(new_write->granted_access & CHIMERA_ACE_READ_DATA));
            chimera_vfs_compound_free(cp);
        }
        chimera_vfs_release(ctx.vfs_thread, new_write);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &user);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        opened = chimera_vfs_compound_add_open(cp, "grant-union", 11,
                                               CHIMERA_VFS_OPEN_WRITE_ONLY, 0, NULL, 0);
        upgraded = chimera_vfs_compound_add_open(cp, NULL, 0,
                                                 CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY,
                                                 0, NULL, 0);
        chimera_vfs_compound_set_op_prepare(cp, upgraded, inherit_open_grant_prepare, old);
        read_index = chimera_vfs_compound_add_read(cp, NULL, 0, 1, read_iov, 1, NULL);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &retry);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &retry);
        wait_done(&ctx);
        assert(retry.finishes == 2 && retry.publications == 1);
        assert(chimera_vfs_compound_op(cp, read_index)->status == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, upgraded);
        assert(op->out_handle == chimera_vfs_compound_op(cp, opened)->out_handle);
        assert(op->out_handle != old);
        assert((op->out_handle->granted_access & (CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA)) ==
               (CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA));
        assert(!(old->granted_access & CHIMERA_ACE_WRITE_DATA));
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &user);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        upgraded = chimera_vfs_compound_add_open(cp, NULL, 0,
                                                 CHIMERA_VFS_OPEN_READ_ONLY, 0, NULL, 0);
        chimera_vfs_compound_set_op_prepare(cp, upgraded, inherit_open_grant_prepare, old);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(!chimera_vfs_compound_op(cp, upgraded)->out_handle);
        chimera_vfs_compound_free(cp);
        sources[0] = NULL;
        sources[1] = old;
        cp         = chimera_vfs_compound_alloc(ctx.vfs_thread, &user);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        upgraded = chimera_vfs_compound_add_open(cp, NULL, 0,
                                                 CHIMERA_VFS_OPEN_READ_ONLY, 0, NULL, 0);
        chimera_vfs_compound_set_op_prepare(cp, upgraded, inherit_two_open_grants_prepare, sources);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(!chimera_vfs_compound_op(cp, upgraded)->out_handle);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, old);
        TEST_PASS("union OPEN inherits only bound same-file grants across finish retry");
    }

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

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
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

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
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

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
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

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, 0);
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
                                               CHIMERA_VFS_ATTR_MASK_STAT);
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
                                               CHIMERA_VFS_ATTR_MASK_STAT);

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
                                               0, &sattr, 0);
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
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT);
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
                                               0, &sattr, 0);
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
                                               0, &sattr, 0);
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
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT);
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
        chimera_vfs_compound_add_lookup(cp, "o1", 2, 0);
        i_open = chimera_vfs_compound_add_open(cp, NULL, 0,
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, NULL, 0);

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
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT);
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
            "nd", 2, NULL, CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(S_ISLNK(chimera_vfs_compound_op(cp, i_ln)->attr.va_mode));
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "sl", 2, 0);
        /* Still the parent: a LOOKUP after the REMOVE resolves through it. */
        i_look = chimera_vfs_compound_add_lookup(cp, "nd", 2, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_rm);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_CHANGE);
        assert(op->fh_len == root_fh_len);
        assert(memcmp(op->fh, root_fh, root_fh_len) == 0);
        assert(chimera_vfs_compound_op(cp, i_look)->status == CHIMERA_VFS_OK);

        chimera_vfs_compound_free(cp);

        /* The name is gone, and a second REMOVE says so rather than the
         * sequence swallowing it. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_rm = chimera_vfs_compound_add_remove(cp, "sl", 2, 0);

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
                                               0, &sattr, 0);
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
        chimera_vfs_compound_add_lookup(cp, "sa", 2, 0);
        i_sa = chimera_vfs_compound_add_setattr(cp, NULL, &sattr,
                                                CHIMERA_VFS_ATTR_MASK_STAT);
        i_ga6 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MODE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_sa)->status == CHIMERA_VFS_OK);
        assert((chimera_vfs_compound_op(cp, i_ga6)->attr.va_mode & 0777) ==
               0640);
        chimera_vfs_compound_free(cp);

        /* Through the borrowed handle, with no current object established at
         * all -- the op does not need one, which is the point. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        sattr.va_size     = 0;

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_sa = chimera_vfs_compound_add_setattr(cp, oh, &sattr,
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

    /* An explicit metadata cursor does not establish a regular-file type.
    * Reject directory I/O before dispatching a backend data operation. */
    for (int write = 0; write < 2; write++) {
        struct evpl_iovec iov[1];
        int               io_index;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp,
                                              CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH, 0);
        io_index = write ? chimera_vfs_compound_add_write(cp, NULL, 0, 0, 0, NULL, 0, NULL) :
            chimera_vfs_compound_add_read(cp, NULL, 0, 1, iov, 1, NULL);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        op = chimera_vfs_compound_op(cp, io_index);
        assert(op->status == CHIMERA_VFS_EISDIR);
        assert(S_ISDIR(op->existing_mode));
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("explicit metadata cursors check directory type before I/O");

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
                                               0, &sattr, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        /* An empty file reads zero bytes at EOF rather than failing. */
        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 4096, rdiov, 16, NULL);

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
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 4096, rdiov, 16, NULL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_rd)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* A READ addressing the current object needs no handle at all. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rd", 2, 0);
        i_rd = chimera_vfs_compound_add_read(cp, NULL, 0, 4096, rdiov, 16, NULL);

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
                                               0, &sattr, 0);
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
        i_wr = chimera_vfs_compound_add_write(cp, oh, 0, 8, 2, &wiov, 1, NULL);
        i_rd = chimera_vfs_compound_add_read(cp, oh, 0, 8, rdiov, 16, NULL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, i_wr);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->written == 8);

        /* The READ in the same sequence saw what the WRITE in front of it
         * put there. */
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->read_len == 8);
        assert(op->niov >= 1);
        assert(memcmp(evpl_iovec_data(&op->iov[0]), payload, 8) == 0);

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
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_NODE, "f", 1, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_dst = chimera_vfs_compound_add_create(
            cp, CHIMERA_VFS_COMPOUND_CREATE_DIR, "rndst", 5, NULL, 0,
            &sattr, CHIMERA_VFS_ATTR_MASK_STAT);
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
        chimera_vfs_compound_add_lookup(cp, "rnsrc", 5, 0);
        chimera_vfs_compound_add_savefh(cp);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rndst", 5, 0);
        i_ren = chimera_vfs_compound_add_rename(cp, "f", 1, "g", 1, 0);
        /* The current object is still the TARGET directory afterwards, so a
         * lookup behind the rename finds the name it just put there. */
        i_look = chimera_vfs_compound_add_lookup(cp, "g", 1,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);

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
        chimera_vfs_compound_add_lookup(cp, "rndst", 5, 0);
        chimera_vfs_compound_add_lookup(cp, "g", 1, 0);
        chimera_vfs_compound_add_savefh(cp);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "rnsrc", 5, 0);
        i_link = chimera_vfs_compound_add_link(cp, "h", 1, 0);
        i_ga   = chimera_vfs_compound_add_lookup(cp, "h", 1,
                                                 CHIMERA_VFS_ATTR_MASK_STAT);

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
        i_bad         = chimera_vfs_compound_add_rename(cp, "a", 1, "b", 1, 0);
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

    /* Reject a mutation before any VFS work, then replay the attempt. */
    {
        struct decision_test d = { .ctx = &ctx };
        int                  denied, later;
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        denied = chimera_vfs_compound_add_remove(cp, "a", 1, 0);
        later  = chimera_vfs_compound_add_getfh(cp);
        chimera_vfs_compound_set_op_callbacks(cp, denied, decision_prepare, NULL, &d);
        chimera_vfs_compound_set_attempt_reset(cp, decision_reset, &d);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EACCES);
        assert(chimera_vfs_compound_op(cp, later)->status == CHIMERA_VFS_UNSET);
        chimera_vfs_compound_retry(cp);
        wait_done(&ctx);
        assert(d.prepares == 2 && d.resets == 2);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EACCES);
        chimera_vfs_compound_free(cp);
        TEST_PASS("prepare veto precedes mutation and retry restores bindings");
    }

    /* A decision can skip a backend operation entirely. Dynamic suffixes are
    * rebuilt rather than accumulated when a rejected attempt is repeated. */
    {
        struct decision_test d = { .ctx = &ctx, .append_count = 100 };
        int                  skipped;
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        skipped = chimera_vfs_compound_add_remove(cp, "a", 1, 0);
        chimera_vfs_compound_set_op_callbacks(cp, skipped, decision_skip,
                                              decision_append, &d);
        chimera_vfs_compound_set_attempt_reset(cp, decision_reset, &d);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_num_completed(cp) == 102);
        chimera_vfs_compound_retry(cp);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_num_completed(cp) == 102);
        assert(d.prepares == 2 && d.completes == 2 && d.resets == 2);
        chimera_vfs_compound_free(cp);
        TEST_PASS("skip and dynamically appended suffix survive retry without duplication");
    }

    /* Each GETHANDLE result owns its own reference, even if the cursor is
     * borrowed or later closed. The original synthetic descriptor survives. */
    {
        struct chimera_vfs_open_handle *borrowed, *one, *two;
        int                             first, second;
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp, CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH, 0);
        first = chimera_vfs_compound_add_gethandle(cp);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        borrowed = chimera_vfs_compound_take_handle(cp, first);
        assert(borrowed);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, borrowed, CHIMERA_VFS_OPEN_INFERRED |
                                           CHIMERA_VFS_OPEN_PATH);
        first  = chimera_vfs_compound_add_gethandle(cp);
        second = chimera_vfs_compound_add_gethandle(cp);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        one = chimera_vfs_compound_take_handle(cp, first);
        two = chimera_vfs_compound_take_handle(cp, second);
        assert(one && two);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, one);
        chimera_vfs_release(ctx.vfs_thread, two);
        chimera_vfs_release(ctx.vfs_thread, borrowed);
        TEST_PASS("GETHANDLE results own references independently of borrowed cursor");
    }

    /* Share admission is a VFS operation inside the same compound, and must
    * reject before a following conditional truncate can change the file. */
    {
        struct chimera_vfs_attrs        attrs = { 0 };
        struct chimera_vfs_open_handle *handle;
        struct chimera_vfs_file_state  *file;
        struct chimera_vfs_claim        held, requested;
        struct chimera_claim_owner      first_owner = { .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
                                                        .client_key = 1,                        .owner_lo = 1 };
        struct chimera_claim_owner      second_owner = { .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
                                                         .client_key = 2,                        .owner_lo = 2 };
        int                             opened, reserved, truncated;
        attrs.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_SIZE;
        attrs.va_mode     = 0666;
        attrs.va_size     = 4096;
        cp                = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        opened = chimera_vfs_compound_add_open(cp, "reserve-test", 12,
                                               CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED, 0,
                                               &attrs, CHIMERA_VFS_ATTR_SIZE);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        handle = chimera_vfs_compound_take_handle(cp, opened);
        assert(handle);
        chimera_vfs_compound_free(cp);
        file = chimera_vfs_state_get(ctx.vfs->vfs_state, handle->fh, handle->fh_len,
                                     handle->fh_hash, true);
        assert(file);
        chimera_vfs_claim_init_nfs4_open(&held, CHIMERA_CLAIM_R, CHIMERA_CLAIM_W,
                                         &first_owner);
        assert(chimera_vfs_claim_try_acquire(ctx.vfs->vfs_state, file, &held, NULL) ==
               CHIMERA_CLAIM_GRANTED);
        chimera_vfs_claim_init_nfs4_open(&requested, CHIMERA_CLAIM_W, 0, &second_owner);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, handle->fh, handle->fh_len);
        opened = chimera_vfs_compound_add_open(cp, NULL, 0,
                                               CHIMERA_VFS_OPEN_INFERRED, 0, NULL, 0);
        reserved = chimera_vfs_compound_add_reserve(cp, opened, &requested);
        memset(&attrs, 0, sizeof(attrs));
        attrs.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        attrs.va_size     = 0;
        truncated         = chimera_vfs_compound_add_setattr(cp, NULL, &attrs, 0);
        chimera_vfs_compound_op_use_handle(cp, truncated, opened);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, reserved)->status == CHIMERA_VFS_EACCES);
        assert(chimera_vfs_compound_op(cp, truncated)->status == CHIMERA_VFS_UNSET);
        chimera_vfs_compound_free(cp);

        {
            struct finish_test             retry  = { .ctx = &ctx, .handle_index = -1 };
            struct chimera_vfs_claim       narrow = { 0 };
            struct reserve_handle_test     test   = { .handle = handle, .original = &held, .proposed = &narrow };
            struct chimera_vfs_file_state *taken_file;
            cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            /* An unrelated cursor must not change which file is reserved. */
            chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
            reserved = chimera_vfs_compound_add_reserve_handle(cp, NULL, &narrow);
            chimera_vfs_compound_set_op_prepare(cp, reserved, reserve_handle_prepare, &test);
            chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &retry);
            chimera_vfs_compound_submit(cp, finish_retry_complete, &retry);
            wait_done(&ctx);
            assert(retry.finishes == 2 && retry.publications == 1 && test.prepares == 2);
            assert(held.file == file && held.denied == CHIMERA_CLAIM_W && narrow.file == file);
            taken_file = chimera_vfs_compound_take_reservation(cp, reserved);
            assert(taken_file == file);
            chimera_vfs_compound_free(cp);
            assert(narrow.file == file && held.file == file);
            chimera_vfs_claim_release(ctx.vfs->vfs_state, taken_file, &narrow);
            chimera_vfs_state_put(ctx.vfs->vfs_state, taken_file);
            TEST_PASS("borrowed-handle reserve retries without reopening or changing public claims");
        }
        chimera_vfs_claim_release(ctx.vfs->vfs_state, file, &held);
        {
            struct chimera_vfs_claim              proposed, final[3];
            struct chimera_vfs_claim             *previous[]    = { &held, &proposed };
            struct chimera_vfs_claim             *replacement[] = { &final[0], &final[1], &final[2] };
            const struct chimera_vfs_claim       *excluded[]    = { &held };
            const struct chimera_vfs_claim       *view[]        = { &final[0], &final[1], &final[2] };
            struct finish_test                    retry         = { .ctx = &ctx, .handle_index = -1 };
            struct reserve_handle_test            test          = { .handle = handle,    .original
                                                                            =
                                                                            &
                                                                            held
                                                                        ,
                                                                    .proposed = &proposed, .ranged
                                                                              =
                                                                            1 };
            chimera_vfs_claim_init_range(&held, true, false, 0, 100, &first_owner);
            assert(chimera_vfs_claim_try_acquire(ctx.vfs->vfs_state, file, &held, NULL) == CHIMERA_CLAIM_GRANTED);
            chimera_vfs_claim_init_range(&requested, false, false, 30, 1, &second_owner);
            cp       = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            reserved = chimera_vfs_compound_add_reserve_handle(cp, handle, &requested);
            int                                   suffix = chimera_vfs_compound_add_checkpoint(cp);
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            wait_done(&ctx);
            const struct chimera_vfs_compound_op *result = chimera_vfs_compound_op(cp, reserved);
            assert(result->status == CHIMERA_VFS_EACCES && result->claim_result == CHIMERA_CLAIM_DENIED);
            assert(result->claim_conflict.offset == 0 && result->claim_conflict.length == 100 &&
                   result->claim_conflict.owner.client_key == first_owner.client_key);
            assert(chimera_vfs_compound_op(cp, suffix)->status == CHIMERA_VFS_UNSET);
            chimera_vfs_compound_free(cp);
            assert(held.file == file);

            chimera_vfs_claim_init_range(&final[0], true, false, 0, 20, &first_owner);
            chimera_vfs_claim_init_range(&final[1], false, false, 20, 40, &first_owner);
            chimera_vfs_claim_init_range(&final[2], true, false, 60, 40, &first_owner);
            chimera_vfs_claim_init_range(&requested, true, false, 30, 1, &second_owner);
            requested.admit_excluded     = excluded;
            requested.admit_num_excluded = 1;
            cp                           = chimera_vfs_compound_alloc(ctx.vfs_thread, &
                                                                      cred);
            reserved = chimera_vfs_compound_add_reserve_handle(cp,
                                                               handle, &requested);
            chimera_vfs_compound_op_args(cp, reserved)->claim_ranges     = view;
            chimera_vfs_compound_op_args(cp, reserved)->num_claim_ranges = 3;
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            wait_done(&ctx);
            result = chimera_vfs_compound_op(cp, reserved);
            assert(result->status == CHIMERA_VFS_EACCES && !requested.file);
            assert(result->claim_conflict.offset == 20 && result->claim_conflict.length == 40 &&
                   result->claim_conflict.used == CHIMERA_CLAIM_LR);
            chimera_vfs_compound_free(cp);

            memset(&proposed, 0, sizeof(proposed));
            cp       = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            reserved = chimera_vfs_compound_add_reserve_handle(cp, NULL, &proposed);
            chimera_vfs_compound_set_op_prepare(cp, reserved, reserve_handle_prepare, &test);
            chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &retry);
            chimera_vfs_compound_submit(cp, finish_retry_complete, &retry);
            wait_done(&ctx);
            assert(retry.finishes == 2 && retry.publications == 1 && test.prepares == 2);
            assert(held.file == file && proposed.file == file);
            chimera_vfs_claim_range_publish(file, previous, 2, replacement, 3);
            chimera_vfs_claim_replacement_complete(file);
            chimera_vfs_compound_free(cp);
            assert(!held.file && !proposed.file && final[0].file == file && final[2].file == file);
            for (int i = 0; i < 3; i++) {
                chimera_vfs_claim_release(ctx.vfs->vfs_state, file, &final[i]);
            }
            TEST_PASS(
                "range reserves copy conflicts, enforce private views, retry, and publish without releasing final claims");
        }
        chimera_vfs_state_put(ctx.vfs->vfs_state, file);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, handle->fh, handle->fh_len);
        int inspected = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_SIZE);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, inspected)->attr.va_size == 4096);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, handle);
        TEST_PASS("share reservation denial stops before truncate in same compound");
    }

    /* External coordination is request-scoped while pure decision callbacks
     * and the entire VFS attempt are repeated after finish-time rejection. */
    for (int scenario = 0; scenario < 5; scenario++) {
        struct coordination_test test = {
            .finish              = { .ctx = &ctx, .handle_index = -1 },
            .first_fh            = a_fh, .first_len = a_fh_len,
            .rejections          = scenario == 1 ? 2 : 1,
            .asynchronous        = scenario < 2,
            .coordination_status = scenario == 2 ? CHIMERA_VFS_EACCES : CHIMERA_VFS_OK,
        };
        test.finish.execution_status = test.coordination_status;
        if (scenario == 1) {
            test.second_fh  = root_fh;
            test.second_len = root_fh_len;
        }
        cp            = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        test.compound = cp;
        int                      put = chimera_vfs_compound_add_putfh(cp, a_fh, a_fh_len);
        chimera_vfs_compound_set_op_prepare(cp, put, coordination_retarget, &test);
        int                      coordinate = chimera_vfs_compound_add_coordinate(cp, coordination_start, &test);
        chimera_vfs_compound_op_args(cp, coordinate)->coordinate_each_attempt = scenario == 4;
        chimera_vfs_compound_set_op_callbacks(cp, coordinate, coordination_prepare, coordination_complete, &test);
        int                      suffix = chimera_vfs_compound_add_checkpoint(cp);
        chimera_vfs_compound_set_op_prepare(cp, suffix, coordination_suffix, &test);
        chimera_vfs_compound_set_finish_handler(cp, coordination_finish, &test);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &test.finish);
        if (test.asynchronous) {
            assert(test.starts == 1 && test.prepares == 1 && test.completes == 0);
            assert(test.suffixes == 0 && test.finish.finishes == 0 && !ctx.done);
            assert(chimera_vfs_compound_num_completed(cp) == 1);
            uint64_t first_token = test.token;
            assert(!chimera_vfs_compound_coordinate_done(cp, first_token + 99, CHIMERA_VFS_OK));
            assert(!chimera_vfs_compound_retry(cp)); /* parked attempts cannot restart */
            assert(test.starts == 1 && test.prepares == 1);
            assert(chimera_vfs_compound_coordinate_done(cp, first_token, CHIMERA_VFS_OK));
            if (scenario == 1) {
                assert(test.starts == 2 && test.prepares == 2 && test.completes == 1);
                assert(test.finish.finishes == 1 && !ctx.done && test.token != first_token);
                assert(!chimera_vfs_compound_coordinate_done(cp, first_token, CHIMERA_VFS_EIO));
                assert(chimera_vfs_compound_coordinate_done(cp, test.token, CHIMERA_VFS_OK));
            }
        }
        wait_done(&ctx);
        assert(test.starts == (scenario == 1 || scenario == 4 ? 2 : 1));
        assert(test.prepares == test.rejections + 1 && test.completes == test.prepares);
        assert(test.finish.publications == 1 && test.finish.callbacks == test.prepares);
        assert(test.suffixes == (scenario == 2 ? 0 : test.prepares));
        assert(!chimera_vfs_compound_coordinate_done(cp, test.token, CHIMERA_VFS_OK));
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("coordination parks once per FH, survives finish retry, rejects stale tokens, caches errors");
    {
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        int check = chimera_vfs_compound_add_checkpoint(cp);
        chimera_vfs_compound_set_op_callbacks(cp, check, NULL, coordination_dynamic_rejected, NULL);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);
        TEST_PASS("dynamic coordination rejected before external work");
    }

    /* Adding runtime authorization must preserve an existing result gate and
     * its private context. Both callbacks see the resolved cursor on retry. */
    {
        struct cursor_test cursor = { .expected_fh = a_fh, .expected_len = a_fh_len };
        struct finish_test finish = { .ctx          = &ctx, .execution_status = CHIMERA_VFS_OK,
                                      .handle_index = -1 };
        int                completes = 0;
        int                checked;
        uint32_t           len = 123;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        assert(chimera_vfs_compound_current_fh(cp, &len) == NULL && len == 0);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "a", 1, CHIMERA_VFS_ATTR_FH);
        checked = chimera_vfs_compound_add_getfh(cp);
        chimera_vfs_compound_set_op_callbacks(cp, checked, NULL, cursor_complete, &completes);
        chimera_vfs_compound_set_op_prepare(cp, checked, cursor_prepare, &cursor);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &finish);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &finish);
        wait_done(&ctx);
        assert(cursor.prepares == 2 && completes == 2);
        assert(finish.finishes == 2 && finish.publications == 1);
        chimera_vfs_compound_free(cp);
        TEST_PASS("independent callback contexts see the execution cursor across finish retry");
    }

    /* Empty compounds and first-op construction failures still have finish
     * semantics. An unavailable retry must be observable by the frontend. */
    for (int invalid = 0; invalid < 2; invalid++) {
        struct finish_test test = { .ctx              = &ctx, .handle_index = -1,
                                    .execution_status = invalid ? CHIMERA_VFS_EINVAL : CHIMERA_VFS_OK };
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        assert(!chimera_vfs_compound_retry(cp));
        if (invalid) {
            assert(chimera_vfs_compound_add_putfh(cp, NULL, 0) < 0);
        }
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &test);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &test);
        wait_done(&ctx);
        assert(test.finishes == 2 && test.callbacks == 2 && test.publications == 1);
        assert(chimera_vfs_compound_num_completed(cp) == 0);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("empty and invalid empty attempts retry; unsubmitted compounds refuse retry explicitly");

    /* A backend finish rejection must not publish successful operation
     * outputs. These attempts perform no filesystem mutations. */
    {
        struct finish_test test = { .ctx = &ctx, .execution_status = CHIMERA_VFS_OK };
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp, CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH, 0);
        test.handle_index = chimera_vfs_compound_add_gethandle(cp);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &test);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &test);
        wait_done(&ctx);
        assert(test.finishes == 2 && test.callbacks == 2 && test.publications == 1);
        assert(!chimera_vfs_compound_retry(cp)); /* accepted handle is already public */
        chimera_vfs_compound_free(cp);

        /* A borrowed CLOSE cannot consume this accepted reference when its
         * first attempt is rejected. It releases exactly once on acceptance. */
        struct finish_test close = { .ctx          = &ctx, .execution_status = CHIMERA_VFS_OK,
                                     .handle_index = -1 };
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, test.taken,
                                           CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH);
        chimera_vfs_compound_add_close(cp);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &close);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &close);
        wait_done(&ctx);
        assert(close.finishes == 2 && close.publications == 1);
        chimera_vfs_compound_free(cp);
        TEST_PASS("finish EAGAIN preserves handles and borrowed CLOSE until acceptance");
    }
    {
        struct finish_test test = { .ctx          = &ctx, .execution_status = CHIMERA_VFS_ENOENT,
                                    .handle_index = -1 };
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_getfh(cp);
        chimera_vfs_compound_add_lookup(cp, "missing-finish-test", 19, 0);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &test);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &test);
        wait_done(&ctx);
        assert(test.finishes == 2 && test.callbacks == 2 && test.publications == 1);
        assert(chimera_vfs_compound_execution_status(cp) == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);
        TEST_PASS("finish rejection overrides failed-op prefix until accepted retry");
    }

    /* SETATTR is idempotent here; the injected finish rejection tests input
     * ownership without pretending the memfs backend supports rollback. */
    {
        struct chimera_acl      *acl   = calloc(1, chimera_acl_size(1));
        struct chimera_vfs_attrs attrs = { 0 };
        struct finish_test       test  = { .ctx          = &ctx, .execution_status = CHIMERA_VFS_OK,
                                           .handle_index = -1 };
        int                      calls = 0, set_index, get_index;

        assert(acl);
        acl->num_aces            = 1;
        acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
        acl->aces[0].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
        acl->aces[0].who.id      = CHIMERA_WHO_EVERYONE;
        acl->aces[0].access_mask = CHIMERA_ACE_MASK_ALL;
        attrs.va_set_mask        = CHIMERA_VFS_ATTR_ACL;
        attrs.va_acl             = acl;
        cp                       = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, (int) a_fh_len);
        set_index = chimera_vfs_compound_add_setattr(cp, NULL, &attrs, 0);
        get_index = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_ACL);
        chimera_vfs_compound_set_op_callbacks(cp, set_index, NULL, acl_attempt_complete, &calls);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &test);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &test);
        wait_done(&ctx);
        assert(calls == 2 && test.finishes == 2 && test.publications == 1);
        assert(acl->num_aces == 1 && acl->aces[0].access_mask == CHIMERA_ACE_MASK_ALL);
        op = chimera_vfs_compound_op(cp, get_index);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(op->attr.va_acl->num_aces == 1);
        assert(op->attr.va_acl->aces[0].access_mask == CHIMERA_ACE_MASK_ALL);
        chimera_vfs_compound_free(cp);
        free(acl);
        TEST_PASS("finish retry restores writable ACL without mutating immutable input");
    }

    /* NFS3 size-setting SETATTR seeds a data cursor. Keep that descriptor:
     * reopening O_PATH changes backend ftruncate into path truncate, which
     * spuriously denies an owner's mode-000 file despite an open data fd. */
    {
        struct chimera_vfs_open_handle *data;
        struct chimera_vfs_attrs        attrs = { .va_set_mask = CHIMERA_VFS_ATTR_MODE,
                                                  .va_mode     = 0666 };
        int                             get_index, set_index;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open(cp, "setattr-data", 12,
                                      CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE,
                                      0, &attrs, CHIMERA_VFS_ATTR_FH);
        get_index = chimera_vfs_compound_add_gethandle(cp);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        data = chimera_vfs_compound_take_handle(cp, get_index);
        assert(data && data->cache_id != CHIMERA_VFS_OPEN_ID_SYNTHETIC);
        chimera_vfs_compound_free(cp);

        attrs.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        attrs.va_size     = 10240;
        cp                = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, data, CHIMERA_VFS_OPEN_INFERRED);
        set_index = chimera_vfs_compound_add_setattr(cp, NULL, &attrs, CHIMERA_VFS_ATTR_SIZE);
        get_index = chimera_vfs_compound_add_gethandle(cp);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, set_index)->attr.va_size == 10240);
        assert(chimera_vfs_compound_op(cp, get_index)->out_handle == data);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, data);
        TEST_PASS("size-setting SETATTR retains its data cursor and extends the file");
    }

    /* Typed named streams share ordinary OPEN provenance and retry ownership. */
    {
        struct chimera_vfs_attrs size = { .va_set_mask = CHIMERA_VFS_ATTR_SIZE, .va_size = 7 };
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, a_fh_len);
        int stream = chimera_vfs_compound_add_open_stream(cp, "compound-stream", 15,
            CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE, NULL, CHIMERA_VFS_ATTR_SIZE);
        int sized = chimera_vfs_compound_add_setattr(cp, NULL, &size, CHIMERA_VFS_ATTR_SIZE);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, stream)->out_handle);
        assert(chimera_vfs_compound_op(cp, sized)->attr.va_size == 7);
        chimera_vfs_compound_free(cp);

        struct finish_test retry = { .ctx = &ctx, .handle_index = -1 };
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, a_fh_len);
        int listed = chimera_vfs_compound_add_list_streams(cp, 0, 4096, true);
        stream = chimera_vfs_compound_add_open_stream(cp, "compound-stream", 15,
            CHIMERA_VFS_OPEN_INFERRED, NULL, CHIMERA_VFS_ATTR_SIZE);
        int kept = chimera_vfs_compound_add_gethandle(cp);
        chimera_vfs_compound_add_puthandle_from(cp, stream, CHIMERA_VFS_OPEN_INFERRED);
        chimera_vfs_compound_add_close(cp);
        chimera_vfs_compound_set_finish_handler(cp, reject_first_finish, &retry);
        chimera_vfs_compound_submit(cp, finish_retry_complete, &retry);
        wait_done(&ctx);
        assert(retry.finishes == 2 && retry.publications == 1);
        /* The base is a directory, so only its named fork is listed. */
        assert(chimera_vfs_compound_op(cp, listed)->buffer_count == 1);
        assert(chimera_vfs_compound_op(cp, listed)->buffer_len > 0);
        assert(chimera_vfs_compound_op(cp, stream)->attr.va_size == 7);
        assert(!chimera_vfs_compound_op(cp, stream)->out_handle);
        assert(chimera_vfs_compound_op(cp, kept)->out_handle);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, a_fh_len);
        chimera_vfs_compound_add_remove_stream(cp, "compound-stream", 15);
        stream = chimera_vfs_compound_add_open_stream(cp, "compound-stream", 15,
            CHIMERA_VFS_OPEN_INFERRED, NULL, 0);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, stream)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);
        TEST_PASS("typed streams preserve open ownership, list buffers and readonly finish retry");
    }

    /* Binary handle-state records use the current FH without changing it.
     * Stack inputs may be overwritten as soon as the builder returns. */
    {
        uint8_t key[sizeof(compound_key)], value[sizeof(compound_value)];
        memcpy(key, compound_key, sizeof(key));
        memcpy(value, compound_value, sizeof(value));
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, a_fh_len);
        assert(chimera_vfs_compound_add_put_key_at(cp, key, sizeof(key), value, sizeof(value)) >= 0);
        int cursor = chimera_vfs_compound_add_getfh(cp);
        memset(key, 42, sizeof(key));
        memset(value, 42, sizeof(value));
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, cursor)->fh_len == a_fh_len);
        assert(!memcmp(chimera_vfs_compound_op(cp, cursor)->fh, a_fh, a_fh_len));
        chimera_vfs_compound_free(cp);
        ctx.callbacks = 0;
        chimera_vfs_search_keys_at(ctx.vfs_thread, &cred, a_fh, a_fh_len,
            compound_key, sizeof(compound_key), compound_key, sizeof(compound_key), 0,
            key_record_cb, key_status_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK && ctx.callbacks == 1);
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, a_fh_len);
        chimera_vfs_compound_add_delete_key_at(cp, compound_key, sizeof(compound_key));
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);
        ctx.callbacks = 0;
        chimera_vfs_search_keys_at(ctx.vfs_thread, &cred, a_fh, a_fh_len,
            compound_key, sizeof(compound_key), compound_key, sizeof(compound_key), 0,
            key_record_cb, key_status_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK && ctx.callbacks == 0);
        /* The routed API bounds its request scratch before copying inputs. */
        chimera_vfs_put_key_at(ctx.vfs_thread, &cred, a_fh, a_fh_len,
            compound_key, sizeof(compound_key), compound_value, UINT32_MAX,
            key_status_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_ERANGE);
        chimera_vfs_delete_key_at(ctx.vfs_thread, &cred, a_fh, a_fh_len,
            compound_key, UINT32_MAX, key_status_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_ERANGE);
        TEST_PASS("typed KV writes own binary inputs, preserve cursor and delete routed records");
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
