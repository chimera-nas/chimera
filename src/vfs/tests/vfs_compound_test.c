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
#include "common/thread.h"
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_kv.h"
#include "vfs/vfs_compound.h"
/* The two things below that are not sequence ops: the pool lifecycle (mkfs,
 * mount, umount, rmfs) and the key-value calls, which are a store beside
 * the filesystem rather than operations on it.  Both come from the core's
 * per-op header, which is where they still live. */
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/tests/compound_test_util.h"
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
compound_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->callbacks++;
    ctx->cb_thread = pthread_self();
    ctx->done      = 1;
} /* compound_cb */

struct dest_finish_test {
    struct evpl_iovec *dest;
    int                finishes;
};

static void
dest_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct dest_finish_test *test = private_data;

    assert(chimera_vfs_compound_execution_status(compound) == CHIMERA_VFS_OK);
    /* Both attempts must leave the application's destination untouched until
     * finish accepts the staged data. */
    const char              *data = evpl_iovec_data(test->dest);
    for (int i = 0; i < 4096; i++) {
        assert(data[i] == 'x');
    }
    chimera_vfs_compound_finish_result(compound,
                                       ++test->finishes == 1 ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* dest_finish */


/* ---- fixture builders ----
 * The tree these tests run against is built the way everything else reaches
 * the VFS: one op, one sequence.  Nothing here is under test -- what is under
 * test starts at the first sequence with more than one op in it. */

/* PUTFH(parent); CREATE(dir).  The new directory's fh lands in ctx->fh. */
static void
mkdir_under(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *parent_fh,
    uint32_t                       parent_fh_len,
    const char                    *name)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              sattr;
    int                                   i_mkdir;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_putfh(cp, parent_fh, (int) parent_fh_len);
    i_mkdir = chimera_vfs_compound_add_create(cp,
                                              CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                              name, (int) strlen(name),
                                              NULL, 0, &sattr,
                                              CHIMERA_VFS_ATTR_FH |
                                              CHIMERA_VFS_ATTR_MASK_STAT, 0, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    assert(ctx->status == CHIMERA_VFS_OK);

    op = chimera_vfs_compound_op(cp, (uint32_t) i_mkdir);
    assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
    memcpy(ctx->fh, op->attr.va_fh, op->attr.va_fh_len);
    ctx->fh_len = op->attr.va_fh_len;

    chimera_vfs_compound_free(cp);
} /* mkdir_under */

/* PUTFH(base); OPEN_PATH(path, CREATE) of a regular file -- a PATH open, so
 * `path` may be several components deep.  The new file's fh lands in ctx->fh;
 * the handle stays the compound's and dies with it, which is what every caller
 * of this wants. */
static void
create_under(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *base_fh,
    uint32_t                       base_fh_len,
    const char                    *path)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              sattr;
    int                                   i_open;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = S_IFREG | 0644;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_putfh(cp, base_fh, (int) base_fh_len);
    i_open = chimera_vfs_compound_add_open_path(cp, path, (int) strlen(path),
                                                CHIMERA_VFS_OPEN_CREATE |
                                                CHIMERA_VFS_OPEN_CREATE_REGULAR,
                                                &sattr,
                                                CHIMERA_VFS_ATTR_FH |
                                                CHIMERA_VFS_ATTR_MASK_STAT);

    ctx->status = compound_test_run(ctx->evpl, cp);
    assert(ctx->status == CHIMERA_VFS_OK);

    op = chimera_vfs_compound_op(cp, (uint32_t) i_open);
    assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
    memcpy(ctx->fh, op->attr.va_fh, op->attr.va_fh_len);
    ctx->fh_len = op->attr.va_fh_len;

    chimera_vfs_compound_free(cp);
} /* create_under */

/* PUTFH(parent); LOOKUP(name) with `attr_mask`.  Used both to resolve a name
 * and, in the ACL-copy check, to make the backend overwrite its own scratch
 * between a sequence finishing and its results being read. */
static void
lookup_under(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *parent_fh,
    uint32_t                       parent_fh_len,
    const char                    *name,
    uint64_t                       attr_mask)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    int                                   i_lookup;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_putfh(cp, parent_fh, (int) parent_fh_len);
    i_lookup = chimera_vfs_compound_add_lookup(cp, name, (int) strlen(name),
                                               attr_mask, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    assert(ctx->status == CHIMERA_VFS_OK);

    op = chimera_vfs_compound_op(cp, (uint32_t) i_lookup);
    if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
        memcpy(ctx->fh, op->attr.va_fh, op->attr.va_fh_len);
        ctx->fh_len = op->attr.va_fh_len;
    }

    chimera_vfs_compound_free(cp);
} /* lookup_under */

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

/*
 * A gate that EDITS the ops that have not run: the SMB2 SET_INFO ALLOCATION
 * shape, where the size to set is a function of the size the op just read, and
 * a skip.  Both are written from what the caller already had plus what the
 * finished op reported -- never accumulated onto the last run's edit -- which
 * is what makes them survive a re-execution unchanged.
 *
 * It also asks for what it may NOT have, and records that it was refused.
 */
#define EDIT_GATE_NONE ((uint32_t) ~0u)

struct edit_gate_ctx {
    int      calls;
    uint32_t at;            /* edit when consulted on this op           */
    uint32_t size_index;    /* the SETATTR to re-size, or EDIT_GATE_NONE */
    uint32_t skip_index;    /* the op to skip, or EDIT_GATE_NONE         */
    uint64_t wrote;         /* the size it assigned, last time it ran    */
    int      refused_self;
    int      refused_below;
    int      refused_past_end;
};

static void
edit_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct edit_gate_ctx                 *e = private_data;
    const struct chimera_vfs_compound_op *done;
    struct chimera_vfs_compound_op       *ahead;

    (void) status;

    e->calls++;

    if (index != e->at) {
        return;
    }

    /* The ops that have run, and one that does not exist: all refused. */
    e->refused_self     = chimera_vfs_compound_op_edit(compound, index) == NULL;
    e->refused_below    = chimera_vfs_compound_op_edit(compound, index - 1) == NULL;
    e->refused_past_end = chimera_vfs_compound_op_edit(
        compound, chimera_vfs_compound_num_ops(compound)) == NULL;

    done = chimera_vfs_compound_op(compound, index);

    if (e->size_index != EDIT_GATE_NONE) {
        ahead = chimera_vfs_compound_op_edit(compound, e->size_index);
        assert(ahead != NULL);

        /* Round the size just read up to the allocation unit and ASSIGN it.
         * A gate that added to what it wrote last time would get a different
         * sequence on the second execution; this one gets the same. */
        ahead->set_attr.va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        ahead->set_attr.va_size      = (done->attr.va_size + 63) & ~((uint64_t) 63);
        e->wrote                     = ahead->set_attr.va_size;
    }

    if (e->skip_index != EDIT_GATE_NONE) {
        ahead = chimera_vfs_compound_op_edit(compound, e->skip_index);
        assert(ahead != NULL);
        ahead->skip = 1;
    }
} /* edit_gate */

/* A gate that completes a later I/O op's OWNER from the object the op it is
 * consulted on resolved.  This is the shape a protocol keying the owner on an
 * open handle uses when the OPEN is IN the run: io_owner and have_io_owner are
 * arguments, so the half the caller could not know at build time is written by
 * the gate that first can. */
struct io_owner_gate_ctx {
    uint32_t                   at;       /* the op that resolves the object */
    uint32_t                   io_index; /* the READ or WRITE to complete   */
    struct chimera_claim_actor base;     /* the half the caller knows       */
    int                        filled;
};

static void
io_owner_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct io_owner_gate_ctx             *g = private_data;
    const struct chimera_vfs_compound_op *done;
    struct chimera_vfs_compound_op       *io;

    if (index != g->at || *status != CHIMERA_VFS_OK) {
        return;
    }

    done = chimera_vfs_compound_op(compound, index);
    io   = chimera_vfs_compound_op_edit(compound, g->io_index);
    assert(io != NULL);
    assert(done->out_handle != NULL);

    /* The identity the whole scheme rests on: a handle's fh_hash is
     * chimera_vfs_hash of its FILE HANDLE and nothing else, so it names the
     * object rather than the open. */
    assert(done->out_handle->fh_hash ==
           chimera_vfs_hash(done->fh, (int) done->fh_len));

    /* ASSIGNED from what the caller already had plus what the op just
     * reported, never accumulated -- so a second execution computes the same
     * owner rather than a different one. */
    io->io_owner                = g->base;
    io->io_owner.owner.owner_lo = done->out_handle->fh_hash;
    io->have_io_owner           = 1;
    g->filled++;
} /* io_owner_gate */

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

/* A cancel asked for from a thread that is NOT the one that submitted, which
 * is where every real cancel trigger arrives: a FUSE_INTERRUPT off the kernel
 * queue, an SMB2 CANCEL on another channel, a teardown on the thread the
 * disconnect landed on.  It needs no VFS thread of its own -- the post only
 * latches and rings the submitting thread's doorbell -- which is itself part
 * of the contract under test. */
struct remote_cancel {
    struct chimera_vfs_compound *compound;
    int                          posts;
    pthread_t                    self;
};

static void *
remote_cancel_main(void *arg)
{
    struct remote_cancel *rc = arg;
    int                   i;

    rc->self = pthread_self();

    for (i = 0; i < rc->posts; i++) {
        chimera_vfs_compound_cancel_post(rc->compound);
    }

    return NULL;
} /* remote_cancel_main */

/* The park notification.  It counts itself -- "exactly once per submission"
 * is most of the contract -- and keeps the op index it was handed, which is
 * the whole of what a front end needs to emit its interim. */
struct park_rec {
    int      calls;
    uint32_t index;
};

static void
park_rec_cb(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct park_rec *p = private_data;

    (void) compound;

    p->calls++;
    p->index = index;
} /* park_rec_cb */

/* A FIND's three callbacks, staging into a request-local array the way the
* S3 consumers do: `reset` truncates it (and counts itself), `filter` prunes
* one named subtree, `append` copies each path in -- or refuses the Nth. */
#define FIND_CTX_MAX 16

struct find_ctx {
    int         resets;
    int         filter_calls;
    int         appended;
    int         stop_at;     /* refuse this append (1-based); 0 never */
    const char *prune;       /* the directory whose subtree is pruned */
    int         count;
    char        paths[FIND_CTX_MAX][64];
};

static void
find_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct find_ctx *f = private_data;

    (void) compound;
    (void) index;

    f->resets++;
    f->appended = 0;
    f->count    = 0;
} /* find_reset */

static int
find_filter(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct find_ctx                      *f  = private_data;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);

    /* Only directories are put to the filter, with what the walk needs. */
    assert(op->type == CHIMERA_VFS_COMPOUND_OP_FIND);
    assert(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE);
    assert(attr->va_set_mask & CHIMERA_VFS_ATTR_FH);
    assert(S_ISDIR(attr->va_mode));
    /* The walker's own attrs, live for this call: an ACL is present only
     * when asked for, and this walk did not ask. */
    assert(!(attr->va_set_mask & CHIMERA_VFS_ATTR_ACL));

    f->filter_calls++;

    return (f->prune && (int) strlen(f->prune) == pathlen &&
            memcmp(path, f->prune, pathlen) == 0) ? 1 : 0;
} /* find_filter */

static int
find_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct find_ctx                      *f  = private_data;
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);

    assert(op->type == CHIMERA_VFS_COMPOUND_OP_FIND);
    assert(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE);
    assert(attr->va_set_mask & CHIMERA_VFS_ATTR_FH);
    assert(!(attr->va_set_mask & CHIMERA_VFS_ATTR_ACL));
    assert(pathlen > 1 && path[0] == '/');
    assert(pathlen < (int) sizeof(f->paths[0]));

    if (f->stop_at && f->appended + 1 == f->stop_at) {
        return -1;
    }

    assert(f->count < FIND_CTX_MAX);
    memcpy(f->paths[f->count], path, pathlen);
    f->paths[f->count][pathlen] = '\0';
    f->count++;
    f->appended++;

    return 0;
} /* find_append */

/* Run a trivial sequence to completion.  Two jobs: it drains the thread's
 * doorbell, so anything a cancelled run wrongly posted would have arrived by
 * the time it returns, and it gives that drain a wakeup of its own to wait
 * for rather than spinning the event loop blind.  It costs exactly one
 * completion, which is what makes "nothing else completed" checkable. */
static void
pump_probe(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    struct chimera_vfs_compound *cp;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_putfh(cp, fh, (int) fh_len);
    chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
    chimera_vfs_compound_submit(cp, compound_cb, ctx);
    wait_done(ctx);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
    chimera_vfs_compound_free(cp);
} /* pump_probe */

/*
 * Drain the thread's doorbell when there is nothing of our own to wait for.
 *
 * A cancel_post that arrives for a run which is already over rides the
 * doorbell home and produces no completion, so there is no wakeup to wait on
 * -- and spinning the event loop blind is how a test waits forever.  So a
 * wakeup is provoked instead: a run parks behind the conflicting claim the
 * caller is holding on `oh`, its cancel is posted from another thread, and
 * the completion that comes back is the thing to wait for.  Anything posted
 * BEFORE this call was on the list before this post was, so the drain that
 * delivers this one has already taken it.
 *
 * Costs exactly one completion, like pump_probe, which is what makes
 * "nothing else completed" checkable around it.
 */
static void
pump_cancel(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *oh,
    uint32_t                        owner_lo)
{
    struct chimera_vfs_claim           claim;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_claim_owner         owner;
    struct chimera_vfs_compound       *cp;
    struct remote_cancel               rc;
    pthread_t                          tid;

    memset(&owner, 0, sizeof(owner));
    owner.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
    owner.owner_lo = owner_lo;
    chimera_vfs_claim_init_range(&claim, true, false, 0, 16, &owner);

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_puthandle(cp, oh,
                                       CHIMERA_VFS_OPEN_READ_ONLY |
                                       CHIMERA_VFS_OPEN_WRITE_ONLY);
    chimera_vfs_compound_add_claim(cp, &claim, &ticket,
                                   CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                   CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                   0, 0, 0, 0);
    chimera_vfs_compound_submit(cp, compound_cb, ctx);
    assert(!ctx->done);

    rc.compound = cp;
    rc.posts    = 1;
    assert(pthread_create(&tid, NULL, remote_cancel_main, &rc) == 0);
    assert(pthread_join(tid, NULL) == 0);

    wait_done(ctx);
    assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ECANCELED);
    chimera_vfs_compound_free(cp);
} /* pump_cancel */

static int
find_has(
    const struct find_ctx *f,
    const char            *path)
{
    int i;

    for (i = 0; i < f->count; i++) {
        if (strcmp(f->paths[i], path) == 0) {
            return 1;
        }
    }

    return 0;
} /* find_has */

/* A caching holder's break callback: counts, and records the mode it was
 * asked to fall to.  It must touch nothing else -- it fires inside the
 * recall, on the submitting thread. */
struct break_rec {
    int     fired;
    uint8_t needed;
};

static void
break_rec_cb(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed_mode,
    void                     *priv)
{
    struct break_rec *r = priv;

    (void) claim;

    r->fired++;
    r->needed = needed_mode;
} /* break_rec_cb */

/* A second VFS thread that acks a broken caching claim, so the drain that
 * resumes a parked RECALL is posted from a thread that is not the one that
 * submitted the sequence.  Records itself so the test can prove the
 * completion did NOT run here. */
struct remote_ack {
    struct chimera_vfs       *vfs;
    struct chimera_vfs_claim *claim;
    uint8_t                   resulting;
    pthread_t                 self;
};

static void *
remote_ack_main(void *arg)
{
    struct remote_ack         *ra = arg;
    struct evpl               *evpl;
    struct chimera_vfs_thread *thread;

    evpl   = evpl_create(NULL);
    thread = chimera_vfs_thread_init(evpl, ra->vfs);

    ra->self = pthread_self();

    chimera_vfs_claim_ack(ra->claim, ra->resulting);

    chimera_vfs_thread_destroy(thread);
    evpl_destroy(evpl);

    return NULL;
} /* remote_ack_main */

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

    assert(compound_test_mount_root(ctx.vfs_thread, ctx.evpl, &cred, "mem",
                                    root_fh, &root_fh_len) == CHIMERA_VFS_OK);

    /* A two-level tree to chain through: /mem/a/b */
    mkdir_under(&ctx, &cred, root_fh, root_fh_len, "a");
    memcpy(a_fh, ctx.fh, ctx.fh_len);
    a_fh_len = ctx.fh_len;
    mkdir_under(&ctx, &cred, a_fh, a_fh_len, "b");

    /* Check the rejected dependency in a fresh process so the gate fixture
     * cannot affect the other sequences in this test. */
    if (argc > 1 && strcmp(argv[1], "--reject-skip-handle-from") == 0) {
        struct edit_gate_ctx g;
        int                  i_open;

        memset(&g, 0, sizeof(g));
        g.at         = 0;
        g.size_index = EDIT_GATE_NONE;
        g.skip_index = 1;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_set_gate(cp, edit_gate, &g);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "die", 3,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, NULL, 0, 0, 0);
        chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_use_handle(cp, 2, (uint32_t) i_open);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_op(cp, 2)->status == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_op(cp, i_open)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_op(cp, i_open)->out_handle == NULL);
        chimera_vfs_compound_free(cp);
        _exit(0);
    }

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
     * from a mode that says nothing about who its ACL admits.  The ACL it was
     * computed from survives to here by value (see
     * chimera_vfs_compound_store_attr_to): memfs synthesizes one from the
     * mode for an object with no explicit ACL, so it is present and
     * non-empty. */
    assert(op->attr_mask & CHIMERA_VFS_ATTR_ACL);
    assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
    assert(op->attr.va_acl != NULL && op->attr.va_acl->num_aces > 0);

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

        create_under(&ctx, &cred, root_fh, root_fh_len, "f");
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

        create_under(&ctx, &cred, root_fh, root_fh_len, "c");
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

    /* ---- ACLs by value; set-side ACLs borrowed ----
     * memfs is CAP_ACL_NATIVE and reports an ACL from a per-thread scratch
     * that is valid only for the completion, which is exactly the hazard the
     * by-value copy exists for.  Set side: the SETATTR's ACL is a heap buffer
     * the test owns and frees only after every run that borrowed it.  Read
     * side: the copies in a LOOKUP's attr and dir_post_attr, a GETATTR's attr
     * and an OPEN's attr and dir pair survive the scratch being overwritten
     * by a later per-op lookup, each op owns its own, an op that did not ask
     * carries none, and a second execution of the same compound (the retry
     * shape) replaces the first copy rather than leaking or double-freeing
     * it.  SIDs, which share va_acl's contract, ride the same path. */
    {
        struct chimera_vfs_attrs              sattr;
        struct chimera_acl                   *acl;
        struct chimera_sid                    sid;
        const struct chimera_vfs_compound_op *lk, *ga, *plain;
        const struct chimera_acl             *first;
        size_t                                acl_size = chimera_acl_size(3);
        int                                   i_open, i_sa, i_lk, i_ga;
        int                                   i_plain;

        /* A 3-ACE ACL the mode could not have synthesized: an explicit
         * named-user entry is not something from_mode produces. */
        acl = malloc(acl_size);
        memset(acl, 0, acl_size);
        acl->num_aces            = 3;
        acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
        acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA |
            CHIMERA_ACE_READ_ATTRIBUTES | CHIMERA_ACE_READ_ACL |
            CHIMERA_ACE_WRITE_ACL;
        acl->aces[0].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
        acl->aces[0].who.special = CHIMERA_WHO_OWNER;
        acl->aces[1].type        = CHIMERA_ACE_ALLOWED;
        acl->aces[1].flags       = CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
        acl->aces[1].access_mask = CHIMERA_ACE_READ_DATA;
        acl->aces[1].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
        acl->aces[1].who.special = CHIMERA_WHO_GROUP;
        acl->aces[2].type        = CHIMERA_ACE_ALLOWED;
        acl->aces[2].access_mask = CHIMERA_ACE_READ_DATA;
        acl->aces[2].who.type    = CHIMERA_PRINCIPAL_USER;
        acl->aces[2].who.id      = 1234;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0644;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open(cp, "aclf", 4,
                                      CHIMERA_VFS_OPEN_CREATE |
                                      CHIMERA_VFS_OPEN_READ_ONLY,
                                      0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* SETATTR the ACL through a sequence.  The op's set_attr carries the
         * caller's pointer -- borrowed, not copied -- and the two readings
         * either side of the change come back by value: the pre reading is
         * the mode-synthesized ACL, the post reading is the one set. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
        sattr.va_acl      = acl;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "aclf", 4, 0, 0);
        i_sa = chimera_vfs_compound_add_setattr(
            cp, NULL, &sattr,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_sa);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->set_attr.va_acl == acl);
        assert(op->pre_attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(op->pre_attr.va_acl != NULL);
        assert(op->pre_attr.va_acl != acl);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(op->attr.va_acl != NULL);
        assert(op->attr.va_acl != acl);
        assert(op->attr.va_acl != op->pre_attr.va_acl);
        assert(op->attr.va_acl->num_aces == 3);
        assert(memcmp(op->attr.va_acl, acl, acl_size) == 0);
        chimera_vfs_compound_free(cp);

        /* Read it back in a NEW sequence: LOOKUP with the ACL in both masks,
         * a GETATTR that asks, and one that does not. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk = chimera_vfs_compound_add_lookup(
            cp, "aclf", 4,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL);
        i_ga = chimera_vfs_compound_add_getattr(
            cp, CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL);
        i_plain = chimera_vfs_compound_add_getattr(cp,
                                                   CHIMERA_VFS_ATTR_MASK_STAT);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);

        /* Overwrite the backend's scratch before reading the results: a
         * per-op lookup of a DIRECTORY with the ACL requested puts a
         * mode-synthesized directory ACL where the file's used to be.  A
         * result that merely pointed at the scratch would now read that. */
        lookup_under(&ctx, &cred, root_fh, root_fh_len, "a",
                     CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
                     CHIMERA_VFS_ATTR_ACL);

        lk = chimera_vfs_compound_op(cp, i_lk);
        assert(lk->status == CHIMERA_VFS_OK);
        assert(lk->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(lk->attr.va_acl != NULL && lk->attr.va_acl != acl);
        assert(lk->attr.va_acl->num_aces == 3);
        assert(memcmp(lk->attr.va_acl, acl, acl_size) == 0);
        /* The directory it was found in has its own copy.  (Its CONTENT is
         * not asserted: memfs maps the directory and then the child through
         * one per-thread ACL scratch, so in a lookup that asks for both the
         * directory's va_acl aliases the child's ACL by the time the
         * completion runs -- a memfs defect, not the executor's, and one the
         * by-value copy faithfully preserves.) */
        assert(lk->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(lk->dir_post_attr.va_acl != NULL);
        assert(lk->dir_post_attr.va_acl != lk->attr.va_acl);

        ga = chimera_vfs_compound_op(cp, i_ga);
        assert(ga->status == CHIMERA_VFS_OK);
        assert(ga->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(ga->attr.va_acl != NULL);
        /* Each op owns its own copy; equal by content, distinct in memory. */
        assert(ga->attr.va_acl != lk->attr.va_acl);
        assert(memcmp(ga->attr.va_acl, acl, acl_size) == 0);

        plain = chimera_vfs_compound_op(cp, i_plain);
        assert(plain->status == CHIMERA_VFS_OK);
        assert(!(plain->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL));
        assert(plain->attr.va_acl == NULL);
        chimera_vfs_compound_free(cp);

        /* OPEN with the ACL in every mask it has: the object's, and the
         * directory pair's. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(
            cp, "aclf", 4, CHIMERA_VFS_OPEN_READ_ONLY, 0, NULL,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(op->attr.va_acl != NULL);
        assert(memcmp(op->attr.va_acl, acl, acl_size) == 0);
        assert(op->dir_pre_attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(op->dir_pre_attr.va_acl != NULL);
        assert(op->dir_post_attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(op->dir_post_attr.va_acl != NULL);
        assert(op->dir_post_attr.va_acl != op->dir_pre_attr.va_acl);
        chimera_vfs_compound_free(cp);

        /* The retry shape: the same compound executed twice without a free
         * between.  The second execution's copy replaces the first; a first
         * copy left behind would be a leak, and one freed twice a crash at
         * compound_free -- both of which ASAN reports here. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(
            cp, "aclf", 4,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL);
        i_ga = chimera_vfs_compound_add_getattr(
            cp, CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        first = chimera_vfs_compound_op(cp, i_ga)->attr.va_acl;
        assert(first != NULL);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        ga = chimera_vfs_compound_op(cp, i_ga);
        assert(ga->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(ga->attr.va_acl != NULL);
        assert(memcmp(ga->attr.va_acl, acl, acl_size) == 0);
        chimera_vfs_compound_free(cp);

        /* The set-side buffer outlived every run that borrowed it. */
        free(acl);

        /* SIDs: set an owner SID (restating the uid, which is what lets the
         * owner attach one), borrowed on the same terms; read it back by
         * value alongside an absent group SID and the ACL. */
        assert(chimera_sid_from_str(&sid, "S-1-5-21-7-8-9-1001") == 0);

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask  = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID;
        sattr.va_uid       = 0;
        sattr.va_owner_sid = &sid;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_lookup(cp, "aclf", 4, 0, 0);
        i_sa = chimera_vfs_compound_add_setattr(
            cp, NULL, &sattr, 0,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_OWNER_SID |
            CHIMERA_VFS_ATTR_GROUP_SID);
        i_ga = chimera_vfs_compound_add_getattr(
            cp, CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL |
            CHIMERA_VFS_ATTR_OWNER_SID | CHIMERA_VFS_ATTR_GROUP_SID);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_sa);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->set_attr.va_owner_sid == &sid);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID);
        assert(op->attr.va_owner_sid != NULL && op->attr.va_owner_sid != &sid);
        assert(chimera_sid_equal(op->attr.va_owner_sid, &sid));
        assert(!(op->attr.va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID));
        assert(op->attr.va_group_sid == NULL);

        ga = chimera_vfs_compound_op(cp, i_ga);
        assert(ga->status == CHIMERA_VFS_OK);
        assert(ga->attr.va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID);
        assert(ga->attr.va_owner_sid != op->attr.va_owner_sid);
        assert(chimera_sid_equal(ga->attr.va_owner_sid, &sid));
        assert(ga->attr.va_group_sid == NULL);
        assert(ga->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL);
        assert(ga->attr.va_acl != NULL && ga->attr.va_acl->num_aces == 3);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("ACLs and SIDs come back by value and outlive the backend's "
              "completion; the set side is borrowed");

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
        i_probe = chimera_vfs_compound_add_claim_test(cp, &probe, 0);
        i_lock  = chimera_vfs_compound_add_claim(cp, &claim_a, &ticket_a, 0, 0, 0, 0, 0);

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
        i_probe = chimera_vfs_compound_add_claim_test(cp, &probe, 0);
        i_lock  = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b, 0, 0, 0, 0, 0);

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

        /* ...but not a LOOKUP, which needs a directory.  The handle does not
         * carry the DIRECTORY bit, so the executor asks the object what it is
         * rather than refusing on the flag -- and the object is a regular
         * file, so the answer is ENOTDIR: the thing that is actually wrong.
         * The sequence stops there rather than opening a directory of its
         * own. */
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
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);
        assert(chimera_vfs_compound_num_completed(cp) == 2);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOTDIR);
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
        i_cl = chimera_vfs_compound_add_close_doc(cp, 0, NULL);
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
        i_cl          = chimera_vfs_compound_add_close_doc(cp, 0, NULL);
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
        i_cl          = chimera_vfs_compound_add_close_doc(cp, 0, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_cl)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("the open cursor ops fill, hand out, close, and move the slot");

    /* ---- use_handle names an op that LEAVES a handle, or nothing at all ----
     * A PUTHANDLE and an OPEN_CURRENT both put a handle on the cursor and
     * neither publishes one in out_handle, so naming either as a use_handle
     * source resolves to NULL -- and the addressing op used to dereference it
     * three frames down in a backend.  The source op's type is known where
     * use_handle is called, so the sequence fails to BUILD and submit answers
     * EINVAL: nothing runs, and nothing crashes. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *h, *h2;
        int                             i_open, i_ph, i_ga, i_oc, i_gh;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "uhsrc", 5,
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

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_ph = chimera_vfs_compound_add_puthandle(cp, h,
                                                  CHIMERA_VFS_OPEN_READ_ONLY);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_ga,
                                           (uint32_t) i_ph);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        /* Refused whole: not one op of it ran. */
        assert(chimera_vfs_compound_num_completed(cp) == 0);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, h);

        /* The same for an OPEN_CURRENT, whose handle the cursor owns... */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_oc = chimera_vfs_compound_add_open_current(cp,
                                                     CHIMERA_VFS_OPEN_READ_ONLY,
                                                     0);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_ga,
                                           (uint32_t) i_oc);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        /* ...and the GETHANDLE behind it is the op to name instead, which is
         * the same sequence, built the way that works. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        chimera_vfs_compound_add_open_current(cp, CHIMERA_VFS_OPEN_READ_ONLY,
                                              0);
        i_gh = chimera_vfs_compound_add_gethandle(cp);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_ga,
                                           (uint32_t) i_gh);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        h2 = chimera_vfs_compound_take_handle(cp, (uint32_t) i_gh);
        assert(h2 != NULL);
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, h2);
    }
    TEST_PASS("a use_handle source that leaves no handle fails the build "
              "instead of handing an op NULL");

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

        /* GRANTED, then the LOOKUP behind it fails (the lent handle's object
         * is a regular file, so it cannot serve one: ENOTDIR). */
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_a, &ticket_a, 0, 0, 0, 0, 0);
        i_lk   = chimera_vfs_compound_add_lookup(cp, "x", 1, 0, 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOTDIR);
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
        i_probe = chimera_vfs_compound_add_claim_test(cp, &probe, 0);
        i_lock  = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b, 0, 0, 0, 0, 0);

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
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_a, &ticket_a, 0, 0, 0, 0, 0);

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
        i_probe       = chimera_vfs_compound_add_claim_test(cp, &probe, 0);
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
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_a, &ticket_a, 0, 0, 0, 0, 0);

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
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b,
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                                0, 0, 0, 0);
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

    /* ---- the park notification, and cancelling a parked CLAIM ----
     * A front end with a client on the other end of a blocking lock needs two
     * things the sequence did not give it: to be told that it is waiting, so
     * it can emit its interim, and to be able to stop waiting.  Four shapes
     * here: a grant that never parks says nothing; a park says so ONCE and
     * names the op; a cancel completes the run ECANCELED at that op and
     * abort-releases what an earlier CLAIM in the same run took; and the
     * holder letting go afterwards does not resurrect it. */
    {
        struct chimera_vfs_state          *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           claim_a, claim_b, claim_c, claim_d;
        struct chimera_vfs_pending_acquire ticket_b, ticket_c, ticket_d;
        struct chimera_claim_owner         owner_a, owner_b, owner_c;
        struct chimera_vfs_claim_conflict  conflict;
        struct chimera_vfs_file_state     *fs, *fs_b;
        struct park_rec                    park;
        int                                i_open, i_keep, i_lock, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "pk", 2,
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

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 31;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 32;
        memset(&owner_c, 0, sizeof(owner_c));
        owner_c.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_c.owner_lo = 33;

        /* A grant that never parks tells the caller nothing. */
        chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16, &owner_b);
        memset(&park, 0, sizeof(park));

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b,
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                                0, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_set_park_cb(cp, park_rec_cb, &park);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(park.calls == 0);
        fs_b = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock);
        assert(fs_b != NULL);
        chimera_vfs_compound_free(cp);

        /* Now a run that parks behind it, with a CLAIM in front of the parked
         * one that IS granted -- that is what the abort release has to undo --
         * and a GETATTR behind it that must never run. */
        chimera_vfs_claim_init_range(&claim_c, true, false, 32, 16, &owner_c);
        chimera_vfs_claim_init_range(&claim_d, true, false, 0, 16, &owner_c);
        memset(&park, 0, sizeof(park));

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_keep = chimera_vfs_compound_add_claim(cp, &claim_c, &ticket_c,
                                                CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                                0, 0, 0, 0);
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_d, &ticket_d,
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                                0, 0, 0, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_set_park_cb(cp, park_rec_cb, &park);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);

        /* Parked, and said so once, naming the op that parked. */
        assert(ctx.callbacks == 0);
        assert(!ctx.done);
        assert(park.calls == 1);
        assert(park.index == (uint32_t) i_lock);

        /* Stop waiting.  The completion runs inside the call. */
        assert(chimera_vfs_compound_cancel(cp) != 0);
        assert(ctx.callbacks == 1);
        assert(ctx.done);
        ctx.done = 0;

        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ECANCELED);
        op = chimera_vfs_compound_op(cp, i_lock);
        assert(op->status == CHIMERA_VFS_ECANCELED);
        /* The op it stopped at ran; the one behind it did not. */
        assert(chimera_vfs_compound_num_completed(cp) == (uint32_t) i_lock + 1);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);
        /* The CLAIM in front of it was granted and has been released with the
         * run: the arbiter did say GRANTED, and the file state is gone. */
        op = chimera_vfs_compound_op(cp, i_keep);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_keep) ==
               NULL);
        assert(park.calls == 1);
        chimera_vfs_compound_free(cp);

        /* Released for real, not merely forgotten: another owner takes it. */
        chimera_vfs_claim_init_range(&claim_a, true, false, 32, 16, &owner_a);
        assert(chimera_vfs_claim_try_acquire(state, fs, &claim_a, &conflict) ==
               CHIMERA_CLAIM_GRANTED);
        chimera_vfs_claim_release_ranged(ctx.vfs_thread, state, fs, &claim_a);

        /* The holder letting go afterwards has nothing to grant: the ticket
         * left the queue with the cancel, and no second completion arrives. */
        chimera_vfs_claim_release_ranged(ctx.vfs_thread, state, fs_b, &claim_b);
        pump_probe(&ctx, &cred, root_fh, root_fh_len);
        assert(ctx.callbacks == 2);

        chimera_vfs_state_put(state, fs_b);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("park_cb fires once for a parked CLAIM and not for a grant; "
              "cancel completes ECANCELED and abort-releases the run's claims");

    /* ---- cancel versus the grant, raced ----
     * The whole of the arbitration is chimera_vfs_claim_cancel's return
     * value, and this is the case it exists for: a holder releasing on one
     * thread at the same instant as a cancel on the submitting one.  Either
     * outcome is correct; what may never happen is both, or neither.  Looped,
     * because a race proved once is a race not proved. */
    {
        struct chimera_vfs_state          *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           claim_a, claim_b;
        struct chimera_vfs_pending_acquire ticket_b;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_claim_conflict  conflict;
        struct chimera_vfs_file_state     *fs, *fs_b;
        struct remote_release              rr;
        struct park_rec                    park;
        pthread_t                          tid;
        int                                cancels = 0, grants = 0;
        int                                joined;
        int                                i_open, i_lock, iter;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "rz", 2,
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

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 41;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 42;

        for (iter = 0; iter < 100; iter++) {
            chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16,
                                         &owner_a);
            assert(chimera_vfs_claim_try_acquire(state, fs, &claim_a,
                                                 &conflict) ==
                   CHIMERA_CLAIM_GRANTED);

            chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16,
                                         &owner_b);
            memset(&park, 0, sizeof(park));

            cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            chimera_vfs_compound_add_puthandle(cp, oh,
                                               CHIMERA_VFS_OPEN_READ_ONLY |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY);
            i_lock = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b,
                                                    CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                                    CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                                    0, 0, 0, 0);
            ctx.callbacks = 0;
            chimera_vfs_compound_set_park_cb(cp, park_rec_cb, &park);
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            assert(ctx.callbacks == 0);
            assert(park.calls == 1);

            /* The holder lets go from another thread; we cancel here.  On odd
             * iterations the release is allowed to finish first, so the "the
             * grant owns the completion" arm is taken whatever the scheduler
             * does -- the pump dequeues the ticket and fires the callback
             * inline, so a cancel behind it can only lose.  On even ones we
             * cancel straight into the race and take whichever side wins. */
            rr.vfs   = ctx.vfs;
            rr.fs    = fs;
            rr.claim = &claim_a;
            assert(pthread_create(&tid, NULL, remote_release_main, &rr) == 0);

            joined = 0;
            if (iter & 1) {
                assert(pthread_join(tid, NULL) == 0);
                joined = 1;
            }

            if (chimera_vfs_compound_cancel(cp)) {
                /* We took it back: the completion already ran, here. */
                cancels++;
                assert(!joined);
                assert(ctx.callbacks == 1);
                assert(ctx.done);
                ctx.done = 0;
                assert(chimera_vfs_compound_status(cp) ==
                       CHIMERA_VFS_ECANCELED);
                assert(chimera_vfs_compound_op(cp, i_lock)->status ==
                       CHIMERA_VFS_ECANCELED);
                assert(chimera_vfs_compound_take_file_state(
                           cp, (uint32_t) i_lock) == NULL);
                assert(pthread_join(tid, NULL) == 0);
                chimera_vfs_compound_free(cp);
            } else {
                /* The grant owns the completion; it comes home as promised. */
                grants++;
                if (!joined) {
                    assert(pthread_join(tid, NULL) == 0);
                }
                wait_done(&ctx);
                assert(ctx.callbacks == 1);
                assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
                assert(chimera_vfs_compound_op(cp, i_lock)->claim_result ==
                       CHIMERA_CLAIM_GRANTED);
                fs_b = chimera_vfs_compound_take_file_state(cp,
                                                            (uint32_t) i_lock);
                assert(fs_b != NULL);
                chimera_vfs_compound_free(cp);
                chimera_vfs_claim_release_ranged(ctx.vfs_thread, state, fs_b,
                                                 &claim_b);
                chimera_vfs_state_put(state, fs_b);
            }

            /* One completion, whichever way it went -- and the park was
             * reported once either way. */
            assert(ctx.callbacks == 1);
            assert(park.calls == 1);
        }

        /* Which side wins a true race is the scheduler's business, so the
         * split is not asserted -- what is, is that every iteration produced
         * exactly one completion (checked above), and that the grant arm was
         * really taken: the odd iterations force it. */
        assert(cancels + grants == 100);
        assert(grants >= 50);

        chimera_vfs_state_put(state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("a grant racing a cancel produces exactly one completion, "
              "whichever wins");

    /* ---- the park notification and cancel on a parked RECALL ----
     * A RECALL parks on the recall drain rather than on a pending acquire, so
     * it is taken back through the other core entrance -- and what it leaves
     * behind is the point: the breaks it already kicked STAY kicked, because
     * a recall hands nobody anything.  NOWAIT never parks and so never
     * reports one. */
    {
        struct chimera_vfs_state         *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs          sattr;
        struct chimera_vfs_open_handle   *oh;
        struct chimera_vfs_claim          deleg, oplock;
        struct chimera_claim_owner        owner_n, owner_s;
        struct chimera_vfs_claim_conflict conflict;
        struct chimera_vfs_file_state    *fs;
        struct break_rec                  rec_d, rec;
        struct park_rec                   park;
        int                               i_open, i_rc, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "rk", 2,
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

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        /* NOWAIT against a holder it really does recall -- another client's
         * write delegation, the NFS4ERR_DELAY shape.  It kicks the break and
         * answers inside submit: no park, so nothing to report, even though
         * the holder is left in the way. */
        memset(&owner_n, 0, sizeof(owner_n));
        owner_n.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_n.client_key = 0xD2;
        owner_n.owner_lo   = 4;
        chimera_vfs_claim_init_delegation(&deleg, true, &owner_n);
        memset(&rec_d, 0, sizeof(rec_d));
        deleg.break_cb   = break_rec_cb;
        deleg.cb_private = &rec_d;
        assert(chimera_vfs_claim_try_acquire(state, fs, &deleg, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        memset(&park, 0, sizeof(park));
        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_rc = chimera_vfs_compound_add_recall(cp, oh->fh, oh->fh_len, 0,
                                               CHIMERA_VFS_COMPOUND_RECALL_NOWAIT);
        ctx.callbacks = 0;
        chimera_vfs_compound_set_park_cb(cp, park_rec_cb, &park);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        assert(ctx.callbacks == 1);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_rc)->recall_still_open == 1);
        assert(rec_d.fired == 1);
        assert(park.calls == 0);
        /* Cancelling a run that never parked is legal and does nothing. */
        assert(chimera_vfs_compound_cancel(cp) == 0);
        assert(ctx.callbacks == 1);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release(state, fs, &deleg);

        /* Another client's batch oplock (RWH).  The parking form with the
         * rename floor breaks the handle cache once and parks on the ack. */
        memset(&owner_s, 0, sizeof(owner_s));
        owner_s.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_s.client_key = 0x5C;
        owner_s.owner_lo   = 3;
        chimera_vfs_claim_init_oplock(&oplock,
                                      CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                      CHIMERA_CLAIM_H,
                                      &owner_s);
        memset(&rec, 0, sizeof(rec));
        oplock.break_cb   = break_rec_cb;
        oplock.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs, &oplock, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        memset(&park, 0, sizeof(park));
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, oh->fh, (int) oh->fh_len);
        i_rc = chimera_vfs_compound_add_recall(cp, NULL, 0,
                                               CHIMERA_CLAIM_CR |
                                               CHIMERA_CLAIM_CW, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_set_park_cb(cp, park_rec_cb, &park);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);

        assert(ctx.callbacks == 0);
        assert(!ctx.done);
        assert(park.calls == 1);
        assert(park.index == (uint32_t) i_rc);
        assert(rec.fired == 1);
        assert(oplock.break_state == CHIMERA_CLAIM_BREAK_BREAKING);

        assert(chimera_vfs_compound_cancel(cp) != 0);
        assert(ctx.callbacks == 1);
        assert(ctx.done);
        ctx.done = 0;
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ECANCELED);
        assert(chimera_vfs_compound_op(cp, i_rc)->status ==
               CHIMERA_VFS_ECANCELED);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);
        assert(park.calls == 1);
        chimera_vfs_compound_free(cp);

        /* What the abandoned recall kicked is still kicked: the holder is
         * BREAKING, and its break was fired once and not retracted. */
        assert(oplock.break_state == CHIMERA_CLAIM_BREAK_BREAKING);
        assert(rec.fired == 1);

        /* And the ack that would have resumed it resumes nothing. */
        chimera_vfs_claim_ack(&oplock, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW);
        pump_probe(&ctx, &cred, root_fh, root_fh_len);
        assert(ctx.callbacks == 2);

        chimera_vfs_claim_release(state, fs, &oplock);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("a parked RECALL reports its park and is cancellable; the breaks "
              "it kicked stay kicked, and NOWAIT never parks");

    /* ---- cancel_post: the cancel whose trigger is on another thread ----
     * chimera_vfs_compound_cancel is the submitting thread's call and runs
     * the completion inline, which is exactly what a FUSE_INTERRUPT, an SMB2
     * CANCEL or a teardown cannot have: they arrive on whatever thread read
     * them, holding their own state lock.  cancel_post is the same cancel
     * marshalled onto the submitting thread, and what is pinned here is that
     * it decides nothing where it is called, that the completion still fires
     * exactly once and still on the submitting thread, and that posting twice
     * is posting once. */
    {
        struct chimera_vfs_state          *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           claim_a, claim_b;
        struct chimera_vfs_pending_acquire ticket_b;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_claim_conflict  conflict;
        struct chimera_vfs_file_state     *fs;
        struct remote_cancel               rc;
        struct park_rec                    park;
        pthread_t                          self = pthread_self();
        pthread_t                          tid;
        int                                i_open, i_lock, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "cx", 2,
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

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 51;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 52;

        /* Someone else holds the range, so the run below parks. */
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);
        assert(chimera_vfs_claim_try_acquire(state, fs, &claim_a, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        /* ---- a post from a second thread takes the park back here ---- */
        chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16, &owner_b);
        memset(&park, 0, sizeof(park));

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b,
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                                0, 0, 0, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_set_park_cb(cp, park_rec_cb, &park);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);

        assert(ctx.callbacks == 0);
        assert(park.calls == 1);
        assert(park.index == (uint32_t) i_lock);

        rc.compound = cp;
        rc.posts    = 1;
        assert(pthread_create(&tid, NULL, remote_cancel_main, &rc) == 0);
        assert(pthread_join(tid, NULL) == 0);

        /* The post decided nothing: no completion ran on the posting thread,
         * and none has run here either -- this thread has not been near its
         * doorbell since. */
        assert(ctx.callbacks == 0);
        assert(!ctx.done);

        /* It comes home, and the cancel is made here. */
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(pthread_equal(ctx.cb_thread, self));
        assert(!pthread_equal(ctx.cb_thread, rc.self));
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ECANCELED);
        assert(chimera_vfs_compound_op(cp, i_lock)->status ==
               CHIMERA_VFS_ECANCELED);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_lock) ==
               NULL);
        chimera_vfs_compound_free(cp);

        /* ---- two posts are one completion ----
         * Both are made before this thread goes near its doorbell, so the
         * second meets the latch the first set. */
        chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16, &owner_b);
        memset(&park, 0, sizeof(park));

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_lock = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b,
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                                CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                                0, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_set_park_cb(cp, park_rec_cb, &park);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        assert(park.calls == 1);

        rc.compound = cp;
        rc.posts    = 2;
        assert(pthread_create(&tid, NULL, remote_cancel_main, &rc) == 0);
        assert(pthread_join(tid, NULL) == 0);
        assert(ctx.callbacks == 0);

        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ECANCELED);

        /* A third post, now that the run is over and the compound is still
         * the caller's: it rides home again and finds nothing to take back.
         * The drain it rides is the one pump_cancel provokes, and the only
         * completion in there is pump_cancel's own. */
        rc.posts = 1;
        assert(pthread_create(&tid, NULL, remote_cancel_main, &rc) == 0);
        assert(pthread_join(tid, NULL) == 0);
        chimera_vfs_compound_free(cp);
        pump_cancel(&ctx, &cred, oh, 53);
        assert(ctx.callbacks == 2);

        chimera_vfs_claim_release_ranged(ctx.vfs_thread, state, fs, &claim_a);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("cancel_post cancels from another thread, completes once on the "
              "submitting one, and two posts are one completion");

    /* ---- cancel_post racing the grant, from a third thread ----
     * The same arbitration as the inline cancel's race, one hop further out:
     * the holder lets go on one thread while the cancel is posted from
     * another, and the submitting thread is where both answers land.  Either
     * outcome is correct; what may never happen is both, or neither.  Looped,
     * because a race proved once is a race not proved. */
    {
        struct chimera_vfs_state          *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           claim_a, claim_b;
        struct chimera_vfs_pending_acquire ticket_b;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_claim_conflict  conflict;
        struct chimera_vfs_file_state     *fs, *fs_b;
        struct remote_release              rr;
        struct remote_cancel               rc;
        pthread_t                          self = pthread_self();
        pthread_t                          rtid, ctid;
        int                                cancels = 0, grants = 0;
        int                                joined;
        int                                i_open, i_lock, iter;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "cy", 2,
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

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 61;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 62;

        for (iter = 0; iter < 100; iter++) {
            chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16,
                                         &owner_a);
            assert(chimera_vfs_claim_try_acquire(state, fs, &claim_a,
                                                 &conflict) ==
                   CHIMERA_CLAIM_GRANTED);

            chimera_vfs_claim_init_range(&claim_b, true, false, 0, 16,
                                         &owner_b);

            cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
            chimera_vfs_compound_add_puthandle(cp, oh,
                                               CHIMERA_VFS_OPEN_READ_ONLY |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY);
            i_lock = chimera_vfs_compound_add_claim(cp, &claim_b, &ticket_b,
                                                    CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                                                    CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD,
                                                    0, 0, 0, 0);
            ctx.callbacks = 0;
            chimera_vfs_compound_submit(cp, compound_cb, &ctx);
            assert(ctx.callbacks == 0);

            /* Two other threads at once: one lets the range go, the other
             * asks for the run to be cancelled.  Neither may complete
             * anything itself.
             *
             * On odd iterations the release is allowed to finish before this
             * thread goes near its doorbell, so the post can only arrive to
             * find the ticket already answered and the "the grant owns the
             * completion" arm is taken whatever the scheduler does.  On even
             * ones the drain runs WHILE both are in flight, which is the real
             * race: whether the posted cancel reaches the claim core before
             * the release does is nobody's to say. */
            rr.vfs      = ctx.vfs;
            rr.fs       = fs;
            rr.claim    = &claim_a;
            rc.compound = cp;
            rc.posts    = 1;
            assert(pthread_create(&rtid, NULL, remote_release_main, &rr) == 0);

            joined = 0;
            if (iter & 1) {
                assert(pthread_join(rtid, NULL) == 0);
                joined = 1;
            }

            assert(pthread_create(&ctid, NULL, remote_cancel_main, &rc) == 0);

            wait_done(&ctx);

            /* Joined only now: the compound is the caller's until it is
             * freed, so a post that has not been made yet is still legal. */
            if (!joined) {
                assert(pthread_join(rtid, NULL) == 0);
            }
            assert(pthread_join(ctid, NULL) == 0);

            /* Exactly one completion, here, whichever side won. */
            assert(ctx.callbacks == 1);
            assert(pthread_equal(ctx.cb_thread, self));

            if (chimera_vfs_compound_status(cp) == CHIMERA_VFS_ECANCELED) {
                cancels++;
                assert(chimera_vfs_compound_op(cp, i_lock)->status ==
                       CHIMERA_VFS_ECANCELED);
                assert(chimera_vfs_compound_take_file_state(
                           cp, (uint32_t) i_lock) == NULL);
                chimera_vfs_compound_free(cp);
            } else {
                grants++;
                assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
                assert(chimera_vfs_compound_op(cp, i_lock)->claim_result ==
                       CHIMERA_CLAIM_GRANTED);
                fs_b = chimera_vfs_compound_take_file_state(cp,
                                                            (uint32_t) i_lock);
                assert(fs_b != NULL);
                /* Freed while the post may still be riding: the core holds
                 * the memory back rather than recycle it underneath the
                 * doorbell, and the drain finishes the job. */
                chimera_vfs_compound_free(cp);
                chimera_vfs_claim_release_ranged(ctx.vfs_thread, state, fs_b,
                                                 &claim_b);
                chimera_vfs_state_put(state, fs_b);
            }

            assert(ctx.callbacks == 1);
        }

        /* Which side wins a true race is the scheduler's business, so the
         * split is not asserted -- what is, is that every iteration produced
         * exactly one completion (checked above), and that the grant arm was
         * really taken: the odd iterations force it. */
        assert(cancels + grants == 100);
        assert(grants >= 50);

        /* One last drain, so a post that was still riding when the final
         * iteration finished is collected rather than left holding a
         * compound the free deferred. */
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);
        assert(chimera_vfs_claim_try_acquire(state, fs, &claim_a, &conflict) ==
               CHIMERA_CLAIM_GRANTED);
        ctx.callbacks = 0;
        pump_cancel(&ctx, &cred, oh, 63);
        assert(ctx.callbacks == 1);
        chimera_vfs_claim_release_ranged(ctx.vfs_thread, state, fs, &claim_a);

        chimera_vfs_state_put(state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("cancel_post racing a grant from a third thread produces exactly "
              "one completion, whichever wins");

    /* ---- cancel_post for a run that is not parked ----
     * The caller learns nothing synchronously, so it cannot know whether the
     * run it posted for is still waiting -- which is the point: a post that
     * finds nothing to take back is legal and silent, and the run answers
     * with whatever it actually did.  Two shapes: a post for a run that has
     * already finished (the compound still the caller's, and then freed while
     * the post is still riding), and a post that lands before the run is even
     * submitted, which must not poison it. */
    {
        struct chimera_vfs_state         *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs          sattr;
        struct chimera_vfs_open_handle   *oh;
        struct chimera_vfs_claim          claim_a;
        struct chimera_claim_owner        owner_a;
        struct chimera_vfs_claim_conflict conflict;
        struct chimera_vfs_file_state    *fs;
        struct remote_cancel              rc;
        pthread_t                         tid;
        int                               i_open, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "cz", 2,
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

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        /* Held for the whole block: it is what pump_cancel's runs park
         * behind. */
        memset(&owner_a, 0, sizeof(owner_a));
        owner_a.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.owner_lo = 71;
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);
        assert(chimera_vfs_claim_try_acquire(state, fs, &claim_a, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        /* A run that never parks: it answers with what it did, and the post
         * that arrives afterwards changes nothing. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_ga = chimera_vfs_compound_add_getattr(cp,
                                                CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);

        rc.compound = cp;
        rc.posts    = 1;
        assert(pthread_create(&tid, NULL, remote_cancel_main, &rc) == 0);
        assert(pthread_join(tid, NULL) == 0);
        assert(ctx.callbacks == 1);

        /* Freed with the post still in flight -- the legal case the caller
         * cannot avoid, and the one the core defends: the free releases what
         * the compound held and leaves the recycle to the drain. */
        chimera_vfs_compound_free(cp);

        pump_cancel(&ctx, &cred, oh, 72);
        assert(ctx.callbacks == 2);

        /* A post that lands before the run does.  It is drained by
         * pump_cancel below, finds a compound that has not been submitted,
         * and leaves it to run normally afterwards. */
        cp          = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        rc.compound = cp;
        rc.posts    = 1;
        assert(pthread_create(&tid, NULL, remote_cancel_main, &rc) == 0);
        assert(pthread_join(tid, NULL) == 0);

        ctx.callbacks = 0;
        pump_cancel(&ctx, &cred, oh, 73);
        assert(ctx.callbacks == 1);

        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 2);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release_ranged(ctx.vfs_thread, state, fs, &claim_a);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("cancel_post on a run that is not parked is silent, and the run "
              "answers with its real outcome");

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

        struct dest_finish_test finish = { .dest = &dest };
        chimera_vfs_compound_set_finish_handler(cp, dest_finish, &finish);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1 && finish.finishes == 1);
        assert(chimera_vfs_compound_finish_status(cp) == CHIMERA_VFS_EAGAIN);
        ctx.callbacks = 0;
        assert(chimera_vfs_compound_retry(cp));
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(finish.finishes == 2);
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

    /* ---- FIND: the recursive walk, streamed ----
     * A small tree under /mem/ft: a/, a/b/, skip/, and a file in each.  The
     * filter prunes skip/ -- its own entry still arrives, nothing below it
     * does -- and append stages every path into a request-local array that
     * reset truncates, so a second execution of the same op starts empty.
     * What the executor owes: reset before every execution, the root's fh on
     * the op before the walk, entries with the fh and mode the walk descends
     * on (and no ACL, since none was asked for), a refusal that ends the op
     * with eof clear, and ENOTDIR for a FIND through a regular file. */
    {
        struct find_ctx          f;
        struct chimera_vfs_attrs sattr;
        uint8_t                  ft_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 ft_fh_len;
        uint8_t                  f1_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 f1_fh_len;
        uint8_t                  sub_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 sub_fh_len;
        const char              *files[] = { "ft/f1", "ft/a/f2", "ft/a/b/f3", "ft/skip/f4" };
        int                      i_find, i_ga, total, i;

        mkdir_under(&ctx, &cred, root_fh, root_fh_len, "ft");
        memcpy(ft_fh, ctx.fh, ctx.fh_len);
        ft_fh_len = ctx.fh_len;
        mkdir_under(&ctx, &cred, ft_fh, ft_fh_len, "a");
        memcpy(sub_fh, ctx.fh, ctx.fh_len);
        sub_fh_len = ctx.fh_len;
        mkdir_under(&ctx, &cred, sub_fh, sub_fh_len, "b");
        mkdir_under(&ctx, &cred, ft_fh, ft_fh_len, "skip");

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0644;

        for (i = 0; i < 4; i++) {
            create_under(&ctx, &cred, root_fh, root_fh_len, files[i]);
            if (i == 0) {
                memcpy(f1_fh, ctx.fh, ctx.fh_len);
                f1_fh_len = ctx.fh_len;
            }
        }

        memset(&f, 0, sizeof(f));
        f.prune = "/skip";

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, ft_fh, (int) ft_fh_len);
        i_find = chimera_vfs_compound_add_find_stream(cp, CHIMERA_VFS_ATTR_MASK_STAT,
                                                      find_filter, find_append,
                                                      find_reset, &f);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        assert(i_find >= 0 && i_ga >= 0);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_find);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->eof);
        /* Streamed, never staged. */
        assert(op->entries == NULL && op->num_entries == 0);
        /* The root, recorded before the walk. */
        assert(op->fh_len == ft_fh_len && memcmp(op->fh, ft_fh, ft_fh_len) == 0);
        assert(f.resets == 1);
        /* Every directory was put to the filter: a, a/b, skip. */
        assert(f.filter_calls == 3);
        /* Everything but the pruned subtree's contents, the pruned directory
         * itself included; "." and ".." never. */
        assert(f.count == 6);
        assert(find_has(&f, "/a"));
        assert(find_has(&f, "/a/b"));
        assert(find_has(&f, "/a/b/f3"));
        assert(find_has(&f, "/a/f2"));
        assert(find_has(&f, "/f1"));
        assert(find_has(&f, "/skip"));
        assert(!find_has(&f, "/skip/f4"));
        total = f.count;
        /* A FIND does not move the current object. */
        op = chimera_vfs_compound_op(cp, i_ga);
        assert(op->status == CHIMERA_VFS_OK);
        assert(S_ISDIR(op->attr.va_mode));
        chimera_vfs_compound_free(cp);

        /* The same context again, un-cleared: reset is what empties it, so
         * the count is the tree's and not twice the tree's. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, ft_fh, (int) ft_fh_len);
        chimera_vfs_compound_add_find_stream(cp, CHIMERA_VFS_ATTR_MASK_STAT,
                                             find_filter, find_append, find_reset, &f);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(f.resets == 2);
        assert(f.count == total);
        chimera_vfs_compound_free(cp);

        /* Refuse the second entry: the op ends there, OK with eof clear, and
         * nothing after the refusal was staged. */
        f.stop_at = 2;
        cp        = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, ft_fh, (int) ft_fh_len);
        i_find = chimera_vfs_compound_add_find_stream(cp, CHIMERA_VFS_ATTR_MASK_STAT,
                                                      find_filter, find_append,
                                                      find_reset, &f);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_find);
        assert(op->status == CHIMERA_VFS_OK);
        assert(!op->eof);
        assert(f.resets == 3);
        assert(f.count == 1);
        chimera_vfs_compound_free(cp);

        /* Through a regular file: opened as a directory first, so ENOTDIR,
         * and reset ran (before the execution) while append never did. */
        f.stop_at = 0;
        cp        = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, f1_fh, (int) f1_fh_len);
        i_find = chimera_vfs_compound_add_find_stream(cp, CHIMERA_VFS_ATTR_MASK_STAT,
                                                      find_filter, find_append,
                                                      find_reset, &f);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);
        op = chimera_vfs_compound_op(cp, i_find);
        assert(op->status == CHIMERA_VFS_ENOTDIR);
        assert(f.count == 0);
        chimera_vfs_compound_free(cp);

        /* A walk missing any of its callbacks does not build. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        assert(chimera_vfs_compound_add_find_stream(cp, 0, NULL, find_append,
                                                    find_reset, &f) == -1);
        assert(chimera_vfs_compound_add_find_stream(cp, 0, find_filter, NULL,
                                                    find_reset, &f) == -1);
        assert(chimera_vfs_compound_add_find_stream(cp, 0, find_filter, find_append,
                                                    NULL, &f) == -1);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        /* ---- GET_LAYOUT on a backend that does not source layouts ----
         * memfs is orchestrated (CAP_LAYOUT), not a source, so the per-op
         * call answers ENOTSUP and the op passes it through: the sequence
         * stops there, and the copies stay empty. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, f1_fh, (int) f1_fh_len);
        i_find = chimera_vfs_compound_add_get_layout(
            cp, 0, UINT64_MAX, 1, CHIMERA_VFS_LAYOUT_CLASS_FLEX,
            CHIMERA_VFS_LAYOUT_MAX_SEGMENTS);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        assert(i_find >= 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTSUP);
        op = chimera_vfs_compound_op(cp, i_find);
        assert(op->status == CHIMERA_VFS_ENOTSUP);
        assert(op->layout_num_segments == 0 && op->layout_segments == NULL);
        assert(op->layout_num_devices == 0 && op->layout_devices == NULL);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("FIND streams the tree, prunes on the filter, stops on append; "
              "GET_LAYOUT is ENOTSUP off a non-source backend");

    /* ---- RECALL ----
     * The caching leases are held straight through the claim core, the way
     * vfs_claim_test does, so the test owns the holder: it sees the break
     * arrive, acks it from another thread, and releases it.  Three shapes:
     * nothing held (both forms answer at once, still_open 0); a delegation
     * held by another owner (NOWAIT kicks the recall, does not park, and
     * reports the holder still in the way); and a batch oplock held by
     * another owner (the parking form parks, the break fires, and the ack
     * from a second VFS thread resumes the sequence on the submitting one). */
    {
        struct chimera_vfs_state         *state = ctx.vfs->vfs_state;
        struct chimera_vfs_file_state    *fs;
        struct chimera_vfs_attrs          sattr;
        struct chimera_vfs_open_handle   *oh;
        struct chimera_vfs_claim          deleg, oplock;
        struct chimera_claim_owner        owner_n, owner_s;
        struct chimera_vfs_claim_conflict conflict;
        struct break_rec                  rec;
        struct remote_ack                 ra;
        pthread_t                         self = pthread_self();
        pthread_t                         tid;
        uint8_t                           too_long[CHIMERA_VFS_FH_SIZE + 1];
        int                               i_open, i_rc, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "rc", 2,
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

        /* Nothing held: the parking form has nothing to wait for, through
         * the lent handle (spared), through the current fh, and by the op's
         * own fh with no current object at all. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_rc = chimera_vfs_compound_add_recall(cp, NULL, 0, CHIMERA_CLAIM_CR, 0);
        i_ga = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        assert(i_rc >= 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rc);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->recall_still_open == 0);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, oh->fh, (int) oh->fh_len);
        i_rc          = chimera_vfs_compound_add_recall(cp, NULL, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_rc)->recall_still_open == 0);
        chimera_vfs_compound_free(cp);

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_rc = chimera_vfs_compound_add_recall(cp, oh->fh, oh->fh_len, 0,
                                               CHIMERA_VFS_COMPOUND_RECALL_NOWAIT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_rc)->recall_still_open == 0);
        chimera_vfs_compound_free(cp);

        /* A recall with no object at all is the caller's bug. */
        cp            = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_rc          = chimera_vfs_compound_add_recall(cp, NULL, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);

        /* Malformed: an fh that cannot be one, a NOWAIT with a floor. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        assert(chimera_vfs_compound_add_recall(cp, too_long, sizeof(too_long),
                                               0, 0) == -1);
        assert(chimera_vfs_compound_add_recall(cp, NULL, 4, 0, 0) == -1);
        assert(chimera_vfs_compound_add_recall(cp, NULL, 0, CHIMERA_CLAIM_CR,
                                               CHIMERA_VFS_COMPOUND_RECALL_NOWAIT)
               == -1);
        chimera_vfs_compound_free(cp);

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        /* Another client's write delegation.  NOWAIT kicks the full recall
        * -- the holder's break fires inside the op -- and answers at once
        * with the holder still in the way, which is the NFS4ERR_DELAY
        * shape.  Once the holder returns it, the same op finds nothing. */
        memset(&owner_n, 0, sizeof(owner_n));
        owner_n.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_n.client_key = 0xD1;
        owner_n.owner_lo   = 1;
        chimera_vfs_claim_init_delegation(&deleg, true, &owner_n);
        memset(&rec, 0, sizeof(rec));
        deleg.break_cb   = break_rec_cb;
        deleg.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs, &deleg, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        /* The op's own fh is an argument and moves no cursor; the GETATTR
         * behind it wants a current object of its own. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, oh->fh, (int) oh->fh_len);
        i_rc = chimera_vfs_compound_add_recall(cp, oh->fh, oh->fh_len, 0,
                                               CHIMERA_VFS_COMPOUND_RECALL_NOWAIT);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        /* Answered inside submit: no park. */
        assert(ctx.callbacks == 1);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rc);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->recall_still_open == 1);
        assert(rec.fired == 1);
        assert(deleg.break_state == CHIMERA_CLAIM_BREAK_BREAKING);
        /* The sequence went on regardless: the caller maps the boolean. */
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release(state, fs, &deleg);

        cp   = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        i_rc = chimera_vfs_compound_add_recall(cp, oh->fh, oh->fh_len, 0,
                                               CHIMERA_VFS_COMPOUND_RECALL_NOWAIT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_rc)->recall_still_open == 0);
        assert(rec.fired == 1);
        chimera_vfs_compound_free(cp);

        /* Another client's batch oplock (RWH).  The parking form with the
         * rename floor (RW) breaks the handle cache once and parks; the
         * holder's ack, from a second VFS thread, is what resumes it -- on
         * the submitting thread, with the GETATTR behind it run there. */
        memset(&owner_s, 0, sizeof(owner_s));
        owner_s.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_s.client_key = 0x51;
        owner_s.owner_lo   = 2;
        chimera_vfs_claim_init_oplock(&oplock,
                                      CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                      CHIMERA_CLAIM_H,
                                      &owner_s);
        memset(&rec, 0, sizeof(rec));
        oplock.break_cb   = break_rec_cb;
        oplock.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs, &oplock, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, oh->fh, (int) oh->fh_len);
        i_rc = chimera_vfs_compound_add_recall(cp, NULL, 0,
                                               CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                                               0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);

        /* Parked: the break went out, and submit returned with nothing to
         * report. */
        assert(ctx.callbacks == 0);
        assert(!ctx.done);
        assert(rec.fired == 1);
        assert(rec.needed == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW));
        assert(oplock.break_state == CHIMERA_CLAIM_BREAK_BREAKING);

        /* The holder acks down to RW, from elsewhere. */
        ra.vfs       = ctx.vfs;
        ra.claim     = &oplock;
        ra.resulting = CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW;
        assert(pthread_create(&tid, NULL, remote_ack_main, &ra) == 0);

        wait_done(&ctx);
        assert(pthread_join(tid, NULL) == 0);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rc);
        assert(op->status == CHIMERA_VFS_OK);
        /* No share holder kept the file open: the oplock is a cache claim. */
        assert(op->recall_still_open == 0);
        op = chimera_vfs_compound_op(cp, i_ga);
        assert(op->status == CHIMERA_VFS_OK);
        assert(S_ISREG(op->attr.va_mode));
        /* On the submitting thread, not the acking one. */
        assert(pthread_equal(ctx.cb_thread, self));
        assert(!pthread_equal(ctx.cb_thread, ra.self));
        /* The holder kept what the floor left it. */
        assert(oplock.used == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW));
        chimera_vfs_compound_free(cp);

        /* Already at the floor: nothing to break, nothing to wait for. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, oh->fh, (int) oh->fh_len);
        i_rc = chimera_vfs_compound_add_recall(cp, NULL, 0,
                                               CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                                               0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        assert(ctx.callbacks == 1);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(rec.fired == 1);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release(state, fs, &oplock);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("RECALL answers at once with nothing held, NOWAIT reports a holder "
              "without parking, and the parking form resumes on the submitting thread");

    /* ---- CLAIM takes any claim kind: shares, and the abort release ----
     * A share reservation is a claim like a range: GRANTED it is inserted
     * and the caller's, DENIED it stops the run and names the holder, and a
     * run that fails after it releases it the way its consumer would
     * (chimera_vfs_claim_release), so the next opener finds the file free. */
    {
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           share_a, share_b;
        struct chimera_vfs_pending_acquire ticket_a, ticket_b;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_file_state     *fs;
        int                                i_open, i_cl, i_ga, i_lk;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "sh", 2,
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
        owner_a.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_a.client_key = 0xA1;
        owner_a.owner_lo   = 31;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.client_key = 0xA2;
        owner_b.owner_lo   = 32;

        /* A opens for read+write, denying write to others. */
        chimera_vfs_claim_init_nfs4_open(&share_a,
                                         CHIMERA_CLAIM_R | CHIMERA_CLAIM_W,
                                         CHIMERA_CLAIM_W, &owner_a);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &share_a, &ticket_a,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        assert(op->claim_grant == NULL);
        /* A share claim is never stamped: the handle is a shared cache
         * entry, and stamping would fold every open-owner using it into
         * one holder. */
        assert(share_a.op_handle == NULL);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        /* B wants write: refused, and the run stops there. */
        chimera_vfs_claim_init_nfs4_open(&share_b, CHIMERA_CLAIM_W, 0, &owner_b);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &share_b, &ticket_b,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EAGAIN);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->status == CHIMERA_VFS_EAGAIN);
        assert(op->claim_result == CHIMERA_CLAIM_DENIED);
        assert(op->conflict.construct == CHIMERA_CONSTRUCT_NFS4_OPEN);
        assert(op->conflict.owner.owner_lo == owner_a.owner_lo);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl) == NULL);
        chimera_vfs_compound_free(cp);

        /* A's share is the caller's; released out of band as its consumer
         * does. */
        chimera_vfs_claim_release(ctx.vfs->vfs_state, fs, &share_a);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs);

        /* The abort release for a share: A GRANTED, then the LOOKUP behind it
         * fails (the lent handle's object is a regular file: ENOTDIR), and the
         * share goes with the failure -- so B's write open, refused a moment
         * ago, is GRANTED. */
        chimera_vfs_claim_init_nfs4_open(&share_a,
                                         CHIMERA_CLAIM_R | CHIMERA_CLAIM_W,
                                         CHIMERA_CLAIM_W, &owner_a);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &share_a, &ticket_a,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        i_lk          = chimera_vfs_compound_add_lookup(cp, "x", 1, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOTDIR);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl) == NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_init_nfs4_open(&share_b, CHIMERA_CLAIM_W, 0, &owner_b);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &share_b, &ticket_b,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_cl)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release(ctx.vfs->vfs_state, fs, &share_b);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs);

        /* Malformed flag words are refused at build. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        assert(chimera_vfs_compound_add_claim(cp, &share_a, &ticket_a,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY |
                                              CHIMERA_VFS_COMPOUND_CLAIM_WAIT,
                                              0, 0, 0, 0) == -1);
        assert(chimera_vfs_compound_add_claim(cp, &share_a, &ticket_a,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND,
                                              0, 0, 0, 0) == -1);
        assert(chimera_vfs_compound_add_claim_test(cp, &share_a,
                                                   CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL)
               == -1);
        chimera_vfs_compound_free(cp);

        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("CLAIM takes a share, refuses a conflicting one, and the abort "
              "release frees it");

    /* ---- OPTIONAL: a refused claim is an answer, not a failure ----
     * A delegation is opportunistic: NONE is a valid outcome, and a run that
     * grants a share and then fails to earn a delegation must not throw the
     * share away.  A delegation is also the single-holder cache the executor
     * stamps op_handle on, and one the abort release covers. */
    {
        struct chimera_vfs_state          *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           deleg, peer;
        struct chimera_vfs_pending_acquire ticket;
        struct chimera_claim_owner         owner_d, owner_x;
        struct chimera_vfs_claim_conflict  conflict;
        struct chimera_vfs_file_state     *fs, *fs_peer;
        int                                i_open, i_cl, i_ga, i_lk;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "dg", 2,
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

        memset(&owner_d, 0, sizeof(owner_d));
        owner_d.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_d.client_key = 0xD2;
        owner_d.owner_lo   = oh->fh_hash;
        memset(&owner_x, 0, sizeof(owner_x));
        owner_x.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_x.client_key = 0xD3;
        owner_x.owner_lo   = 41;

        /* Another client's open denies everyone else read and write, which
         * a delegation's data bits collide with -- a hard refusal. */
        fs_peer = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                        oh->fh_hash, true);
        assert(fs_peer != NULL);
        chimera_vfs_claim_init_nfs4_open(&peer, CHIMERA_CLAIM_R,
                                         CHIMERA_CLAIM_R | CHIMERA_CLAIM_W,
                                         &owner_x);
        assert(chimera_vfs_claim_try_acquire(state, fs_peer, &peer, &conflict)
               == CHIMERA_CLAIM_GRANTED);

        chimera_vfs_claim_init_delegation(&deleg, true, &owner_d);
        deleg.break_cb = break_rec_cb;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &deleg, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY |
                                              CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL,
                                              0, 0, 0, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        /* The run finished OK and went on past the refusal... */
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        /* ...with the refusal recorded on the op. */
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_DENIED);
        assert(op->conflict.construct == CHIMERA_CONSTRUCT_NFS4_OPEN);
        assert(op->conflict.owner.owner_lo == owner_x.owner_lo);
        /* Nothing inserted, nothing owned. */
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl) == NULL);
        assert(deleg.file == NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release(state, fs_peer, &peer);

        /* Clear now: GRANTED, stamped with the handle it ran against, and
         * then released by the abort when the LOOKUP behind it fails. */
        chimera_vfs_claim_init_delegation(&deleg, true, &owner_d);
        deleg.break_cb = break_rec_cb;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &deleg, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        i_lk          = chimera_vfs_compound_add_lookup(cp, "x", 1, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOTDIR);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        assert(deleg.op_handle == oh);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl) == NULL);
        assert(deleg.file == NULL);
        chimera_vfs_compound_free(cp);

        /* Proof it was released: the peer's deny-everything open is GRANTED
         * again, which a standing write delegation would have refused. */
        chimera_vfs_claim_init_nfs4_open(&peer, CHIMERA_CLAIM_R,
                                         CHIMERA_CLAIM_R | CHIMERA_CLAIM_W,
                                         &owner_x);
        assert(chimera_vfs_claim_try_acquire(state, fs_peer, &peer, &conflict)
               == CHIMERA_CLAIM_GRANTED);
        chimera_vfs_claim_release(state, fs_peer, &peer);

        /* And a run that finishes OK hands it over. */
        chimera_vfs_claim_init_delegation(&deleg, true, &owner_d);
        deleg.break_cb = break_rec_cb;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &deleg, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY |
                                              CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL,
                                              0, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_cl)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_release(state, fs, &deleg);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_state_put(state, fs_peer);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("an OPTIONAL CLAIM that is refused completes OK and the run goes on; "
              "a delegation is stamped, released by the abort, or handed over");

    /* ---- coalition grants settle in the core ----
     * An RqLs lease or oplock is not inserted as the caller's claim: the
     * claim is a TEMPLATE, and what stands is a core-allocated grant that N
     * opens share.  One CLAIM does what SMB's create path did by hand:
     * coalesce, cap to what breaks nobody, acquire, settle. */
    {
        struct chimera_vfs_state          *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           tmpl, tmpl2, oplock_ii, legacy;
        struct chimera_vfs_pending_acquire ticket, ticket2;
        struct chimera_claim_owner         owner_l, owner_p, owner_o;
        struct chimera_vfs_claim_conflict  conflict;
        struct chimera_vfs_claim_grant    *grant, *grant2;
        struct chimera_vfs_file_state     *fs, *fs2, *fs_peer;
        struct break_rec                   rec, rec_peer;
        int                                seed_a, seed_b;
        int                                i_open, i_cl, i_cl2, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "gs", 2,
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

        /* An RqLs owner: the lease key is the identity, its halves in lo/hi
         * as SMB stamps them. */
        memset(&owner_l, 0, sizeof(owner_l));
        owner_l.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_l.client_key = 0x51;
        owner_l.key[0]     = 0xEE;
        owner_l.key[15]    = 0x01;
        memcpy(&owner_l.owner_lo, owner_l.key, 8);
        memcpy(&owner_l.owner_hi, owner_l.key + 8, 8);
        /* A peer client's legacy oplock, and a legacy oplock of the lease
         * holder's own client (a file id, no key). */
        memset(&owner_p, 0, sizeof(owner_p));
        owner_p.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_p.client_key = 0x52;
        owner_p.owner_lo   = 7;
        memset(&owner_o, 0, sizeof(owner_o));
        owner_o.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_o.client_key = 0x51;
        owner_o.owner_lo   = 8;

        /* A clean file grants the full RWH, seeded with the member. */
        chimera_vfs_claim_init_rqls(&tmpl,
                                    CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                    CHIMERA_CLAIM_H, &owner_l);
        memset(&rec, 0, sizeof(rec));
        tmpl.break_cb   = break_rec_cb;
        tmpl.cb_private = &rec;
        tmpl.policy_tag = 77;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &tmpl, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY |
                                              CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL,
                                              0, 0, 0, 0);
        chimera_vfs_compound_op_set_claim_grant_opts(cp, (uint32_t) i_cl,
                                                     1 /* v2 */, 0, &seed_a);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        /* Always answered inside the call. */
        assert(ctx.callbacks == 1);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        grant = op->claim_grant;
        assert(grant != NULL);
        assert(grant->claim.used == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                     CHIMERA_CLAIM_H));
        assert(grant->claim.construct == CHIMERA_CONSTRUCT_RQLS);
        assert(grant->is_v2 == 1);
        assert(grant->refcount == 1);
        /* The seed became the fresh grant's member head, and the template's
         * shape travelled: the break callback, the tag, and the handle the
         * op stamped for it. */
        assert(op->claim_member_seeded == 1);
        assert(grant->members == &seed_a);
        assert(grant->claim.break_cb == break_rec_cb);
        assert(grant->claim.policy_tag == 77);
        assert(grant->claim.op_handle == oh);
        assert(tmpl.op_handle == oh);
        /* The template itself was never inserted. */
        assert(tmpl.file == NULL);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        /* A second open under the same lease key coalesces: the same grant,
         * one more reference, and the seed is NOT consumed -- the caller
         * registers its member on the grant it got back, as before. */
        chimera_vfs_claim_init_rqls(&tmpl2, CHIMERA_CLAIM_CR, &owner_l);
        tmpl2.break_cb   = break_rec_cb;
        tmpl2.cb_private = &rec;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl2 = chimera_vfs_compound_add_claim(cp, &tmpl2, &ticket2,
                                               CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                               0, 0, 0, 0);
        chimera_vfs_compound_op_set_claim_grant_opts(cp, (uint32_t) i_cl2,
                                                     1, 0, &seed_b);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl2);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        grant2 = op->claim_grant;
        assert(grant2 == grant);
        assert(grant->refcount == 2);
        assert(op->claim_member_seeded == 0);
        assert(grant->members == &seed_a);
        /* Fewer bits requested keeps the lease at its state (3.3.5.9.8). */
        assert(grant->claim.used == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                     CHIMERA_CLAIM_H));
        fs2 = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl2);
        assert(fs2 != NULL);
        chimera_vfs_compound_free(cp);

        /* A legacy oplock request by the lease holder's own client, behind
        * its own H lease, asks for nothing: the policy the core now reads
        * from claim state.  DENIED with a ZERO conflict -- capped to
        * nothing, no holder to name -- and OPTIONAL carries the run on. */
        chimera_vfs_claim_init_oplock(&legacy,
                                      CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                      CHIMERA_CLAIM_H, &owner_o);
        legacy.break_cb = break_rec_cb;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &legacy, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY |
                                              CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL,
                                              0, 0, 0, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result == CHIMERA_CLAIM_DENIED);
        assert(op->conflict.construct == CHIMERA_CONSTRUCT_NONE);
        assert(op->claim_grant == NULL);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl) == NULL);
        /* Nobody was broken for it. */
        assert(rec.fired == 0);
        chimera_vfs_compound_free(cp);

        /* Both references dropped: the coalition is gone. */
        chimera_vfs_claim_grant_release(state, grant2, true);
        chimera_vfs_state_put(state, fs2);
        chimera_vfs_claim_grant_release(state, grant, true);
        chimera_vfs_state_put(state, fs);

        /* Behind a peer's shared read cache (LEVEL_II), a full RWH request
         * caps to what coexists -- R|H -- and breaks nobody: the step is
         * reported as the grant's mode, not as a refusal. */
        fs_peer = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                        oh->fh_hash, true);
        assert(fs_peer != NULL);
        chimera_vfs_claim_init_oplock(&oplock_ii, CHIMERA_CLAIM_CR, &owner_p);
        memset(&rec_peer, 0, sizeof(rec_peer));
        oplock_ii.break_cb   = break_rec_cb;
        oplock_ii.cb_private = &rec_peer;
        assert(chimera_vfs_claim_try_acquire(state, fs_peer, &oplock_ii,
                                             &conflict) == CHIMERA_CLAIM_GRANTED);

        chimera_vfs_claim_init_rqls(&tmpl,
                                    CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                    CHIMERA_CLAIM_H, &owner_l);
        tmpl.break_cb   = break_rec_cb;
        tmpl.cb_private = &rec;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &tmpl, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY |
                                              CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL,
                                              0, 0, 0, 0);
        chimera_vfs_compound_op_set_claim_grant_opts(cp, (uint32_t) i_cl,
                                                     0, 0, &seed_a);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        grant = op->claim_grant;
        assert(grant != NULL);
        assert(grant->claim.used == (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H));
        assert(rec_peer.fired == 0);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        /* The abort release for a grant: a fresh coalition granted in a run
         * that then fails is dropped, and the file is left as it was found. */
        chimera_vfs_claim_grant_release(state, grant, true);
        chimera_vfs_state_put(state, fs);

        chimera_vfs_claim_init_rqls(&tmpl, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H,
                                    &owner_l);
        tmpl.break_cb   = break_rec_cb;
        tmpl.cb_private = &rec;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &tmpl, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        chimera_vfs_compound_add_lookup(cp, "x", 1, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->claim_result == CHIMERA_CLAIM_GRANTED);
        assert(op->claim_grant == NULL);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl) == NULL);
        chimera_vfs_compound_free(cp);
        /* Only the peer's oplock is left on the file. */
        assert(fs_peer->grants == NULL);
        assert(fs_peer->claims[CHIMERA_CLAIM_CLASS_CACHE] == &oplock_ii);
        assert(oplock_ii.next == NULL);

        chimera_vfs_claim_release(state, fs_peer, &oplock_ii);
        chimera_vfs_state_put(state, fs_peer);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("a coalition grant settles in the core: full on a clean file, "
              "coalesced on re-open, capped behind a peer, none behind its own "
              "lease, and dropped by the abort");

    /* ---- the trigger words ----
     * `pre` breaks before admission is asked (SMB's phase-1 handle break),
     * `post` after the grant (the phase-2 write-cache break), `deny` on a
     * refusal answered on the spot.  Observed through another client's
     * caching claim, which the test holds directly. */
    {
        struct chimera_vfs_state          *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh;
        struct chimera_vfs_claim           batch, ex, peer_share, share;
        struct chimera_vfs_pending_acquire ticket;
        struct chimera_claim_owner         owner_h, owner_s;
        struct chimera_vfs_claim_conflict  conflict;
        struct chimera_vfs_file_state     *fs, *fs_h;
        struct break_rec                   rec;
        int                                i_open, i_cl;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "tr", 2,
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

        memset(&owner_h, 0, sizeof(owner_h));
        owner_h.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_h.client_key = 0x61;
        owner_h.owner_lo   = 1;
        memset(&owner_s, 0, sizeof(owner_s));
        owner_s.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_s.client_key = 0x62;
        owner_s.owner_lo   = 2;

        fs_h = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                     oh->fh_hash, true);
        assert(fs_h != NULL);

        /* pre: a batch oplock holder loses its handle cache before the
         * opener's share is even asked for; the share itself is GRANTED. */
        chimera_vfs_claim_init_oplock(&batch,
                                      CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                      CHIMERA_CLAIM_H, &owner_h);
        memset(&rec, 0, sizeof(rec));
        batch.break_cb   = break_rec_cb;
        batch.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs_h, &batch, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        chimera_vfs_claim_init_smb_open(&share, CHIMERA_CLAIM_R, 0, &owner_s);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &share, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              CHIMERA_TRIGGER_OPEN_H,
                                              CHIMERA_CLAIM_CR, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_cl)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        assert(rec.fired == 1);
        assert(batch.break_state == CHIMERA_CLAIM_BREAK_BREAKING);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_ack(&batch, CHIMERA_CLAIM_CR);
        chimera_vfs_claim_release(state, fs, &share);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_claim_release(state, fs_h, &batch);

        /* post: an exclusive holder's write cache is broken once the
         * opener's share is granted, not before. */
        chimera_vfs_claim_init_oplock(&ex, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                                      &owner_h);
        memset(&rec, 0, sizeof(rec));
        ex.break_cb   = break_rec_cb;
        ex.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs_h, &ex, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        chimera_vfs_claim_init_smb_open(&share, CHIMERA_CLAIM_R, 0, &owner_s);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &share, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        chimera_vfs_compound_op_set_claim_post(cp, (uint32_t) i_cl,
                                               CHIMERA_TRIGGER_OPEN_W,
                                               CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_cl)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        assert(rec.fired == 1);
        assert(ex.break_state == CHIMERA_CLAIM_BREAK_BREAKING);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_ack(&ex, CHIMERA_CLAIM_CR);
        chimera_vfs_claim_release(state, fs, &share);
        chimera_vfs_state_put(state, fs);
        chimera_vfs_claim_release(state, fs_h, &ex);

        /* deny: the holder's open denies write; the opener's write share is
         * refused on the spot, and the forced handle break strips the
         * holder's H (break_twice).  Not OPTIONAL, so the run stops. */
        chimera_vfs_claim_init_smb_open(&peer_share, CHIMERA_CLAIM_R,
                                        CHIMERA_CLAIM_W, &owner_h);
        assert(chimera_vfs_claim_try_acquire(state, fs_h, &peer_share,
                                             &conflict) == CHIMERA_CLAIM_GRANTED);
        chimera_vfs_claim_init_oplock(&batch,
                                      CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW |
                                      CHIMERA_CLAIM_H, &owner_h);
        memset(&rec, 0, sizeof(rec));
        batch.break_cb   = break_rec_cb;
        batch.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs_h, &batch, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        chimera_vfs_claim_init_smb_open(&share, CHIMERA_CLAIM_W, 0, &owner_s);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &share, &ticket,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0,
                                              CHIMERA_TRIGGER_OPEN_H_FORCE,
                                              CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EAGAIN);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->claim_result == CHIMERA_CLAIM_DENIED);
        assert(op->conflict.construct == CHIMERA_CONSTRUCT_SMB_OPEN);
        assert(rec.fired == 1);
        assert(batch.break_state == CHIMERA_CLAIM_BREAK_BREAKING);
        assert(chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl) == NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_ack(&batch, CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW);
        chimera_vfs_claim_release(state, fs_h, &batch);
        chimera_vfs_claim_release(state, fs_h, &peer_share);
        chimera_vfs_state_put(state, fs_h);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("pre fires before admission, post after the grant, deny on a "
              "synchronous refusal");

    /* ---- an io_owner the run's own gate completes ----
     * A write cache is held by an owner whose owner_lo is the object's
     * fh_hash, which is how a protocol keying the owner on an open handle
     * names itself (NFSv4: the stateid's handle).  Its own write must not
     * recall it.
     *
     * The adders' io_owner is a value the caller already has, and when the
     * OPEN is IN the run there is no such value: the handle does not exist
     * when the sequence is written.  But io_owner and have_io_owner are
     * ARGUMENTS, so the gate consulted on the OPEN -- the first moment the
     * object exists -- writes them into the WRITE ahead of it.  No second
     * mechanism: this is chimera_vfs_compound_op_edit doing what it does for
     * every other argument a run computes for itself.
     */
    {
        struct chimera_vfs_state         *state = ctx.vfs->vfs_state;
        struct chimera_vfs_attrs          sattr;
        struct chimera_vfs_open_handle   *oh;
        struct chimera_vfs_claim          cache;
        struct chimera_claim_owner        owner_w;
        struct io_owner_gate_ctx          g;
        struct chimera_claim_actor        stranger;
        struct chimera_vfs_claim_conflict conflict;
        struct chimera_vfs_file_state    *fs;
        struct break_rec                  rec;
        struct evpl_iovec                 wiov;
        int                               i_open, i_wr;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "ioown", 5,
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

        fs = chimera_vfs_state_get(state, oh->fh, (uint8_t) oh->fh_len,
                                   oh->fh_hash, true);
        assert(fs != NULL);

        /* The holder: owner_lo is the object's fh_hash, exactly as
         * nfs4_vfs_io_authorize builds it from the stateid's handle. */
        memset(&owner_w, 0, sizeof(owner_w));
        owner_w.proto      = CHIMERA_CLAIM_PROTO_SMB2;
        owner_w.client_key = 0x10;
        owner_w.owner_lo   = oh->fh_hash;

        assert(evpl_iovec_alloc(ctx.evpl, 8, 0, 1, 0, &wiov) == 1);
        memcpy(evpl_iovec_data(&wiov), "bythegat", 8);

        /* The half a caller CAN write at build time: its proto and its client,
         * with owner_lo left for the gate. */
        memset(&g, 0, sizeof(g));
        g.base.owner          = owner_w;
        g.base.owner.owner_lo = 0;

        chimera_vfs_claim_init_oplock(&cache,
                                      CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                                      &owner_w);
        memset(&rec, 0, sizeof(rec));
        cache.break_cb   = break_rec_cb;
        cache.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs, &cache, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "ioown", 5,
                                               CHIMERA_VFS_OPEN_READ_ONLY |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, NULL, 0, 0, 0);
        i_wr = chimera_vfs_compound_add_write(cp, NULL, 0, 8, 0, &wiov, 1,
                                              0, 0, NULL);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_wr,
                                           (uint32_t) i_open);
        g.at       = (uint32_t) i_open;
        g.io_index = (uint32_t) i_wr;
        chimera_vfs_compound_set_gate(cp, io_owner_gate, &g);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_wr)->status == CHIMERA_VFS_OK);
        assert(g.filled == 1);

        assert(rec.fired == 0);
        assert(cache.break_state == CHIMERA_CLAIM_BREAK_IDLE);

        /* Submitted again, the gate computes the same owner rather than one
         * built on what it wrote last time -- so the second run recalls no
         * more than the first. */
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(g.filled == 2);
        assert(rec.fired == 0);
        assert(cache.break_state == CHIMERA_CLAIM_BREAK_IDLE);
        chimera_vfs_compound_free(cp);

        /* The same run with the owner the caller COULD name unaided -- the
         * half it knows, owner_lo unset because there was nothing to put there
         * -- is a stranger, and the holder loses its write cache. */
        stranger = g.base;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "ioown", 5,
                                               CHIMERA_VFS_OPEN_READ_ONLY |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, NULL, 0, 0, 0);
        i_wr = chimera_vfs_compound_add_write(cp, NULL, 0, 8, 0, &wiov, 1,
                                              0, 0, &stranger);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_wr,
                                           (uint32_t) i_open);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_wr)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        assert(rec.fired == 1);
        assert(cache.break_state == CHIMERA_CLAIM_BREAK_BREAKING);

        chimera_vfs_claim_ack(&cache, CHIMERA_CLAIM_CR);
        chimera_vfs_claim_release(state, fs, &cache);

        /* And a caller that already KNOWS the file handle needs no gate at
         * all: owner_lo is chimera_vfs_hash of it, computable at build.  This
         * is the delegation-stateid shape, where the object is the one the run
         * starts from. */
        chimera_vfs_claim_init_oplock(&cache,
                                      CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                                      &owner_w);
        memset(&rec, 0, sizeof(rec));
        cache.break_cb   = break_rec_cb;
        cache.cb_private = &rec;
        assert(chimera_vfs_claim_try_acquire(state, fs, &cache, &conflict) ==
               CHIMERA_CLAIM_GRANTED);

        stranger                = g.base;
        stranger.owner.owner_lo = chimera_vfs_hash(oh->fh, (int) oh->fh_len);
        assert(stranger.owner.owner_lo == oh->fh_hash);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_wr = chimera_vfs_compound_add_write(cp, NULL, 0, 8, 0, &wiov, 1,
                                              0, 0, &stranger);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_wr)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        assert(rec.fired == 0);
        assert(cache.break_state == CHIMERA_CLAIM_BREAK_IDLE);

        chimera_vfs_claim_release(state, fs, &cache);
        chimera_vfs_state_put(state, fs);
        evpl_iovec_release(ctx.evpl, &wiov);
        chimera_vfs_release(ctx.vfs_thread, oh);
    }
    TEST_PASS("a gate completes a later WRITE's io_owner from the OPEN that "
              "produced the handle, so a run's own I/O keeps its own cache");

    /* ---- CLAIM_TEST with TEST_BACKEND, and the PATH open ----
     * memfs arbitrates no byte ranges, so the projection has nowhere to go
     * and the local answer stands, answered inside submit.  A probe uses
     * only the fh, so it wants a PATH open: through a FIFO's fh it must not
     * open the FIFO for data (which blocks on a real backend), and a lent
     * PATH handle serves it where a data want would be refused. */
    {
        struct chimera_vfs_attrs           sattr;
        struct chimera_vfs_open_handle    *oh, *oh_path;
        struct chimera_vfs_claim           probe, claim_a;
        struct chimera_vfs_pending_acquire ticket_a;
        struct chimera_claim_owner         owner_a, owner_b;
        struct chimera_vfs_file_state     *fs;
        uint8_t                            fifo_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                           fifo_fh_len;
        int                                i_open, i_cn, i_probe, i_cl, i_ga;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "tb", 2,
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
        owner_a.owner_lo = 51;
        memset(&owner_b, 0, sizeof(owner_b));
        owner_b.proto    = CHIMERA_CLAIM_PROTO_NFSV4;
        owner_b.owner_lo = 52;

        chimera_vfs_claim_init_range(&probe, true, false, 0, 16, &owner_b);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_probe = chimera_vfs_compound_add_claim_test(
            cp, &probe, CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        assert(ctx.callbacks == 1);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_probe)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        /* A probe is never stamped. */
        assert(probe.op_handle == NULL);
        chimera_vfs_compound_free(cp);

        /* A local holder is found before any projection is considered. */
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_cl = chimera_vfs_compound_add_claim(cp, &claim_a, &ticket_a,
                                              CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                              0, 0, 0, 0);
        i_probe = chimera_vfs_compound_add_claim_test(
            cp, &probe, CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_probe);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->claim_result != CHIMERA_CLAIM_GRANTED);
        assert(op->conflict.owner.owner_lo == owner_a.owner_lo);
        /* A range claim is never stamped either. */
        assert(claim_a.op_handle == NULL);
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);
        chimera_vfs_claim_release_ranged(ctx.vfs_thread, ctx.vfs->vfs_state,
                                         fs, &claim_a);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs);
        chimera_vfs_release(ctx.vfs_thread, oh);

        /* A FIFO, probed and claimed through its fh: the executor's own open
         * of it is a PATH open. */
        sattr.va_mode = S_IFIFO | 0600;
        cp            = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_cn = chimera_vfs_compound_add_create_path(
            cp, CHIMERA_VFS_COMPOUND_CREATE_NODE, "cfifo", 5, NULL, 0, &sattr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cn);
        assert(S_ISFIFO(op->attr.va_mode));
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        memcpy(fifo_fh, op->attr.va_fh, op->attr.va_fh_len);
        fifo_fh_len = op->attr.va_fh_len;
        chimera_vfs_compound_free(cp);

        chimera_vfs_claim_init_range(&probe, true, false, 0, 16, &owner_b);
        chimera_vfs_claim_init_range(&claim_a, true, false, 0, 16, &owner_a);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, fifo_fh, (int) fifo_fh_len);
        i_probe = chimera_vfs_compound_add_claim_test(cp, &probe, 0);
        i_cl    = chimera_vfs_compound_add_claim(cp, &claim_a, &ticket_a,
                                                 CHIMERA_VFS_COMPOUND_CLAIM_TRY,
                                                 0, 0, 0, 0);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_probe)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        assert(chimera_vfs_compound_op(cp, i_cl)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        assert(S_ISFIFO(chimera_vfs_compound_op(cp, i_ga)->attr.va_mode));
        fs = chimera_vfs_compound_take_file_state(cp, (uint32_t) i_cl);
        assert(fs != NULL);
        chimera_vfs_compound_free(cp);
        chimera_vfs_claim_release_ranged(ctx.vfs_thread, ctx.vfs->vfs_state,
                                         fs, &claim_a);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs);

        /* A lent PATH handle serves the probe -- a data want through it
         * would be EINVAL (the lent-handle rule), so this is what says the
         * want is PATH. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "cfifo", 5,
                                               CHIMERA_VFS_OPEN_PATH,
                                               0, NULL, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh_path = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh_path != NULL);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh_path, CHIMERA_VFS_OPEN_PATH);
        i_probe       = chimera_vfs_compound_add_claim_test(cp, &probe, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_probe)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_probe)->claim_result ==
               CHIMERA_CLAIM_GRANTED);
        chimera_vfs_compound_free(cp);

        chimera_vfs_release(ctx.vfs_thread, oh_path);
    }
    TEST_PASS("CLAIM_TEST with TEST_BACKEND falls back to the local answer on "
              "memfs, and a claim through a FIFO takes a PATH open");

    /* ---- the gate edits the ops that have not run ----
     * The SMB2 SET_INFO ALLOCATION shape: the size a SETATTR applies is a
     * function of the size the GETATTR in front of it just read, which the
     * caller cannot know when it builds the sequence.  The gate writes it, and
     * writes it again on a second execution -- from the caller's own argument
     * plus what the run observed, so the sequence is the same sequence both
     * times.  A skip is the same kind of edit: the op is run PAST, not run.
     * What the gate may NOT reach is anything at or below the op it is
     * consulted on. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *oh;
        struct edit_gate_ctx            g;
        int                             i_open, i_ga, i_sa, i_ga2;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "gedit", 5,
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

        /* Ten bytes of file, so the rounding has something to round. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        sattr.va_size     = 10;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        chimera_vfs_compound_add_setattr(cp, NULL, &sattr, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* The caller's own argument: a size of 0, which the gate replaces. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        sattr.va_size     = 0;

        memset(&g, 0, sizeof(g));
        g.at         = 1;
        g.size_index = 2;
        g.skip_index = EDIT_GATE_NONE;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_set_gate(cp, edit_gate, &g);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_ga  = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_SIZE);
        i_sa  = chimera_vfs_compound_add_setattr(cp, NULL, &sattr, 0, 0);
        i_ga2 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_SIZE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga)->attr.va_size == 10);
        /* The gate was refused everything it may not edit, and the SETATTR it
         * may edit applied the size it wrote. */
        assert(g.refused_self && g.refused_below && g.refused_past_end);
        assert(g.wrote == 64);
        assert(chimera_vfs_compound_op(cp, i_sa)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga2)->attr.va_size == 64);

        /* The SAME sequence again.  The gate is consulted again and re-applies
         * its edit -- computed from what the run reports, not added to what it
         * wrote last time -- so the second execution is the first one's
         * sequence, and the file ends where it ended before. */
        g.calls       = 0;
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(g.calls == 4);
        assert(g.wrote == 64);
        assert(chimera_vfs_compound_op(cp, i_ga)->attr.va_size == 64);
        assert(chimera_vfs_compound_op(cp, i_ga2)->attr.va_size == 64);
        chimera_vfs_compound_free(cp);

        /* A SKIPPED op: the same shape, but the gate runs the sequence past
         * the SETATTR instead of editing it.  Nothing is dispatched for it,
         * its status stays UNSET, and the file is untouched -- while the op
         * BEHIND it still runs. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        sattr.va_size     = 0;

        memset(&g, 0, sizeof(g));
        g.at         = 1;
        g.size_index = EDIT_GATE_NONE;
        g.skip_index = 2;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_set_gate(cp, edit_gate, &g);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_ga  = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_SIZE);
        i_sa  = chimera_vfs_compound_add_setattr(cp, NULL, &sattr, 0, 0);
        i_ga2 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_SIZE);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_sa)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_op(cp, i_ga2)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga2)->attr.va_size == 64);
        /* The skipped op is not an op the gate is consulted about. */
        assert(g.calls == 3);

        /* Again: the skip does not persist, it is re-decided -- and decided
         * the same way, so the SETATTR is skipped again and the file is still
         * 64 bytes. */
        g.calls       = 0;
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(g.calls == 3);
        assert(chimera_vfs_compound_op(cp, i_sa)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_op(cp, i_ga2)->attr.va_size == 64);
        chimera_vfs_compound_free(cp);

        /* ...and a skip whose op is the LAST one: the sequence finishes
         * having run one fewer op than it holds. */
        memset(&g, 0, sizeof(g));
        g.at         = 0;
        g.size_index = EDIT_GATE_NONE;
        g.skip_index = 1;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_set_gate(cp, edit_gate, &g);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_READ_ONLY |
                                           CHIMERA_VFS_OPEN_WRITE_ONLY);
        i_ga          = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_SIZE);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_num_ops(cp) == 2);
        assert(chimera_vfs_compound_num_completed(cp) == 1);
        assert(chimera_vfs_compound_op(cp, i_ga)->status == CHIMERA_VFS_UNSET);
        chimera_vfs_compound_free(cp);

        chimera_vfs_release(ctx.vfs_thread, oh);

        /* A skipped handle producer is rejected with EINVAL before a
         * dependent operation can dispatch with a NULL handle. */
        {
            char cmd[4096];
            int  rc;

            snprintf(cmd, sizeof(cmd),
                     "exec '%s' --reject-skip-handle-from >/dev/null 2>&1",
                     argv[0]);
            rc = system(cmd);
            assert(rc == 0);
        }
    }
    TEST_PASS("a gate rewrites and skips the ops ahead of it, is refused the "
              "ones behind it, and re-applies both on a second execution");

    /* ---- the caller's own skip, decided at build ----
     * The same effect as a gate's, and the opposite lifetime: a gate's is
     * cleared by every submit so it is decided afresh per execution, and this
     * one is an argument of the sequence as built, so a compound submitted
     * twice runs the same shape both times.  They are ORed, and neither clears
     * the other. */
    {
        struct chimera_vfs_attrs sattr;
        struct edit_gate_ctx     g;
        int                      i_ga1, i_ga2, i_ga3, i_open, i_uh;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        /* The middle op is skipped before the run starts: it never dispatches,
         * its status stays UNSET, and it is not counted. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_ga1 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_ga2 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_ga3 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_set_skip(cp, (uint32_t) i_ga2, 1);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga1)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga2)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_op(cp, i_ga3)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_num_completed(cp) == 3);

        /* Submitted again WITHOUT rebuilding: submit clears the gate's skip
         * and not this one, so the second run is the same run. */
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga2)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_op(cp, i_ga3)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* Both skips at once, on different ops: each is run past, and a gate
         * skipping an op the caller already skipped changes nothing. */
        memset(&g, 0, sizeof(g));
        g.at         = 1;
        g.size_index = EDIT_GATE_NONE;
        g.skip_index = 2;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_ga1 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_ga2 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        i_ga3 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        assert(i_ga1 == 1 && i_ga2 == 2 && i_ga3 == 3);
        chimera_vfs_compound_op_set_skip(cp, (uint32_t) i_ga3, 1);
        chimera_vfs_compound_set_gate(cp, edit_gate, &g);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga1)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_ga2)->status == CHIMERA_VFS_UNSET);
        assert(chimera_vfs_compound_op(cp, i_ga3)->status == CHIMERA_VFS_UNSET);
        /* The last op skipped: the count reports fewer than the sequence has. */
        assert(chimera_vfs_compound_num_completed(cp) == 2);
        chimera_vfs_compound_free(cp);

        /* Clearing it puts the op back. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_ga1 = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_set_skip(cp, (uint32_t) i_ga1, 1);
        chimera_vfs_compound_op_set_skip(cp, (uint32_t) i_ga1, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_ga1)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* An op whose handle a later op addresses cannot be skipped -- it
         * would produce none.  Refused in either order. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "bskip", 5,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        i_uh = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_uh,
                                           (uint32_t) i_open);
        chimera_vfs_compound_op_set_skip(cp, (uint32_t) i_open, 1);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        assert(chimera_vfs_compound_num_completed(cp) == 0);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "bskip", 5,
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, NULL, 0, 0, 0);
        i_uh = chimera_vfs_compound_add_getattr(cp, CHIMERA_VFS_ATTR_MASK_STAT);
        chimera_vfs_compound_op_set_skip(cp, (uint32_t) i_open, 1);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_uh,
                                           (uint32_t) i_open);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_EINVAL);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("a build-time skip runs past an op, survives resubmission, "
              "ORs with a gate's, and is refused on a use_handle source");

    /* ---- CLOSE performs the delete-on-close unlink ----
     * Arming the flag is out of band; the unlink it eventually causes is the
     * CLOSE's, because it addresses an object and has to happen before the
     * backend handle the release detached is closed. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *oh;
        struct chimera_vfs_file_state  *fs;
        uint8_t                         d_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        d_fh_len;
        uint8_t                         nd_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        nd_fh_len;
        int                             i_open, i_cl, i_lk;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        /* A file, armed for delete-on-close, closed WITH the flag: the name
         * goes, and the op says so. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "doc1", 4,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_set_delete_on_close(ctx.vfs_thread, oh,
                                        root_fh, (uint16_t) root_fh_len,
                                        "doc1", 4, &cred);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh, CHIMERA_VFS_OPEN_READ_ONLY);
        i_cl = chimera_vfs_compound_add_close_doc(
            cp, CHIMERA_VFS_COMPOUND_CLOSE_DOC, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->doc_fired == 1);
        assert(op->doc_base_deferred == 0);
        assert(op->doc_status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk          = chimera_vfs_compound_add_lookup(cp, "doc1", 4, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_ENOENT);
        chimera_vfs_compound_free(cp);

        /* A NON-EMPTY directory: the unlink fails, the caller is told which
         * failure it was -- MS-FSA reports it to the client, because the
         * object survived -- and the name is still there. */
        mkdir_under(&ctx, &cred, root_fh, root_fh_len, "docd");
        memcpy(d_fh, ctx.fh, ctx.fh_len);
        d_fh_len = ctx.fh_len;
        mkdir_under(&ctx, &cred, d_fh, d_fh_len, "child");

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, d_fh, (int) d_fh_len);
        chimera_vfs_compound_add_open_current(cp,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        i_open        = chimera_vfs_compound_add_gethandle(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_set_delete_on_close(ctx.vfs_thread, oh,
                                        root_fh, (uint16_t) root_fh_len,
                                        "docd", 4, &cred);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh,
                                           CHIMERA_VFS_OPEN_INFERRED |
                                           CHIMERA_VFS_OPEN_PATH |
                                           CHIMERA_VFS_OPEN_DIRECTORY);
        i_cl = chimera_vfs_compound_add_close_doc(
            cp, CHIMERA_VFS_COMPOUND_CLOSE_DOC, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        /* The CLOSE itself succeeded: the handle is gone either way. */
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->doc_fired == 1);
        assert(op->doc_status == CHIMERA_VFS_ENOTEMPTY);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk          = chimera_vfs_compound_add_lookup(cp, "docd", 4, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        /* A named stream still holding the base open: the removal is DEFERRED
         * rather than done, the file is left delete-pending for the stream's
         * own last close to finish, and the name survives for now. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "doc2", 4,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr,
                                               CHIMERA_VFS_ATTR_FH, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_open);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        memcpy(nd_fh, op->attr.va_fh, op->attr.va_fh_len);
        nd_fh_len = op->attr.va_fh_len;
        oh        = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        fs = chimera_vfs_state_get(ctx.vfs->vfs_state, nd_fh,
                                   (uint8_t) nd_fh_len,
                                   chimera_vfs_hash(nd_fh, (int) nd_fh_len),
                                   true);
        assert(fs != NULL);
        chimera_vfs_state_stream_holder_inc(fs);

        chimera_vfs_set_delete_on_close(ctx.vfs_thread, oh,
                                        root_fh, (uint16_t) root_fh_len,
                                        "doc2", 4, &cred);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh, CHIMERA_VFS_OPEN_READ_ONLY);
        i_cl = chimera_vfs_compound_add_close_doc(
            cp, CHIMERA_VFS_COMPOUND_CLOSE_DOC, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->doc_base_deferred == 1);
        assert(op->doc_fired == 0);
        assert(op->doc_status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        assert(chimera_vfs_state_is_delete_pending(fs));

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk          = chimera_vfs_compound_add_lookup(cp, "doc2", 4, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        chimera_vfs_state_stream_holder_dec(fs);
        chimera_vfs_state_clear_delete_pending(fs);
        chimera_vfs_state_put(ctx.vfs->vfs_state, fs);

        /* A CLOSE WITHOUT the flag on an armed handle is the bare release it
         * always was: the name stays. */
        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "doc3", 4,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        oh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(oh != NULL);
        chimera_vfs_compound_free(cp);

        chimera_vfs_set_delete_on_close(ctx.vfs_thread, oh,
                                        root_fh, (uint16_t) root_fh_len,
                                        "doc3", 4, &cred);
        chimera_vfs_clear_delete_on_close(ctx.vfs_thread, oh);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, oh, CHIMERA_VFS_OPEN_READ_ONLY);
        i_cl          = chimera_vfs_compound_add_close_doc(cp, 0, NULL);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_cl);
        assert(op->doc_fired == 0);
        assert(op->doc_status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_lk          = chimera_vfs_compound_add_lookup(cp, "doc3", 4, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_OK);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("CLOSE(CLOSE_DOC) unlinks what the arming named, reports the "
              "unlink's own status, and defers to a stream holder");

    /* ---- a SETATTR through the handle an OPEN in the same run produced is
     * authorized by that open, not by the object's mode ----
     * The SMB2 "create a read-only file, then truncate through the handle"
     * shape: the create grants what it was asked for, and the truncate behind
     * it rides on that grant.  The same SETATTR addressing the object by name
     * is checked against the mode, and refused. */
    {
        struct chimera_vfs_cred  ucred;
        struct chimera_vfs_attrs sattr, tattr;
        uint8_t                  e_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                 e_fh_len;
        int                      i_open, i_sa, i_lk, i_mk;

        chimera_vfs_cred_init_unix(&ucred, 1000, 1000, 0, NULL);

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0777;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_mk = chimera_vfs_compound_add_create(cp,
                                               CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                               "e14", 3, NULL, 0, &sattr,
                                               CHIMERA_VFS_ATTR_FH |
                                               CHIMERA_VFS_ATTR_MASK_STAT,
                                               0, 0);
        assert(compound_test_run(ctx.evpl, cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, (uint32_t) i_mk);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        memcpy(e_fh, op->attr.va_fh, op->attr.va_fh_len);
        e_fh_len = op->attr.va_fh_len;
        chimera_vfs_compound_free(cp);

        /* Created read-only, opened for write: open_at grants a freshly
         * created file the access it was opened with, and stamps it. */
        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0400;

        memset(&tattr, 0, sizeof(tattr));
        tattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        tattr.va_size     = 16;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &ucred);
        chimera_vfs_compound_add_putfh(cp, e_fh, (int) e_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "ro", 2,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_WRITE_ONLY,
                                               0, &sattr, 0, 0, 0);
        i_sa = chimera_vfs_compound_add_setattr(cp, NULL, &tattr, 0,
                                                CHIMERA_VFS_ATTR_SIZE);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_sa,
                                           (uint32_t) i_open);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_sa);
        assert(op->status == CHIMERA_VFS_OK);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
        assert(op->attr.va_size == 16);
        chimera_vfs_compound_free(cp);

        /* ...and the same truncate through a PATH open's handle is NOT: an
         * O_PATH descriptor cannot ftruncate, and an open that asked for no
         * access grants none to ride.  The mode decides, and refuses -- which
         * is what keeps a path-addressed truncate(2) or utimensat(2), the
         * shape an SDK caller reaching an object by path builds, checked
         * against the file's current permissions. */
        memset(&tattr, 0, sizeof(tattr));
        tattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        tattr.va_size     = 24;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &ucred);
        chimera_vfs_compound_add_putfh(cp, e_fh, (int) e_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "ro", 2,
                                               CHIMERA_VFS_OPEN_PATH,
                                               0, NULL, 0, 0, 0);
        i_sa = chimera_vfs_compound_add_setattr(cp, NULL, &tattr, 0, 0);
        chimera_vfs_compound_op_use_handle(cp, (uint32_t) i_sa,
                                           (uint32_t) i_open);

        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_op(cp, i_open)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_sa)->status == CHIMERA_VFS_EACCES);
        chimera_vfs_compound_free(cp);

        /* The same truncate by NAME, with no open of this run's to authorize
         * it, is checked against the 0400 mode and refused.  That difference
         * is the whole of ftruncate(2) versus truncate(2). */
        memset(&tattr, 0, sizeof(tattr));
        tattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        tattr.va_size     = 32;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &ucred);
        chimera_vfs_compound_add_putfh(cp, e_fh, (int) e_fh_len);
        i_lk          = chimera_vfs_compound_add_lookup(cp, "ro", 2, 0, 0);
        i_sa          = chimera_vfs_compound_add_setattr(cp, NULL, &tattr, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(chimera_vfs_compound_op(cp, i_lk)->status == CHIMERA_VFS_OK);
        assert(chimera_vfs_compound_op(cp, i_sa)->status == CHIMERA_VFS_EACCES);
        chimera_vfs_compound_free(cp);
    }
    TEST_PASS("a SETATTR through the handle an earlier op produced takes the "
              "descriptor-rights path, unless that open was a PATH open");

    /* ---- a lent handle is judged by what it ADDRESSES, not by its flags ----
     * A dirfd from open(dir, O_RDONLY) is an open directory that cannot report
     * CHIMERA_VFS_OPEN_DIRECTORY.  It serves a READDIR because the object is a
     * directory; a regular file's handle does not, and the answer is the
     * ENOTDIR that says what is actually wrong. */
    {
        struct chimera_vfs_attrs        sattr;
        struct chimera_vfs_open_handle *dh, *fh_handle;
        int                             i_gh, i_open, i_rd;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, a_fh, (int) a_fh_len);
        chimera_vfs_compound_add_open_current(cp,
                                              CHIMERA_VFS_OPEN_READ_ONLY, 0);
        i_gh          = chimera_vfs_compound_add_gethandle(cp);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        dh = chimera_vfs_compound_take_handle(cp, (uint32_t) i_gh);
        assert(dh != NULL);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, dh, CHIMERA_VFS_OPEN_READ_ONLY);
        i_rd = chimera_vfs_compound_add_readdir(cp, 0, 0, 0, 0, 16,
                                                CHIMERA_VFS_ATTR_MASK_STAT, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        op = chimera_vfs_compound_op(cp, i_rd);
        assert(op->status == CHIMERA_VFS_OK);
        /* /mem/a holds b, and the enumeration reached it. */
        assert(op->num_entries >= 1);
        chimera_vfs_compound_free(cp);

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = S_IFREG | 0600;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_putfh(cp, root_fh, (int) root_fh_len);
        i_open = chimera_vfs_compound_add_open(cp, "notdir", 6,
                                               CHIMERA_VFS_OPEN_CREATE |
                                               CHIMERA_VFS_OPEN_READ_ONLY,
                                               0, &sattr, 0, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_OK);
        fh_handle = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
        assert(fh_handle != NULL);
        chimera_vfs_compound_free(cp);

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &cred);
        chimera_vfs_compound_add_puthandle(cp, fh_handle,
                                           CHIMERA_VFS_OPEN_READ_ONLY);
        i_rd          = chimera_vfs_compound_add_readdir(cp, 0, 0, 0, 0, 16, 0, 0);
        ctx.callbacks = 0;
        chimera_vfs_compound_submit(cp, compound_cb, &ctx);
        wait_done(&ctx);

        assert(ctx.callbacks == 1);
        assert(chimera_vfs_compound_op(cp, i_rd)->status == CHIMERA_VFS_ENOTDIR);
        assert(chimera_vfs_compound_status(cp) == CHIMERA_VFS_ENOTDIR);
        chimera_vfs_compound_free(cp);

        chimera_vfs_release(ctx.vfs_thread, dh);
        chimera_vfs_release(ctx.vfs_thread, fh_handle);
    }
    TEST_PASS("a lent dirfd without the DIRECTORY bit serves a READDIR; a "
              "regular file's handle is ENOTDIR");

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
