// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Byte-range locks for the posix client.
 *
 * TAKING one is a VFS SEQUENCE: PUTHANDLE of the descriptor's own open file,
 * lent with the flags it was opened with, then a CLAIM against it (F_SETLK is
 * TRY, F_SETLKW is WAIT|WAIT_HARD) or a CLAIM_TEST (F_GETLK).  The claim core
 * is the in-process arbiter -- protocol claims and other posix threads -- and
 * the executor projects to an OP_LOCK backend passthrough beneath it so
 * cross-process conflicts keep working: each process has its own core
 * instance, and the kernel is the shared arbiter.  Claims are heap
 * chimera_posix_ofd_lock nodes tracked per open file description; the
 * description's last close releases them.
 *
 * fcntl runs on an application thread, which has no VFS thread to submit a
 * sequence with and no event loop to be called back on, so each run is built
 * and submitted on a worker and the application thread blocks on its
 * completion.
 *
 * WHAT STAYS OUT OF BAND is what a sequence op cannot be.  A release is not
 * reversible and pumps waiters, so it must not sit behind an op that can
 * fail: F_UNLCK's carve, the same-owner replace carve behind a downgrade (a
 * re-insert the owner held the covering range throughout, which cannot fail),
 * the close-time release, and the backend token release are all calls, not
 * ops.  So is the whole SEEK_END arm -- see chimera_posix_lock_claim_seek_end.
 *
 * Locking: every ofd->locks list is guarded by the client's fd_lock (one
 * coarse guard so a carve can unlink fragments tracked on ANY description).
 * The order fd_lock -> file->lock is used consistently (carve, teardown).
 */

#ifdef _WIN32
#include "common/thread.h"
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */

#include "posix_internal.h"
#include "vfs/sdk/vfs_module.h"

/* A mid-range carve splits one claim into head and tail fragments, so two
 * spares.  The claim core's interface fixes this: vfs_claim.h declares
 * spare[2]. */
#define POSIX_LOCK_CARVE_SPARES 2

static FORCE_INLINE struct chimera_vfs_state *
chimera_posix_vfs_state(struct chimera_posix_client *posix)
{
    return posix->client->vfs->vfs_state;
} /* chimera_posix_vfs_state */

struct chimera_posix_ofd_lock *
chimera_posix_ofd_lock_alloc(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle,
    bool                            exclusive,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_vfs_state      *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_ofd_lock *node;
    struct chimera_claim_owner     owner;

    node = calloc(1, sizeof(*node));

    if (!node) {
        return NULL;
    }

    chimera_posix_lock_owner_init(&owner);

    /* Advisory (non-SMB) construct; binding claim -- no break/alive/revoked
     * callbacks are set, so the core never recalls it. */
    chimera_vfs_claim_init_range(&node->claim, exclusive, /* smb */ false,
                                 offset, length, &owner);

    node->file = chimera_vfs_state_get(state,
                                       handle->fh,
                                       (uint8_t) handle->fh_len,
                                       handle->fh_hash,
                                       /* create */ true);

    return node;
} /* chimera_posix_ofd_lock_alloc */

void
chimera_posix_ofd_lock_free(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node)
{
    struct chimera_vfs_state *state = chimera_posix_vfs_state(posix);

    chimera_vfs_state_put(state, node->file);
    free(node);
} /* chimera_posix_ofd_lock_free */

void
chimera_posix_ofd_lock_track(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd      *ofd,
    struct chimera_posix_ofd_lock *node)
{
    evpl_mutex_lock(&posix->fd_lock);
    node->ofd = ofd;
    DL_APPEND(ofd->locks, node);
    atomic_fetch_add(&posix->n_range_locks, 1);
    evpl_mutex_unlock(&posix->fd_lock);
} /* chimera_posix_ofd_lock_track */

void
chimera_posix_ofd_lock_untrack_release(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node)
{
    struct chimera_vfs_state *state = chimera_posix_vfs_state(posix);

    evpl_mutex_lock(&posix->fd_lock);
    if (node->ofd) {
        DL_DELETE(node->ofd->locks, node);
        node->ofd = NULL;
        atomic_fetch_sub(&posix->n_range_locks, 1);
    }
    evpl_mutex_unlock(&posix->fd_lock);

    chimera_vfs_claim_release(state, node->file, &node->claim);
    chimera_vfs_state_put(state, node->file);
    free(node);
} /* chimera_posix_ofd_lock_untrack_release */

void
chimera_posix_ofd_locks_release(
    struct chimera_posix_client *posix,
    struct chimera_posix_ofd    *ofd)
{
    struct chimera_vfs_state      *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_ofd_lock *node;

    while ((node = ofd->locks)) {
        DL_DELETE(ofd->locks, node);
        atomic_fetch_sub(&posix->n_range_locks, 1);
        chimera_vfs_claim_release(state, node->file, &node->claim);
        chimera_vfs_state_put(state, node->file);
        free(node);
    }

    /* Backend records with no local claim behind them (SEEK_END grants):
     * released by token.  Queued rather than waited on -- this runs under
     * the client's fd_lock, and the projection service drains it. */
    while (ofd->backend_tokens) {
        struct chimera_posix_ofd_token *t = ofd->backend_tokens;
        struct chimera_vfs_file_state  *file;

        ofd->backend_tokens = t->next;

        file = chimera_vfs_state_get(state, t->fh, t->fh_len, t->fh_hash,
                                     true);
        if (file) {
            chimera_vfs_claim_backend_release_token(state, file, t->token);
            chimera_vfs_state_put(state, file);
        }
        free(t);
    }
} /* chimera_posix_ofd_locks_release */
void
chimera_posix_ofd_track_token(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        token)
{
    struct chimera_posix_ofd_token *t;

    if (!token) {
        return;
    }

    t = calloc(1, sizeof(*t));

    if (!t) {
        return;
    }

    memcpy(t->fh, handle->fh, handle->fh_len);
    t->fh_len  = (uint8_t) handle->fh_len;
    t->fh_hash = handle->fh_hash;
    t->token   = token;

    evpl_mutex_lock(&posix->fd_lock);
    t->next             = ofd->backend_tokens;
    ofd->backend_tokens = t;
    evpl_mutex_unlock(&posix->fd_lock);
} /* chimera_posix_ofd_track_token */

/* -------------------------------------------------------------------- */
/* F_UNLCK carve                                                        */
/* -------------------------------------------------------------------- */

struct chimera_posix_lock_carve_ctx {
    struct chimera_posix_client   *posix;
    struct chimera_posix_ofd_lock *freed; /* chained via ->next */
};

/* Released fragments come back as bare claim pointers; every inserted range
 * claim under the POSIX owner is one of our nodes (claim first member), so
 * recover the node, unlink it from whichever description tracks it (fd_lock
 * is held by the carve caller), and defer the free past the core call. */
static void
chimera_posix_lock_carve_released(
    struct chimera_vfs_claim *claim,
    void                     *arg)
{
    struct chimera_posix_lock_carve_ctx *ctx  = arg;
    struct chimera_posix_ofd_lock       *node =
        (struct chimera_posix_ofd_lock *) claim;

    if (node->ofd) {
        DL_DELETE(node->ofd->locks, node);
        node->ofd = NULL;
        atomic_fetch_sub(&ctx->posix->n_range_locks, 1);
    }
    /* CLAIMTODO: a node granted by the core but not yet tracked (the window
     * between a blocking grant and track) would arrive here with ofd NULL
     * and be freed under the granter's feet; the rough pass accepts the
     * race and relies on the compile/iterate cycle to close it. */

    node->next = ctx->freed;
    ctx->freed = node;
} /* chimera_posix_lock_carve_released */

void
chimera_posix_ofd_lock_carve(
    struct chimera_posix_client      *posix,
    struct chimera_posix_ofd         *ofd,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_claim_owner *owner,
    const struct chimera_vfs_claim   *except,
    uint64_t                          offset,
    uint64_t                          length)
{
    struct chimera_vfs_state           *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_ofd_lock      *spare_nodes[POSIX_LOCK_CARVE_SPARES];
    struct chimera_vfs_claim           *spare[POSIX_LOCK_CARVE_SPARES];
    struct chimera_posix_lock_carve_ctx ctx = { .posix = posix,
                                                .freed = NULL };
    int                                 spare_used = 0;
    struct chimera_vfs_file_state      *file;
    struct chimera_posix_ofd_lock      *node;
    int                                 i;

    file = chimera_vfs_state_get(state,
                                 handle->fh,
                                 (uint8_t) handle->fh_len,
                                 handle->fh_hash,
                                 /* create */ true);

    /* Two heap spares: a mid-range carve splits one claim into head and
     * tail fragments.  The core copies the split claim into a consumed
     * spare (offset/length rewritten) and links it, so the claim needs no
     * init here; each spare carries its own anchor reference in case it is
     * inserted. */
    for (i = 0; i < POSIX_LOCK_CARVE_SPARES; i++) {
        spare_nodes[i] = calloc(1, sizeof(*spare_nodes[i]));

        if (!spare_nodes[i]) {
            /* Drop the spares already built and skip the carve.  Leaving the
             * claim unsplit holds more of the range than was asked for, which
             * is a far better failure than dereferencing NULL. */
            while (i-- > 0) {
                chimera_vfs_state_put(state, spare_nodes[i]->file);
                free(spare_nodes[i]);
            }
            chimera_vfs_state_put(state, file);
            return;
        }

        spare_nodes[i]->file = chimera_vfs_state_get(state,
                                                     handle->fh,
                                                     (uint8_t) handle->fh_len,
                                                     handle->fh_hash,
                                                     /* create */ true);
        spare[i] = &spare_nodes[i]->claim;
    }

    evpl_mutex_lock(&posix->fd_lock);

    chimera_vfs_claim_range_replace(state, file, owner, except, offset, length,
                                    /* new_mask */ 0,
                                    spare, &spare_used,
                                    chimera_posix_lock_carve_released, &ctx);

    /* The core takes at most the spares it was handed -- vfs_claim.h fixes the
     * array at spare[2] and vfs_claim.c guards with n_spare < 2 -- but it lives
     * in another translation unit, so bound the count here to keep both loops
     * below inside spare_nodes[]. */
    if (spare_used > POSIX_LOCK_CARVE_SPARES) {
        spare_used = POSIX_LOCK_CARVE_SPARES;
    }

    /* Consumed spares are now inserted head/tail fragments: track them.
     * CLAIMTODO: fragments attribute to the UNLOCKING description even when
     * the split lock was taken through a different description of the same
     * file -- harmless for arbitration (one per-process owner) but release
     * timing follows this OFD's last close. */
    for (i = 0; i < spare_used; i++) {
        spare_nodes[i]->ofd = ofd;
        DL_APPEND(ofd->locks, spare_nodes[i]);
        atomic_fetch_add(&posix->n_range_locks, 1);
    }

    evpl_mutex_unlock(&posix->fd_lock);

    for (i = spare_used; i < POSIX_LOCK_CARVE_SPARES; i++) {
        chimera_vfs_state_put(state, spare_nodes[i]->file);
        free(spare_nodes[i]);
    }

    while (ctx.freed) {
        node      = ctx.freed;
        ctx.freed = node->next;
        chimera_vfs_state_put(state, node->file);
        free(node);
    }

    chimera_vfs_state_put(state, file);
} /* chimera_posix_ofd_lock_carve */

/* -------------------------------------------------------------------- */
/* F_SETLK / F_SETLKW / F_GETLK: one sequence each                      */
/* -------------------------------------------------------------------- */

/* The CLAIM's (or CLAIM_TEST's) index in a lock sequence: PUTHANDLE is 0. */
#define CHIMERA_POSIX_LOCK_OP_CLAIM 1

/*
 * Everything a lock run needs, on the calling application thread's stack.
 * That thread blocks on the sequence's completion, so the storage outlives
 * the run -- but the CLAIM's claim and ticket are deliberately NOT here:
 * they are the caller's lock node, which outlives the LOCK, because an
 * inserted claim's address is its identity to the claim core.
 *
 * The request is only the vehicle.  A sequence is submitted by a
 * chimera_vfs_thread and fcntl runs on an application thread that has none,
 * so the run is built and submitted on a worker's thread; the worker calls
 * the exec callback and never looks at an opcode.
 */
struct chimera_posix_lock_claim_ctx {
    struct chimera_client_request     request;
    struct chimera_posix_completion   comp;
    struct chimera_posix_client      *posix;
    struct chimera_vfs_open_handle   *handle;
    unsigned int                      open_flags;
    /* F_SETLK / F_SETLKW: the node whose claim is being taken. */
    struct chimera_posix_ofd_lock    *node;
    bool                              wait;
    /* F_GETLK: the probe, which the sequence borrows but never inserts, so
    * it need only outlive the submission the waiting thread is holding. */
    struct chimera_vfs_claim          probe;
    /* Results, read by the application thread once the completion fires. */
    enum chimera_vfs_error status;
    enum chimera_vfs_claim_result result;
    struct chimera_vfs_claim_conflict conflict;
};

/*
 * A lock run has finished, on the worker thread that submitted it.
 *
 * CHIMERA_CLAIM_GRANTED is 0, so an op that never ran would read back as a
 * grant: the SEQUENCE's status is what says the claim was taken, and the
 * claim_result only says why it was not.
 */
static void
chimera_posix_lock_claim_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_posix_lock_claim_ctx  *ctx = private_data;
    struct chimera_vfs_state             *state;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_file_state        *taken;

    state = chimera_posix_vfs_state(ctx->posix);

    ctx->status = chimera_vfs_compound_status(compound);
    op          = chimera_vfs_compound_op(compound,
                                          CHIMERA_POSIX_LOCK_OP_CLAIM);

    ctx->result   = op->claim_result;
    ctx->conflict = op->conflict;

    /*
     * On GRANTED the claim is inserted and the sequence hands over the file
     * state it resolved to insert it into; the contract is that the caller
     * takes it.  We take it and PUT it straight back, because the lock node
     * already holds a reference to the very same state --
     * chimera_vfs_state_get is refcounted per fh, and the executor resolved
     * it from the same handle chimera_posix_ofd_lock_alloc resolved it from.
     *
     * The NODE's reference is the one to keep.  It is taken when the node is
     * allocated and put when the node is freed, so it covers the node's whole
     * life -- including the window before any grant, and the refused acquire
     * that never reaches a GRANTED op at all -- and it is what every release
     * path that never ran through a sequence already uses: the carve's freed
     * fragments, ofd_lock_free, untrack_release, and the last close.  Keeping
     * the sequence's instead would mean one reference for a granted node and
     * none for a refused one, with a branch at every free.
     *
     * A CLAIM_TEST inserts nothing, so it hands over nothing and this is
     * simply NULL for the F_GETLK run.
     */
    taken = chimera_vfs_compound_take_file_state(compound,
                                                 CHIMERA_POSIX_LOCK_OP_CLAIM);

    if (taken) {
        chimera_vfs_state_put(state, taken);
    }

    chimera_vfs_compound_free(compound);

    chimera_posix_complete(&ctx->comp, ctx->status);
} /* chimera_posix_lock_claim_complete */

static void
chimera_posix_lock_claim_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_lock_claim_ctx *ctx = request->lock_probe_private;
    struct chimera_vfs_compound         *compound;
    unsigned int                         flags;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                          chimera_client_req_cred(request));

    /* The descriptor's own open file, lent as it was opened.  A claim is
     * arbitrated per FILE and uses only the fh, so the data handle fcntl was
     * called on serves the op's PATH want and nothing is re-opened.  Lending
     * it is safe for a range claim precisely because the executor stamps
     * op_handle on a CACHE-class claim only: handles are cached per (fh,
     * access mode, cred) and SHARED, so a stamp here would fold every posix
     * lock taken through one such handle into a single holder. */
    chimera_vfs_compound_add_puthandle(compound, ctx->handle, ctx->open_flags);

    if (ctx->node) {
        /* F_SETLKW queues behind a breaking caching holder AND behind
         * another owner's incompatible lock; F_SETLK waits for neither, and
         * the BREAKING it can be answered with is the same EAGAIN a DENIED
         * is -- the recalls are kicked, the claim is not inserted. */
        flags = ctx->wait
            ? (CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
               CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD)
            : CHIMERA_VFS_COMPOUND_CLAIM_TRY;

        chimera_vfs_compound_add_claim(compound, &ctx->node->claim,
                                       &ctx->node->ticket, flags,
                                       /* pre */ 0, 0, /* deny */ 0, 0);
    } else {
        /* One op for both halves of F_GETLK: the local core answers first,
         * and the executor projects the probe to a range-arbitrating backend
         * when it comes back clear. */
        chimera_vfs_compound_add_claim_test(
            compound, &ctx->probe, CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND);
    }

    chimera_vfs_compound_submit(compound, chimera_posix_lock_claim_complete,
                                ctx);
} /* chimera_posix_lock_claim_exec */

/*
 * Submit the run and block this application thread on its completion.
 *
 * A parked F_SETLKW waits here for as long as the holder keeps the lock, and
 * nothing in this client can take that wait back: fcntl(2) has no caller to
 * answer, this thread is inside the call and cannot see a signal, and a
 * close(2) from another thread releases locks without touching a pending
 * acquire.  So no park callback is registered and no cancel is posted -- the
 * compound's own chimera_vfs_compound_cancel_post is what a front end with an
 * out-of-band cancel (a FUSE INTERRUPT, an SMB2 CANCEL) uses, and there is no
 * such event to route here.
 */
static void
chimera_posix_lock_claim_run(struct chimera_posix_lock_claim_ctx *ctx)
{
    ctx->request.lock_probe_private = ctx;

    chimera_posix_worker_enqueue(chimera_posix_choose_worker(ctx->posix),
                                 &ctx->request, chimera_posix_lock_claim_exec);

    (void) chimera_posix_wait(&ctx->comp);
    chimera_posix_completion_destroy(&ctx->comp);
} /* chimera_posix_lock_claim_run */

int
chimera_posix_lock_claim_acquire(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle,
    unsigned int                    open_flags,
    struct chimera_posix_ofd_lock  *node,
    bool                            wait)
{
    struct chimera_posix_lock_claim_ctx ctx;

    memset(&ctx, 0, sizeof(ctx));
    chimera_posix_completion_init(&ctx.comp, &ctx.request);

    ctx.posix      = posix;
    ctx.handle     = handle;
    ctx.open_flags = open_flags;
    ctx.node       = node;
    ctx.wait       = wait;

    chimera_posix_lock_claim_run(&ctx);

    if (ctx.status == CHIMERA_VFS_OK) {
        return 0;
    }

    /* A refused claim is the op's answer, reported as EAGAIN by the
     * executor; anything else is a sequence that could not ask, and carries
     * its own errno. */
    errno = (ctx.status == CHIMERA_VFS_EAGAIN)
        ? EAGAIN : chimera_posix_errno_from_status(ctx.status);

    return -1;
} /* chimera_posix_lock_claim_acquire */

int
chimera_posix_lock_claim_getlk(
    struct chimera_posix_client       *posix,
    struct chimera_vfs_open_handle    *handle,
    unsigned int                       open_flags,
    bool                               exclusive,
    uint64_t                           offset,
    uint64_t                           length,
    struct chimera_vfs_claim_conflict *conflict)
{
    struct chimera_posix_lock_claim_ctx ctx;
    struct chimera_claim_owner          owner;

    memset(&ctx, 0, sizeof(ctx));
    chimera_posix_completion_init(&ctx.comp, &ctx.request);

    ctx.posix      = posix;
    ctx.handle     = handle;
    ctx.open_flags = open_flags;

    chimera_posix_lock_owner_init(&owner);
    chimera_vfs_claim_init_range(&ctx.probe, exclusive, /* smb */ false,
                                 offset, length, &owner);

    chimera_posix_lock_claim_run(&ctx);

    if (ctx.status != CHIMERA_VFS_OK) {
        errno = chimera_posix_errno_from_status(ctx.status);
        return -1;
    }

    /* A probe that says "denied" has ANSWERED -- the op succeeds either way
     * and the result is the answer. */
    if (ctx.result == CHIMERA_CLAIM_GRANTED) {
        return 0;
    }

    *conflict = ctx.conflict;
    return 1;
} /* chimera_posix_lock_claim_getlk */

/* -------------------------------------------------------------------- */
/* The SEEK_END passthrough: a backend RANGE op, reached by hand         */
/* -------------------------------------------------------------------- */

/*
 * The one lock path that is not a sequence, and the worker-and-condvar
 * bridge that survives for it.  CLAIM takes a RESOLVED range and carries no
 * `whence`, while a SEEK_END range's absolute geometry is the backend's to
 * resolve atomically with the operation -- resolving EOF on this side is
 * exactly the fstat TOCTOU the whence passthrough exists to avoid.  So a
 * SEEK_END fcntl bypasses local arbitration entirely and asks the backend
 * directly, which still needs a VFS thread to be asked from and still has to
 * answer an application thread that is blocked.
 */

struct chimera_posix_lock_probe_ctx {
    struct chimera_client_request       request;
    struct chimera_posix_client        *posix;
    struct chimera_vfs_open_handle     *handle;
    uint8_t                             exclusive;
    uint8_t                             flags;
    int32_t                             whence;
    uint64_t                            offset;
    uint64_t                            length;
    struct chimera_claim_owner          owner;
    /* Results. */
    enum chimera_vfs_error status;
    uint8_t                             granted;
    uint64_t                            token;
    struct chimera_claim_range_conflict conflict;
    evpl_mutex_t                        mutex;
    evpl_cond_t                         cond;
    int                                 done;
};

static void
chimera_posix_lock_probe_cb(
    enum chimera_vfs_error                     status,
    uint8_t                                    granted,
    uint64_t                                   token,
    const struct chimera_claim_range_conflict *conflict,
    void                                      *private_data)
{
    struct chimera_posix_lock_probe_ctx *ctx = private_data;

    evpl_mutex_lock(&ctx->mutex);
    ctx->status  = status;
    ctx->granted = granted;
    ctx->token   = token;
    if (conflict) {
        ctx->conflict = *conflict;
    }
    ctx->done = 1;
    evpl_cond_signal(&ctx->cond);
    evpl_mutex_unlock(&ctx->mutex);
} /* chimera_posix_lock_probe_cb */

static void
chimera_posix_lock_probe_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_lock_probe_ctx *ctx = request->lock_probe_private;

    chimera_vfs_claim_acquire_backend(
        thread->vfs_thread,
        ctx->handle->fh, (uint8_t) ctx->handle->fh_len, ctx->handle->fh_hash,
        CHIMERA_VFS_CLAIM_KLASS_RANGE, 0, 0,
        ctx->exclusive, ctx->flags, ctx->whence, ctx->offset, ctx->length,
        &ctx->owner, 0, NULL, NULL,
        chimera_posix_lock_probe_cb, ctx);
} /* chimera_posix_lock_probe_exec */

/* Run one RANGE op against the backend on a worker's vfs thread and wait.
 * Returns false without dispatching when no backend arbitrates ranges. */
static bool
chimera_posix_lock_probe(
    struct chimera_posix_client         *posix,
    struct chimera_vfs_open_handle      *handle,
    struct chimera_posix_lock_probe_ctx *ctx)
{
    struct chimera_vfs_state *state = chimera_posix_vfs_state(posix);

    if (!chimera_vfs_claim_backend_range_capable(state)) {
        return false;
    }

    ctx->posix  = posix;
    ctx->handle = handle;
    ctx->status = CHIMERA_VFS_OK;
    ctx->done   = 0;
    chimera_posix_lock_owner_init(&ctx->owner);
    evpl_mutex_init(&ctx->mutex, NULL);
    evpl_cond_init(&ctx->cond, NULL);

    ctx->request.heap_allocated     = 0;
    ctx->request.lock_probe_private = ctx;

    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx->request,
                                 chimera_posix_lock_probe_exec);

    evpl_mutex_lock(&ctx->mutex);
    while (!ctx->done) {
        evpl_cond_wait(&ctx->cond, &ctx->mutex);
    }
    evpl_mutex_unlock(&ctx->mutex);

    evpl_mutex_destroy(&ctx->mutex);
    evpl_cond_destroy(&ctx->cond);
    return true;
} /* chimera_posix_lock_probe */

int
chimera_posix_lock_claim_seek_end(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    int                             cmd,
    struct flock                   *fl,
    uint32_t                        lock_type,
    int32_t                         whence,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_posix_lock_probe_ctx ctx;

    memset(&ctx, 0, sizeof(ctx));
    ctx.exclusive = (lock_type == CHIMERA_VFS_LOCK_WRITE) ? 1 : 0;
    ctx.whence    = whence;
    ctx.offset    = offset;
    ctx.length    = length;

    if (cmd == F_GETLK) {
        ctx.flags = CHIMERA_VFS_CLAIM_TEST;
    } else if (cmd == F_SETLKW) {
        ctx.flags = CHIMERA_VFS_CLAIM_WAIT;
    }

    /* An unlock names the range rather than a token: this node never
     * learned the absolute geometry, so it hands the backend the same
     * EOF-relative range it locked with and lets it resolve. */
    if (lock_type == CHIMERA_VFS_LOCK_UNLOCK) {
        return chimera_posix_lock_claim_unlock_ranged(posix, handle, whence,
                                                      offset, length);
    }

    if (!chimera_posix_lock_probe(posix, handle, &ctx)) {
        errno = ENOTSUP;
        return -1;
    }

    if (ctx.status != CHIMERA_VFS_OK) {
        errno = chimera_posix_errno_from_status(ctx.status);
        return -1;
    }

    if (cmd == F_GETLK) {
        if (ctx.conflict.type == CHIMERA_VFS_LOCK_UNLOCK) {
            fl->l_type = F_UNLCK;
        } else {
            fl->l_type = (ctx.conflict.type == CHIMERA_VFS_LOCK_READ)
                ? F_RDLCK : F_WRLCK;
            fl->l_whence = SEEK_SET;
            fl->l_start  = (chimera_off_t) ctx.conflict.offset;
            fl->l_len    = (chimera_off_t) ctx.conflict.length;
            fl->l_pid    = (pid_t) ctx.conflict.pid;
        }
        return 0;
    }

    if (!ctx.granted) {
        errno = EAGAIN;
        return -1;
    }

    /* Tracked by TOKEN rather than by range: the absolute geometry is the
     * backend's answer and it does not report it back, but a token is all a
     * release needs. */
    chimera_posix_ofd_track_token(posix, ofd, handle, ctx.token);
    return 0;
} /* chimera_posix_lock_claim_seek_end */

/* -------------------------------------------------------------------- */
/* Unlock: carve locally, then wait for the backend to let go            */
/* -------------------------------------------------------------------- */

/*
 * F_UNLCK has to be observable the moment it returns, including to another
 * PROCESS asking the shared arbiter -- which cannot wait on this node's
 * projection work queue.  So the carve runs on a worker's vfs thread and
 * this call blocks until every backend release it produced has completed.
 * Without a range-arbitrating backend there is nothing to wait for and the
 * carve happens in place.
 */
struct chimera_posix_unlock_ctx {
    struct chimera_client_request   request;
    struct chimera_posix_client    *posix;
    struct chimera_posix_ofd       *ofd;
    struct chimera_vfs_open_handle *handle;
    struct chimera_claim_owner      owner;
    const struct chimera_vfs_claim *except;
    uint64_t                        offset;
    uint64_t                        length;
    evpl_mutex_t                    mutex;
    evpl_cond_t                     cond;
    int                             done;
};

static void
chimera_posix_unlock_flushed(void *private_data)
{
    struct chimera_posix_unlock_ctx *ctx = private_data;

    evpl_mutex_lock(&ctx->mutex);
    ctx->done = 1;
    evpl_cond_signal(&ctx->cond);
    evpl_mutex_unlock(&ctx->mutex);
} /* chimera_posix_unlock_flushed */

static void
chimera_posix_unlock_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_unlock_ctx *ctx   = request->lock_probe_private;
    struct chimera_vfs_state        *state = chimera_posix_vfs_state(ctx->posix);
    struct chimera_vfs_file_state   *file;

    chimera_posix_ofd_lock_carve(ctx->posix, ctx->ofd, ctx->handle,
                                 &ctx->owner, ctx->except, ctx->offset,
                                 ctx->length);

    file = chimera_vfs_state_get(state, ctx->handle->fh,
                                 (uint8_t) ctx->handle->fh_len,
                                 ctx->handle->fh_hash, true);

    chimera_vfs_claim_backend_flush_releases(thread->vfs_thread, state, file,
                                             chimera_posix_unlock_flushed, ctx);

    chimera_vfs_state_put(state, file);
} /* chimera_posix_unlock_exec */

static void
chimera_posix_lock_claim_carve_wait(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_claim *except,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_vfs_state       *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_unlock_ctx ctx;
    struct chimera_claim_owner      owner;

    /* Capture the owner HERE, on the application thread: the identity is a
     * property of the caller, and the carve below runs on a worker. */
    chimera_posix_lock_owner_init(&owner);

    if (!chimera_vfs_claim_backend_range_capable(state)) {
        chimera_posix_ofd_lock_carve(posix, ofd, handle, &owner, except,
                                     offset, length);
        return;
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.posix  = posix;
    ctx.ofd    = ofd;
    ctx.handle = handle;
    ctx.owner  = owner;
    ctx.except = except;
    ctx.offset = offset;
    ctx.length = length;
    evpl_mutex_init(&ctx.mutex, NULL);
    evpl_cond_init(&ctx.cond, NULL);

    ctx.request.heap_allocated     = 0;
    ctx.request.lock_probe_private = &ctx;

    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx.request, chimera_posix_unlock_exec);

    evpl_mutex_lock(&ctx.mutex);
    while (!ctx.done) {
        evpl_cond_wait(&ctx.cond, &ctx.mutex);
    }
    evpl_mutex_unlock(&ctx.mutex);

    evpl_mutex_destroy(&ctx.mutex);
    evpl_cond_destroy(&ctx.cond);
} /* chimera_posix_lock_claim_carve_wait */

void
chimera_posix_lock_claim_unlock(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length)
{
    chimera_posix_lock_claim_carve_wait(posix, ofd, handle, NULL,
                                        offset, length);
} /* chimera_posix_lock_claim_unlock */

/* Release a backend range this node holds without a local claim, by
 * geometry rather than by token (a SEEK_END unlock).  Waits, like every
 * other unlock, so the range really is free when fcntl returns. */
struct chimera_posix_unlock_ranged_ctx {
    struct chimera_client_request   request;
    struct chimera_posix_client    *posix;
    struct chimera_vfs_open_handle *handle;
    int32_t                         whence;
    uint64_t                        offset;
    uint64_t                        length;
    struct chimera_claim_owner      owner;
    enum chimera_vfs_error status;
    evpl_mutex_t                    mutex;
    evpl_cond_t                     cond;
    int                             done;
};

static void
chimera_posix_unlock_ranged_cb(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_posix_unlock_ranged_ctx *ctx = private_data;

    evpl_mutex_lock(&ctx->mutex);
    ctx->status = status;
    ctx->done   = 1;
    evpl_cond_signal(&ctx->cond);
    evpl_mutex_unlock(&ctx->mutex);
} /* chimera_posix_unlock_ranged_cb */

static void
chimera_posix_unlock_ranged_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_unlock_ranged_ctx *ctx = request->lock_probe_private;

    chimera_vfs_claim_release_backend(thread->vfs_thread,
                                      ctx->handle->fh,
                                      (uint8_t) ctx->handle->fh_len,
                                      ctx->handle->fh_hash,
                                      CHIMERA_VFS_CLAIM_KLASS_RANGE,
                                      /* token */ 0, /* retained */ 0,
                                      ctx->whence, ctx->offset, ctx->length,
                                      &ctx->owner,
                                      chimera_posix_unlock_ranged_cb, ctx);
} /* chimera_posix_unlock_ranged_exec */

int
chimera_posix_lock_claim_unlock_ranged(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle,
    int32_t                         whence,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_vfs_state              *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_unlock_ranged_ctx ctx;

    if (!chimera_vfs_claim_backend_range_capable(state)) {
        /* Nothing arbitrates ranges, so nothing was ever taken. */
        return 0;
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.posix  = posix;
    ctx.handle = handle;
    ctx.whence = whence;
    ctx.offset = offset;
    ctx.length = length;
    chimera_posix_lock_owner_init(&ctx.owner);
    evpl_mutex_init(&ctx.mutex, NULL);
    evpl_cond_init(&ctx.cond, NULL);

    ctx.request.heap_allocated     = 0;
    ctx.request.lock_probe_private = &ctx;

    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx.request, chimera_posix_unlock_ranged_exec);

    evpl_mutex_lock(&ctx.mutex);
    while (!ctx.done) {
        evpl_cond_wait(&ctx.cond, &ctx.mutex);
    }
    evpl_mutex_unlock(&ctx.mutex);

    evpl_mutex_destroy(&ctx.mutex);
    evpl_cond_destroy(&ctx.cond);

    if (ctx.status != CHIMERA_VFS_OK && ctx.status != CHIMERA_VFS_ENOTSUP) {
        errno = chimera_posix_errno_from_status(ctx.status);
        return -1;
    }

    return 0;
} /* chimera_posix_lock_claim_unlock_ranged */

/*
 * POSIX close() semantics.  XSH fcntl: "All locks associated with a file for
 * a given process shall be removed when a file descriptor for that file is
 * closed by that process."  ANY descriptor -- not merely the last one, and
 * not merely the description the lock was taken through -- so this carves
 * the whole address space for this owner rather than walking one list.
 *
 * The claim core is the authority on which claims that owner holds, so a
 * lock taken through a sibling description is released here even though
 * this description never tracked it; the carve's released callback unlinks
 * each node from whichever description does track it.
 */
void
chimera_posix_locks_release_file(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_state      *state = chimera_posix_vfs_state(posix);
    struct chimera_vfs_file_state *file;
    struct chimera_claim_owner     owner;
    bool                           held;

    /* Two fast paths, because close() is hot and the release below is not:
     * it marshals onto a worker and blocks.  First, the client may hold no
     * byte-range locks at all, which is the overwhelmingly common case and
     * costs one atomic read. */
    if (atomic_load(&posix->n_range_locks) == 0) {
        return;
    }

    /* Second, it may hold locks but none on THIS file.  Answering that is a
     * short walk of one claim list, entirely local. */
    file = chimera_vfs_state_get(state, handle->fh, (uint8_t) handle->fh_len,
                                 handle->fh_hash, /* create */ false);

    if (!file) {
        return;
    }

    chimera_posix_lock_owner_init(&owner);
    held = chimera_vfs_claim_range_owner_holds(file, &owner, NULL,
                                               0, UINT64_MAX);
    chimera_vfs_state_put(state, file);

    if (!held) {
        return;
    }

    /* The whole address space, through the ordinary unlock path -- so the
     * backend records go too, and are WAITED for.  POSIX makes close() a
     * release point, and another process asking the same backend cannot
     * wait on our projection queue any more than it could for F_UNLCK. */
    chimera_posix_lock_claim_unlock(posix, ofd, handle, 0, UINT64_MAX);
} /* chimera_posix_locks_release_file */

void
chimera_posix_ofd_lock_replace(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    struct chimera_posix_ofd_lock  *node,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_vfs_state      *state = chimera_posix_vfs_state(posix);
    struct chimera_vfs_file_state *file;
    struct chimera_claim_owner     owner;
    bool                           held;

    /* A first lock on a range has nothing to replace, which is the common
     * case; answering that is one short walk of a local claim list, versus
     * a worker round trip and a backend flush. */
    file = chimera_vfs_state_get(state, handle->fh, (uint8_t) handle->fh_len,
                                 handle->fh_hash, /* create */ false);

    if (!file) {
        return;
    }

    chimera_posix_lock_owner_init(&owner);
    held = chimera_vfs_claim_range_owner_holds(file, &owner, &node->claim,
                                               offset, length);
    chimera_vfs_state_put(state, file);

    if (!held) {
        return;
    }

    /* Waited, like an unlock: until the backend has dropped the older,
     * possibly STRONGER record, it still answers for this range, and a
     * F_GETLK from another owner would be told the downgrade never
     * happened. */
    chimera_posix_lock_claim_carve_wait(posix, ofd, handle, &node->claim,
                                        offset, length);
} /* chimera_posix_ofd_lock_replace */
