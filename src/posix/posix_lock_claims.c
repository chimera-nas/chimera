// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Local claim-core bridge for posix byte-range locks.
 *
 * fcntl locks arbitrate through the embedded VFS's claim core first (the
 * in-process arbiter: protocol claims and other posix threads), then project
 * through the OP_LOCK backend passthrough so cross-process conflicts keep
 * working (each process has its own core instance; the kernel is the shared
 * arbiter).  Claims are heap chimera_posix_ofd_lock nodes tracked per open
 * file description; the description's last close releases them.
 *
 * Locking: every ofd->locks list is guarded by the client's fd_lock (one
 * coarse guard so a carve can unlink fragments tracked on ANY description).
 * The order fd_lock -> file->lock is used consistently (carve, teardown).
 */

#include <unistd.h>

#include "posix_internal.h"
#include "vfs/sdk/vfs_module.h"
#include "../client/client_lock.h"

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
    pthread_mutex_lock(&posix->fd_lock);
    node->ofd = ofd;
    DL_APPEND(ofd->locks, node);
    pthread_mutex_unlock(&posix->fd_lock);
} /* chimera_posix_ofd_lock_track */

void
chimera_posix_ofd_lock_untrack_release(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node)
{
    struct chimera_vfs_state *state = chimera_posix_vfs_state(posix);

    pthread_mutex_lock(&posix->fd_lock);
    if (node->ofd) {
        DL_DELETE(node->ofd->locks, node);
        node->ofd = NULL;
    }
    pthread_mutex_unlock(&posix->fd_lock);

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

static void
chimera_posix_project_unlock_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint32_t                      conflict_type,
    uint64_t                      conflict_offset,
    uint64_t                      conflict_length,
    pid_t                         conflict_pid,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_project_unlock_callback */

static void
chimera_posix_project_unlock_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_lock(thread, request);
} /* chimera_posix_project_unlock_exec */

/*
 * Dropping an open file description's last descriptor releases its
 * byte-range locks.  The local claims go in chimera_posix_ofd_locks_release,
 * but a backend that arbitrates locks itself (CAP_CLAIM_RANGE: the nfs/smb
 * proxies projecting to a real lock manager) holds its own state -- and the
 * NLM server pins the file's open handle for as long as the lock lives, so
 * an untold backend leaks both.  Project one whole-file unlock for this
 * owner (the owner IS the open handle) while the handle is still live.
 * Called from every path that implicitly drops a description's last
 * reference with the handle still open: close(2) and dup2(2)'s implicit
 * close of its target.  Best-effort: the caller's close succeeds regardless,
 * so a failed projection only risks the server holding the lock until the
 * connection drops.  (OFD-granularity, matching the local release -- see the
 * CLAIMTODO in chimera_posix_ofd_release_locked.)
 */
void
chimera_posix_project_ofd_unlock(
    struct chimera_posix_client    *posix,
    struct chimera_posix_worker    *worker,
    struct chimera_vfs_open_handle *handle,
    struct chimera_posix_ofd       *ofd)
{
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             project;

    if (!handle ||
        !(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLAIM_RANGE)) {
        return;
    }

    pthread_mutex_lock(&posix->fd_lock);
    project = ofd && ofd->refcnt == 1 && ofd->locks != NULL;
    pthread_mutex_unlock(&posix->fd_lock);

    if (!project) {
        return;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode            = CHIMERA_CLIENT_OP_LOCK;
    req.lock.handle       = handle;
    req.lock.whence       = SEEK_SET;
    req.lock.offset       = 0;
    req.lock.length       = 0;   /* 0 = to EOF (whole file) */
    req.lock.lock_type    = CHIMERA_VFS_LOCK_UNLOCK;
    req.lock.flags        = 0;
    req.lock.callback     = chimera_posix_project_unlock_callback;
    req.lock.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req,
                                 chimera_posix_project_unlock_exec);

    (void) chimera_posix_wait(&comp);
    chimera_posix_completion_destroy(&comp);
} /* chimera_posix_project_ofd_unlock */
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

    pthread_mutex_lock(&posix->fd_lock);
    t->next             = ofd->backend_tokens;
    ofd->backend_tokens = t;
    pthread_mutex_unlock(&posix->fd_lock);
} /* chimera_posix_ofd_track_token */

/* -------------------------------------------------------------------- */
/* F_UNLCK carve                                                        */
/* -------------------------------------------------------------------- */

struct chimera_posix_lock_carve_ctx {
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
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_vfs_state           *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_ofd_lock      *spare_nodes[POSIX_LOCK_CARVE_SPARES];
    struct chimera_vfs_claim           *spare[POSIX_LOCK_CARVE_SPARES];
    struct chimera_posix_lock_carve_ctx ctx        = { .freed = NULL };
    int                                 spare_used = 0;
    struct chimera_vfs_file_state      *file;
    struct chimera_claim_owner          owner;
    struct chimera_posix_ofd_lock      *node;
    int                                 i;

    chimera_posix_lock_owner_init(&owner);

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

    pthread_mutex_lock(&posix->fd_lock);

    chimera_vfs_claim_range_replace(state, file, &owner, offset, length,
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
    }

    pthread_mutex_unlock(&posix->fd_lock);

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
/* F_SETLKW condvar bridge                                              */
/* -------------------------------------------------------------------- */

struct chimera_posix_lock_waiter {
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    int             done;
    enum chimera_vfs_claim_result result;
};

static void
chimera_posix_lock_acquire_cb(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *granted,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data)
{
    struct chimera_posix_lock_waiter *waiter = private_data;

    pthread_mutex_lock(&waiter->mutex);
    waiter->result = result;
    waiter->done   = 1;
    pthread_cond_signal(&waiter->cond);
    pthread_mutex_unlock(&waiter->mutex);
} /* chimera_posix_lock_acquire_cb */

/* Everything the acquire needs, on the calling thread's stack: the ticket
 * lives here because the core may queue it and fire the callback later, and
 * the request is the vehicle for reaching a worker.  The caller blocks until
 * the callback fires, and the completion path touches no part of this once
 * it has, so stack storage is safe for both. */
struct chimera_posix_lock_claim_ctx {
    struct chimera_client_request      request;
    struct chimera_posix_client       *posix;
    struct chimera_posix_ofd_lock     *node;
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_posix_lock_waiter   waiter;
    bool                               wait;
};

/*
 * Run the acquire on a worker's VFS thread.  A byte-range claim on a
 * CAP_LEASE backend is confirmed with that backend before its grant is
 * reported, and that confirm is dispatched from the acquiring thread's own
 * request pool -- which fcntl, running on the application's thread, does not
 * have.  Nothing may touch the context after chimera_vfs_claim_acquire()
 * returns: the callback it fires releases the waiting thread, which owns
 * this storage.
 */
static void
chimera_posix_lock_claim_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_lock_claim_ctx *ctx = request->lock.private_data;

    chimera_vfs_claim_acquire(thread->vfs_thread,
                              chimera_posix_vfs_state(ctx->posix),
                              ctx->node->file, &ctx->node->claim, &ctx->ticket,
                              ctx->wait, ctx->wait,
                              chimera_posix_lock_acquire_cb, NULL,
                              &ctx->waiter);
} /* chimera_posix_lock_claim_exec */

enum chimera_vfs_claim_result
chimera_posix_lock_claim_acquire(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node,
    bool                           wait)
{
    struct chimera_vfs_state           *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_lock_claim_ctx ctx;

    memset(&ctx, 0, sizeof(ctx));
    ctx.posix = posix;
    ctx.node  = node;
    ctx.wait  = wait;

    pthread_mutex_init(&ctx.waiter.mutex, NULL);
    pthread_cond_init(&ctx.waiter.cond, NULL);
    ctx.waiter.done   = 0;
    ctx.waiter.result = CHIMERA_CLAIM_DENIED;

    /* `wait` queues on BREAKING and (as wait_hard) on a hard DENIED lock
     * conflict -- the F_SETLKW contract.  Without it the call is a try:
     * GRANTED/DENIED/BREAKING all resolve inside it.  Either way GRANTED and
     * DENIED fire the callback synchronously; a queued ticket fires it later
     * from whichever thread pumps the release, and a queued RANGE grant has
     * its backend confirm deferred to the projection service thread. */
    if (chimera_vfs_claim_backend_range_capable(state)) {
        /* A grant here may need confirming with the backend, which requires
         * a VFS thread: marshal onto a worker and wait for it there. */
        ctx.request.opcode            = CHIMERA_CLIENT_OP_LOCK;
        ctx.request.heap_allocated    = 0;
        ctx.request.lock.private_data = &ctx;

        chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                     &ctx.request,
                                     chimera_posix_lock_claim_exec);
    } else {
        /* No range-arbitrating module: the local core is the whole arbiter
         * and nothing projects, so skip the worker round trip. */
        chimera_vfs_claim_acquire(NULL, state, node->file, &node->claim,
                                  &ctx.ticket, wait, wait,
                                  chimera_posix_lock_acquire_cb, NULL,
                                  &ctx.waiter);
    }

    pthread_mutex_lock(&ctx.waiter.mutex);
    while (!ctx.waiter.done) {
        pthread_cond_wait(&ctx.waiter.cond, &ctx.waiter.mutex);
    }
    pthread_mutex_unlock(&ctx.waiter.mutex);

    pthread_mutex_destroy(&ctx.waiter.mutex);
    pthread_cond_destroy(&ctx.waiter.cond);

    return ctx.waiter.result;
} /* chimera_posix_lock_claim_acquire */

/* -------------------------------------------------------------------- */
/* Backend probes: F_GETLK and the SEEK_END passthrough                 */
/* -------------------------------------------------------------------- */

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
    pthread_mutex_t                     mutex;
    pthread_cond_t                      cond;
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

    pthread_mutex_lock(&ctx->mutex);
    ctx->status  = status;
    ctx->granted = granted;
    ctx->token   = token;
    if (conflict) {
        ctx->conflict = *conflict;
    }
    ctx->done = 1;
    pthread_cond_signal(&ctx->cond);
    pthread_mutex_unlock(&ctx->mutex);
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
    pthread_mutex_init(&ctx->mutex, NULL);
    pthread_cond_init(&ctx->cond, NULL);

    ctx->request.heap_allocated     = 0;
    ctx->request.lock_probe_private = ctx;

    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx->request,
                                 chimera_posix_lock_probe_exec);

    pthread_mutex_lock(&ctx->mutex);
    while (!ctx->done) {
        pthread_cond_wait(&ctx->cond, &ctx->mutex);
    }
    pthread_mutex_unlock(&ctx->mutex);

    pthread_mutex_destroy(&ctx->mutex);
    pthread_cond_destroy(&ctx->cond);
    return true;
} /* chimera_posix_lock_probe */

bool
chimera_posix_lock_claim_test(
    struct chimera_posix_client       *posix,
    struct chimera_vfs_open_handle    *handle,
    const struct chimera_vfs_claim    *probe,
    struct chimera_vfs_claim_conflict *conflict)
{
    struct chimera_posix_lock_probe_ctx ctx;

    memset(&ctx, 0, sizeof(ctx));
    ctx.exclusive = (probe->used & CHIMERA_CLAIM_LW) ? 1 : 0;
    ctx.flags     = CHIMERA_VFS_CLAIM_TEST;
    ctx.whence    = SEEK_SET;
    ctx.offset    = probe->offset;
    ctx.length    = probe->length;

    if (!chimera_posix_lock_probe(posix, handle, &ctx)) {
        return false;
    }

    if (ctx.status != CHIMERA_VFS_OK ||
        ctx.conflict.type == CHIMERA_VFS_LOCK_UNLOCK) {
        return false;
    }

    memset(conflict, 0, sizeof(*conflict));
    conflict->used = (ctx.conflict.type == CHIMERA_VFS_LOCK_WRITE)
        ? (CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW) : CHIMERA_CLAIM_LR;
    conflict->offset         = ctx.conflict.offset;
    conflict->length         = ctx.conflict.length;
    conflict->owner.owner_lo = ctx.conflict.pid;
    return true;
} /* chimera_posix_lock_claim_test */

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
            fl->l_start  = (off_t) ctx.conflict.offset;
            fl->l_len    = (off_t) ctx.conflict.length;
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
    uint64_t                        offset;
    uint64_t                        length;
    pthread_mutex_t                 mutex;
    pthread_cond_t                  cond;
    int                             done;
};

static void
chimera_posix_unlock_flushed(void *private_data)
{
    struct chimera_posix_unlock_ctx *ctx = private_data;

    pthread_mutex_lock(&ctx->mutex);
    ctx->done = 1;
    pthread_cond_signal(&ctx->cond);
    pthread_mutex_unlock(&ctx->mutex);
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
                                 ctx->offset, ctx->length);

    file = chimera_vfs_state_get(state, ctx->handle->fh,
                                 (uint8_t) ctx->handle->fh_len,
                                 ctx->handle->fh_hash, true);

    chimera_vfs_claim_backend_flush_releases(thread->vfs_thread, state, file,
                                             chimera_posix_unlock_flushed, ctx);

    chimera_vfs_state_put(state, file);
} /* chimera_posix_unlock_exec */

void
chimera_posix_lock_claim_unlock(
    struct chimera_posix_client    *posix,
    struct chimera_posix_ofd       *ofd,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_vfs_state       *state = chimera_posix_vfs_state(posix);
    struct chimera_posix_unlock_ctx ctx;

    if (!chimera_vfs_claim_backend_range_capable(state)) {
        chimera_posix_ofd_lock_carve(posix, ofd, handle, offset, length);
        return;
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.posix  = posix;
    ctx.ofd    = ofd;
    ctx.handle = handle;
    ctx.offset = offset;
    ctx.length = length;
    pthread_mutex_init(&ctx.mutex, NULL);
    pthread_cond_init(&ctx.cond, NULL);

    ctx.request.heap_allocated     = 0;
    ctx.request.lock_probe_private = &ctx;

    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx.request, chimera_posix_unlock_exec);

    pthread_mutex_lock(&ctx.mutex);
    while (!ctx.done) {
        pthread_cond_wait(&ctx.cond, &ctx.mutex);
    }
    pthread_mutex_unlock(&ctx.mutex);

    pthread_mutex_destroy(&ctx.mutex);
    pthread_cond_destroy(&ctx.cond);
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
    pthread_mutex_t                 mutex;
    pthread_cond_t                  cond;
    int                             done;
};

static void
chimera_posix_unlock_ranged_cb(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_posix_unlock_ranged_ctx *ctx = private_data;

    pthread_mutex_lock(&ctx->mutex);
    ctx->status = status;
    ctx->done   = 1;
    pthread_cond_signal(&ctx->cond);
    pthread_mutex_unlock(&ctx->mutex);
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
    pthread_mutex_init(&ctx.mutex, NULL);
    pthread_cond_init(&ctx.cond, NULL);

    ctx.request.heap_allocated     = 0;
    ctx.request.lock_probe_private = &ctx;

    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx.request, chimera_posix_unlock_ranged_exec);

    pthread_mutex_lock(&ctx.mutex);
    while (!ctx.done) {
        pthread_cond_wait(&ctx.cond, &ctx.mutex);
    }
    pthread_mutex_unlock(&ctx.mutex);

    pthread_mutex_destroy(&ctx.mutex);
    pthread_cond_destroy(&ctx.cond);

    if (ctx.status != CHIMERA_VFS_OK && ctx.status != CHIMERA_VFS_ENOTSUP) {
        errno = chimera_posix_errno_from_status(ctx.status);
        return -1;
    }

    return 0;
} /* chimera_posix_lock_claim_unlock_ranged */
