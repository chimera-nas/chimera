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
 * but a backend that arbitrates locks itself (CAP_FS_LOCK: the nfs/smb
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
        !(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_FS_LOCK)) {
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

enum chimera_vfs_claim_result
chimera_posix_lock_claim_acquire_wait(
    struct chimera_posix_client   *posix,
    struct chimera_posix_ofd_lock *node)
{
    struct chimera_vfs_state          *state = chimera_posix_vfs_state(posix);
    struct chimera_vfs_pending_acquire ticket;
    struct chimera_posix_lock_waiter   waiter;

    pthread_mutex_init(&waiter.mutex, NULL);
    pthread_cond_init(&waiter.cond, NULL);
    waiter.done   = 0;
    waiter.result = CHIMERA_CLAIM_DENIED;

    memset(&ticket, 0, sizeof(ticket));

    /* wait queues on BREAKING; wait_hard additionally queues on a hard
     * DENIED lock conflict -- the F_SETLKW contract.  GRANTED/DENIED fire
     * the callback synchronously inside the call; a queued ticket fires it
     * later from whichever thread pumps the release. */
    /* CLAIMTODO: pass the worker's vfs thread once plumbed so posix locks
     * project to CAP_LEASE backends; the kernel OP_LOCK passthrough covers
     * cross-process arbitration meanwhile. */
    chimera_vfs_claim_acquire(NULL, state, node->file, &node->claim, &ticket,
                              /* wait */ true, /* wait_hard */ true,
                              chimera_posix_lock_acquire_cb, NULL, &waiter);

    pthread_mutex_lock(&waiter.mutex);
    while (!waiter.done) {
        pthread_cond_wait(&waiter.cond, &waiter.mutex);
    }
    pthread_mutex_unlock(&waiter.mutex);

    pthread_mutex_destroy(&waiter.mutex);
    pthread_cond_destroy(&waiter.cond);

    return waiter.result;
} /* chimera_posix_lock_claim_acquire_wait */
