// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* POSIX owns argument normalization and the synchronous application boundary.
 * The shared VFS lock domain owns intervals, backend tokens, wait tickets and
 * accepted publication. No granted node is subsequently tracked by an OFD. */
#include "posix_internal.h"
#include "vfs/vfs_lock.h"

struct chimera_posix_lock_ctx {
    struct chimera_client_request     request;
    struct chimera_posix_completion   completion;
    struct chimera_posix_client      *posix;
    struct chimera_vfs_open_handle   *handle;
    struct chimera_vfs_lock_request   input;
    struct chimera_vfs_claim_conflict conflict;
    uint32_t                          pid;
    int                               test, release, index;
};

static void
chimera_posix_lock_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_posix_lock_ctx *ctx    = private_data;
    enum chimera_vfs_error         status = chimera_vfs_compound_status(compound);

    /* No application flock or ownership state is changed before acceptance. */
    if (status == CHIMERA_VFS_OK && ctx->test && ctx->index >= 0) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, ctx->index);
        ctx->conflict = op->claim_conflict;
        ctx->pid      = op->lock_pid;
    }
    chimera_vfs_compound_free(compound);
    chimera_posix_complete(&ctx->completion, status);
} /* chimera_posix_lock_complete */

static void
chimera_posix_lock_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_lock_ctx *ctx      = request->lock_probe_private;
    struct chimera_vfs_compound   *compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, chimera_client_req_cred(request));

    if (ctx->release) {
        ctx->index = chimera_vfs_compound_add_lock_release_owner(compound,
                                                                 ctx->posix->lock_domain, ctx->handle, &ctx->input.owner
                                                                 );
    } else if (ctx->test) {
        ctx->index = chimera_vfs_compound_add_lock_test(compound,
                                                        ctx->posix->lock_domain, ctx->handle, &ctx->input);
    } else {
        ctx->index = chimera_vfs_compound_add_lock_change(compound,
                                                          ctx->posix->lock_domain, ctx->handle, &ctx->input);
    }
    chimera_frontend_compound_submit(compound, chimera_posix_lock_complete, ctx);
} /* chimera_posix_lock_exec */

int
chimera_posix_lock_compound(
    struct chimera_posix_client   *posix,
    struct chimera_posix_fd_entry *entry,
    int                            cmd,
    struct flock                  *fl,
    uint32_t                       type,
    int32_t                        whence,
    uint64_t                       offset,
    uint64_t                       length)
{
    struct chimera_posix_lock_ctx ctx = { 0 };
    int                           error;

    ctx.posix                 = posix;
    ctx.handle                = entry->handle;
    ctx.test                  = cmd == F_GETLK;
    ctx.input.type            = type;
    ctx.input.whence          = whence;
    ctx.input.offset          = offset;
    ctx.input.length          = length;
    ctx.input.wait            = cmd == F_SETLKW;
    ctx.input.project_backend = true;
    chimera_posix_lock_owner_init(&ctx.input.owner);
    chimera_posix_completion_init(&ctx.completion, &ctx.request);
    ctx.request.lock_probe_private = &ctx;

    /* Capture admission while close cannot cross this descriptor's cutoff.
     * The immutable generation also covers a close on another descriptor for
     * this process/file, including close before the worker starts execution. */
    pthread_mutex_lock(&entry->lock);
    if (entry->flags & (CHIMERA_POSIX_FD_CLOSING | CHIMERA_POSIX_FD_CLOSED)) {
        pthread_mutex_unlock(&entry->lock);
        chimera_posix_completion_destroy(&ctx.completion);
        errno = EBADF;
        return -1;
    }
    ctx.input.generation = chimera_vfs_lock_domain_admit(posix->lock_domain,
                                                         ctx.handle, &ctx.input.owner);
    pthread_mutex_unlock(&entry->lock);
    if (!ctx.input.generation) {
        chimera_posix_completion_destroy(&ctx.completion);
        errno = ENOMEM;
        return -1;
    }
    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx.request, chimera_posix_lock_exec);
    error = chimera_posix_wait(&ctx.completion);
    chimera_posix_completion_destroy(&ctx.completion);
    if (error) {
        errno = error;
        return -1;
    }
    if (ctx.test) {
        if (!(ctx.conflict.used & (CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW))) {
            fl->l_type = F_UNLCK;
        } else {
            fl->l_type   = (ctx.conflict.used & CHIMERA_CLAIM_LW) ? F_WRLCK : F_RDLCK;
            fl->l_whence = SEEK_SET;
            fl->l_start  = (off_t) ctx.conflict.offset;
            fl->l_len    = ctx.conflict.length == UINT64_MAX ? 0 : (off_t) ctx.conflict.length;
            fl->l_pid    = (pid_t) ctx.pid;
        }
    }
    return 0;
} /* chimera_posix_lock_compound */

void
chimera_posix_locks_retire_file(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_claim_owner owner;

    chimera_posix_lock_owner_init(&owner);
    chimera_vfs_lock_domain_retire(posix->lock_domain, handle->fh, handle->fh_len, &owner);
} /* chimera_posix_locks_retire_file */

int
chimera_posix_locks_release_file(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_posix_lock_ctx ctx = { 0 };

    ctx.posix   = posix;
    ctx.handle  = handle;
    ctx.release = 1;
    chimera_posix_lock_owner_init(&ctx.input.owner);
    chimera_posix_completion_init(&ctx.completion, &ctx.request);
    ctx.request.lock_probe_private = &ctx;
    chimera_posix_worker_enqueue(chimera_posix_choose_worker(posix),
                                 &ctx.request, chimera_posix_lock_exec);
    int error = chimera_posix_wait(&ctx.completion);
    chimera_posix_completion_destroy(&ctx.completion);
    return error;
} /* chimera_posix_locks_release_file */

static void
chimera_posix_locks_shutdown_done(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_posix_completion *completion = private_data;

    chimera_posix_complete(completion, status);
} /* chimera_posix_locks_shutdown_done */

static void
chimera_posix_locks_shutdown_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_lock_ctx *ctx = request->lock_probe_private;

    chimera_vfs_lock_domain_shutdown_async(thread->vfs_thread, ctx->posix->lock_domain,
                                           chimera_posix_locks_shutdown_done, &ctx->completion);
} /* chimera_posix_locks_shutdown_exec */

void
chimera_posix_locks_shutdown(struct chimera_posix_client *posix)
{
    struct chimera_posix_lock_ctx ctx = { .posix = posix };

    chimera_posix_completion_init(&ctx.completion, &ctx.request);
    ctx.request.lock_probe_private = &ctx;
    chimera_posix_worker_enqueue(&posix->workers[0], &ctx.request, chimera_posix_locks_shutdown_exec);
    (void) chimera_posix_wait(&ctx.completion);
    chimera_posix_completion_destroy(&ctx.completion);
} /* chimera_posix_locks_shutdown */
