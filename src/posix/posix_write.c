// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_write.h"
#include "../client/client_fstat.h"

static void
chimera_posix_write_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    if (status == CHIMERA_VFS_OK) {
        request->sync_result = (ssize_t) request->write.length;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_write_callback */

static void
chimera_posix_write_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_write(thread, request);
} /* chimera_posix_write_exec */

static void
chimera_posix_fd_eof_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    if (status == CHIMERA_VFS_OK && st) {
        request->sync_stat = *st;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_fd_eof_callback */

static void
chimera_posix_fd_eof_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_fstat(thread, request);
} /* chimera_posix_fd_eof_exec */

int
chimera_posix_fd_eof(
    struct chimera_posix_worker   *worker,
    struct chimera_posix_fd_entry *entry,
    uint64_t                      *eof)
{
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             err;

    chimera_posix_completion_init(&comp, &req);

    req.opcode             = CHIMERA_CLIENT_OP_FSTAT;
    req.fstat.handle       = entry->handle;
    req.fstat.callback     = chimera_posix_fd_eof_callback;
    req.fstat.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fd_eof_exec);

    err = chimera_posix_wait(&comp);

    if (!err) {
        *eof = (uint64_t) req.sync_stat.st_size;
    }

    chimera_posix_completion_destroy(&comp);

    return err;
} /* chimera_posix_fd_eof */

/*
 * O_APPEND write: ONE caller-owned compound on one worker covering the
 * EOF-resolving getattr and the write itself, replacing the previous two
 * independent per-op compounds (fstat, then write).  The IO_ACTIVE gate
 * still serializes appends through one descriptor; cross-descriptor append
 * atomicity would need append support in the VFS write path, exactly as
 * before -- the compound groups the two ops, it does not make the
 * read-EOF/write pair atomic against other writers.
 */
struct chimera_posix_write_append_ctx {
    struct chimera_posix_completion comp;      /* must be first */
    struct chimera_vfs_compound    *compound;
    struct chimera_vfs_open_handle *handle;
    const void                     *buf;
    uint32_t                        count;
    uint64_t                        write_offset;
    enum chimera_vfs_error op_status;
};

static void
chimera_posix_write_append_end_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_posix_write_append_ctx *ctx = private_data;

    if (ctx->op_status == CHIMERA_VFS_OK && error_code != CHIMERA_VFS_OK) {
        ctx->op_status = error_code;
    }

    chimera_posix_complete(&ctx->comp, ctx->op_status);
} /* chimera_posix_write_append_end_callback */

static void
chimera_posix_write_append_write_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_write_append_ctx *ctx = private_data;

    ctx->op_status = status;

    if (status == CHIMERA_VFS_OK) {
        ctx->comp.request->sync_result = (ssize_t) ctx->count;
    }

    chimera_client_compound_end(thread,
                                chimera_client_req_cred(ctx->comp.request),
                                ctx->compound,
                                status == CHIMERA_VFS_OK ?
                                CHIMERA_VFS_COMPOUND_COMMIT_DURABLE :
                                CHIMERA_VFS_COMPOUND_ABORT,
                                chimera_posix_write_append_end_callback,
                                ctx);
} /* chimera_posix_write_append_write_callback */

static void
chimera_posix_write_append_eof_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_write_append_ctx *ctx     = private_data;
    struct chimera_client_request         *request = ctx->comp.request;

    if (status != CHIMERA_VFS_OK || !st) {
        ctx->op_status = status != CHIMERA_VFS_OK ? status : CHIMERA_VFS_EIO;
        chimera_client_compound_end(thread,
                                    chimera_client_req_cred(request),
                                    ctx->compound,
                                    CHIMERA_VFS_COMPOUND_ABORT,
                                    chimera_posix_write_append_end_callback,
                                    ctx);
        return;
    }

    ctx->write_offset = (uint64_t) st->st_size;

    request->opcode             = CHIMERA_CLIENT_OP_WRITE;
    request->write.handle       = ctx->handle;
    request->write.offset       = ctx->write_offset;
    request->write.length       = ctx->count;
    request->write.buf          = ctx->buf;
    request->write.callback     = chimera_posix_write_append_write_callback;
    request->write.private_data = ctx;

    chimera_dispatch_write_in(thread, request, ctx->compound);
} /* chimera_posix_write_append_eof_callback */

static void
chimera_posix_write_append_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_write_append_ctx *ctx = request->write.private_data;

    ctx->op_status = CHIMERA_VFS_OK;
    ctx->compound  = chimera_client_compound_begin(thread,
                                                   chimera_client_req_cred(request),
                                                   ctx->handle->fh,
                                                   ctx->handle->fh_len,
                                                   CHIMERA_VFS_COMPOUND_WRITE);

    request->opcode             = CHIMERA_CLIENT_OP_FSTAT;
    request->fstat.handle       = ctx->handle;
    request->fstat.callback     = chimera_posix_write_append_eof_callback;
    request->fstat.private_data = ctx;

    chimera_client_compound_run_in(thread, request, ctx->compound,
                                   chimera_fstat_start, chimera_fstat_reply);
} /* chimera_posix_write_append_exec */

SYMBOL_EXPORT ssize_t
chimera_posix_write(
    int         fd,
    const void *buf,
    size_t      count)
{
    struct chimera_posix_client          *posix = chimera_posix_get_global();
    struct chimera_posix_worker          *worker;
    struct chimera_client_request         req;
    struct chimera_posix_write_append_ctx ctx;
    struct chimera_posix_fd_entry        *entry;

    entry = chimera_posix_fd_acquire(posix, fd, CHIMERA_POSIX_FD_IO_ACTIVE);

    if (!entry) {
        return -1;
    }

    if (!chimera_posix_fd_may_write(entry)) {
        chimera_posix_fd_release(entry, CHIMERA_POSIX_FD_IO_ACTIVE);
        errno = EBADF;
        return -1;
    }

    chimera_posix_completion_init(&ctx.comp, &req);

    worker = chimera_posix_call_worker(posix, &ctx.comp);

    /* POSIX write(): under O_APPEND the file offset is set to the end of
     * the file prior to each write.  Both the EOF resolution and the write
     * run in one caller compound on the pinned worker (see the block
     * comment above). */
    if (entry->ofd->oflags & O_APPEND) {
        ctx.handle       = entry->handle;
        ctx.buf          = buf;
        ctx.count        = (uint32_t) count;
        ctx.write_offset = 0;

        req.opcode             = CHIMERA_CLIENT_OP_WRITE;
        req.write.private_data = &ctx;

        chimera_posix_worker_enqueue(worker, &req, chimera_posix_write_append_exec);
    } else {
        ctx.write_offset = entry->ofd->offset;

        req.opcode             = CHIMERA_CLIENT_OP_WRITE;
        req.write.callback     = chimera_posix_write_callback;
        req.write.private_data = &ctx.comp;
        req.write.handle       = entry->handle;
        req.write.offset       = ctx.write_offset;
        req.write.length       = count;
        req.write.buf          = buf;

        chimera_posix_worker_enqueue(worker, &req, chimera_posix_write_exec);
    }

    int     err = chimera_posix_wait(&ctx.comp);

    if (!err && req.sync_result >= 0) {
        entry->ofd->offset = ctx.write_offset + (uint64_t) req.sync_result;
    }

    ssize_t ret = req.sync_result;

    chimera_posix_completion_destroy(&ctx.comp);

    chimera_posix_fd_release(entry, CHIMERA_POSIX_FD_IO_ACTIVE);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
} /* chimera_posix_write */
