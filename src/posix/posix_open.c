// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>
#include <stdarg.h>

#include "posix_internal.h"
#include "../client/client_open.h"
#include "../client/client_fstat.h"
#include "../client/client_fsetattr.h"

static void
chimera_posix_open_callback(
    struct chimera_client_thread   *thread,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    request->sync_open_handle = oh;
    chimera_posix_complete(comp, status);
} /* chimera_posix_open_callback */

static void
chimera_posix_open_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_open(thread, request);
} /* chimera_posix_open_exec */

/*
 * O_TRUNC (writable) open: ONE caller-owned compound on one worker covering
 * open + getattr + the fallback truncate.
 *
 * The CHIMERA_VFS_OPEN_TRUNCATE flag rides into the open itself, and the
 * memfs/cairn/diskfs backends honor it on their open paths, so the open
 * usually truncates atomically.  The explicit in-compound truncate leg stays
 * because the flag alone is not honest everywhere:
 *   - the linux/io_uring passthrough backends silently DROP the flag (their
 *     open-flag translators never emit O_TRUNC);
 *   - a non-create open resolves through the open cache, and a cache HIT
 *     returns the existing handle without ever dispatching a backend open,
 *     so even an honoring backend can be skipped;
 *   - O_TRUNC marks mtime/ctime and clears set-user/group-ID even when the
 *     file is already empty, which a skipped backend open would lose.
 * The fstat leg exists only to skip the truncate on non-regular targets
 * (O_TRUNC is unspecified for them); as before, failures of the fallback
 * legs do not fail the open.
 */
struct chimera_posix_open_trunc_ctx {
    struct chimera_posix_completion comp;      /* must be first */
    struct chimera_vfs_compound    *compound;
    struct chimera_vfs_open_handle *oh;
    enum chimera_vfs_error op_status;
};

static void
chimera_posix_open_trunc_end_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_posix_open_trunc_ctx *ctx     = private_data;
    struct chimera_client_request       *request = ctx->comp.request;

    if (ctx->op_status == CHIMERA_VFS_OK && error_code != CHIMERA_VFS_OK) {
        /* The open succeeded but the commit did not (e.g. the retriable
         * ECOMPOUND_EXHAUSTED): the handle is not deliverable as a result. */
        if (ctx->oh) {
            chimera_vfs_release(request->thread->vfs_thread, ctx->oh);
            ctx->oh                   = NULL;
            request->sync_open_handle = NULL;
        }
        ctx->op_status = error_code;
    }

    chimera_posix_complete(&ctx->comp, ctx->op_status);
} /* chimera_posix_open_trunc_end_callback */

static void
chimera_posix_open_trunc_commit(
    struct chimera_client_thread        *thread,
    struct chimera_posix_open_trunc_ctx *ctx)
{
    chimera_client_compound_end(thread,
                                chimera_client_req_cred(ctx->comp.request),
                                ctx->compound,
                                ctx->op_status == CHIMERA_VFS_OK ?
                                CHIMERA_VFS_COMPOUND_COMMIT_DURABLE :
                                CHIMERA_VFS_COMPOUND_ABORT,
                                chimera_posix_open_trunc_end_callback,
                                ctx);
} /* chimera_posix_open_trunc_commit */

static void
chimera_posix_open_trunc_setattr_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_open_trunc_ctx *ctx = private_data;

    /* The open stands regardless of the fallback truncate's outcome
     * (matches the historical behavior, which ignored the ftruncate
     * result). */
    (void) status;

    chimera_posix_open_trunc_commit(thread, ctx);
} /* chimera_posix_open_trunc_setattr_callback */

static void
chimera_posix_open_trunc_fstat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_open_trunc_ctx *ctx     = private_data;
    struct chimera_client_request       *request = ctx->comp.request;

    if (status == CHIMERA_VFS_OK && st && S_ISREG(st->st_mode)) {
        request->opcode                = CHIMERA_CLIENT_OP_FSETATTR;
        request->fsetattr.handle       = ctx->oh;
        request->fsetattr.callback     = chimera_posix_open_trunc_setattr_callback;
        request->fsetattr.private_data = ctx;

        request->fsetattr.set_attr.va_req_mask = 0;
        request->fsetattr.set_attr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        request->fsetattr.set_attr.va_size     = 0;

        chimera_dispatch_fsetattr_in(thread, request, ctx->compound);
        return;
    }

    /* fstat failure or a non-regular target: keep the open, skip the
     * truncate (matches the historical behavior). */
    chimera_posix_open_trunc_commit(thread, ctx);
} /* chimera_posix_open_trunc_fstat_callback */

static void
chimera_posix_open_trunc_open_callback(
    struct chimera_client_thread   *thread,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_posix_open_trunc_ctx *ctx     = private_data;
    struct chimera_client_request       *request = ctx->comp.request;

    request->sync_open_handle = oh;
    ctx->oh                   = oh;

    if (status != CHIMERA_VFS_OK || !oh) {
        ctx->op_status = status != CHIMERA_VFS_OK ? status : CHIMERA_VFS_EIO;
        chimera_posix_open_trunc_commit(thread, ctx);   /* aborts */
        return;
    }

    request->opcode             = CHIMERA_CLIENT_OP_FSTAT;
    request->fstat.handle       = oh;
    request->fstat.callback     = chimera_posix_open_trunc_fstat_callback;
    request->fstat.private_data = ctx;

    chimera_dispatch_fstat_in(thread, request, ctx->compound);
} /* chimera_posix_open_trunc_open_callback */

static void
chimera_posix_open_trunc_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_open_trunc_ctx *ctx = request->open.private_data;

    ctx->op_status = CHIMERA_VFS_OK;
    ctx->oh        = NULL;
    ctx->compound  = chimera_client_compound_begin(thread,
                                                   chimera_client_req_cred(request),
                                                   thread->client->root_fh,
                                                   thread->client->root_fh_len,
                                                   CHIMERA_VFS_COMPOUND_WRITE);

    chimera_dispatch_open_in(thread, request, ctx->compound);
} /* chimera_posix_open_trunc_exec */

SYMBOL_EXPORT int
chimera_posix_open(
    const char *path,
    int         flags,
    ...)
{
    struct chimera_posix_client        *posix = chimera_posix_get_global();
    struct chimera_posix_worker        *worker;
    struct chimera_client_request       req;
    struct chimera_posix_open_trunc_ctx ctx;
    const char                         *slash;
    int                                 path_len;
    int                                 want_trunc;

    mode_t                              mode = 0;

    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t) va_arg(ap, int);
        va_end(ap);
    }

    path_len = chimera_posix_check_path(path);
    if (path_len < 0) {
        return -1;
    }

    /* O_TRUNC on a writable open: truncate to zero length on open.  The
     * truncate leg runs in the open's own caller compound (see the block
     * comment above); it runs even when the file is already empty, because
     * O_TRUNC marks mtime/ctime and clears set-user/group-ID regardless of
     * the size.  Read-only O_TRUNC keeps the historical behavior: the flag
     * still rides into the open, no explicit truncate leg. */
    want_trunc = (flags & O_TRUNC) && (flags & O_ACCMODE) != O_RDONLY;

    chimera_posix_completion_init(&ctx.comp, &req);

    worker = chimera_posix_call_worker(posix, &ctx.comp);

    slash = rindex(path, '/');

    req.opcode          = CHIMERA_CLIENT_OP_OPEN;
    req.open.flags      = chimera_posix_to_chimera_flags(flags);
    req.open.path_len   = path_len;
    req.open.parent_len = slash ? slash - path : path_len;

    if (want_trunc) {
        req.open.callback     = chimera_posix_open_trunc_open_callback;
        req.open.private_data = &ctx;
    } else {
        req.open.callback     = chimera_posix_open_callback;
        req.open.private_data = &ctx.comp;
    }

    while (slash && *slash == '/') {
        slash++;
    }

    req.open.name_offset = slash ? slash - path : -1;

    if (flags & O_CREAT) {
        chimera_posix_set_create_mode(&req.open.set_attr, mode);
    } else {
        chimera_posix_no_create_mode(&req.open.set_attr);
    }

    memcpy(req.open.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req,
                                 want_trunc ? chimera_posix_open_trunc_exec
                                            : chimera_posix_open_exec);

    int err = chimera_posix_wait(&ctx.comp);
    int fd  = -1;

    if (!err && req.sync_open_handle) {
        fd = chimera_posix_fd_alloc(posix, req.sync_open_handle);
        if (fd < 0) {
            chimera_posix_close_on_worker(worker, req.sync_open_handle);
            err = EMFILE;
        } else {
            posix->fds[fd].ofd->oflags = (unsigned int) flags;
        }
    }

    chimera_posix_completion_destroy(&ctx.comp);

    if (err) {
        errno = err;
        return -1;
    }

    return fd;
} /* chimera_posix_open */
