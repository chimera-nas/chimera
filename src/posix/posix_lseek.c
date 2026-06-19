// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE 1
#include <errno.h>
#include <unistd.h>

#include "posix_internal.h"
#include "../client/client_seek.h"

/* Carries the VFS seek result back through the blocking completion. */
struct chimera_posix_seek_ctx {
    struct chimera_posix_completion comp;
    uint64_t                        r_offset;
    int                             r_eof;
};

static void
chimera_posix_seek_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    int                           eof,
    uint64_t                      offset,
    void                         *private_data)
{
    struct chimera_posix_seek_ctx *ctx = private_data;

    ctx->r_eof    = eof;
    ctx->r_offset = offset;
    chimera_posix_complete(&ctx->comp, status);
} /* chimera_posix_seek_callback */

static void
chimera_posix_seek_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_seek(thread, request);
} /* chimera_posix_seek_exec */

/*
 * lseek(2) SEEK_DATA / SEEK_HOLE: ask the backend for the next data region or
 * hole at/after `offset`, set the descriptor offset to the result, and return
 * it.  `what` is 0 for SEEK_DATA, 1 for SEEK_HOLE (the VFS/NFSv4 convention).
 * Returns -1 with errno (ENXIO at/after EOF, or for SEEK_DATA past the last
 * data region).
 */
static off_t
chimera_posix_lseek_hole_data(
    struct chimera_posix_client *posix,
    int                          fd,
    off_t                        offset,
    uint32_t                     what)
{
    struct chimera_posix_worker   *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry *entry;
    struct chimera_client_request  req;
    struct chimera_posix_seek_ctx  ctx;

    if (offset < 0) {
        errno = EINVAL;
        return -1;
    }

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    ctx.r_offset = 0;
    ctx.r_eof    = 0;
    chimera_posix_completion_init(&ctx.comp, &req);

    req.opcode            = CHIMERA_CLIENT_OP_SEEK;
    req.seek.handle       = entry->handle;
    req.seek.offset       = (uint64_t) offset;
    req.seek.what         = what;
    req.seek.callback     = chimera_posix_seek_callback;
    req.seek.private_data = &ctx;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_seek_exec);

    int err = chimera_posix_wait(&ctx.comp);

    if (!err) {
        pthread_mutex_lock(&entry->lock);
        entry->offset = ctx.r_offset;
        pthread_mutex_unlock(&entry->lock);
    }

    chimera_posix_fd_release(entry, 0);
    chimera_posix_completion_destroy(&ctx.comp);

    if (err) {
        errno = err;
        return -1;
    }

    return (off_t) ctx.r_offset;
} /* chimera_posix_lseek_hole_data */

SYMBOL_EXPORT off_t
chimera_posix_lseek(
    int   fd,
    off_t offset,
    int   whence)
{
    struct chimera_posix_client *posix     = chimera_posix_get_global();
    off_t                        file_size = 0;

    if (whence == SEEK_DATA) {
        return chimera_posix_lseek_hole_data(posix, fd, offset, 0);
    }

    if (whence == SEEK_HOLE) {
        return chimera_posix_lseek_hole_data(posix, fd, offset, 1);
    }

    if (whence == SEEK_END) {
        struct stat st;

        if (chimera_posix_fstat(fd, &st) < 0) {
            return -1;
        }

        file_size = st.st_size;
    }

    return chimera_posix_fd_lseek(posix, fd, offset, whence, file_size);
} /* chimera_posix_lseek */

SYMBOL_EXPORT int64_t
chimera_posix_lseek64(
    int     fd,
    int64_t offset,
    int     whence)
{
    struct chimera_posix_client *posix     = chimera_posix_get_global();
    int64_t                      file_size = 0;

    if (whence == SEEK_DATA) {
        return chimera_posix_lseek_hole_data(posix, fd, (off_t) offset, 0);
    }

    if (whence == SEEK_HOLE) {
        return chimera_posix_lseek_hole_data(posix, fd, (off_t) offset, 1);
    }

    if (whence == SEEK_END) {
        struct stat st;

        if (chimera_posix_fstat(fd, &st) < 0) {
            return -1;
        }

        file_size = st.st_size;
    }

    return (int64_t) chimera_posix_fd_lseek(posix, fd, (off_t) offset, whence,
                                            (off_t) file_size);
} /* chimera_posix_lseek64 */
