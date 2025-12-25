// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"
#include "../client/client_readdir.h"

struct chimera_posix_readdir_ctx {
    struct chimera_posix_completion  comp;
    struct chimera_posix_dir        *dir;
    int                              got_entry;
};

static int
chimera_posix_readdir_callback(
    struct chimera_client_thread *thread,
    const struct chimera_dirent  *dirent,
    void                         *private_data)
{
    struct chimera_posix_readdir_ctx *ctx = private_data;
    struct chimera_posix_dir         *dir = ctx->dir;

    // Copy the entry to our buffer
    dir->buf       = *dirent;
    dir->buf_valid = 1;
    dir->cookie    = dirent->cookie;
    ctx->got_entry = 1;

    // Return non-zero to stop iteration after first entry
    return 1;
} /* chimera_posix_readdir_callback */

static void
chimera_posix_readdir_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint64_t                      cookie,
    int                           eof,
    void                         *private_data)
{
    struct chimera_posix_readdir_ctx *ctx = private_data;
    struct chimera_posix_dir         *dir = ctx->dir;

    dir->eof    = eof;
    dir->cookie = cookie;

    chimera_posix_complete(&ctx->comp, status);
} /* chimera_posix_readdir_complete */

static void
chimera_posix_readdir_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_readdir(thread->vfs_thread,
                        request->readdir.handle,
                        0,  // attr_mask for entries
                        0,  // dir_attr_mask
                        request->readdir.cookie,
                        chimera_readdir_entry_callback,
                        chimera_readdir_complete,
                        request);
} /* chimera_posix_readdir_exec */

SYMBOL_EXPORT struct dirent *
chimera_posix_readdir(CHIMERA_DIR *dirp)
{
    struct chimera_posix_client      *posix  = chimera_posix_get_global();
    struct chimera_posix_worker      *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request     req;
    struct chimera_posix_readdir_ctx  ctx;
    struct chimera_posix_fd_entry    *entry;
    int                               err;

    if (!dirp) {
        errno = EBADF;
        return NULL;
    }

    // If we've reached EOF, return NULL
    if (dirp->eof) {
        return NULL;
    }

    // Get the fd entry
    entry = chimera_posix_fd_acquire(posix, dirp->fd, 0);
    if (!entry) {
        return NULL;
    }

    chimera_posix_completion_init(&ctx.comp, &req);
    ctx.dir       = dirp;
    ctx.got_entry = 0;

    req.opcode               = CHIMERA_CLIENT_OP_READDIR;
    req.readdir.callback     = chimera_posix_readdir_callback;
    req.readdir.complete     = chimera_posix_readdir_complete;
    req.readdir.private_data = &ctx;
    req.readdir.handle       = entry->handle;
    req.readdir.cookie       = dirp->cookie;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_readdir_exec);

    err = chimera_posix_wait(&ctx.comp);

    chimera_posix_completion_destroy(&ctx.comp);
    chimera_posix_fd_release(entry, 0);

    if (err) {
        errno = err;
        return NULL;
    }

    if (!ctx.got_entry) {
        // No more entries (EOF)
        return NULL;
    }

    // Fill in the POSIX dirent structure
    dirp->entry.d_ino = dirp->buf.ino;
    memcpy(dirp->entry.d_name, dirp->buf.name, dirp->buf.namelen);
    dirp->entry.d_name[dirp->buf.namelen] = '\0';

    return &dirp->entry;
} /* chimera_posix_readdir */
