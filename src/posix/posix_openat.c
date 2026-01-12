// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_open.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif /* ifndef AT_FDCWD */

static void
chimera_posix_openat_callback(
    struct chimera_client_thread   *thread,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    comp->request->sync_open_handle = oh;
    chimera_posix_complete(comp, status);
} /* chimera_posix_openat_callback */

static void
chimera_posix_openat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    // If we have a parent handle (from a real fd), use open_at dispatch
    if (request->open.parent_handle) {
        chimera_dispatch_open_at(thread, request->open.parent_handle, request);
    } else {
        // Use the normal path-based open
        chimera_dispatch_open(thread, request);
    }
} /* chimera_posix_openat_exec */

SYMBOL_EXPORT int
chimera_posix_openat(
    int         dirfd,
    const char *pathname,
    int         flags,
    ...)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *dir_entry = NULL;
    mode_t                          mode      = 0;
    int                             path_len;
    const char                     *slash;

    // Handle optional mode argument
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, int);
        va_end(ap);
    }

    (void) mode;

    chimera_posix_completion_init(&comp, &req);

    // Handle AT_FDCWD case
    if (dirfd == AT_FDCWD) {
        // For AT_FDCWD with relative path, prepend "/"
        // For absolute path, use as-is
        if (pathname[0] == '/') {
            path_len = strlen(pathname);
            memcpy(req.open.path, pathname, path_len);
        } else {
            req.open.path[0] = '/';
            path_len         = strlen(pathname);
            memcpy(req.open.path + 1, pathname, path_len);
            path_len++;
        }

        req.open.path[path_len] = '\0';
        slash                   = rindex(req.open.path, '/');

        req.open.parent_handle = NULL;
        req.open.path_len      = path_len;
        req.open.parent_len    = slash ? slash - req.open.path : path_len;

        while (slash && *slash == '/') {
            slash++;
        }

        req.open.name_offset = slash ? slash - req.open.path : -1;
    } else {
        // Get the directory handle from the fd
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);
        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&comp);
            return -1;
        }

        // Copy the relative path
        path_len = strlen(pathname);
        memcpy(req.open.path, pathname, path_len);

        req.open.parent_handle = dir_entry->handle;
        req.open.path_len      = path_len;
        req.open.parent_len    = 0;
        req.open.name_offset   = 0;
    }

    req.opcode            = CHIMERA_CLIENT_OP_OPEN;
    req.open.callback     = chimera_posix_openat_callback;
    req.open.private_data = &comp;
    req.open.flags        = chimera_posix_to_chimera_flags(flags);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_openat_exec);

    int err = chimera_posix_wait(&comp);

    if (dir_entry) {
        chimera_posix_fd_release(dir_entry, 0);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    int fd = chimera_posix_fd_alloc(posix, req.sync_open_handle);

    if (fd < 0) {
        chimera_close(worker->client_thread, req.sync_open_handle);
        errno = EMFILE;
        return -1;
    }

    return fd;
} /* chimera_posix_openat */
