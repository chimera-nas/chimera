// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_remove.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

#ifndef AT_REMOVEDIR
#define AT_REMOVEDIR 0x200
#endif

static void
chimera_posix_unlinkat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_unlinkat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    // If we have a parent handle (from a real fd), use remove_at dispatch
    if (request->remove.parent_handle) {
        chimera_dispatch_remove_at(thread, request->remove.parent_handle, request);
    } else {
        // Use the normal path-based remove
        chimera_dispatch_remove(thread, request);
    }
}

SYMBOL_EXPORT int
chimera_posix_unlinkat(
    int         dirfd,
    const char *pathname,
    int         flags)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    struct chimera_posix_fd_entry   *dir_entry = NULL;
    int                              path_len;
    const char                      *slash;

    // Note: AT_REMOVEDIR flag is handled by the VFS layer
    // which will enforce directory-only removal semantics
    (void) flags;

    chimera_posix_completion_init(&comp, &req);

    // Handle AT_FDCWD case
    if (dirfd == AT_FDCWD) {
        if (pathname[0] == '/') {
            path_len = strlen(pathname);
            memcpy(req.remove.path, pathname, path_len);
        } else {
            req.remove.path[0] = '/';
            path_len           = strlen(pathname);
            memcpy(req.remove.path + 1, pathname, path_len);
            path_len++;
        }

        req.remove.path[path_len] = '\0';
        slash                     = rindex(req.remove.path, '/');

        req.remove.parent_handle = NULL;
        req.remove.path_len      = path_len;
        req.remove.parent_len    = slash ? slash - req.remove.path : path_len;

        while (slash && *slash == '/') {
            slash++;
        }

        req.remove.name_offset = slash ? slash - req.remove.path : -1;
    } else {
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);
        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&comp);
            return -1;
        }

        path_len = strlen(pathname);
        memcpy(req.remove.path, pathname, path_len);

        req.remove.parent_handle = dir_entry->handle;
        req.remove.path_len      = path_len;
        req.remove.parent_len    = 0;
        req.remove.name_offset   = 0;
    }

    req.opcode              = CHIMERA_CLIENT_OP_REMOVE;
    req.remove.callback     = chimera_posix_unlinkat_callback;
    req.remove.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_unlinkat_exec);

    int err = chimera_posix_wait(&comp);

    if (dir_entry) {
        chimera_posix_fd_release(dir_entry, 0);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_unlinkat */
