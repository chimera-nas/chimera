// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_mkdir.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif /* ifndef AT_FDCWD */

static void
chimera_posix_mkdirat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_mkdirat_callback */

static void
chimera_posix_mkdirat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    // If we have a parent handle (from a real fd), use mkdir_at dispatch
    if (request->mkdir.parent_handle) {
        chimera_dispatch_mkdir_at(thread, request->mkdir.parent_handle, request);
    } else {
        // Use the normal path-based mkdir
        chimera_dispatch_mkdir(thread, request);
    }
} /* chimera_posix_mkdirat_exec */

SYMBOL_EXPORT int
chimera_posix_mkdirat(
    int         dirfd,
    const char *pathname,
    mode_t      mode)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *dir_entry = NULL;
    int                             path_len;
    const char                     *slash;

    chimera_posix_completion_init(&comp, &req);

    // Handle AT_FDCWD case
    if (dirfd == AT_FDCWD) {
        if (pathname[0] == '/') {
            path_len = strlen(pathname);
            memcpy(req.mkdir.path, pathname, path_len);
        } else {
            req.mkdir.path[0] = '/';
            path_len          = strlen(pathname);
            memcpy(req.mkdir.path + 1, pathname, path_len);
            path_len++;
        }

        req.mkdir.path[path_len] = '\0';
        slash                    = rindex(req.mkdir.path, '/');

        req.mkdir.parent_handle = NULL;
        req.mkdir.path_len      = path_len;
        req.mkdir.parent_len    = slash ? slash - req.mkdir.path : path_len;

        while (slash && *slash == '/') {
            slash++;
        }

        req.mkdir.name_offset = slash ? slash - req.mkdir.path : -1;
    } else {
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);
        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&comp);
            return -1;
        }

        path_len = strlen(pathname);
        memcpy(req.mkdir.path, pathname, path_len);

        req.mkdir.parent_handle = dir_entry->handle;
        req.mkdir.path_len      = path_len;
        req.mkdir.parent_len    = 0;
        req.mkdir.name_offset   = 0;
    }

    req.opcode             = CHIMERA_CLIENT_OP_MKDIR;
    req.mkdir.callback     = chimera_posix_mkdirat_callback;
    req.mkdir.private_data = &comp;

    req.mkdir.set_attr.va_req_mask = 0;
    req.mkdir.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    req.mkdir.set_attr.va_mode     = mode;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_mkdirat_exec);

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
} /* chimera_posix_mkdirat */
