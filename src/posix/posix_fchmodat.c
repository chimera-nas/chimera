// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_setattr.h"

#ifndef AT_FDCWD
#define AT_FDCWD            -100
#endif /* ifndef AT_FDCWD */

#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif /* ifndef AT_SYMLINK_NOFOLLOW */

static void
chimera_posix_fchmodat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_fchmodat_callback */

static void
chimera_posix_fchmodat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* A real dirfd resolves relative to the descriptor; AT_FDCWD and an
     * absolute path take the path-based setattr from the export root, the
     * symlink-keeping variant under AT_SYMLINK_NOFOLLOW. */
    if (request->setattr.parent_handle) {
        chimera_dispatch_setattr_at(thread, request);
    } else if (request->setattr.nofollow) {
        chimera_dispatch_lsetattr(thread, request);
    } else {
        chimera_dispatch_setattr(thread, request);
    }
} /* chimera_posix_fchmodat_exec */

SYMBOL_EXPORT int
chimera_posix_fchmodat(
    int         dirfd,
    const char *pathname,
    mode_t      mode,
    int         flags)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *dir_entry = NULL;
    int                             path_len;
    int                             err;

    /* An empty path names nothing -- see openat. */
    if (pathname[0] == '\0') {
        errno = ENOENT;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    /* AT_FDCWD, and an absolute path whatever dirfd holds (POSIX -- see
     * openat): the path-based setattr. */
    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        if (pathname[0] == '/') {
            path_len = strlen(pathname);
            memcpy(req.setattr.path, pathname, path_len);
        } else {
            req.setattr.path[0] = '/';
            path_len            = strlen(pathname);
            memcpy(req.setattr.path + 1, pathname, path_len);
            path_len++;
        }

        req.setattr.path[path_len] = '\0';
        req.setattr.parent_handle  = NULL;
        req.setattr.dir_open_flags = 0;
    } else {
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);
        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&comp);
            return -1;
        }

        path_len = strlen(pathname);
        memcpy(req.setattr.path, pathname, path_len);

        req.setattr.parent_handle  = dir_entry->handle;
        req.setattr.dir_open_flags = chimera_posix_fd_open_flags(dir_entry);
    }

    req.opcode               = CHIMERA_CLIENT_OP_SETATTR;
    req.setattr.callback     = chimera_posix_fchmodat_callback;
    req.setattr.private_data = &comp;
    req.setattr.path_len     = path_len;
    req.setattr.nofollow     = (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0;

    req.setattr.set_attr.va_req_mask = 0;
    req.setattr.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    req.setattr.set_attr.va_mode     = mode;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fchmodat_exec);

    err = chimera_posix_wait(&comp);

    if (dir_entry) {
        chimera_posix_fd_release(dir_entry, 0);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fchmodat */
