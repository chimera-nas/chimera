// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_stat.h"

#ifndef AT_FDCWD
#define AT_FDCWD            -100
#endif /* ifndef AT_FDCWD */

#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif /* ifndef AT_SYMLINK_NOFOLLOW */

static void
chimera_posix_fstatat_callback(
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
} /* chimera_posix_fstatat_callback */

static void
chimera_posix_fstatat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_stat(thread, request);
} /* chimera_posix_fstatat_exec */

SYMBOL_EXPORT int
chimera_posix_fstatat(
    int          dirfd,
    const char  *pathname,
    struct stat *statbuf,
    int          flags)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *dir_entry = NULL;
    int                             path_len;

    chimera_posix_completion_init(&comp, &req);

    /*
     * An absolute path ignores dirfd entirely (POSIX); a relative one walks
     * from the descriptor's open directory handle, the way openat/mkdirat
     * already do.  AT_FDCWD keeps the historical behaviour of rooting a
     * relative path at the export root.
     */
    if (dirfd != AT_FDCWD && pathname[0] != '/') {
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);

        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&comp);
            return -1;
        }

        path_len = strlen(pathname);
        memcpy(req.stat.path, pathname, path_len);
    } else if (pathname[0] == '/') {
        path_len = strlen(pathname);
        memcpy(req.stat.path, pathname, path_len);
    } else {
        req.stat.path[0] = '/';
        path_len         = strlen(pathname);
        memcpy(req.stat.path + 1, pathname, path_len);
        path_len++;
    }

    req.stat.handle       = dir_entry ? dir_entry->handle : NULL;
    req.opcode            = CHIMERA_CLIENT_OP_STAT;
    req.stat.callback     = chimera_posix_fstatat_callback;
    req.stat.private_data = &comp;
    /* AT_SYMLINK_NOFOLLOW selects lstat semantics; the flag was previously
     * ignored and req.stat.flags left uninitialized. */
    req.stat.flags = (flags & AT_SYMLINK_NOFOLLOW) ?
        0 : CHIMERA_VFS_LOOKUP_FOLLOW;
    req.stat.path_len = path_len;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fstatat_exec);

    int err = chimera_posix_wait(&comp);

    if (dir_entry) {
        chimera_posix_fd_release(dir_entry, 0);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    chimera_posix_fill_stat(statbuf, &req.sync_stat);

    return 0;
} /* chimera_posix_fstatat */
