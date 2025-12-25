// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_readlink.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

static void
chimera_posix_readlinkat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const char                   *target,
    int                           target_len,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    if (status == CHIMERA_VFS_OK) {
        request->sync_target_len = target_len;
    }

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_readlinkat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_readlink(thread, request);
}

SYMBOL_EXPORT ssize_t
chimera_posix_readlinkat(
    int    dirfd,
    const char *pathname,
    char       *buf,
    size_t      bufsiz)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    int                              path_len;

    // For now, only support AT_FDCWD
    if (dirfd != AT_FDCWD) {
        errno = ENOSYS;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    // Build path
    if (pathname[0] == '/') {
        path_len = strlen(pathname);
        memcpy(req.readlink.path, pathname, path_len);
    } else {
        req.readlink.path[0] = '/';
        path_len             = strlen(pathname);
        memcpy(req.readlink.path + 1, pathname, path_len);
        path_len++;
    }

    req.opcode                    = CHIMERA_CLIENT_OP_READLINK;
    req.readlink.callback         = chimera_posix_readlinkat_callback;
    req.readlink.private_data     = &comp;
    req.readlink.path_len         = path_len;
    req.readlink.target           = buf;
    req.readlink.target_maxlength = bufsiz;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_readlinkat_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return req.sync_target_len;
} /* chimera_posix_readlinkat */
