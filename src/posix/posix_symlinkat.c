// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_symlink.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

static void
chimera_posix_symlinkat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_symlinkat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_symlink(thread, request);
}

SYMBOL_EXPORT int
chimera_posix_symlinkat(
    const char *target,
    int         newdirfd,
    const char *linkpath)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    int                              path_len, target_len;
    const char                      *slash;

    // For now, only support AT_FDCWD
    if (newdirfd != AT_FDCWD) {
        errno = ENOSYS;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    // Build linkpath
    if (linkpath[0] == '/') {
        path_len = strlen(linkpath);
        memcpy(req.symlink.path, linkpath, path_len);
    } else {
        req.symlink.path[0] = '/';
        path_len            = strlen(linkpath);
        memcpy(req.symlink.path + 1, linkpath, path_len);
        path_len++;
    }

    req.symlink.path[path_len] = '\0';
    slash                      = rindex(req.symlink.path, '/');

    req.symlink.path_len   = path_len;
    req.symlink.parent_len = slash ? slash - req.symlink.path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    req.symlink.name_offset = slash ? slash - req.symlink.path : -1;

    // Copy target (symlink content)
    target_len = strlen(target);
    memcpy(req.symlink.target, target, target_len);
    req.symlink.target_len = target_len;

    req.opcode               = CHIMERA_CLIENT_OP_SYMLINK;
    req.symlink.callback     = chimera_posix_symlinkat_callback;
    req.symlink.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_symlinkat_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_symlinkat */
