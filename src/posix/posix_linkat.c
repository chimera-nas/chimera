// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_link.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif /* ifndef AT_FDCWD */

static void
chimera_posix_linkat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_linkat_callback */

static void
chimera_posix_linkat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_link(thread, request);
} /* chimera_posix_linkat_exec */

SYMBOL_EXPORT int
chimera_posix_linkat(
    int         olddirfd,
    const char *oldpath,
    int         newdirfd,
    const char *newpath,
    int         flags)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             old_path_len, new_path_len;
    const char                     *new_slash;

    // Note: flags like AT_SYMLINK_FOLLOW are not yet implemented
    (void) flags;

    // For now, only support AT_FDCWD for both paths
    if (olddirfd != AT_FDCWD || newdirfd != AT_FDCWD) {
        errno = ENOSYS;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    // Build source path
    if (oldpath[0] == '/') {
        old_path_len = strlen(oldpath);
        memcpy(req.link.source_path, oldpath, old_path_len);
    } else {
        req.link.source_path[0] = '/';
        old_path_len            = strlen(oldpath);
        memcpy(req.link.source_path + 1, oldpath, old_path_len);
        old_path_len++;
    }

    req.link.source_path_len   = old_path_len;
    req.link.source_parent_len = old_path_len;

    // Build dest path
    if (newpath[0] == '/') {
        new_path_len = strlen(newpath);
        memcpy(req.link.dest_path, newpath, new_path_len);
    } else {
        req.link.dest_path[0] = '/';
        new_path_len          = strlen(newpath);
        memcpy(req.link.dest_path + 1, newpath, new_path_len);
        new_path_len++;
    }

    req.link.dest_path[new_path_len] = '\0';
    new_slash                        = rindex(req.link.dest_path, '/');

    req.link.dest_path_len   = new_path_len;
    req.link.dest_parent_len = new_slash ? new_slash - req.link.dest_path : new_path_len;

    while (new_slash && *new_slash == '/') {
        new_slash++;
    }

    req.link.dest_name_offset = new_slash ? new_slash - req.link.dest_path : -1;

    req.opcode            = CHIMERA_CLIENT_OP_LINK;
    req.link.callback     = chimera_posix_linkat_callback;
    req.link.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_linkat_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_linkat */
