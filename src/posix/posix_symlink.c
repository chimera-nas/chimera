// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_symlink.h"

static void
chimera_posix_symlink_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_symlink_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_symlink(thread, request);
}

SYMBOL_EXPORT int
chimera_posix_symlink(
    const char *target,
    const char *path)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    const char                      *slash;
    int                              path_len;
    int                              target_len;

    chimera_posix_completion_init(&comp, &req);

    path_len   = strlen(path);
    target_len = strlen(target);
    slash      = rindex(path, '/');

    req.opcode               = CHIMERA_CLIENT_OP_SYMLINK;
    req.symlink.callback     = chimera_posix_symlink_callback;
    req.symlink.private_data = &comp;
    req.symlink.path_len     = path_len;
    req.symlink.parent_len   = slash ? slash - path : path_len;
    req.symlink.target_len   = target_len;

    while (slash && *slash == '/') {
        slash++;
    }

    req.symlink.name_offset = slash ? slash - path : -1;

    memcpy(req.symlink.path, path, path_len);
    memcpy(req.symlink.target, target, target_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_symlink_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
