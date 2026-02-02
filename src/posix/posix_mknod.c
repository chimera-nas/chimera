// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_mknod.h"

static void
chimera_posix_mknod_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_mknod_callback */

static void
chimera_posix_mknod_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_mknod(thread, request);
} /* chimera_posix_mknod_exec */

SYMBOL_EXPORT int
chimera_posix_mknod(
    const char *path,
    mode_t      mode,
    dev_t       dev)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    const char                     *slash;
    int                             path_len;

    chimera_posix_completion_init(&comp, &req);

    path_len = strlen(path);
    slash    = rindex(path, '/');

    req.opcode             = CHIMERA_CLIENT_OP_MKNOD;
    req.mknod.callback     = chimera_posix_mknod_callback;
    req.mknod.private_data = &comp;
    req.mknod.path_len     = path_len;
    req.mknod.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    req.mknod.name_offset = slash ? slash - path : -1;

    memcpy(req.mknod.path, path, path_len);

    req.mknod.set_attr.va_req_mask = 0;
    req.mknod.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
    req.mknod.set_attr.va_mode     = mode;
    req.mknod.set_attr.va_rdev     = dev;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_mknod_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_mknod */
