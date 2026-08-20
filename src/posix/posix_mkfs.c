// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_mkfs_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_mkfs_callback */

static void
chimera_posix_mkfs_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_mkfs(thread, request);
} /* chimera_posix_mkfs_exec */

SYMBOL_EXPORT int
chimera_posix_mkfs(
    const char *module_name,
    const char *fsname,
    const char *options)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    chimera_posix_completion_init(&comp, &req);

    req.opcode            = CHIMERA_CLIENT_OP_MKFS;
    req.mkfs.callback     = chimera_posix_mkfs_callback;
    req.mkfs.private_data = &comp;

    memcpy(req.mkfs.module_name, module_name, strlen(module_name) + 1);
    memcpy(req.mkfs.fsname, fsname, strlen(fsname) + 1);

    if (options) {
        memcpy(req.mkfs.options, options, strlen(options) + 1);
    } else {
        req.mkfs.options[0] = '\0';
    }

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_mkfs_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_mkfs */

static void
chimera_posix_rmfs_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_rmfs(thread, request);
} /* chimera_posix_rmfs_exec */

SYMBOL_EXPORT int
chimera_posix_rmfs(
    const char *module_name,
    const char *fsname)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    chimera_posix_completion_init(&comp, &req);

    req.opcode            = CHIMERA_CLIENT_OP_RMFS;
    req.rmfs.callback     = chimera_posix_mkfs_callback;
    req.rmfs.private_data = &comp;

    memcpy(req.rmfs.module_name, module_name, strlen(module_name) + 1);
    memcpy(req.rmfs.fsname, fsname, strlen(fsname) + 1);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_rmfs_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_rmfs */
