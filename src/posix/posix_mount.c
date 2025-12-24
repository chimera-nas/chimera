// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_mount_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_mount_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_mount(thread, request);
}

int
chimera_posix_mount(
    const char *mount_path,
    const char *module_name,
    const char *module_path)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;

    chimera_posix_completion_init(&comp, &req);

    req.opcode             = CHIMERA_CLIENT_OP_MOUNT;
    req.mount.callback     = chimera_posix_mount_callback;
    req.mount.private_data = &comp;

    memcpy(req.mount.mount_path, mount_path, strlen(mount_path) + 1);
    memcpy(req.mount.module_name, module_name, strlen(module_name) + 1);
    memcpy(req.mount.module_path, module_path, strlen(module_path) + 1);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_mount_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
