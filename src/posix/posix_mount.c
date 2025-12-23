// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_mount_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_request *request = private_data;

    chimera_posix_request_finish(request, status);
}

void
chimera_posix_exec_mount(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_mount(worker->client_thread,
                  request->u.mount.mount_path,
                  request->u.mount.module_name,
                  request->u.mount.module_path,
                  chimera_posix_mount_complete,
                  request);
}

int
chimera_posix_mount(
    const char *mount_path,
    const char *module_name,
    const char *module_path)
{
    struct chimera_posix_client  *posix  = chimera_posix_get_global();
    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_request *req    = chimera_posix_request_create(worker);

    req->u.mount.mount_path  = mount_path;
    req->u.mount.module_name = module_name;
    req->u.mount.module_path = module_path;

    chimera_posix_worker_enqueue(worker, req, chimera_posix_exec_mount);

    int err = chimera_posix_wait(req);

    chimera_posix_request_release(worker, req);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
