// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_umount_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_request *request = private_data;

    chimera_posix_request_finish(request, status);
}

void
chimera_posix_exec_umount(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_umount(worker->client_thread,
                   request->u.umount.mount_path,
                   chimera_posix_umount_complete,
                   request);
}

int
chimera_posix_umount(
    const char *mount_path)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (!posix) {
        errno = EINVAL;
        return -1;
    }

    struct chimera_posix_request *req    = chimera_posix_request_create(CHIMERA_POSIX_REQ_UMOUNT);
    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);

    req->u.umount.mount_path = strdup(mount_path);

    chimera_posix_worker_enqueue(worker, req);

    int err = chimera_posix_wait(req);

    free(req->u.umount.mount_path);
    chimera_posix_request_destroy(req);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}

