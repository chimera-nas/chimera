// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_symlink_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_request *request = private_data;

    chimera_posix_request_finish(request, status);
}

void
chimera_posix_exec_symlink(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_symlink(worker->client_thread,
                    request->u.symlink.path,
                    strlen(request->u.symlink.path),
                    request->u.symlink.target,
                    strlen(request->u.symlink.target),
                    chimera_posix_symlink_complete,
                    request);
}

int
chimera_posix_symlink(
    const char *target,
    const char *path)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (!posix) {
        errno = EINVAL;
        return -1;
    }

    struct chimera_posix_request *req    = chimera_posix_request_create(CHIMERA_POSIX_REQ_SYMLINK);
    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);

    req->u.symlink.path   = path;
    req->u.symlink.target = target;

    chimera_posix_worker_enqueue(worker, req);

    int err = chimera_posix_wait(req);

    chimera_posix_request_destroy(req);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}

