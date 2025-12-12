// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_remove_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_request *request = private_data;

    chimera_posix_request_finish(request, status);
} // chimera_posix_remove_complete

void
chimera_posix_exec_remove(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request)
{
    chimera_remove(worker->client_thread,
                   request->u.remove.path,
                   strlen(request->u.remove.path),
                   chimera_posix_remove_complete,
                   request);
} // chimera_posix_exec_remove

int
chimera_posix_unlink(const char *path)
{
    struct chimera_posix_client  *posix = chimera_posix_get_global();

    if (!posix) {
        errno = EINVAL;
        return -1;
    }

    struct chimera_posix_request *req    = chimera_posix_request_create(CHIMERA_POSIX_REQ_REMOVE);
    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);

    req->u.remove.path = strdup(path);

    chimera_posix_worker_enqueue(worker, req);

    int                           err = chimera_posix_wait(req);

    free(req->u.remove.path);
    chimera_posix_request_destroy(req);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} // chimera_posix_unlink

