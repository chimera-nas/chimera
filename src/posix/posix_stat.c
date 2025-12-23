// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_stat_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_request *request = private_data;

    if (status == CHIMERA_VFS_OK && st) {
        request->st = *st;
    }

    chimera_posix_request_finish(request, status);
}

void
chimera_posix_exec_stat(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_stat(worker->client_thread,
                 request->u.stat.path,
                 strlen(request->u.stat.path),
                 chimera_posix_stat_complete,
                 request);
}

int
chimera_posix_stat(
    const char  *path,
    struct stat *st)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (!posix) {
        errno = EINVAL;
        return -1;
    }

    struct chimera_posix_request *req    = chimera_posix_request_create(CHIMERA_POSIX_REQ_STAT);
    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);

    req->u.stat.path = path;

    chimera_posix_worker_enqueue(worker, req);

    int err = chimera_posix_wait(req);

    if (!err) {
        chimera_posix_fill_stat(st, &req->st);
    }

    chimera_posix_request_destroy(req);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}

