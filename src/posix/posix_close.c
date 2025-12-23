// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"

void
chimera_posix_exec_close(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_close(worker->client_thread, request->u.close.handle);
    chimera_posix_request_finish(request, CHIMERA_VFS_OK);
}

int
chimera_posix_close(
    int fd)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    pthread_mutex_lock(&posix->fd_lock);
    struct chimera_posix_fd_entry *entry = chimera_posix_fd_get(posix, fd);
    struct chimera_vfs_open_handle *handle = entry ? entry->handle : NULL;
    pthread_mutex_unlock(&posix->fd_lock);

    if (!handle) {
        errno = EBADF;
        return -1;
    }

    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_request *req    = chimera_posix_request_create(worker);

    req->u.close.handle = handle;

    chimera_posix_worker_enqueue(worker, req, chimera_posix_exec_close);

    int err = chimera_posix_wait(req);

    chimera_posix_fd_clear(posix, fd);
    chimera_posix_request_release(worker, req);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
