// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_read_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    struct evpl_iovec            *iov,
    int                           niov,
    void                         *private_data)
{
    struct chimera_posix_request *request = private_data;
    size_t                        copied  = 0;
    int                           i;

    if (status == CHIMERA_VFS_OK) {
        for (i = 0; i < niov; i++) {
            size_t chunk = iov[i].length;

            if (copied + chunk > request->u.read.length) {
                chunk = request->u.read.length - copied;
            }

            memcpy((char *) request->u.read.buf + copied, iov[i].data, chunk);
            copied += chunk;
        }
    }

    for (i = 0; i < niov; i++) {
        evpl_iovec_release(&iov[i]);
    }

    if (status == CHIMERA_VFS_OK) {
        request->result = (ssize_t) copied;
    }

    chimera_posix_request_finish(request, status);
}

void
chimera_posix_exec_read(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_read(worker->client_thread,
                 request->u.read.handle,
                 request->u.read.offset,
                 request->u.read.length,
                 chimera_posix_read_complete,
                 request);
}

ssize_t
chimera_posix_read(
    int     fd,
    void   *buf,
    size_t  count)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    pthread_mutex_lock(&posix->fd_lock);
    struct chimera_posix_fd_entry *entry = chimera_posix_fd_get(posix, fd);
    struct chimera_vfs_open_handle *handle = entry ? entry->handle : NULL;
    uint64_t offset = entry ? entry->offset : 0;
    pthread_mutex_unlock(&posix->fd_lock);

    if (!handle) {
        errno = EBADF;
        return -1;
    }

    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_request *req    = chimera_posix_request_create(worker);

    req->u.read.handle = handle;
    req->u.read.offset = offset;
    req->u.read.length = count;
    req->u.read.buf    = buf;

    chimera_posix_worker_enqueue(worker, req, chimera_posix_exec_read);

    int err = chimera_posix_wait(req);

    if (!err && req->result >= 0) {
        pthread_mutex_lock(&posix->fd_lock);
        entry = chimera_posix_fd_get(posix, fd);
        if (entry) {
            entry->offset += (uint64_t) req->result;
        }
        pthread_mutex_unlock(&posix->fd_lock);
    }

    ssize_t ret = req->result;

    chimera_posix_request_release(worker, req);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
}
