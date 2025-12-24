// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_read_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    struct evpl_iovec            *iov,
    int                           niov,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;
    size_t                           copied  = 0;

    if (status == CHIMERA_VFS_OK) {
        for (int i = 0; i < niov; i++) {
            size_t chunk = iov[i].length;

            if (copied + chunk > request->read.length) {
                chunk = request->read.length - copied;
            }

            memcpy((char *) request->read.buf + copied, iov[i].data, chunk);
            copied += chunk;
        }
        request->sync_result = (ssize_t) copied;
    }

    for (int i = 0; i < niov; i++) {
        evpl_iovec_release(&iov[i]);
    }

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_read_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_read(thread, request);
}

ssize_t
chimera_posix_read(
    int     fd,
    void   *buf,
    size_t  count)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    struct chimera_posix_fd_entry   *entry;
    struct chimera_vfs_open_handle  *handle;
    uint64_t                         offset;

    entry = chimera_posix_fd_get(posix, fd);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    chimera_posix_fd_lock(entry);

    if (!entry->in_use) {
        chimera_posix_fd_unlock(entry);
        errno = EBADF;
        return -1;
    }

    handle = entry->handle;
    offset = entry->offset;
    chimera_posix_fd_unlock(entry);

    chimera_posix_completion_init(&comp, &req);

    req.opcode            = CHIMERA_CLIENT_OP_READ;
    req.read.callback     = chimera_posix_read_callback;
    req.read.private_data = &comp;
    req.read.handle       = handle;
    req.read.offset       = offset;
    req.read.length       = count;
    req.read.buf          = buf;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_read_exec);

    int err = chimera_posix_wait(&comp);

    if (!err && req.sync_result >= 0) {
        chimera_posix_fd_lock(entry);
        if (entry->in_use) {
            entry->offset += (uint64_t) req.sync_result;
        }
        chimera_posix_fd_unlock(entry);
    }

    ssize_t ret = req.sync_result;

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
}
