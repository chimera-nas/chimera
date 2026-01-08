// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_read.h"

static void
chimera_posix_pread_callback(
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
        evpl_iovec_release(thread->vfs_thread->evpl, &iov[i]);
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_pread_callback */

static void
chimera_posix_pread_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_read(thread, request);
} /* chimera_posix_pread_exec */

SYMBOL_EXPORT ssize_t
chimera_posix_pread(
    int    fd,
    void  *buf,
    size_t count,
    off_t  offset)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *entry;

    // pread doesn't need IO_ACTIVE serialization - just validate the fd
    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode            = CHIMERA_CLIENT_OP_READ;
    req.read.callback     = chimera_posix_pread_callback;
    req.read.private_data = &comp;
    req.read.handle       = entry->handle;
    req.read.offset       = (uint64_t) offset;  // Use caller-provided offset
    req.read.length       = count;
    req.read.buf          = buf;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_pread_exec);

    int     err = chimera_posix_wait(&comp);

    // pread does NOT update the file offset

    ssize_t ret = req.sync_result;

    chimera_posix_completion_destroy(&comp);

    chimera_posix_fd_release(entry, 0);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
} /* chimera_posix_pread */

SYMBOL_EXPORT ssize_t
chimera_posix_pread64(
    int     fd,
    void   *buf,
    size_t  count,
    int64_t offset)
{
    return chimera_posix_pread(fd, buf, count, (off_t) offset);
} /* chimera_posix_pread64 */
