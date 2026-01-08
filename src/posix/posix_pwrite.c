// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_write.h"

static void
chimera_posix_pwrite_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    if (status == CHIMERA_VFS_OK) {
        request->sync_result = (ssize_t) request->write.length;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_pwrite_callback */

static void
chimera_posix_pwrite_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_write(thread, request);
} /* chimera_posix_pwrite_exec */

SYMBOL_EXPORT ssize_t
chimera_posix_pwrite(
    int         fd,
    const void *buf,
    size_t      count,
    off_t       offset)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *entry;

    // pwrite doesn't need IO_ACTIVE serialization - just validate the fd
    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode             = CHIMERA_CLIENT_OP_WRITE;
    req.write.callback     = chimera_posix_pwrite_callback;
    req.write.private_data = &comp;
    req.write.handle       = entry->handle;
    req.write.offset       = (uint64_t) offset;  // Use caller-provided offset
    req.write.length       = count;

    int niov = evpl_iovec_alloc(worker->evpl, count, 1, CHIMERA_CLIENT_IOV_MAX, 0, req.write.iov);
    if (niov < 0) {
        chimera_posix_completion_destroy(&comp);
        chimera_posix_fd_release(entry, 0);
        errno = ENOMEM;
        return -1;
    }

    req.write.niov = niov;
    chimera_posix_iovec_memcpy(req.write.iov, buf, count);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_pwrite_exec);

    int     err = chimera_posix_wait(&comp);

    // pwrite does NOT update the file offset

    ssize_t ret = req.sync_result;

    chimera_posix_completion_destroy(&comp);

    chimera_posix_fd_release(entry, 0);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
} /* chimera_posix_pwrite */

SYMBOL_EXPORT ssize_t
chimera_posix_pwrite64(
    int         fd,
    const void *buf,
    size_t      count,
    int64_t     offset)
{
    return chimera_posix_pwrite(fd, buf, count, (off_t) offset);
} /* chimera_posix_pwrite64 */
