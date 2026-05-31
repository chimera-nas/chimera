// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"
#include "../client/client_read_into.h"

static void
chimera_posix_read_into_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint32_t                      count,
    uint32_t                      eof,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    /* The data is already in the caller's evpl_iovec(s) -- nothing to copy. */
    if (status == CHIMERA_VFS_OK) {
        request->sync_result = (ssize_t) count;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_read_into_callback */

static void
chimera_posix_read_into_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_read_into(thread, request);
} /* chimera_posix_read_into_exec */

static ssize_t
chimera_posix_read_into_common(
    int                fd,
    struct evpl_iovec *iov,
    int                niov,
    size_t             count,
    off_t              offset,
    int                use_fd_offset)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *entry;

    if (niov < 0 || niov > CHIMERA_CLIENT_IOV_MAX) {
        errno = EINVAL;
        return -1;
    }

    entry = chimera_posix_fd_acquire(posix, fd, CHIMERA_POSIX_FD_IO_ACTIVE);

    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode                 = CHIMERA_CLIENT_OP_READ;
    req.read_into.callback     = chimera_posix_read_into_callback;
    req.read_into.private_data = &comp;
    req.read_into.handle       = entry->handle;
    req.read_into.offset       = use_fd_offset ? entry->offset : (uint64_t) offset;
    req.read_into.length       = count;
    req.read_into.dest_niov    = niov;

    for (int i = 0; i < niov; i++) {
        req.read_into.dest_iov[i] = iov[i];
    }

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_read_into_exec);

    int     err = chimera_posix_wait(&comp);

    if (!err && use_fd_offset && req.sync_result >= 0) {
        entry->offset += (uint64_t) req.sync_result;
    }

    ssize_t ret = req.sync_result;

    chimera_posix_completion_destroy(&comp);

    chimera_posix_fd_release(entry, CHIMERA_POSIX_FD_IO_ACTIVE);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
} /* chimera_posix_read_into_common */

SYMBOL_EXPORT ssize_t
chimera_posix_read_into(
    int                fd,
    struct evpl_iovec *iov,
    int                niov,
    size_t             count)
{
    return chimera_posix_read_into_common(fd, iov, niov, count, 0, 1);
} /* chimera_posix_read_into */

SYMBOL_EXPORT ssize_t
chimera_posix_pread_into(
    int                fd,
    struct evpl_iovec *iov,
    int                niov,
    size_t             count,
    off_t              offset)
{
    return chimera_posix_read_into_common(fd, iov, niov, count, offset, 0);
} /* chimera_posix_pread_into */
