// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <limits.h>
#include <string.h>
#include <sys/uio.h>

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif /* ifndef IOV_MAX */

#include "posix_internal.h"
#include "../client/client_write.h"

static void
chimera_posix_writev_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    for (int i = 0; i < request->write.niov; i++) {
        evpl_iovec_release(&request->write.iov[i]);
    }

    if (status == CHIMERA_VFS_OK) {
        request->sync_result = (ssize_t) request->write.length;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_writev_callback */

static void
chimera_posix_writev_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_write(thread, request);
} /* chimera_posix_writev_exec */

static ssize_t
chimera_posix_writev_internal(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset,
    int                 use_fd_offset)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *entry;
    size_t                          total_len = 0;
    unsigned int                    flags;

    if (iovcnt <= 0 || iovcnt > IOV_MAX) {
        errno = EINVAL;
        return -1;
    }

    // Calculate total length
    for (int i = 0; i < iovcnt; i++) {
        total_len += iov[i].iov_len;
    }

    // If using fd offset, need IO_ACTIVE serialization
    flags = use_fd_offset ? CHIMERA_POSIX_FD_IO_ACTIVE : 0;
    entry = chimera_posix_fd_acquire(posix, fd, flags);

    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode             = CHIMERA_CLIENT_OP_WRITE;
    req.write.callback     = chimera_posix_writev_callback;
    req.write.private_data = &comp;
    req.write.handle       = entry->handle;
    req.write.offset       = use_fd_offset ? entry->offset : (uint64_t) offset;
    req.write.length       = total_len;

    int niov = evpl_iovec_alloc(worker->evpl, total_len, 1, CHIMERA_CLIENT_IOV_MAX, req.write.iov);
    if (niov < 0) {
        chimera_posix_completion_destroy(&comp);
        chimera_posix_fd_release(entry, flags);
        errno = ENOMEM;
        return -1;
    }

    req.write.niov = niov;

    // Copy from user iovecs to evpl iovecs
    size_t copied  = 0;
    int    dst_idx = 0;
    size_t dst_off = 0;

    for (int src_idx = 0; src_idx < iovcnt && dst_idx < niov; src_idx++) {
        size_t src_off = 0;

        while (src_off < iov[src_idx].iov_len && dst_idx < niov) {
            size_t src_avail = iov[src_idx].iov_len - src_off;
            size_t dst_avail = req.write.iov[dst_idx].length - dst_off;
            size_t chunk     = src_avail < dst_avail ? src_avail : dst_avail;

            memcpy((char *) req.write.iov[dst_idx].data + dst_off,
                   (char *) iov[src_idx].iov_base + src_off,
                   chunk);

            copied  += chunk;
            src_off += chunk;
            dst_off += chunk;

            if (dst_off >= req.write.iov[dst_idx].length) {
                dst_idx++;
                dst_off = 0;
            }
        }
    }

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_writev_exec);

    int     err = chimera_posix_wait(&comp);

    if (!err && req.sync_result >= 0 && use_fd_offset) {
        entry->offset += (uint64_t) req.sync_result;
    }

    ssize_t ret = req.sync_result;

    chimera_posix_completion_destroy(&comp);

    chimera_posix_fd_release(entry, flags);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
} /* chimera_posix_writev_internal */

SYMBOL_EXPORT ssize_t
chimera_posix_writev(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt)
{
    return chimera_posix_writev_internal(fd, iov, iovcnt, 0, 1);
} /* chimera_posix_writev */

SYMBOL_EXPORT ssize_t
chimera_posix_pwritev(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset)
{
    return chimera_posix_writev_internal(fd, iov, iovcnt, offset, 0);
} /* chimera_posix_pwritev */

SYMBOL_EXPORT ssize_t
chimera_posix_pwritev64(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset)
{
    return chimera_posix_writev_internal(fd, iov, iovcnt, (off_t) offset, 0);
} /* chimera_posix_pwritev64 */

SYMBOL_EXPORT ssize_t
chimera_posix_pwritev2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset,
    int                 flags)
{
    // Ignore RWF_HIPRI and RWF_NOWAIT for now - just behave as pwritev
    (void) flags;
    return chimera_posix_writev_internal(fd, iov, iovcnt, offset, 0);
} /* chimera_posix_pwritev2 */

SYMBOL_EXPORT ssize_t
chimera_posix_pwritev64v2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset,
    int                 flags)
{
    (void) flags;
    return chimera_posix_writev_internal(fd, iov, iovcnt, (off_t) offset, 0);
} /* chimera_posix_pwritev64v2 */
