// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <limits.h>
#include <string.h>
#include <sys/uio.h>

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

#include "posix_internal.h"
#include "../client/client_read.h"

static void
chimera_posix_readv_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    struct evpl_iovec            *iov,
    int                           niov,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;
    size_t                           copied  = 0;
    size_t                           src_off = 0;
    int                              src_idx = 0;

    if (status == CHIMERA_VFS_OK) {
        // Copy from evpl iovecs to user iovecs stored in request
        struct iovec *user_iov  = (struct iovec *) request->read.buf;
        int           user_niov = request->read.niov;
        size_t        dst_off   = 0;
        int           dst_idx   = 0;

        while (src_idx < niov && dst_idx < user_niov) {
            size_t src_avail = iov[src_idx].length - src_off;
            size_t dst_avail = user_iov[dst_idx].iov_len - dst_off;
            size_t chunk     = src_avail < dst_avail ? src_avail : dst_avail;

            memcpy((char *) user_iov[dst_idx].iov_base + dst_off,
                   (char *) iov[src_idx].data + src_off,
                   chunk);

            copied  += chunk;
            src_off += chunk;
            dst_off += chunk;

            if (src_off >= iov[src_idx].length) {
                src_idx++;
                src_off = 0;
            }

            if (dst_off >= user_iov[dst_idx].iov_len) {
                dst_idx++;
                dst_off = 0;
            }
        }

        request->sync_result = (ssize_t) copied;
    }

    for (int i = 0; i < niov; i++) {
        evpl_iovec_release(&iov[i]);
    }

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_readv_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_read(thread, request);
}

static ssize_t
chimera_posix_readv_internal(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset,
    int                 use_fd_offset)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    struct chimera_posix_fd_entry   *entry;
    size_t                           total_len = 0;
    unsigned int                     flags;

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

    req.opcode            = CHIMERA_CLIENT_OP_READ;
    req.read.callback     = chimera_posix_readv_callback;
    req.read.private_data = &comp;
    req.read.handle       = entry->handle;
    req.read.offset       = use_fd_offset ? entry->offset : (uint64_t) offset;
    req.read.length       = total_len;
    req.read.buf          = (void *) iov;  // Store user iovec pointer
    req.read.niov         = iovcnt;        // Store user iovec count

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_readv_exec);

    int err = chimera_posix_wait(&comp);

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
}

SYMBOL_EXPORT ssize_t
chimera_posix_readv(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt)
{
    return chimera_posix_readv_internal(fd, iov, iovcnt, 0, 1);
}

SYMBOL_EXPORT ssize_t
chimera_posix_preadv(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset)
{
    return chimera_posix_readv_internal(fd, iov, iovcnt, offset, 0);
}

SYMBOL_EXPORT ssize_t
chimera_posix_preadv64(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset)
{
    return chimera_posix_readv_internal(fd, iov, iovcnt, (off_t) offset, 0);
}

SYMBOL_EXPORT ssize_t
chimera_posix_preadv2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset,
    int                 flags)
{
    // Ignore RWF_HIPRI and RWF_NOWAIT for now - just behave as preadv
    (void) flags;
    return chimera_posix_readv_internal(fd, iov, iovcnt, offset, 0);
}

SYMBOL_EXPORT ssize_t
chimera_posix_preadv64v2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset,
    int                 flags)
{
    (void) flags;
    return chimera_posix_readv_internal(fd, iov, iovcnt, (off_t) offset, 0);
}
