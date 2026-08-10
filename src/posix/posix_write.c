// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_write.h"
#include "../client/client_fstat.h"

static void
chimera_posix_write_callback(
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
} /* chimera_posix_write_callback */

static void
chimera_posix_write_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_write(thread, request);
} /* chimera_posix_write_exec */

static void
chimera_posix_fd_eof_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    if (status == CHIMERA_VFS_OK && st) {
        request->sync_stat = *st;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_fd_eof_callback */

static void
chimera_posix_fd_eof_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_fstat(thread, request);
} /* chimera_posix_fd_eof_exec */

int
chimera_posix_fd_eof(
    struct chimera_posix_worker   *worker,
    struct chimera_posix_fd_entry *entry,
    uint64_t                      *eof)
{
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             err;

    chimera_posix_completion_init(&comp, &req);

    req.opcode             = CHIMERA_CLIENT_OP_FSTAT;
    req.fstat.handle       = entry->handle;
    req.fstat.callback     = chimera_posix_fd_eof_callback;
    req.fstat.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fd_eof_exec);

    err = chimera_posix_wait(&comp);

    if (!err) {
        *eof = (uint64_t) req.sync_stat.st_size;
    }

    chimera_posix_completion_destroy(&comp);

    return err;
} /* chimera_posix_fd_eof */

SYMBOL_EXPORT ssize_t
chimera_posix_write(
    int         fd,
    const void *buf,
    size_t      count)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *entry;

    entry = chimera_posix_fd_acquire(posix, fd, CHIMERA_POSIX_FD_IO_ACTIVE);

    if (!entry) {
        return -1;
    }

    if (!chimera_posix_fd_may_write(entry)) {
        chimera_posix_fd_release(entry, CHIMERA_POSIX_FD_IO_ACTIVE);
        errno = EBADF;
        return -1;
    }

    uint64_t write_offset = entry->offset;

    /* POSIX write(): under O_APPEND the file offset is set to the end of
     * the file prior to each write.  The IO_ACTIVE gate serializes appends
     * through this descriptor; cross-descriptor append atomicity would
     * need append support in the VFS write path. */
    if (entry->oflags & O_APPEND) {
        int aerr = chimera_posix_fd_eof(worker, entry, &write_offset);

        if (aerr) {
            chimera_posix_fd_release(entry, CHIMERA_POSIX_FD_IO_ACTIVE);
            errno = aerr;
            return -1;
        }
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode             = CHIMERA_CLIENT_OP_WRITE;
    req.write.callback     = chimera_posix_write_callback;
    req.write.private_data = &comp;
    req.write.handle       = entry->handle;
    req.write.offset       = write_offset;
    req.write.length       = count;
    req.write.buf          = buf;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_write_exec);

    int     err = chimera_posix_wait(&comp);

    if (!err && req.sync_result >= 0) {
        entry->offset = write_offset + (uint64_t) req.sync_result;
    }

    ssize_t ret = req.sync_result;

    chimera_posix_completion_destroy(&comp);

    chimera_posix_fd_release(entry, CHIMERA_POSIX_FD_IO_ACTIVE);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
} /* chimera_posix_write */
