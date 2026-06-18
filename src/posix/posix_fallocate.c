// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_allocate.h"

static void
chimera_posix_fallocate_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_fallocate_callback */

static void
chimera_posix_fallocate_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_allocate(thread, request);
} /* chimera_posix_fallocate_exec */

/*
 * posix_fallocate(3): reserve space for [offset, offset+len) and grow the file
 * to offset+len if it is currently shorter.  Returns 0 on success or -1 with
 * errno set.  len == 0 (and negative offset/len) are invalid -> EINVAL; a
 * descriptor not open for writing -> EBADF.
 */
SYMBOL_EXPORT int
chimera_posix_fallocate(
    int   fd,
    off_t offset,
    off_t len)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    if (offset < 0 || len <= 0) {
        errno = EINVAL;
        return -1;
    }

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    /* posix_fallocate requires the descriptor be open for writing. */
    if ((entry->oflags & O_ACCMODE) == O_RDONLY) {
        chimera_posix_fd_release(entry, 0);
        errno = EBADF;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode                = CHIMERA_CLIENT_OP_ALLOCATE;
    req.allocate.handle       = entry->handle;
    req.allocate.offset       = (uint64_t) offset;
    req.allocate.length       = (uint64_t) len;
    req.allocate.flags        = 0;
    req.allocate.callback     = chimera_posix_fallocate_callback;
    req.allocate.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fallocate_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fallocate */
