// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"
#include "../client/client_commit.h"

static void
chimera_posix_fsync_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_fsync_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_commit(thread, request);
}

SYMBOL_EXPORT int
chimera_posix_fsync(int fd)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry   *entry;
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode              = CHIMERA_CLIENT_OP_COMMIT;
    req.commit.handle       = entry->handle;
    req.commit.callback     = chimera_posix_fsync_callback;
    req.commit.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fsync_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fsync */
