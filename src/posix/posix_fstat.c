// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_fstat.h"

static void
chimera_posix_fstat_callback(
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
} /* chimera_posix_fstat_callback */

static void
chimera_posix_fstat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_fstat(thread, request);
} /* chimera_posix_fstat_exec */

SYMBOL_EXPORT int
chimera_posix_fstat(
    int          fd,
    struct stat *st)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode             = CHIMERA_CLIENT_OP_FSTAT;
    req.fstat.handle       = entry->handle;
    req.fstat.callback     = chimera_posix_fstat_callback;
    req.fstat.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fstat_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    if (!err) {
        chimera_posix_fill_stat(st, &req.sync_stat);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fstat */
