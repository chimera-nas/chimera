// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_stat_callback(
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
}

static void
chimera_posix_stat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_stat(thread, request);
}

int
chimera_posix_stat(
    const char  *path,
    struct stat *st)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;
    int                              path_len;

    chimera_posix_completion_init(&comp, &req);

    path_len = strlen(path);

    req.opcode            = CHIMERA_CLIENT_OP_STAT;
    req.stat.callback     = chimera_posix_stat_callback;
    req.stat.private_data = &comp;
    req.stat.path_len     = path_len;

    memcpy(req.stat.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_stat_exec);

    int err = chimera_posix_wait(&comp);

    if (!err) {
        chimera_posix_fill_stat(st, &req.sync_stat);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
