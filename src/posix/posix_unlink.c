// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_remove_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_posix_request_complete(request, status);
}

static void
chimera_posix_remove_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_remove(thread, request);
}

int
chimera_posix_unlink(const char *path)
{
    struct chimera_posix_client   *posix    = chimera_posix_get_global();
    struct chimera_posix_worker   *worker   = chimera_posix_choose_worker(posix);
    struct chimera_client_request  req;
    pthread_mutex_t                mutex    = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t                 cond     = PTHREAD_COND_INITIALIZER;
    const char                    *slash;
    int                            path_len;

    chimera_posix_request_init(&req, &mutex, &cond);

    path_len = strlen(path);
    slash    = rindex(path, '/');

    req.opcode              = CHIMERA_CLIENT_OP_REMOVE;
    req.remove.callback     = chimera_posix_remove_callback;
    req.remove.private_data = &req;
    req.remove.path_len     = path_len;
    req.remove.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    req.remove.name_offset = slash ? slash - path : -1;

    memcpy(req.remove.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_remove_exec);

    int err = chimera_posix_wait(&req);

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
