// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_readlink_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const char                   *target,
    int                           targetlen,
    void                         *private_data)
{
    struct chimera_client_request *request = private_data;

    if (status == CHIMERA_VFS_OK) {
        request->sync_result     = (ssize_t) targetlen;
        request->sync_target_len = targetlen;
    }

    chimera_posix_request_complete(request, status);
}

static void
chimera_posix_readlink_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_readlink(thread, request);
}

ssize_t
chimera_posix_readlink(
    const char *path,
    char       *buf,
    size_t      bufsiz)
{
    struct chimera_posix_client   *posix    = chimera_posix_get_global();
    struct chimera_posix_worker   *worker   = chimera_posix_choose_worker(posix);
    struct chimera_client_request  req;
    pthread_mutex_t                mutex    = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t                 cond     = PTHREAD_COND_INITIALIZER;
    int                            path_len;

    chimera_posix_request_init(&req, &mutex, &cond);

    path_len = strlen(path);

    req.opcode                    = CHIMERA_CLIENT_OP_READLINK;
    req.readlink.callback         = chimera_posix_readlink_callback;
    req.readlink.private_data     = &req;
    req.readlink.path_len         = path_len;
    req.readlink.target           = buf;
    req.readlink.target_maxlength = bufsiz;

    memcpy(req.readlink.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_readlink_exec);

    int err = chimera_posix_wait(&req);

    ssize_t ret = req.sync_result;

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
}
