// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_umount_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_posix_request_complete(request, status);
}

static void
chimera_posix_umount_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_umount(thread, request);
}

int
chimera_posix_umount(const char *mount_path)
{
    struct chimera_posix_client   *posix  = chimera_posix_get_global();
    struct chimera_posix_worker   *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request  req;
    pthread_mutex_t                mutex  = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t                 cond   = PTHREAD_COND_INITIALIZER;

    chimera_posix_request_init(&req, &mutex, &cond);

    req.opcode              = CHIMERA_CLIENT_OP_UMOUNT;
    req.umount.callback     = chimera_posix_umount_callback;
    req.umount.private_data = &req;

    memcpy(req.umount.mount_path, mount_path, strlen(mount_path) + 1);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_umount_exec);

    int err = chimera_posix_wait(&req);

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
}
