// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>
#include <stdarg.h>

#include "posix_internal.h"

static void
chimera_posix_open_callback(
    struct chimera_client_thread   *thread,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    request->sync_open_handle = oh;
    chimera_posix_request_complete(request, status);
}

static void
chimera_posix_open_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_open(thread, request);
}

int
chimera_posix_open(
    const char *path,
    int         flags,
    ...)
{
    struct chimera_posix_client   *posix  = chimera_posix_get_global();
    struct chimera_posix_worker   *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request  req;
    pthread_mutex_t                mutex  = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t                 cond   = PTHREAD_COND_INITIALIZER;
    const char                    *slash;
    int                            path_len;

    mode_t mode = 0;

    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t) va_arg(ap, int);
        va_end(ap);
    }

    (void) mode;

    chimera_posix_request_init(&req, &mutex, &cond);

    path_len = strlen(path);
    slash    = rindex(path, '/');

    req.opcode            = CHIMERA_CLIENT_OP_OPEN;
    req.open.callback     = chimera_posix_open_callback;
    req.open.private_data = &req;
    req.open.flags        = chimera_posix_to_chimera_flags(flags);
    req.open.path_len     = path_len;
    req.open.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    req.open.name_offset = slash ? slash - path : -1;

    memcpy(req.open.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_open_exec);

    int err = chimera_posix_wait(&req);
    int fd  = -1;

    if (!err && req.sync_open_handle) {
        fd = chimera_posix_fd_put(posix, req.sync_open_handle);
        if (fd < 0) {
            chimera_close(worker->client_thread, req.sync_open_handle);
            err = EMFILE;
        }
    }

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);

    if (err) {
        errno = err;
        return -1;
    }

    return fd;
}
