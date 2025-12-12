// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>
#include <stdarg.h>

#include "posix_internal.h"

static void
chimera_posix_open_complete(
    struct chimera_client_thread   *thread,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_posix_request *request = private_data;

    if (status == CHIMERA_VFS_OK) {
        request->handle = oh;
    }

    chimera_posix_request_finish(request, status);
}

void
chimera_posix_exec_open(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_open(worker->client_thread,
                 request->u.open.path,
                 strlen(request->u.open.path),
                 request->u.open.flags,
                 chimera_posix_open_complete,
                 request);
}

int
chimera_posix_open(
    const char *path,
    int         flags,
    ...)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (!posix) {
        errno = EINVAL;
        return -1;
    }

    mode_t mode = 0;

    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t) va_arg(ap, int);
        va_end(ap);
    }

    (void) mode;

    struct chimera_posix_request *req    = chimera_posix_request_create(CHIMERA_POSIX_REQ_OPEN);
    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);

    req->u.open.path  = strdup(path);
    req->u.open.flags = chimera_posix_to_chimera_flags(flags);

    chimera_posix_worker_enqueue(worker, req);

    int err = chimera_posix_wait(req);
    int fd  = -1;

    if (!err && req->handle) {
        fd = chimera_posix_fd_put(posix, req->handle);
        if (fd < 0) {
            chimera_close(worker->client_thread, req->handle);
            err = EMFILE;
        }
    }

    free(req->u.open.path);
    chimera_posix_request_destroy(req);

    if (err) {
        errno = err;
        return -1;
    }

    return fd;
}

