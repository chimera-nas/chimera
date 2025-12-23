// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"

static void
chimera_posix_readlink_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const char                   *target,
    int                           targetlen,
    void                         *private_data)
{
    struct chimera_posix_request *request = private_data;

    if (status == CHIMERA_VFS_OK) {
        size_t copy_len = targetlen < (int) request->u.readlink.buflen ? (size_t) targetlen : request->u.readlink.buflen;
        memcpy(request->u.readlink.buf, target, copy_len);
        request->result     = (ssize_t) copy_len;
        request->target_len = targetlen;
    }

    chimera_posix_request_finish(request, status);
}

void
chimera_posix_exec_readlink(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
    chimera_readlink(worker->client_thread,
                     request->u.readlink.path,
                     strlen(request->u.readlink.path),
                     request->u.readlink.buf,
                     request->u.readlink.buflen,
                     chimera_posix_readlink_complete,
                     request);
}

ssize_t
chimera_posix_readlink(
    const char *path,
    char       *buf,
    size_t      bufsiz)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (!posix) {
        errno = EINVAL;
        return -1;
    }

    struct chimera_posix_request *req    = chimera_posix_request_create(CHIMERA_POSIX_REQ_READLINK);
    struct chimera_posix_worker  *worker = chimera_posix_choose_worker(posix);

    req->u.readlink.path   = path;
    req->u.readlink.buf    = buf;
    req->u.readlink.buflen = bufsiz;

    chimera_posix_worker_enqueue(worker, req);

    int err = chimera_posix_wait(req);

    ssize_t ret = req->result;

    chimera_posix_request_destroy(req);

    if (err) {
        errno = err;
        return -1;
    }

    return ret;
}

