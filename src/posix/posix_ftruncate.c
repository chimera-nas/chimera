// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>

#include "posix_internal.h"
#include "../client/client_fsetattr.h"

static void
chimera_posix_ftruncate_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
}

static void
chimera_posix_ftruncate_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_fsetattr(thread, request);
}

SYMBOL_EXPORT int
chimera_posix_ftruncate(
    int   fd,
    off_t length)
{
    struct chimera_posix_client     *posix  = chimera_posix_get_global();
    struct chimera_posix_worker     *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry   *entry;
    struct chimera_client_request    req;
    struct chimera_posix_completion  comp;

    if (length < 0) {
        errno = EINVAL;
        return -1;
    }

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode                = CHIMERA_CLIENT_OP_FSETATTR;
    req.fsetattr.handle       = entry->handle;
    req.fsetattr.callback     = chimera_posix_ftruncate_callback;
    req.fsetattr.private_data = &comp;

    req.fsetattr.set_attr.va_req_mask = CHIMERA_VFS_ATTR_SIZE;
    req.fsetattr.set_attr.va_set_mask = 0;
    req.fsetattr.set_attr.va_size     = (uint64_t) length;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_ftruncate_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_ftruncate */
