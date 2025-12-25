// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"
#include "../client/client_dup.h"

SYMBOL_EXPORT int
chimera_posix_dup(int oldfd)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_vfs_open_handle *handle;
    int                             newfd;

    entry = chimera_posix_fd_acquire(posix, oldfd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    handle = entry->handle;

    /* Increment the opencnt on the handle */
    chimera_dup_handle(worker->client_thread, handle);

    /* Allocate a new fd entry pointing to the same handle */
    newfd = chimera_posix_fd_alloc(posix, handle);

    if (newfd < 0) {
        /* Failed to allocate new fd - release the extra reference */
        chimera_close(worker->client_thread, handle);
        chimera_posix_fd_release(entry, 0);
        errno = EMFILE;
        return -1;
    }

    chimera_posix_fd_release(entry, 0);

    return newfd;
} /* chimera_posix_dup */
