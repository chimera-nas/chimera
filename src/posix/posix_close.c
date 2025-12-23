// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"

int
chimera_posix_close(int fd)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_vfs_open_handle *handle;

    pthread_mutex_lock(&posix->fd_lock);
    struct chimera_posix_fd_entry *entry = chimera_posix_fd_get(posix, fd);
    handle = entry ? entry->handle : NULL;
    pthread_mutex_unlock(&posix->fd_lock);

    if (!handle) {
        errno = EBADF;
        return -1;
    }

    chimera_close(worker->client_thread, handle);
    chimera_posix_fd_clear(posix, fd);

    return 0;
}
