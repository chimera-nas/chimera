// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_close(int fd)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_vfs_open_handle *handle;

    entry = chimera_posix_fd_acquire(posix, fd, CHIMERA_POSIX_FD_CLOSING);

    if (!entry) {
        return -1;
    }

    handle = entry->handle;

    chimera_close(worker->client_thread, handle);

    chimera_posix_fd_release(entry, CHIMERA_POSIX_FD_CLOSING);

    chimera_posix_fd_free(posix, fd);

    return 0;
} /* chimera_posix_close */
