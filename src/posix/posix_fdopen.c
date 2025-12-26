// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"

SYMBOL_EXPORT CHIMERA_FILE *
chimera_posix_fdopen(
    int         fd,
    const char *mode)
{
    struct chimera_posix_client   *posix = chimera_posix_get_global();
    struct chimera_posix_fd_entry *entry;

    (void) mode; /* In our implementation, mode is not used since fd already has access mode */

    /* Validate the fd */
    if (fd < 0 || fd >= posix->max_fds) {
        errno = EBADF;
        return NULL;
    }

    entry = &posix->fds[fd];

    /* Check if the fd is valid (has a handle) */
    pthread_mutex_lock(&entry->lock);

    if (!entry->handle || (entry->flags & CHIMERA_POSIX_FD_CLOSED)) {
        pthread_mutex_unlock(&entry->lock);
        errno = EBADF;
        return NULL;
    }

    pthread_mutex_unlock(&entry->lock);

    /* In our implementation, CHIMERA_FILE is the same as fd_entry pointer */
    return entry;
} /* chimera_posix_fdopen */
