// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT CHIMERA_DIR *
chimera_posix_opendir(const char *path)
{
    struct chimera_posix_dir *dir;
    int                       fd;

    // Open the directory with O_DIRECTORY flag
    fd = chimera_posix_open(path, O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return NULL;
    }

    dir = calloc(1, sizeof(*dir));
    if (!dir) {
        chimera_posix_close(fd);
        errno = ENOMEM;
        return NULL;
    }

    dir->fd        = fd;
    dir->cookie    = 0;
    dir->eof       = 0;
    dir->buf_valid = 0;

    return dir;
} /* chimera_posix_opendir */

SYMBOL_EXPORT int
chimera_posix_closedir(CHIMERA_DIR *dirp)
{
    int rc;

    if (!dirp) {
        errno = EBADF;
        return -1;
    }

    rc = chimera_posix_close(dirp->fd);

    free(dirp);

    return rc;
} /* chimera_posix_closedir */

SYMBOL_EXPORT int
chimera_posix_dirfd(CHIMERA_DIR *dirp)
{
    if (!dirp) {
        errno = EINVAL;
        return -1;
    }

    return dirp->fd;
} /* chimera_posix_dirfd */
