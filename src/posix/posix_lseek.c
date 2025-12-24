// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"

SYMBOL_EXPORT off_t
chimera_posix_lseek(
    int   fd,
    off_t offset,
    int   whence)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    off_t                        file_size = 0;

    if (whence == SEEK_END) {
        struct stat st;

        if (chimera_posix_fstat(fd, &st) < 0) {
            return -1;
        }

        file_size = st.st_size;
    }

    return chimera_posix_fd_lseek(posix, fd, offset, whence, file_size);
}

SYMBOL_EXPORT int64_t
chimera_posix_lseek64(
    int     fd,
    int64_t offset,
    int     whence)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int64_t                      file_size = 0;

    if (whence == SEEK_END) {
        struct stat st;

        if (chimera_posix_fstat(fd, &st) < 0) {
            return -1;
        }

        file_size = st.st_size;
    }

    return (int64_t) chimera_posix_fd_lseek(posix, fd, (off_t) offset, whence,
                                            (off_t) file_size);
}
