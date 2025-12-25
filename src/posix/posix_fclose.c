// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_fclose(CHIMERA_FILE *stream)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();
    int                          fd;

    if (!stream) {
        errno = EBADF;
        return EOF;
    }

    fd = chimera_posix_file_to_fd(posix, stream);

    if (chimera_posix_close(fd) < 0) {
        return EOF;
    }

    return 0;
} /* chimera_posix_fclose */
