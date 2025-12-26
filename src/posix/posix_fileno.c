// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_fileno(CHIMERA_FILE *stream)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (!stream) {
        errno = EBADF;
        return -1;
    }

    return chimera_posix_file_to_fd(posix, stream);
} /* chimera_posix_fileno */
