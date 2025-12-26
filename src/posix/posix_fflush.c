// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_fflush(CHIMERA_FILE *stream)
{
    // Since we don't do any buffering, fflush is a no-op
    if (!stream) {
        // Per POSIX, NULL means flush all streams - no-op for us
        return 0;
    }

    // Check if stream is valid
    if (stream->flags & CHIMERA_POSIX_FD_CLOSED) {
        errno = EBADF;
        return EOF;
    }

    return 0;
} /* chimera_posix_fflush */
