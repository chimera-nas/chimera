// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_fputs(
    const char   *s,
    CHIMERA_FILE *stream)
{
    size_t len;
    size_t result;

    if (!s || !stream) {
        return EOF;
    }

    len    = strlen(s);
    result = chimera_posix_fwrite(s, 1, len, stream);

    if (result != len) {
        return EOF;
    }

    return (int) result;
} /* chimera_posix_fputs */
