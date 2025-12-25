// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_fputc(
    int           c,
    CHIMERA_FILE *stream)
{
    unsigned char ch = (unsigned char) c;
    size_t        result;

    if (!stream) {
        return EOF;
    }

    result = chimera_posix_fwrite(&ch, 1, 1, stream);
    if (result != 1) {
        return EOF;
    }

    return (int) ch;
} /* chimera_posix_fputc */
