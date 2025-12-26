// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_fgetc(CHIMERA_FILE *stream)
{
    unsigned char c;
    size_t        result;

    if (!stream) {
        return EOF;
    }

    // Check for ungetc character first
    if (stream->ungetc_char >= 0) {
        int ch = stream->ungetc_char;
        stream->ungetc_char = -1;
        return ch;
    }

    result = chimera_posix_fread(&c, 1, 1, stream);
    if (result != 1) {
        return EOF;
    }

    return (int) c;
} /* chimera_posix_fgetc */
