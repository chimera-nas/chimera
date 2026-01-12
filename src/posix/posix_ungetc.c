// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_ungetc(
    int           c,
    CHIMERA_FILE *stream)
{
    if (!stream || c == EOF) {
        return EOF;
    }

    // Only support one pushed-back character
    if (stream->ungetc_char >= 0) {
        return EOF;
    }

    stream->ungetc_char = (unsigned char) c;
    stream->eof_flag    = 0;

    return (unsigned char) c;
} /* chimera_posix_ungetc */
