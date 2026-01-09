// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_feof(CHIMERA_FILE *stream)
{
    if (!stream) {
        return 0;
    }

    return stream->eof_flag;
} /* chimera_posix_feof */

SYMBOL_EXPORT int
chimera_posix_ferror(CHIMERA_FILE *stream)
{
    if (!stream) {
        return 0;
    }

    return stream->error_flag;
} /* chimera_posix_ferror */

SYMBOL_EXPORT void
chimera_posix_clearerr(CHIMERA_FILE *stream)
{
    if (!stream) {
        return;
    }

    stream->eof_flag   = 0;
    stream->error_flag = 0;
} /* chimera_posix_clearerr */
