// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "posix_internal.h"

SYMBOL_EXPORT char *
chimera_posix_fgets(
    char         *s,
    int           size,
    CHIMERA_FILE *stream)
{
    int i;
    int c;

    if (!s || size <= 0 || !stream) {
        return NULL;
    }

    for (i = 0; i < size - 1; i++) {
        c = chimera_posix_fgetc(stream);
        if (c == EOF) {
            if (i == 0) {
                return NULL;
            }
            break;
        }

        s[i] = (char) c;

        if (c == '\n') {
            i++;
            break;
        }
    }

    s[i] = '\0';
    return s;
} /* chimera_posix_fgets */
