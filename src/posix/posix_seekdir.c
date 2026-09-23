// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_internal.h"

SYMBOL_EXPORT void
chimera_posix_rewinddir(CHIMERA_DIR *dirp)
{
    if (!dirp) {
        return;
    }

    dirp->cookie    = 0;
    dirp->eof       = 0;
    dirp->buf_valid = 0;
} /* chimera_posix_rewinddir */

SYMBOL_EXPORT void
chimera_posix_seekdir(
    CHIMERA_DIR     *dirp,
    chimera_dirpos_t loc)
{
    if (!dirp) {
        return;
    }

    dirp->cookie    = (uint64_t) loc;
    dirp->eof       = 0;
    dirp->buf_valid = 0;
} /* chimera_posix_seekdir */

SYMBOL_EXPORT chimera_dirpos_t
chimera_posix_telldir(CHIMERA_DIR *dirp)
{
    if (!dirp) {
        errno = EBADF;
        return -1;
    }

    return (chimera_dirpos_t) dirp->cookie;
} /* chimera_posix_telldir */
