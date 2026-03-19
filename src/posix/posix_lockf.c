// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "posix.h"
#include "posix_internal.h"

SYMBOL_EXPORT int
chimera_posix_lockf(
    int   fd,
    int   cmd,
    off_t len)
{
    struct flock fl;
    int          fcntl_cmd;
    int          rc;

    fl.l_whence = SEEK_CUR;
    fl.l_start  = 0;
    fl.l_len    = len;

    switch (cmd) {
        case F_LOCK:
            fl.l_type = F_WRLCK;
            fcntl_cmd = F_SETLKW;
            break;
        case F_TLOCK:
            fl.l_type = F_WRLCK;
            fcntl_cmd = F_SETLK;
            break;
        case F_ULOCK:
            fl.l_type = F_UNLCK;
            fcntl_cmd = F_SETLK;
            break;
        case F_TEST:
            fl.l_type = F_WRLCK;
            fcntl_cmd = F_GETLK;
            break;
        default:
            errno = EINVAL;
            return -1;
    } /* switch */

    rc = chimera_posix_fcntl(fd, fcntl_cmd, &fl);

    if (cmd == F_TEST && rc == 0) {
        /* F_GETLK probes without acquiring; if a conflict exists
         * l_type will be non-F_UNLCK - signal EAGAIN like lockf(). */
        if (fl.l_type != F_UNLCK) {
            errno = EAGAIN;
            return -1;
        }
    }

    return rc;
} /* chimera_posix_lockf */
