// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "posix.h"
#include "posix_internal.h"

/* lockf() takes exclusive locks, so a descriptor that cannot be written
 * cannot take one -- but only for the two commands that DO take one.  XSH
 * lockf's EBADF condition is "The fildes argument is not a valid open file
 * descriptor; or function is F_LOCK or F_TLOCK and fildes is not a valid
 * file descriptor open for writing": F_ULOCK and F_TEST are deliberately
 * absent from that list, and Linux (where lockf is a glibc wrapper over
 * fcntl) accepts both on a read-only descriptor.  fcntl() cannot make the
 * check on our behalf, since F_ULOCK reaches it as F_UNLCK and F_TEST as
 * F_GETLK and neither is access-constrained there. */
static int
chimera_posix_lockf_takes_lock(int cmd)
{
    return cmd == F_LOCK || cmd == F_TLOCK;
} /* chimera_posix_lockf_takes_lock */

static int
chimera_posix_lockf_writable(int fd)
{
    struct chimera_posix_fd_entry *entry;
    unsigned int                   acc;

    entry = chimera_posix_fd_acquire(chimera_posix_get_global(), fd, 0);

    if (!entry) {
        return 0;
    }

    acc = entry->ofd->oflags & O_ACCMODE;

    chimera_posix_fd_release(entry, 0);

    return acc == O_WRONLY || acc == O_RDWR;
} /* chimera_posix_lockf_writable */

SYMBOL_EXPORT int
chimera_posix_lockf(
    int   fd,
    int   cmd,
    off_t len)
{
    struct chimera_posix_client   *posix = chimera_posix_get_global();
    struct chimera_posix_fd_entry *entry;
    struct flock                   fl;
    int                            fcntl_cmd;
    int                            rc;

    /* The descriptor must be open at all, whatever the command. */
    entry = chimera_posix_fd_acquire(posix, fd, 0);
    if (!entry) {
        errno = EBADF;
        return -1;
    }
    chimera_posix_fd_release(entry, 0);

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

    /* ... and open for WRITING only when the command takes a lock. */
    if (chimera_posix_lockf_takes_lock(cmd) &&
        !chimera_posix_lockf_writable(fd)) {
        errno = EBADF;
        return -1;
    }

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
