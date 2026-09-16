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
    struct chimera_posix_client   *posix = chimera_posix_get_global();
    struct chimera_posix_fd_entry *entry;
    struct flock                   fl;
    int                            fcntl_cmd;
    int                            rc;

    /* The descriptor must exist whatever the command asks for. */
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
            /* glibc probes with F_RDLCK, not F_WRLCK: F_TEST asks whether a
             * READER would conflict, so another process's read lock is no
             * conflict at all -- only a write lock is. */
            fl.l_type = F_RDLCK;
            fcntl_cmd = F_GETLK;
            break;
        default:
            errno = EINVAL;
            return -1;
    } /* switch */

    /* XSH lockf scopes the write requirement to the commands that take a
     * lock: "[EBADF] The fildes argument is not a valid open file descriptor;
     * or function is F_LOCK or F_TLOCK and fildes is not a valid file
     * descriptor open for writing."  F_TEST and F_ULOCK ask for a valid
     * descriptor and nothing more, and glibc lands on exactly that split --
     * F_LOCK/F_TLOCK become fcntl F_WRLCK, which fcntl refuses on a read-only
     * fd, while F_TEST is F_GETLK and F_ULOCK is F_UNLCK and neither consults
     * the access mode. */
    if (cmd == F_LOCK || cmd == F_TLOCK) {
        entry = chimera_posix_fd_acquire(posix, fd, 0);
        if (!entry) {
            errno = EBADF;
            return -1;
        }
        if (!chimera_posix_fd_may_write(entry)) {
            chimera_posix_fd_release(entry, 0);
            errno = EBADF;
            return -1;
        }
        chimera_posix_fd_release(entry, 0);
    }

    rc = chimera_posix_fcntl(fd, fcntl_cmd, &fl);

    if (cmd == F_TEST && rc == 0) {
        /* F_GETLK probes without acquiring; if a conflict exists l_type
         * comes back non-F_UNLCK.  glibc's lockf() names the errno for this
         * itself -- "__set_errno (EACCES)" -- rather than passing on the
         * EAGAIN the kernel gives F_LOCK/F_TLOCK.  XSH lockf permits either
         * for F_TEST, but the answer callers see from lockf(3) is EACCES. */
        if (fl.l_type != F_UNLCK) {
            errno = EACCES;
            return -1;
        }
    }

    return rc;
} /* chimera_posix_lockf */
