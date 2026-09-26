// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>

#include "posix_internal.h"
#include "../client/client_dup.h"

/* Status flags F_SETFL may change; everything else in the argument is
 * ignored per POSIX (the access mode and creation flags are immutable). */
#define CHIMERA_POSIX_SETFL_MASK (O_APPEND | O_NONBLOCK)

static int
chimera_posix_fcntl_dupfd(
    struct chimera_posix_client *posix,
    struct chimera_posix_worker *worker,
    int                          fd,
    int                          minfd)
{
    struct chimera_posix_fd_entry  *entry;
    struct chimera_vfs_open_handle *handle;
    int                             newfd;

    if (minfd < 0 || minfd >= posix->max_fds) {
        errno = EINVAL;
        return -1;
    }

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    handle = entry->handle;

    chimera_dup_handle(worker->client_thread, handle);

    newfd = chimera_posix_fd_alloc_description(posix, handle, minfd, entry->ofd);

    if (newfd < 0) {
        chimera_posix_close_on_worker(worker, handle);
        chimera_posix_fd_release(entry, 0);
        errno = EMFILE;
        return -1;
    }

    chimera_posix_fd_release(entry, 0);

    return newfd;
} /* chimera_posix_fcntl_dupfd */

static int
chimera_posix_fcntl_getfl(
    struct chimera_posix_client *posix,
    int                          fd)
{
    struct chimera_posix_fd_entry *entry;
    int                            flags;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    flags = (int) (entry->ofd->oflags &
                   (O_ACCMODE | CHIMERA_POSIX_SETFL_MASK));

    chimera_posix_fd_release(entry, 0);

    return flags;
} /* chimera_posix_fcntl_getfl */

static int
chimera_posix_fcntl_setfl(
    struct chimera_posix_client *posix,
    int                          fd,
    int                          arg)
{
    struct chimera_posix_fd_entry *entry;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    entry->ofd->oflags = (entry->ofd->oflags & ~(unsigned int) CHIMERA_POSIX_SETFL_MASK)
        | ((unsigned int) arg & CHIMERA_POSIX_SETFL_MASK);

    chimera_posix_fd_release(entry, 0);

    return 0;
} /* chimera_posix_fcntl_setfl */

SYMBOL_EXPORT int
chimera_posix_fcntl(
    int fd,
    int cmd,
    ...)
{
    struct chimera_posix_client   *posix  = chimera_posix_get_global();
    struct chimera_posix_worker   *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry *entry;
    struct flock                  *fl;
    uint32_t                       lock_type;
    int32_t                        whence;
    uint64_t                       offset;
    uint64_t                       length;
    va_list                        args;

    switch (cmd) {
        case F_DUPFD: {
            int minfd;

            va_start(args, cmd);
            minfd = va_arg(args, int);
            va_end(args);
            return chimera_posix_fcntl_dupfd(posix, worker, fd, minfd);
        }
        case F_GETFL:
            return chimera_posix_fcntl_getfl(posix, fd);
        case F_SETFL: {
            int arg;

            va_start(args, cmd);
            arg = va_arg(args, int);
            va_end(args);
            return chimera_posix_fcntl_setfl(posix, fd, arg);
        }
        case F_GETLK:
        case F_SETLK:
        case F_SETLKW:
            va_start(args, cmd);
            fl = va_arg(args, struct flock *);
            va_end(args);
            break;
        default:
            errno = EINVAL;
            return -1;
    } /* switch */

    if (!fl) {
        errno = EFAULT;
        return -1;
    }
    switch (fl->l_type) {
        case F_RDLCK:
            lock_type = CHIMERA_VFS_LOCK_READ;
            break;
        case F_WRLCK:
            lock_type = CHIMERA_VFS_LOCK_WRITE;
            break;
        case F_UNLCK:
            if (cmd == F_GETLK) {
                errno = EINVAL;
                return -1;
            }
            lock_type = CHIMERA_VFS_LOCK_UNLOCK;
            break;
        default:
            errno = EINVAL;
            return -1;
    } /* switch */

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    /* fcntl(2) SETLK/SETLKW: a read lock requires a descriptor open for
     * reading and a write lock one open for writing, else EBADF.  F_GETLK
     * only queries and F_UNLCK only releases; neither is access-gated. */
    if (cmd != F_GETLK &&
        ((lock_type == CHIMERA_VFS_LOCK_READ &&
          !chimera_posix_fd_may_read(entry)) ||
         (lock_type == CHIMERA_VFS_LOCK_WRITE &&
          !chimera_posix_fd_may_write(entry)))) {
        chimera_posix_fd_release(entry, 0);
        errno = EBADF;
        return -1;
    }

    switch (fl->l_whence) {
        case SEEK_SET: {
            if (fl->l_start < 0) {
                chimera_posix_fd_release(entry, 0);
                errno = EINVAL;
                return -1;
            }
            whence = SEEK_SET;
            offset = (uint64_t) fl->l_start;
            break;
        }
        case SEEK_CUR: {
            int64_t  abs_offset;
            uint64_t base = entry->ofd->offset;

            if (base > INT64_MAX ||
                __builtin_add_overflow((int64_t) base,
                                       (int64_t) fl->l_start, &abs_offset)) {
                chimera_posix_fd_release(entry, 0);
                errno = EOVERFLOW;
                return -1;
            }
            if (abs_offset < 0) {
                chimera_posix_fd_release(entry, 0);
                errno = EINVAL;
                return -1;
            }
            whence = SEEK_SET;
            offset = (uint64_t) abs_offset;
            break;
        }
        case SEEK_END:
            /*
             * Pass SEEK_END through to the backend so the kernel resolves
             * the offset relative to EOF atomically, avoiding a TOCTOU race
             * between a separate fstat and the subsequent fcntl call.
             * offset and length are stored as bit-casts of the signed values;
             * the backends cast them back to off_t when whence == SEEK_END.
             */
            whence = SEEK_END;
            offset = (uint64_t) (int64_t) fl->l_start;
            break;
        default:
            chimera_posix_fd_release(entry, 0);
            errno = EINVAL;
            return -1;
    } /* switch */

    /*
     * Normalize negative l_len for pre-resolved (SEEK_SET) cases.
     * A negative l_len means the region extends backwards from the start:
     * the actual range is [start + l_len, start - 1].  Reject if that
     * would place the start before byte 0.
     * For SEEK_END the raw l_len is passed through to the backend as a
     * bit-cast; the kernel handles negative l_len natively.
     */
    if (whence == SEEK_END) {
        length = (uint64_t) (int64_t) fl->l_len;
    } else if (fl->l_len < 0) {
        /* Unsigned magnitude avoids negating INT64_MIN. */
        uint64_t backwards = (uint64_t) (-(fl->l_len + 1)) + 1;
        if (backwards > offset) {
            chimera_posix_fd_release(entry, 0);
            errno = EINVAL;
            return -1;
        }
        offset -= backwards;
        length  = backwards;
    } else {
        length = (uint64_t) fl->l_len; /* POSIX zero = through EOF. */
        if (length && length - 1 > (uint64_t) INT64_MAX - offset) {
            chimera_posix_fd_release(entry, 0);
            errno = EOVERFLOW;
            return -1;
        }
    }

    int rc = chimera_posix_lock_compound(posix, entry, cmd, fl, lock_type,
                                         whence, offset,
                                         whence == SEEK_END || length ? length : UINT64_MAX);
    chimera_posix_fd_release(entry, 0);
    return rc;
} /* chimera_posix_fcntl */
