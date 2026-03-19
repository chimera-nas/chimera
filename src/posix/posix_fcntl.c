// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>

#include "posix_internal.h"
#include "../client/client_lock.h"

static void
chimera_posix_fcntl_lock_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint32_t                      conflict_type,
    uint64_t                      conflict_offset,
    uint64_t                      conflict_length,
    pid_t                         conflict_pid,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_fcntl_lock_callback */

static void
chimera_posix_fcntl_lock_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_lock(thread, request);
} /* chimera_posix_fcntl_lock_exec */

SYMBOL_EXPORT int
chimera_posix_fcntl(
    int fd,
    int cmd,
    ...)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct flock                   *fl;
    uint32_t                        lock_type;
    uint32_t                        flags = 0;
    int32_t                         whence;
    uint64_t                        offset;
    uint64_t                        length;
    va_list                         args;

    switch (cmd) {
        case F_GETLK:
            flags |= CHIMERA_VFS_LOCK_TEST;
        /* fall through */
        case F_SETLK:
        case F_SETLKW:
            if (cmd == F_SETLKW) {
                flags |= CHIMERA_VFS_LOCK_WAIT;
            }
            va_start(args, cmd);
            fl = va_arg(args, struct flock *);
            va_end(args);
            break;
        default:
            errno = EINVAL;
            return -1;
    } /* switch */

    switch (fl->l_type) {
        case F_RDLCK:
            lock_type = CHIMERA_VFS_LOCK_READ;
            break;
        case F_WRLCK:
            lock_type = CHIMERA_VFS_LOCK_WRITE;
            break;
        case F_UNLCK:
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
            int64_t abs_offset = (int64_t) entry->offset + (int64_t) fl->l_start;

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
        int64_t signed_offset = (int64_t) offset + (int64_t) fl->l_len;

        if (signed_offset < 0) {
            chimera_posix_fd_release(entry, 0);
            errno = EINVAL;
            return -1;
        }
        offset = (uint64_t) signed_offset;
        length = (uint64_t) (-(int64_t) fl->l_len);
    } else {
        length = (uint64_t) fl->l_len;   /* 0 = lock to EOF (POSIX) */
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode            = CHIMERA_CLIENT_OP_LOCK;
    req.lock.handle       = entry->handle;
    req.lock.whence       = whence;
    req.lock.offset       = offset;
    req.lock.length       = length;
    req.lock.lock_type    = lock_type;
    req.lock.flags        = flags;
    req.lock.callback     = chimera_posix_fcntl_lock_callback;
    req.lock.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fcntl_lock_exec);

    int err = chimera_posix_wait(&comp);

    if (cmd == F_GETLK && !err) {
        if (req.lock.r_conflict_type == CHIMERA_VFS_LOCK_UNLOCK) {
            fl->l_type = F_UNLCK;
        } else {
            fl->l_type = (req.lock.r_conflict_type == CHIMERA_VFS_LOCK_READ)
                ? F_RDLCK : F_WRLCK;
            fl->l_whence = SEEK_SET;
            fl->l_start  = (off_t) req.lock.r_conflict_offset;
            fl->l_len    = (off_t) req.lock.r_conflict_length;
            fl->l_pid    = req.lock.r_conflict_pid;
        }
    }

    chimera_posix_fd_release(entry, 0);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fcntl */
