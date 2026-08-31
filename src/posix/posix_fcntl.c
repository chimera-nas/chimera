// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>

#include "posix_internal.h"
#include "../client/client_lock.h"
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

    newfd = chimera_posix_fd_alloc_at_least(posix, handle, minfd);

    if (newfd < 0) {
        chimera_posix_close_on_worker(worker, handle);
        chimera_posix_fd_release(entry, 0);
        errno = EMFILE;
        return -1;
    }

    /* Like dup(): the duplicate SHARES the open file description. */
    chimera_posix_ofd_adopt(posix, &posix->fds[newfd], entry);

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
    struct chimera_vfs_open_handle *handle;
    struct chimera_posix_ofd_lock  *node = NULL;
    uint32_t                        lock_type;
    uint32_t                        flags = 0;
    int32_t                         whence;
    uint64_t                        offset;
    uint64_t                        length;
    uint64_t                        core_length = 0;
    bool                            local_arbiter;
    bool                            backend_locks;
    va_list                         args;

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
            int64_t abs_offset = (int64_t) entry->ofd->offset + (int64_t) fl->l_start;

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

    /*
     * Local claim-core arbitration first: the client's embedded VFS core
     * arbitrates this process's share of the cluster (protocol claims and
     * other posix threads), then the OP_LOCK backend passthrough projects
     * the lock into the kernel so cross-PROCESS conflicts keep working
     * (each process has its own core instance; the kernel is the shared
     * arbiter).
     *
     * SEEK_END ranges stay backend-only: the kernel resolves the offset
     * relative to EOF atomically (the TOCTOU note above), so the local core
     * cannot know the absolute range.
     * CLAIMTODO: SEEK_END locks therefore bypass local arbitration
     * entirely; resolving EOF locally would reintroduce the TOCTOU race.
     */
    handle        = entry->handle;
    local_arbiter = (whence != SEEK_END);
    backend_locks = (handle->vfs_module->capabilities &
                     CHIMERA_VFS_CAP_FS_LOCK) != 0;

    if (local_arbiter) {
        /* POSIX l_len 0 = to-EOF sentinel; the core spells to-EOF as
         * UINT64_MAX (its 0 is a genuine zero-byte range). */
        core_length = (length == 0) ? UINT64_MAX : length;
    }

    if (local_arbiter && cmd == F_GETLK) {
        struct chimera_vfs_state         *vstate = posix->client->vfs->vfs_state;
        struct chimera_vfs_file_state    *file;
        struct chimera_vfs_claim          probe;
        struct chimera_vfs_claim_conflict conf;
        struct chimera_claim_owner        owner;
        enum chimera_vfs_claim_result     result;

        chimera_posix_lock_owner_init(&owner);
        chimera_vfs_claim_init_range(&probe,
                                     lock_type == CHIMERA_VFS_LOCK_WRITE,
                                     /* smb */ false,
                                     offset, core_length, &owner);

        file = chimera_vfs_state_get(vstate, handle->fh,
                                     (uint8_t) handle->fh_len,
                                     handle->fh_hash, true);

        result = chimera_vfs_claim_test(file, &probe, &conf);

        chimera_vfs_state_put(vstate, file);

        if (result != CHIMERA_CLAIM_GRANTED) {
            /* Local conflict: report it without consulting the backend.
             * WRITE_LT iff the holder's used mode carries a write-flavored
             * bit (a write delegation reports WRITE_LT though it holds no
             * LW). */
            fl->l_type = (conf.used & (CHIMERA_CLAIM_W |
                                       CHIMERA_CLAIM_CW |
                                       CHIMERA_CLAIM_LW))
                ? F_WRLCK : F_RDLCK;
            fl->l_whence = SEEK_SET;
            fl->l_start  = (off_t) conf.offset;
            fl->l_len    = (conf.length == UINT64_MAX)
                ? 0 : (off_t) conf.length;
            fl->l_pid = (pid_t) conf.owner.owner_lo;

            chimera_posix_fd_release(entry, 0);
            return 0;
        }
        /* No local conflict: fall through to the backend OP_LOCK TEST so
         * the kernel sees other processes. */
    }

    if (local_arbiter && cmd != F_GETLK &&
        lock_type == CHIMERA_VFS_LOCK_UNLOCK) {
        /* Carve the owner's local coverage of the range (REPLACE geometry);
         * the backend unlock below is the kernel projection. */
        chimera_posix_ofd_lock_carve(posix, entry->ofd, handle,
                                     offset, core_length);
    }

    if (local_arbiter && cmd != F_GETLK &&
        lock_type != CHIMERA_VFS_LOCK_UNLOCK) {
        /* CLAIMTODO: POSIX re-lock of an overlapping range REPLACES the
         * owner's coverage (including a WRLCK->RDLCK downgrade); this pass
         * accumulates same-owner fragments instead (self-exempt at the
         * OWNER circle, so harmless to other owners' arbitration, and
         * carved correctly on F_UNLCK), pending a carve-then-insert. */
        struct chimera_vfs_state     *vstate = posix->client->vfs->vfs_state;
        enum chimera_vfs_claim_result result;

        node = chimera_posix_ofd_lock_alloc(posix, handle,
                                            lock_type == CHIMERA_VFS_LOCK_WRITE,
                                            offset, core_length);

        if (!node) {
            chimera_posix_fd_release(entry, 0);
            errno = ENOMEM;
            return -1;
        }

        if (cmd == F_SETLKW) {
            /* Blocking acquire: wait on BREAKING and (wait_hard) on a hard
             * DENIED lock conflict, bridged onto this thread's condvar. */
            result = chimera_posix_lock_claim_acquire_wait(posix, node);
        } else {
            /* F_SETLK: try; BREAKING (recalls kicked, claim not inserted)
             * maps to EAGAIN for a non-blocking request like DENIED. */
            result = chimera_vfs_claim_try_acquire(vstate, node->file,
                                                   &node->claim, NULL);
        }

        if (result != CHIMERA_CLAIM_GRANTED) {
            chimera_posix_ofd_lock_free(posix, node);
            chimera_posix_fd_release(entry, 0);
            errno = EAGAIN;
            return -1;
        }

        chimera_posix_ofd_lock_track(posix, entry->ofd, node);
    }

    if (!backend_locks) {
        /* Backend without CAP_FS_LOCK (memfs/cairn/diskfs): the local core
         * is the arbiter -- skip the projection step instead of failing
         * ENOTSUP.  SEEK_END ranges had no arbiter at all, so keep the old
         * ENOTSUP for them. */
        chimera_posix_fd_release(entry, 0);

        if (!local_arbiter) {
            errno = ENOTSUP;
            return -1;
        }

        if (cmd == F_GETLK) {
            fl->l_type = F_UNLCK;
        }

        return 0;
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

    if (err && node) {
        /* The kernel projection denied a lock the local core granted:
         * drop the local claim and surface the backend's error. */
        chimera_posix_ofd_lock_untrack_release(posix, node);
        node = NULL;
    }

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
