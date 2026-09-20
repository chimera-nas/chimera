// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

#include "posix_internal.h"
#include "../client/client_setattr.h"
#include "../client/client_fsetattr.h"
#include "../client/client_stat.h"

#ifndef AT_FDCWD
#define AT_FDCWD            -100
#endif /* ifndef AT_FDCWD */

#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0x100
#endif /* ifndef AT_SYMLINK_NOFOLLOW */

/*
 * Translate a POSIX utimensat(2) `times[2]` array into a VFS set_attr.  The
 * atime/mtime mask bits are always set so that the backend applies consistent
 * ctime semantics; per-field UTIME_NOW / UTIME_OMIT are conveyed through the
 * CHIMERA_VFS_TIME_NOW / CHIMERA_VFS_TIME_OMIT tv_nsec sentinels that backends
 * already honor (see vfs_attrs.h).  A NULL `times` means "set both to now".
 */
static void
chimera_posix_fill_utimes(
    struct chimera_vfs_attrs *sa,
    const struct timespec     times[2])
{
    sa->va_req_mask = 0;
    sa->va_set_mask = 0;

    if (times == NULL) {
        sa->va_set_mask      = CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
        sa->va_atime.tv_sec  = 0;
        sa->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
        sa->va_mtime.tv_sec  = 0;
        sa->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
        return;
    }

    /* An omitted field must not appear in the set mask at all: a stray
     * OMIT sentinel under CHIMERA_VFS_ATTR_ATIME/MTIME made the setattr
     * permission gate treat it as an EXPLICIT timestamp, demanding
     * ownership (EPERM) for calls POSIX tiers by write access -- e.g. a
     * to-now update paired with one omitted field, or a both-omitted
     * no-op. */
    if (times[0].tv_nsec == UTIME_NOW) {
        sa->va_set_mask     |= CHIMERA_VFS_ATTR_ATIME;
        sa->va_atime.tv_sec  = 0;
        sa->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    } else if (times[0].tv_nsec != UTIME_OMIT) {
        sa->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        sa->va_atime     = times[0];
    }

    if (times[1].tv_nsec == UTIME_NOW) {
        sa->va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
        sa->va_mtime.tv_sec  = 0;
        sa->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    } else if (times[1].tv_nsec != UTIME_OMIT) {
        sa->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        sa->va_mtime     = times[1];
    }
} /* chimera_posix_fill_utimes */

/* Both fields omitted: POSIX requires no ownership or permission check and
 * no timestamp change -- only resolution/descriptor errors may surface. */
static inline int
chimera_posix_utimes_noop(const struct timespec times[2])
{
    return times != NULL &&
           times[0].tv_nsec == UTIME_OMIT &&
           times[1].tv_nsec == UTIME_OMIT;
} /* chimera_posix_utimes_noop */

/* ---- futimens(fd, times) : fsetattr on the open handle ---- */

static void
chimera_posix_futimens_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_futimens_callback */

static void
chimera_posix_futimens_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_fsetattr(thread, request);
} /* chimera_posix_futimens_exec */

SYMBOL_EXPORT int
chimera_posix_futimens(
    int                   fd,
    const struct timespec times[2])
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    if (chimera_posix_utimes_noop(times)) {
        /* The descriptor is valid and a both-omitted call changes
         * nothing; POSIX forbids any further permission check. */
        chimera_posix_fd_release(entry, 0);
        return 0;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode                = CHIMERA_CLIENT_OP_FSETATTR;
    req.fsetattr.handle       = entry->handle;
    req.fsetattr.open_flags   = chimera_posix_fd_open_flags(entry);
    req.fsetattr.callback     = chimera_posix_futimens_callback;
    req.fsetattr.private_data = &comp;

    chimera_posix_fill_utimes(&req.fsetattr.set_attr, times);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_futimens_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_futimens */

/* ---- utimensat(dirfd, path, times, flags) ---- */

/*
 * utimensat is a setattr by path: the path-based dispatchers from the export
 * root for AT_FDCWD and an absolute path, chimera_dispatch_setattr_at from a
 * real dirfd -- which lends the descriptor and checks it is a directory
 * against the live inode, so an unlinked-while-open non-directory answers
 * ENOTDIR rather than the ENOENT a re-resolve of its stale name would give.
 * AT_SYMLINK_NOFOLLOW applies the times to a final-component symlink itself.
 *
 * Both timestamps omitted is a resolve and nothing else (see
 * chimera_posix_utimes_noop): POSIX forbids any permission check beyond the
 * path walk, so that case is a stat's sequence -- a LOOKUP_PATH from the
 * root or the descriptor, on the same dircheck -- whose answer is thrown
 * away.
 */

static void
chimera_posix_utimensat_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_utimensat_callback */

static void
chimera_posix_utimensat_validate_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    /* Resolution is the whole of the answer; the attributes are not kept. */
    (void) st;

    chimera_posix_complete(comp, status);
} /* chimera_posix_utimensat_validate_callback */

static void
chimera_posix_utimensat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (request->setattr.parent_handle) {
        chimera_dispatch_setattr_at(thread, request);
    } else if (request->setattr.nofollow) {
        chimera_dispatch_lsetattr(thread, request);
    } else {
        chimera_dispatch_setattr(thread, request);
    }
} /* chimera_posix_utimensat_exec */

static void
chimera_posix_utimensat_validate_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_stat(thread, request);
} /* chimera_posix_utimensat_validate_exec */

SYMBOL_EXPORT int
chimera_posix_utimensat(
    int                   dirfd,
    const char           *pathname,
    const struct timespec times[2],
    int                   flags)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    struct chimera_posix_fd_entry  *dir_entry     = NULL;
    int                             validate_only = chimera_posix_utimes_noop(times);
    int                             nofollow      = (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0;
    int                             at_root;
    int                             path_len, err;
    char                           *path;

    /* An empty path names nothing -- see openat. */
    if (pathname[0] == '\0') {
        errno = ENOENT;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    /* Both request shapes carry the path in the same fixed buffer; which one
     * is filled is decided by validate_only below. */
    path = validate_only ? req.stat.path : req.setattr.path;

    at_root = (dirfd == AT_FDCWD || pathname[0] == '/');

    if (!at_root) {
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);
        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&comp);
            return -1;
        }
    }

    path_len = strlen(pathname);

    /* A relative path from the root is rooted by a leading '/' the way every
     * other path-based call roots one. */
    if (at_root && pathname[0] != '/') {
        path_len++;
    }

    if (path_len >= CHIMERA_VFS_PATH_MAX) {
        if (dir_entry) {
            chimera_posix_fd_release(dir_entry, 0);
        }
        errno = ENAMETOOLONG;
        chimera_posix_completion_destroy(&comp);
        return -1;
    }

    if (at_root && pathname[0] != '/') {
        path[0] = '/';
        memcpy(path + 1, pathname, path_len - 1);
    } else {
        memcpy(path, pathname, path_len);
    }
    path[path_len] = '\0';

    if (validate_only) {
        req.opcode            = CHIMERA_CLIENT_OP_STAT;
        req.stat.handle       = dir_entry ? dir_entry->handle : NULL;
        req.stat.open_flags   = dir_entry ? chimera_posix_fd_open_flags(dir_entry) : 0;
        req.stat.callback     = chimera_posix_utimensat_validate_callback;
        req.stat.private_data = &comp;
        req.stat.flags        = nofollow ? 0 : CHIMERA_VFS_LOOKUP_FOLLOW;
        req.stat.path_len     = path_len;

        chimera_posix_worker_enqueue(worker, &req,
                                     chimera_posix_utimensat_validate_exec);
    } else {
        req.opcode                 = CHIMERA_CLIENT_OP_SETATTR;
        req.setattr.parent_handle  = dir_entry ? dir_entry->handle : NULL;
        req.setattr.dir_open_flags = dir_entry ? chimera_posix_fd_open_flags(dir_entry) : 0;
        req.setattr.nofollow       = nofollow;
        req.setattr.callback       = chimera_posix_utimensat_callback;
        req.setattr.private_data   = &comp;
        req.setattr.path_len       = path_len;

        chimera_posix_fill_utimes(&req.setattr.set_attr, times);

        chimera_posix_worker_enqueue(worker, &req, chimera_posix_utimensat_exec);
    }

    err = chimera_posix_wait(&comp);

    if (dir_entry) {
        chimera_posix_fd_release(dir_entry, 0);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_utimensat */
