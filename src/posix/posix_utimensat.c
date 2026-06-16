// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>
#include <sys/stat.h>

#include "posix_internal.h"
#include "../client/client_setattr.h"
#include "../client/client_fsetattr.h"

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
    sa->va_set_mask = CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;

    if (times == NULL) {
        sa->va_atime.tv_sec  = 0;
        sa->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
        sa->va_mtime.tv_sec  = 0;
        sa->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
        return;
    }

    if (times[0].tv_nsec == UTIME_NOW) {
        sa->va_atime.tv_sec  = 0;
        sa->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    } else if (times[0].tv_nsec == UTIME_OMIT) {
        sa->va_atime.tv_sec  = 0;
        sa->va_atime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    } else {
        sa->va_atime = times[0];
    }

    if (times[1].tv_nsec == UTIME_NOW) {
        sa->va_mtime.tv_sec  = 0;
        sa->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    } else if (times[1].tv_nsec == UTIME_OMIT) {
        sa->va_mtime.tv_sec  = 0;
        sa->va_mtime.tv_nsec = CHIMERA_VFS_TIME_OMIT;
    } else {
        sa->va_mtime = times[1];
    }
} /* chimera_posix_fill_utimes */

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

    chimera_posix_completion_init(&comp, &req);

    req.opcode                = CHIMERA_CLIENT_OP_FSETATTR;
    req.fsetattr.handle       = entry->handle;
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
 * utimensat resolves `pathname` (relative to `dirfd`, or absolute) via the
 * generic path-walker chimera_vfs_lookup, which follows symlinks in
 * intermediate components and (when CHIMERA_VFS_LOOKUP_FOLLOW is set) the final
 * component as well, using each backend's readlink primitive.  This gives
 * consistent symlink-follow semantics across every VFS backend (memfs, cairn,
 * diskfs and the NFS3/NFS4 clients).  AT_SYMLINK_NOFOLLOW drops the FOLLOW flag
 * so the times are applied to the symlink itself.
 *
 * Once the target file handle is known we open it (O_PATH) and apply the
 * setattr, mirroring the lookup -> open_fh -> setattr chain in
 * client_setattr.h.
 */
struct chimera_posix_utimensat_ctx {
    struct chimera_posix_completion comp;
    struct chimera_vfs_open_handle *file_handle;
    struct chimera_vfs_attrs        set_attr;
    uint32_t                        lookup_flags;   /* CHIMERA_VFS_LOOKUP_FOLLOW or 0 */
    int                             start_fh_len;
    uint8_t                         start_fh[CHIMERA_VFS_FH_SIZE + 16];
    char                            path[CHIMERA_VFS_PATH_MAX];
    int                             path_len;
};

static void
chimera_posix_utimensat_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_posix_utimensat_ctx *ctx = private_data;

    chimera_vfs_release(ctx->comp.request->thread->vfs_thread, ctx->file_handle);
    chimera_posix_complete(&ctx->comp, error_code);
} /* chimera_posix_utimensat_setattr_complete */

static void
chimera_posix_utimensat_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_posix_utimensat_ctx *ctx     = private_data;
    struct chimera_client_request      *request = ctx->comp.request;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_posix_complete(&ctx->comp, error_code);
        return;
    }

    ctx->file_handle = oh;

    chimera_vfs_setattr(
        request->thread->vfs_thread,
        chimera_client_req_cred(request),
        oh,
        &ctx->set_attr,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_posix_utimensat_setattr_complete,
        ctx);
} /* chimera_posix_utimensat_open_complete */

static void
chimera_posix_utimensat_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_posix_utimensat_ctx *ctx     = private_data;
    struct chimera_client_request      *request = ctx->comp.request;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_posix_complete(&ctx->comp, error_code);
        return;
    }

    chimera_vfs_open_fh(
        request->thread->vfs_thread,
        chimera_client_req_cred(request),
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        chimera_posix_utimensat_open_complete,
        ctx);
} /* chimera_posix_utimensat_lookup_complete */

static void
chimera_posix_utimensat_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_posix_utimensat_ctx *ctx = request->setattr.private_data;

    chimera_vfs_lookup(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        ctx->start_fh,
        ctx->start_fh_len,
        ctx->path,
        ctx->path_len,
        CHIMERA_VFS_ATTR_FH,
        ctx->lookup_flags,
        chimera_posix_utimensat_lookup_complete,
        ctx);
} /* chimera_posix_utimensat_exec */

SYMBOL_EXPORT int
chimera_posix_utimensat(
    int                   dirfd,
    const char           *pathname,
    const struct timespec times[2],
    int                   flags)
{
    struct chimera_posix_client       *posix  = chimera_posix_get_global();
    struct chimera_posix_worker       *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request      req;
    struct chimera_posix_utimensat_ctx ctx;
    struct chimera_posix_fd_entry     *dir_entry = NULL;
    int                                err;

    chimera_posix_completion_init(&ctx.comp, &req);

    ctx.file_handle = NULL;

    /* By default follow a final symlink and set times on its target; with
     * AT_SYMLINK_NOFOLLOW operate on the symlink itself. */
    ctx.lookup_flags = (flags & AT_SYMLINK_NOFOLLOW) ? 0 : CHIMERA_VFS_LOOKUP_FOLLOW;

    if (dirfd == AT_FDCWD || pathname[0] == '/') {
        /* Resolve relative to the namespace root.  chimera_vfs_lookup strips
         * leading slashes, so an absolute path works as-is. */
        struct chimera_client *client = worker->client_thread->client;

        memcpy(ctx.start_fh, client->root_fh, client->root_fh_len);
        ctx.start_fh_len = client->root_fh_len;
    } else {
        dir_entry = chimera_posix_fd_acquire(posix, dirfd, 0);
        if (!dir_entry) {
            errno = EBADF;
            chimera_posix_completion_destroy(&ctx.comp);
            return -1;
        }

        memcpy(ctx.start_fh, dir_entry->handle->fh, dir_entry->handle->fh_len);
        ctx.start_fh_len = dir_entry->handle->fh_len;
    }

    ctx.path_len = strlen(pathname);
    if (ctx.path_len >= CHIMERA_VFS_PATH_MAX) {
        if (dir_entry) {
            chimera_posix_fd_release(dir_entry, 0);
        }
        errno = ENAMETOOLONG;
        chimera_posix_completion_destroy(&ctx.comp);
        return -1;
    }
    memcpy(ctx.path, pathname, ctx.path_len);
    ctx.path[ctx.path_len] = '\0';

    chimera_posix_fill_utimes(&ctx.set_attr, times);

    req.opcode               = CHIMERA_CLIENT_OP_SETATTR;
    req.setattr.callback     = NULL;
    req.setattr.private_data = &ctx;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_utimensat_exec);

    err = chimera_posix_wait(&ctx.comp);

    if (dir_entry) {
        chimera_posix_fd_release(dir_entry, 0);
    }

    chimera_posix_completion_destroy(&ctx.comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_utimensat */
