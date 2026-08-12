// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>
#include <sys/statvfs.h>

#include "common/platform.h"
#include "posix_internal.h"
#include "../client/client_statfs.h"
#include "../client/client_fstatfs.h"

/*
 * Project a chimera_statvfs onto the host's struct statfs.
 *
 * The two platforms disagree on more than spelling: glibc's statfs carries
 * f_frsize/f_namelen and a fsid whose array member is __val[], while Darwin's
 * has neither of those fields, names the fsid array val[], and adds f_iosize
 * (the optimal transfer size, which chimera reports as the block size).
 * Fields with no source in chimera_statvfs are left zeroed by the caller.
 */
static void
chimera_posix_fill_statfs(
    struct statfs                *buf,
    const struct chimera_statvfs *st)
{
    memset(buf, 0, sizeof(*buf));

    buf->f_type   = 0; // Chimera filesystem
    buf->f_bsize  = st->f_bsize;
    buf->f_blocks = st->f_blocks;
    buf->f_bfree  = st->f_bfree;
    buf->f_bavail = st->f_bavail;
    buf->f_files  = st->f_files;
    buf->f_ffree  = st->f_ffree;

#ifdef __APPLE__
    buf->f_iosize      = st->f_bsize;
    buf->f_flags       = st->f_flag;
    buf->f_fsid.val[0] = (int32_t) (st->f_fsid & 0xFFFFFFFF);
    buf->f_fsid.val[1] = (int32_t) (st->f_fsid >> 32);
#else  /* ifdef __APPLE__ */
    buf->f_frsize        = st->f_frsize;
    buf->f_namelen       = st->f_namemax;
    buf->f_flags         = st->f_flag;
    buf->f_fsid.__val[0] = (int) (st->f_fsid & 0xFFFFFFFF);
    buf->f_fsid.__val[1] = (int) (st->f_fsid >> 32);
#endif /* ifdef __APPLE__ */
} /* chimera_posix_fill_statfs */

static void
chimera_posix_statfs_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_statvfs *st,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    if (status == CHIMERA_VFS_OK && st) {
        request->sync_statvfs = *st;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_statfs_callback */

static void
chimera_posix_statfs_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_statfs(thread, request);
} /* chimera_posix_statfs_exec */

SYMBOL_EXPORT int
chimera_posix_statfs(
    const char    *path,
    struct statfs *buf)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             path_len;

    chimera_posix_completion_init(&comp, &req);

    path_len = strlen(path);

    req.opcode              = CHIMERA_CLIENT_OP_STATFS;
    req.statfs.callback     = chimera_posix_statfs_callback;
    req.statfs.private_data = &comp;
    req.statfs.path_len     = path_len;

    memcpy(req.statfs.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_statfs_exec);

    int err = chimera_posix_wait(&comp);

    if (!err) {
        chimera_posix_fill_statfs(buf, &req.sync_statvfs);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_statfs */

static void
chimera_posix_fstatfs_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    const struct chimera_statvfs *st,
    void                         *private_data)
{
    struct chimera_posix_completion *comp    = private_data;
    struct chimera_client_request   *request = comp->request;

    if (status == CHIMERA_VFS_OK && st) {
        request->sync_statvfs = *st;
    }

    chimera_posix_complete(comp, status);
} /* chimera_posix_fstatfs_callback */

static void
chimera_posix_fstatfs_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_fstatfs(thread, request);
} /* chimera_posix_fstatfs_exec */

SYMBOL_EXPORT int
chimera_posix_fstatfs(
    int            fd,
    struct statfs *buf)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode               = CHIMERA_CLIENT_OP_FSTATFS;
    req.fstatfs.handle       = entry->handle;
    req.fstatfs.callback     = chimera_posix_fstatfs_callback;
    req.fstatfs.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fstatfs_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    if (!err) {
        chimera_posix_fill_statfs(buf, &req.sync_statvfs);
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fstatfs */

SYMBOL_EXPORT int
chimera_posix_statvfs(
    const char     *path,
    struct statvfs *buf)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;
    int                             path_len;

    chimera_posix_completion_init(&comp, &req);

    path_len = strlen(path);

    req.opcode              = CHIMERA_CLIENT_OP_STATFS;
    req.statfs.callback     = chimera_posix_statfs_callback;
    req.statfs.private_data = &comp;
    req.statfs.path_len     = path_len;

    memcpy(req.statfs.path, path, path_len);

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_statfs_exec);

    int err = chimera_posix_wait(&comp);

    if (!err) {
        buf->f_bsize   = req.sync_statvfs.f_bsize;
        buf->f_frsize  = req.sync_statvfs.f_frsize;
        buf->f_blocks  = req.sync_statvfs.f_blocks;
        buf->f_bfree   = req.sync_statvfs.f_bfree;
        buf->f_bavail  = req.sync_statvfs.f_bavail;
        buf->f_files   = req.sync_statvfs.f_files;
        buf->f_ffree   = req.sync_statvfs.f_ffree;
        buf->f_favail  = req.sync_statvfs.f_favail;
        buf->f_fsid    = req.sync_statvfs.f_fsid;
        buf->f_flag    = req.sync_statvfs.f_flag;
        buf->f_namemax = req.sync_statvfs.f_namemax;
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_statvfs */

SYMBOL_EXPORT int
chimera_posix_fstatvfs(
    int             fd,
    struct statvfs *buf)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode               = CHIMERA_CLIENT_OP_FSTATFS;
    req.fstatfs.handle       = entry->handle;
    req.fstatfs.callback     = chimera_posix_fstatfs_callback;
    req.fstatfs.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fstatfs_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    if (!err) {
        buf->f_bsize   = req.sync_statvfs.f_bsize;
        buf->f_frsize  = req.sync_statvfs.f_frsize;
        buf->f_blocks  = req.sync_statvfs.f_blocks;
        buf->f_bfree   = req.sync_statvfs.f_bfree;
        buf->f_bavail  = req.sync_statvfs.f_bavail;
        buf->f_files   = req.sync_statvfs.f_files;
        buf->f_ffree   = req.sync_statvfs.f_ffree;
        buf->f_favail  = req.sync_statvfs.f_favail;
        buf->f_fsid    = req.sync_statvfs.f_fsid;
        buf->f_flag    = req.sync_statvfs.f_flag;
        buf->f_namemax = req.sync_statvfs.f_namemax;
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_fstatvfs */
