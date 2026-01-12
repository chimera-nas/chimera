// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include <string.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>

#include "posix_internal.h"
#include "../client/client_statfs.h"

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
        buf->f_type          = 0; // Chimera filesystem
        buf->f_bsize         = req.sync_statvfs.f_bsize;
        buf->f_blocks        = req.sync_statvfs.f_blocks;
        buf->f_bfree         = req.sync_statvfs.f_bfree;
        buf->f_bavail        = req.sync_statvfs.f_bavail;
        buf->f_files         = req.sync_statvfs.f_files;
        buf->f_ffree         = req.sync_statvfs.f_ffree;
        buf->f_fsid.__val[0] = (int) (req.sync_statvfs.f_fsid & 0xFFFFFFFF);
        buf->f_fsid.__val[1] = (int) (req.sync_statvfs.f_fsid >> 32);
        buf->f_namelen       = req.sync_statvfs.f_namemax;
        buf->f_frsize        = req.sync_statvfs.f_frsize;
        buf->f_flags         = req.sync_statvfs.f_flag;
    }

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_statfs */

SYMBOL_EXPORT int
chimera_posix_fstatfs(
    int            fd,
    struct statfs *buf)
{
    // For now, just return an error - we'd need to get the path from the fd
    // This could be implemented by storing the path in the fd entry
    errno = ENOSYS;
    return -1;
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
    // For now, just return an error - we'd need to get the path from the fd
    errno = ENOSYS;
    return -1;
} /* chimera_posix_fstatvfs */
