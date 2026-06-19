// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE 1
#include <errno.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <string.h>

#include "posix_internal.h"
#include "vfs/vfs.h"
#include "../client/client_allocate.h"

static void
chimera_posix_fallocate_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_fallocate_callback */

static void
chimera_posix_fallocate_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_allocate(thread, request);
} /* chimera_posix_fallocate_exec */

/* Dispatch a VFS ALLOCATE (vfs_flags 0) or DEALLOCATE
 * (CHIMERA_VFS_ALLOCATE_DEALLOCATE) over [offset, offset+len) on a writable
 * descriptor.  Returns 0 or -1 with errno set. */
static int
chimera_posix_do_fallocate(
    struct chimera_posix_client *posix,
    int                          fd,
    uint32_t                     vfs_flags,
    off_t                        offset,
    off_t                        len)
{
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    entry = chimera_posix_fd_acquire(posix, fd, 0);

    if (!entry) {
        errno = EBADF;
        return -1;
    }

    /* (de)allocate requires the descriptor be open for writing. */
    if ((entry->oflags & O_ACCMODE) == O_RDONLY) {
        chimera_posix_fd_release(entry, 0);
        errno = EBADF;
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode                = CHIMERA_CLIENT_OP_ALLOCATE;
    req.allocate.handle       = entry->handle;
    req.allocate.offset       = (uint64_t) offset;
    req.allocate.length       = (uint64_t) len;
    req.allocate.flags        = vfs_flags;
    req.allocate.callback     = chimera_posix_fallocate_callback;
    req.allocate.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_fallocate_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(entry, 0);

    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_do_fallocate */

/*
 * posix_fallocate(3): reserve space for [offset, offset+len) and grow the file
 * to offset+len if it is currently shorter.  Returns 0 on success or -1 with
 * errno set.  len == 0 (and negative offset/len) are invalid -> EINVAL; a
 * descriptor not open for writing -> EBADF.
 */
SYMBOL_EXPORT int
chimera_posix_fallocate(
    int   fd,
    off_t offset,
    off_t len)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (offset < 0 || len <= 0) {
        errno = EINVAL;
        return -1;
    }

    return chimera_posix_do_fallocate(posix, fd, 0, offset, len);
} /* chimera_posix_fallocate */

/*
 * fallocate(2) with a mode.  mode == 0 behaves like posix_fallocate; the
 * FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE combination deallocates the range
 * (punches a hole), leaving the file size unchanged.  Other modes are
 * unsupported -> EOPNOTSUPP.
 */
SYMBOL_EXPORT int
chimera_posix_fallocate_mode(
    int   fd,
    int   mode,
    off_t offset,
    off_t len)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    if (offset < 0 || len <= 0) {
        errno = EINVAL;
        return -1;
    }

    if (mode == 0) {
        return chimera_posix_do_fallocate(posix, fd, 0, offset, len);
    }

    /* Hole punch: Linux requires KEEP_SIZE alongside PUNCH_HOLE. */
    if (mode == (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)) {
        return chimera_posix_do_fallocate(posix, fd,
                                          CHIMERA_VFS_ALLOCATE_DEALLOCATE,
                                          offset, len);
    }

    errno = EOPNOTSUPP;
    return -1;
} /* chimera_posix_fallocate_mode */
