// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"
#include "../client/client_clone_range.h"

static void
chimera_posix_clone_file_range_callback(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct chimera_posix_completion *comp = private_data;

    chimera_posix_complete(comp, status);
} /* chimera_posix_clone_file_range_callback */

static void
chimera_posix_clone_file_range_exec(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_dispatch_clone_range(thread, request);
} /* chimera_posix_clone_file_range_exec */

SYMBOL_EXPORT int
chimera_posix_clone_file_range(
    int    dst_fd,
    off_t  dst_offset,
    int    src_fd,
    off_t  src_offset,
    size_t len)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *src_entry;
    struct chimera_posix_fd_entry  *dst_entry;
    struct chimera_client_request   req;
    struct chimera_posix_completion comp;

    src_entry = chimera_posix_fd_acquire(posix, src_fd, 0);
    if (!src_entry) {
        return -1;
    }

    dst_entry = chimera_posix_fd_acquire(posix, dst_fd, 0);
    if (!dst_entry) {
        chimera_posix_fd_release(src_entry, 0);
        return -1;
    }

    chimera_posix_completion_init(&comp, &req);

    req.opcode                   = CHIMERA_CLIENT_OP_CLONE_RANGE;
    req.clone_range.src_handle   = src_entry->handle;
    req.clone_range.dst_handle   = dst_entry->handle;
    req.clone_range.src_offset   = (uint64_t) src_offset;
    req.clone_range.dst_offset   = (uint64_t) dst_offset;
    req.clone_range.length       = len;
    req.clone_range.callback     = chimera_posix_clone_file_range_callback;
    req.clone_range.private_data = &comp;

    chimera_posix_worker_enqueue(worker, &req, chimera_posix_clone_file_range_exec);

    int err = chimera_posix_wait(&comp);

    chimera_posix_fd_release(dst_entry, 0);
    chimera_posix_fd_release(src_entry, 0);
    chimera_posix_completion_destroy(&comp);

    if (err) {
        errno = err;
        return -1;
    }

    return 0;
} /* chimera_posix_clone_file_range */
