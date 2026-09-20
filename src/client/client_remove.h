// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif // ifdef _WIN32

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_remove_vfs_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_remove_callback_t      callback     = request->remove.callback;
    void                          *callback_arg = request->remove.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, callback_arg);
} /* chimera_remove_vfs_complete */

static void
chimera_remove_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    chimera_remove_vfs_complete(status, private_data);
} /* chimera_remove_sequence_complete */

static inline void
chimera_dispatch_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    if (unlikely(request->remove.name_offset == -1)) {
        chimera_dispatch_error_remove(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    compound = chimera_client_compound_at_root(thread, request);

    chimera_vfs_compound_add_remove_path(compound,
                                         request->remove.path,
                                         request->remove.path_len,
                                         request->remove.flags);

    chimera_vfs_compound_submit(compound, chimera_remove_sequence_complete,
                                request);
} /* chimera_dispatch_remove */

/*
 * unlinkat(2) with a real directory descriptor: the descriptor is lent and
 * checked to be a directory, then the same REMOVE_PATH the path form issues
 * from the root resolves the relative path from it -- see
 * chimera_client_compound_at_dir.  The path remove resolves the doomed
 * object itself: it enforces the rmdir-vs-unlink assertion in `flags` for
 * every backend (the NFSv4 proxy's REMOVE is type-agnostic on the wire), and
 * hands the backend the child's fh as the object to recall leases on and, on
 * a path-only mount, to evict from the open caches -- what the per-op chain
 * this replaced looked the child up for.  The descriptor is the caller's and
 * is not released.
 */
static inline void
chimera_dispatch_remove_at(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_client_compound_at_dir(thread, request,
                                              request->remove.parent_handle,
                                              request->remove.dir_open_flags);

    chimera_vfs_compound_add_remove_path(compound,
                                         request->remove.path,
                                         request->remove.path_len,
                                         request->remove.flags);

    chimera_vfs_compound_submit(compound, chimera_remove_sequence_complete,
                                request);
} /* chimera_dispatch_remove_at */
