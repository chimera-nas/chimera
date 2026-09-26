// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_symlink_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_symlink_callback_t     callback     = request->symlink.callback;
    void                          *callback_arg = request->symlink.private_data;
    enum chimera_vfs_error         status       = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_symlink_sequence_complete */

static inline void
chimera_dispatch_symlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    if (unlikely(request->symlink.name_offset == -1)) {
        chimera_dispatch_error_symlink(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    request->symlink.set_attr.va_req_mask = 0;
    request->symlink.set_attr.va_set_mask = 0;

    compound = chimera_client_compound_at_root(thread, request);

    chimera_vfs_compound_add_create_path(compound,
                                         CHIMERA_VFS_COMPOUND_CREATE_SYMLINK,
                                         request->symlink.path,
                                         request->symlink.path_len,
                                         request->symlink.target,
                                         request->symlink.target_len,
                                         &request->symlink.set_attr,
                                         CHIMERA_VFS_ATTR_FH);

    chimera_frontend_compound_submit(compound, chimera_symlink_sequence_complete,
                                     request);
} /* chimera_dispatch_symlink */
