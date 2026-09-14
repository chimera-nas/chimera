// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_rename_vfs_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_rename_callback_t      callback     = request->rename.callback;
    void                          *callback_arg = request->rename.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, callback_arg);
} /* chimera_rename_vfs_complete */

static void
chimera_rename_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    chimera_rename_vfs_complete(status, private_data);
} /* chimera_rename_sequence_complete */

static inline void
chimera_dispatch_rename(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    if (unlikely(request->rename.source_name_offset == -1 || request->rename.dest_name_offset == -1)) {
        chimera_dispatch_error_rename(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    compound = chimera_client_compound_at_root(thread, request);

    /* Both paths resolve from the root, so this needs no saved slot -- unlike
    * the name-addressed RENAME, which renames between two current objects. */
    chimera_vfs_compound_add_rename_path(compound,
                                         request->rename.source_path,
                                         request->rename.source_path_len,
                                         request->rename.dest_path,
                                         request->rename.dest_path_len);

    chimera_vfs_compound_submit(compound, chimera_rename_sequence_complete,
                                request);
} /* chimera_dispatch_rename */
