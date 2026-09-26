// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_mkdir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_mkdir_callback_t       callback     = request->mkdir.callback;
    void                          *callback_arg = request->mkdir.private_data;
    enum chimera_vfs_error         status       = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_mkdir_sequence_complete */

static inline void
chimera_dispatch_mkdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    if (unlikely(request->mkdir.name_offset == -1)) {
        /* Caller is trying to mkdir the root directory, which always exists already */
        chimera_dispatch_error_mkdir(thread, request, CHIMERA_VFS_EEXIST);
        return;
    }

    compound = chimera_client_compound_at_root(thread, request);

    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_compound_add_create_path(compound,
                                         CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                         request->mkdir.path,
                                         request->mkdir.path_len,
                                         NULL, 0,
                                         &request->mkdir.set_attr, 0);

    chimera_frontend_compound_submit(compound, chimera_mkdir_sequence_complete,
                                     request);
} /* chimera_dispatch_mkdir */

static inline void
chimera_dispatch_mkdir_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, chimera_client_req_cred(request));

    request->compound = compound;
    chimera_vfs_compound_add_puthandle(compound, parent_handle, CHIMERA_VFS_OPEN_INFERRED);
    int                          created = chimera_vfs_compound_add_create(compound, CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                                                           request->mkdir.path, request->mkdir.path_len,
                                                                           NULL, 0,
                                                                           &request->mkdir.set_attr, CHIMERA_VFS_ATTR_FH
                                                                           );
    if (created >= 0) {
        chimera_vfs_compound_op_set_handle(compound, created, parent_handle);
    }
    chimera_frontend_compound_submit(compound, chimera_mkdir_sequence_complete, request);
} /* chimera_dispatch_mkdir_at */
