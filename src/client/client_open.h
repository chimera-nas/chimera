// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_open_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request  *request      = private_data;
    struct chimera_client_thread   *thread       = request->thread;
    chimera_open_callback_t         callback     = request->open.callback;
    void                           *callback_arg = request->open.private_data;
    struct chimera_vfs_open_handle *oh           = NULL;
    enum chimera_vfs_error          status;
    uint32_t                        last;

    status = chimera_vfs_compound_status(compound);
    last   = chimera_vfs_compound_num_ops(compound) - 1;

    if (status == CHIMERA_VFS_OK) {
        /* The handle is the caller's result and outlives the sequence. */
        oh = chimera_vfs_compound_take_handle(compound, last);
    }

    chimera_vfs_compound_free(compound);

    chimera_client_request_free(thread, request);

    callback(thread, status, oh, callback_arg);
} /* chimera_open_sequence_complete */

static inline void
chimera_dispatch_open(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_client_compound_at_root(thread, request);

    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_compound_add_open_path(compound,
                                       request->open.path,
                                       request->open.path_len,
                                       request->open.flags,
                                       &request->open.set_attr,
                                       CHIMERA_VFS_ATTR_FH);

    chimera_frontend_compound_submit(compound, chimera_open_sequence_complete,
                                     request);
} /* chimera_dispatch_open */

/* The caller retains the directory; only the created/opened child is taken. */
static inline void
chimera_dispatch_open_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, chimera_client_req_cred(request));

    request->compound = compound;
    chimera_vfs_compound_add_puthandle(compound, parent_handle, CHIMERA_VFS_OPEN_INFERRED);
    int                          opened = chimera_vfs_compound_add_open_at(compound, request->open.path, request->open.
                                                                           path_len,
                                                                           request->open.flags, &request->open.set_attr,
                                                                           CHIMERA_VFS_ATTR_FH);
    if (opened >= 0) {
        chimera_vfs_compound_op_set_handle(compound, opened, parent_handle);
    }
    chimera_frontend_compound_submit(compound, chimera_open_sequence_complete, request);
} /* chimera_dispatch_open_at */
