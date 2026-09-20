// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_open_vfs_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_open_callback_t        callback     = request->open.callback;
    void                          *callback_arg = request->open.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, oh, callback_arg);
} /* chimera_open_vfs_complete */

static void
chimera_open_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_open_handle       *oh = NULL;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;
    uint32_t                              last;

    status = chimera_vfs_compound_status(compound);
    last   = chimera_vfs_compound_num_ops(compound) - 1;

    memset(&attr, 0, sizeof(attr));

    if (status == CHIMERA_VFS_OK) {
        op   = chimera_vfs_compound_op(compound, last);
        attr = op->attr;

        /* The handle is the caller's result and outlives the sequence. */
        oh = chimera_vfs_compound_take_handle(compound, last);
    }

    chimera_vfs_compound_free(compound);

    chimera_open_vfs_complete(status, oh, &attr, private_data);
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

    chimera_vfs_compound_submit(compound, chimera_open_sequence_complete,
                                request);
} /* chimera_dispatch_open */

/*
 * openat(2) with a real directory descriptor: the descriptor is lent and
 * checked to be a directory, then the same OPEN_PATH the path form issues
 * from the root resolves the relative path from it -- see
 * chimera_client_compound_at_dir for why a path op and not a named OPEN.
 * The completion is the path form's: the handle is taken from the last op.
 */
static inline void
chimera_dispatch_open_at(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    compound = chimera_client_compound_at_dir(thread, request,
                                              request->open.parent_handle,
                                              request->open.dir_open_flags);

    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_compound_add_open_path(compound,
                                       request->open.path,
                                       request->open.path_len,
                                       request->open.flags,
                                       &request->open.set_attr,
                                       CHIMERA_VFS_ATTR_FH);

    chimera_vfs_compound_submit(compound, chimera_open_sequence_complete,
                                request);
} /* chimera_dispatch_open_at */
