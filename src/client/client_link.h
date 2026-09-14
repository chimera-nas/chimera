// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_link_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_link_callback_t        callback     = request->link.callback;
    void                          *callback_arg = request->link.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, callback_arg);
} /* chimera_link_vfs_complete */

static void
chimera_link_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        attr = op->attr;
    }

    /* Taken out before the free: a freed sequence is recycled and reset. */
    chimera_vfs_compound_free(compound);

    chimera_link_vfs_complete(status, &attr, private_data);
} /* chimera_link_sequence_complete */

static inline void
chimera_dispatch_link(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    if (unlikely(request->link.dest_name_offset == -1)) {
        chimera_dispatch_error_link(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    compound = chimera_client_compound_at_root(thread, request);

    chimera_vfs_compound_add_link_path(compound,
                                       request->link.source_path,
                                       request->link.source_path_len,
                                       request->link.source_lookup_flags,
                                       request->link.dest_path,
                                       request->link.dest_path_len,
                                       CHIMERA_VFS_ATTR_FH);

    chimera_vfs_compound_submit(compound, chimera_link_sequence_complete,
                                request);
} /* chimera_dispatch_link */
