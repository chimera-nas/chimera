// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_stat.h"

static void
chimera_fstat_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request = private_data;
    struct chimera_client_thread         *thread  = request->thread;
    const struct chimera_vfs_compound_op *op;
    chimera_fstat_callback_t              callback     = request->fstat.callback;
    void                                 *callback_arg = request->fstat.private_data;
    struct chimera_stat                   st;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        chimera_attrs_to_stat(&op->attr, &st);
    }

    chimera_vfs_compound_free(compound);
    chimera_client_request_free(thread, request);

    callback(thread, status, status == CHIMERA_VFS_OK ? &st : NULL,
             callback_arg);
} /* chimera_fstat_sequence_complete */

static inline void
chimera_dispatch_fstat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* The caller's handle becomes the current open handle; GETATTR reads it.
     * The flags say what the following op needs rather than what the handle
     * is, so the sequence accepts it instead of opening one of its own. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->fstat.handle,
                                       CHIMERA_VFS_OPEN_INFERRED |
                                       CHIMERA_VFS_OPEN_PATH);
    chimera_vfs_compound_add_getattr(request->compound,
                                     CHIMERA_VFS_ATTR_MASK_STAT);

    chimera_frontend_compound_submit(request->compound,
                                     chimera_fstat_sequence_complete, request);
} /* chimera_dispatch_fstat */
