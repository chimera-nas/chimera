// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_readlink_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request = private_data;
    struct chimera_client_thread         *thread  = request->thread;
    const struct chimera_vfs_compound_op *op;
    chimera_readlink_callback_t           callback       = request->readlink.callback;
    void                                 *callback_arg   = request->readlink.private_data;
    char                                 *target         = request->readlink.target;
    int                                   heap_allocated = request->heap_allocated;
    enum chimera_vfs_error                status;
    int                                   len = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);

        /* The sequence read into its own buffer; copy out what the caller has
         * room for, as the per-op readlink bounded it on the way in. */
        len = (int) op->target_len;

        if (len > request->readlink.target_maxlength) {
            len = request->readlink.target_maxlength;
        }

        if (len > 0) {
            memcpy(target, op->target, len);
        }
    }

    /* The compound owns and releases the handle used for this request. */
    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_compound_free(compound);

    callback(thread, status, status == CHIMERA_VFS_OK ? target : NULL, len,
             callback_arg);
} /* chimera_readlink_sequence_complete */

static inline void
chimera_dispatch_readlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;
    int                          open_idx, link_idx;

    compound = chimera_client_compound_at_root(thread, request);

    /*
     * Resolve the path through an OPEN so this works for both path-only
     * backends (which return no re-openable child fh from lookup) and
     * FH-relative backends.  NOFOLLOW keeps the final symlink itself (its
     * target is what we want to read), rather than following it.
     */
    open_idx = chimera_vfs_compound_add_open_path(
        compound, request->readlink.path, request->readlink.path_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
        CHIMERA_VFS_OPEN_NOFOLLOW, NULL, 0);

    link_idx = chimera_vfs_compound_add_readlink(compound);

    if (open_idx >= 0 && link_idx >= 0) {
        chimera_vfs_compound_op_use_handle(compound, (uint32_t) link_idx,
                                           (uint32_t) open_idx);
    }

    chimera_frontend_compound_submit(compound, chimera_readlink_sequence_complete,
                                     request);
} /* chimera_dispatch_readlink */
