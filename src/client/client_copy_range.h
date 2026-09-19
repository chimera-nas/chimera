// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_copy_range_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_copy_range_callback_t  callback       = request->copy_range.callback;
    void                          *callback_arg   = request->copy_range.private_data;
    int                            heap_allocated = request->heap_allocated;

    request->copy_range.r_length = length;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, length, callback_arg);
} /* chimera_copy_range_complete */

static void
chimera_copy_range_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              written = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        written = op->written;
    }

    chimera_vfs_compound_free(compound);

    chimera_copy_range_complete(status, written, NULL, NULL, private_data);
} /* chimera_copy_range_sequence_complete */

static inline void
chimera_dispatch_copy_range(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* See chimera_dispatch_clone_range on the cursor shape. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->copy_range.src_handle,
                                       request->copy_range.src_open_flags);
    chimera_vfs_compound_add_savehandle(request->compound);
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->copy_range.dst_handle,
                                       request->copy_range.dst_open_flags);
    chimera_vfs_compound_add_copy_range(request->compound,
                                        request->copy_range.src_handle,
                                        request->copy_range.src_offset,
                                        request->copy_range.dst_handle,
                                        request->copy_range.dst_offset,
                                        request->copy_range.length,
                                        request->copy_range.flags,
                                        0, 0);

    chimera_vfs_compound_submit(request->compound,
                                chimera_copy_range_sequence_complete, request);
} /* chimera_dispatch_copy_range */
