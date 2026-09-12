// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_seek_complete(
    enum chimera_vfs_error error_code,
    int                    eof,
    uint64_t               offset,
    void                  *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_seek_callback_t        callback       = request->seek.callback;
    void                          *callback_arg   = request->seek.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, eof, offset, callback_arg);
} /* chimera_seek_complete */

static void
chimera_seek_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint64_t                              offset = 0;
    int                                   eof    = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        offset = op->seek_offset;
        eof    = (int) op->seek_eof;
    }

    chimera_vfs_compound_free(compound);

    chimera_seek_complete(status, eof, offset, private_data);
} /* chimera_seek_sequence_complete */

static inline void
chimera_dispatch_seek(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    int idx;

    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* The caller holds the handle; the sequence borrows it, so it has no
     * current object of its own. */
    idx = chimera_vfs_compound_add_seek(request->compound, NULL,
                                        request->seek.offset, request->seek.what);
    chimera_vfs_compound_op_set_handle(request->compound, (uint32_t) idx,
                                       request->seek.handle);

    chimera_vfs_compound_submit(request->compound,
                                chimera_seek_sequence_complete, request);
} /* chimera_dispatch_seek */
