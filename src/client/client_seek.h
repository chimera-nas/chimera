// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_seek_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request        = private_data;
    struct chimera_client_thread         *client_thread  = request->thread;
    chimera_seek_callback_t               callback       = request->seek.callback;
    void                                 *callback_arg   = request->seek.private_data;
    int                                   heap_allocated = request->heap_allocated;
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

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, status, eof, offset, callback_arg);
} /* chimera_seek_sequence_complete */

static inline void
chimera_dispatch_seek(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* SEEK reads the allocation map, which wants the data open.  The
     * PUTHANDLE carries what the handle was really opened with -- see
     * open_flags on the request. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->seek.handle,
                                       request->seek.open_flags);
    chimera_vfs_compound_add_seek(request->compound, NULL,
                                  request->seek.offset, request->seek.what);

    chimera_frontend_compound_submit(request->compound,
                                     chimera_seek_sequence_complete, request);
} /* chimera_dispatch_seek */
