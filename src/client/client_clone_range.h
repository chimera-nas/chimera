// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_clone_range_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_clone_range_callback_t callback       = request->clone_range.callback;
    void                          *callback_arg   = request->clone_range.private_data;
    int                            heap_allocated = request->heap_allocated;
    enum chimera_vfs_error         status         = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, status, callback_arg);
} /* chimera_clone_range_sequence_complete */

static inline void
chimera_dispatch_clone_range(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* Source into the saved open slot, destination into the current one --
     * the shape the range ops read in the cursor model.  The handles stay on
     * the op until the dispatch reads the cursors instead.  Each PUTHANDLE
     * carries what its handle was really opened with -- see open_flags on the
     * request. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->clone_range.src_handle,
                                       request->clone_range.src_open_flags);
    chimera_vfs_compound_add_savehandle(request->compound);
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->clone_range.dst_handle,
                                       request->clone_range.dst_open_flags);
    chimera_vfs_compound_add_clone_range(request->compound,
                                         request->clone_range.src_handle,
                                         request->clone_range.src_offset,
                                         request->clone_range.dst_handle,
                                         request->clone_range.dst_offset,
                                         request->clone_range.length,
                                         0, 0);

    chimera_frontend_compound_submit(request->compound,
                                     chimera_clone_range_sequence_complete, request);
} /* chimera_dispatch_clone_range */
