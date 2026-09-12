// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_allocate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_commit_callback_t      callback       = request->allocate.callback;
    void                          *callback_arg   = request->allocate.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, callback_arg);
} /* chimera_allocate_complete */

static void
chimera_allocate_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    /* Read the status BEFORE the free: a freed sequence is recycled and reset,
     * so asking it afterwards reports success whatever happened.  The request
     * does not own it -- see the note on ->compound. */
    chimera_vfs_compound_free(compound);

    chimera_allocate_complete(status, NULL, NULL, private_data);
} /* chimera_allocate_sequence_complete */

static inline void
chimera_dispatch_allocate(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* The caller holds the handle; the sequence borrows it. */
    chimera_vfs_compound_add_allocate(request->compound,
                                      request->allocate.handle,
                                      request->allocate.offset,
                                      request->allocate.length,
                                      request->allocate.flags,
                                      0, 0);

    chimera_vfs_compound_submit(request->compound,
                                chimera_allocate_sequence_complete, request);
} /* chimera_dispatch_allocate */
