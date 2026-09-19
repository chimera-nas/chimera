// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_fsetattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_fsetattr_callback_t    callback       = request->fsetattr.callback;
    void                          *callback_arg   = request->fsetattr.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, callback_arg);
} /* chimera_fsetattr_complete */

static void
chimera_fsetattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error status = chimera_vfs_compound_status(compound);

    /* Read the status BEFORE the free: a freed sequence is recycled and reset,
     * so asking it afterwards reports success whatever happened.  The request
     * does not own it -- see the note on ->compound. */
    chimera_vfs_compound_free(compound);

    chimera_fsetattr_complete(status, NULL, NULL, NULL, private_data);
} /* chimera_fsetattr_sequence_complete */

static inline void
chimera_dispatch_fsetattr(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* Descriptor-originated: WRITE_DATA-only mutations (ftruncate,
     * futimens-to-now) ride the descriptor's open-time grant, which is what a
     * SETATTR against a borrowed handle does -- the executor reaches for
     * chimera_vfs_fsetattr exactly when one is supplied.  The PUTHANDLE
     * carries what the handle was really opened with -- see open_flags on
     * the request. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->fsetattr.handle,
                                       request->fsetattr.open_flags);
    /* The handle stays on the op as well: SETATTR reaches for
     * chimera_vfs_fsetattr only when one is supplied, and addressing the
     * cursor instead would quietly become chimera_vfs_setattr -- truncate(2)
     * semantics where ftruncate(2) is meant. */
    chimera_vfs_compound_add_setattr(request->compound,
                                     request->fsetattr.handle,
                                     &request->fsetattr.set_attr,
                                     0, 0);

    chimera_vfs_compound_submit(request->compound,
                                chimera_fsetattr_sequence_complete, request);
} /* chimera_dispatch_fsetattr */
