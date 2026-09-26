// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_read_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request        *request       = private_data;
    struct chimera_client_thread         *client_thread = request->thread;
    chimera_read_callback_t               callback      = request->read.callback;
    void                                 *callback_arg  = request->read.private_data;
    const struct chimera_vfs_compound_op *op;
    struct evpl_iovec                    *iov  = NULL;
    int                                   niov = 0;
    enum chimera_vfs_error                status;
    uint32_t                              last, count = 0, eof = 0;

    status = chimera_vfs_compound_status(compound);
    last   = chimera_vfs_compound_num_ops(compound) - 1;

    if (status == CHIMERA_VFS_OK) {
        op    = chimera_vfs_compound_op(compound, last);
        count = op->read_len;
        eof   = op->eof_read;

        /* The buffers go to the caller, who releases them, so they leave the
         * sequence's ownership before it is torn down. */
        chimera_vfs_compound_take_iov(compound, last, &iov, &niov);
    }

    /* Everything needed is out of the sequence now, on every path; the request
     * does not own it (see the note on ->compound). */
    chimera_vfs_compound_free(compound);

    // Store the actual count and eof for use by the callback
    request->read.result_count = count;
    request->read.result_eof   = eof;

    chimera_client_request_free(client_thread, request);

    callback(client_thread, status, iov, niov, callback_arg);
} /* chimera_read_sequence_complete */

static inline void
chimera_dispatch_read(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* The descriptor array is the request's and stays the request's: an
     * evpl_iovec records its owner's address, so it cannot be written into the
     * sequence and copied out afterwards. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->read.handle,
                                       CHIMERA_VFS_OPEN_INFERRED |
                                       CHIMERA_VFS_OPEN_READ_ONLY);
    /* The handle stays on the op too: the two-step I/O type check is guarded
     * on in_handle rather than on the cursor, so addressing the cursor alone
     * would put a GETATTR in front of every read. */
    chimera_vfs_compound_add_read(request->compound,
                                  request->read.handle,
                                  request->read.offset,
                                  request->read.length,
                                  request->read.iov,
                                  CHIMERA_CLIENT_IOV_MAX,
                                  NULL);

    chimera_frontend_compound_submit(request->compound,
                                     chimera_read_sequence_complete, request);
} /* chimera_dispatch_read */
