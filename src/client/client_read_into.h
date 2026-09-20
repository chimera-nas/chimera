// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_read_into_complete(
    enum chimera_vfs_error         error_code,
    uint32_t                       count,
    uint32_t                       eof,
    struct chimera_client_request *request)
{
    struct chimera_client_thread *client_thread = request->thread;
    chimera_read_into_callback_t  callback      = request->read_into.callback;
    void                         *callback_arg  = request->read_into.private_data;

    /* The data has already landed in the caller's destination buffers.  The
     * caller owns and releases those buffers, so nothing is released here. */
    request->read_into.result_count = count;
    request->read_into.result_eof   = eof;

    chimera_client_request_free(client_thread, request);

    callback(client_thread, error_code, count, eof, callback_arg);
} /* chimera_read_into_complete */

static void
chimera_read_into_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              count = 0, eof = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        count = op->read_len;
        eof   = op->eof_read;
    }

    /* A READ with dest_iov leaves the compound owning nothing of the data:
     * the op's iov IS the caller's dest_iov, handed back, and take_iov would
     * answer NULL / 0.  So the free releases no data, and the caller's
     * buffers are untouched by it. */
    chimera_vfs_compound_free(compound);

    chimera_read_into_complete(status, count, eof, private_data);
} /* chimera_read_into_sequence_complete */

static inline void
chimera_dispatch_read_into(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* The PUTHANDLE says what the handle was really opened with -- see
     * open_flags on the request -- and the handle stays on the op too, for
     * the reason given in chimera_dispatch_read.  dest_iov turns the READ
     * into chimera_vfs_read_into: the scratch array is the request's, the
     * destination the caller's, both borrowed for the run. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->read_into.handle,
                                       request->read_into.open_flags);
    chimera_vfs_compound_add_read(request->compound,
                                  request->read_into.handle,
                                  request->read_into.offset,
                                  request->read_into.length,
                                  request->read_into.iov,
                                  CHIMERA_CLIENT_IOV_MAX,
                                  0,
                                  NULL,
                                  request->read_into.dest_iov,
                                  request->read_into.dest_niov);

    chimera_vfs_compound_submit(request->compound,
                                chimera_read_into_sequence_complete, request);
} /* chimera_dispatch_read_into */
