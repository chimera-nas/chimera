// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

static void
chimera_seek_complete(
    enum chimera_vfs_error error_code,
    int                    eof,
    uint64_t               offset,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    request->seek.r_eof    = eof;
    request->seek.r_offset = offset;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_seek_complete */

static void
chimera_seek_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_seek(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->seek.handle,
        request->seek.offset,
        request->seek.what,
        chimera_seek_complete,
        request);
} /* chimera_seek_start */

static void
chimera_seek_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_seek_callback_t callback     = request->seek.callback;
    void                   *callback_arg = request->seek.private_data;
    enum chimera_vfs_error  status       = request->txn_op_status;
    int                     eof          = request->seek.r_eof;
    uint64_t                offset       = request->seek.r_offset;

    chimera_client_request_free(thread, request);

    callback(thread, status, eof, offset, callback_arg);
} /* chimera_seek_reply */

static inline void
chimera_dispatch_seek(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->seek.handle->fh,
                           request->seek.handle->fh_len,
                           CHIMERA_VFS_TXN_READ,
                           chimera_seek_start, chimera_seek_reply);
} /* chimera_dispatch_seek */
