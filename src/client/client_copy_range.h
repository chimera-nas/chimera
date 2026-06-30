// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

static void
chimera_copy_range_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    request->copy_range.r_length = length;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_copy_range_complete */

static void
chimera_copy_range_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_copy_range(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->copy_range.src_handle,
        request->copy_range.src_offset,
        request->copy_range.dst_handle,
        request->copy_range.dst_offset,
        request->copy_range.length,
        0,
        0,
        chimera_copy_range_complete,
        request);
} /* chimera_copy_range_start */

static void
chimera_copy_range_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_copy_range_callback_t callback     = request->copy_range.callback;
    void                         *callback_arg = request->copy_range.private_data;
    enum chimera_vfs_error        status       = request->txn_op_status;
    uint64_t                      length       = request->copy_range.r_length;

    chimera_client_request_free(thread, request);

    callback(thread, status, length, callback_arg);
} /* chimera_copy_range_reply */

static inline void
chimera_dispatch_copy_range(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->copy_range.dst_handle->fh,
                           request->copy_range.dst_handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_copy_range_start, chimera_copy_range_reply);
} /* chimera_dispatch_copy_range */
