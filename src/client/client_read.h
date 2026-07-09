// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

static void
chimera_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    /* Stash the result for the txn reply.  The read buffers (iov) outlive the
    * read transaction's async commit, so they are safe to hand back later. */
    request->read.result_count = count;
    request->read.result_eof   = eof;
    request->read.r_iov        = iov;
    request->read.r_niov       = niov;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_read_complete */

static void
chimera_read_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_read(thread->vfs_thread,
                     chimera_client_req_cred(request), request->txn,
                     request->read.handle,
                     request->read.offset,
                     request->read.length,
                     request->read.iov,
                     CHIMERA_CLIENT_IOV_MAX,
                     0,
                     chimera_read_complete,
                     request);
} /* chimera_read_start */

static void
chimera_read_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_read_callback_t callback     = request->read.callback;
    void                   *callback_arg = request->read.private_data;
    enum chimera_vfs_error  status       = request->txn_op_status;
    struct evpl_iovec      *iov          = request->read.r_iov;
    int                     niov         = request->read.r_niov;

    chimera_client_request_free(thread, request);

    callback(thread, status, iov, niov, callback_arg);
} /* chimera_read_reply */

static inline void
chimera_dispatch_read(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->read.handle->fh,
                           request->read.handle->fh_len,
                           CHIMERA_VFS_TXN_READ,
                           chimera_read_start, chimera_read_reply);
} /* chimera_dispatch_read */
