// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

static void
chimera_lock_complete(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    request->lock.r_conflict_type   = conflict_type;
    request->lock.r_conflict_offset = conflict_offset;
    request->lock.r_conflict_length = conflict_length;
    request->lock.r_conflict_pid    = conflict_pid;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_lock_complete */

static void
chimera_lock_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_lock(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->lock.handle,
        request->lock.whence,
        request->lock.offset,
        request->lock.length,
        request->lock.lock_type,
        request->lock.flags,
        chimera_lock_complete,
        request);
} /* chimera_lock_start */

static void
chimera_lock_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_lock_callback_t callback     = request->lock.callback;
    void                   *callback_arg = request->lock.private_data;
    enum chimera_vfs_error  status       = request->txn_op_status;
    uint32_t                ct           = request->lock.r_conflict_type;
    uint64_t                co           = request->lock.r_conflict_offset;
    uint64_t                cl           = request->lock.r_conflict_length;
    pid_t                   cp           = request->lock.r_conflict_pid;

    chimera_client_request_free(thread, request);

    callback(thread, status, ct, co, cl, cp, callback_arg);
} /* chimera_lock_reply */

static inline void
chimera_dispatch_lock(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->lock.handle->fh,
                           request->lock.handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_lock_start, chimera_lock_reply);
} /* chimera_dispatch_lock */
