// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_stat.h"
#include "client_txn.h"

static void
chimera_fstat_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        chimera_attrs_to_stat(attr, &request->sync_stat);
    }

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_fstat_getattr_complete */

static void
chimera_fstat_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_getattr(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->fstat.handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        chimera_fstat_getattr_complete,
        request);
} /* chimera_fstat_start */

static void
chimera_fstat_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_fstat_callback_t callback     = request->fstat.callback;
    void                    *callback_arg = request->fstat.private_data;
    enum chimera_vfs_error   status       = request->txn_op_status;
    struct chimera_stat      st           = request->sync_stat;

    chimera_client_request_free(thread, request);

    if (status != CHIMERA_VFS_OK) {
        callback(thread, status, NULL, callback_arg);
        return;
    }

    callback(thread, CHIMERA_VFS_OK, &st, callback_arg);
} /* chimera_fstat_reply */

static inline void
chimera_dispatch_fstat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->fstat.handle->fh,
                           request->fstat.handle->fh_len,
                           CHIMERA_VFS_TXN_READ,
                           chimera_fstat_start, chimera_fstat_reply);
} /* chimera_dispatch_fstat */
