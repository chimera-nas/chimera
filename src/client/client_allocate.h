// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

static void
chimera_allocate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_allocate_complete */

static void
chimera_allocate_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_allocate(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->allocate.handle,
        request->allocate.offset,
        request->allocate.length,
        request->allocate.flags,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_allocate_complete,
        request);
} /* chimera_allocate_start */

static void
chimera_allocate_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_commit_callback_t callback     = request->allocate.callback;
    void                     *callback_arg = request->allocate.private_data;
    enum chimera_vfs_error    status       = request->txn_op_status;

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_allocate_reply */

static inline void
chimera_dispatch_allocate(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->allocate.handle->fh,
                           request->allocate.handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_allocate_start, chimera_allocate_reply);
} /* chimera_dispatch_allocate */
