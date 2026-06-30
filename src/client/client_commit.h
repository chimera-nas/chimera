// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_txn.h"

static void
chimera_commit_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_commit_complete */

static void
chimera_commit_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_commit(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->commit.handle,
        0,  /* offset - sync entire file */
        0,  /* count - sync entire file */
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_commit_complete,
        request);
} /* chimera_commit_start */

static void
chimera_commit_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_commit_callback_t callback     = request->commit.callback;
    void                     *callback_arg = request->commit.private_data;
    enum chimera_vfs_error    status       = request->txn_op_status;

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_commit_reply */

static inline void
chimera_dispatch_commit(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_client_txn_run(thread, request,
                           request->commit.handle->fh,
                           request->commit.handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_commit_start, chimera_commit_reply);
} /* chimera_dispatch_commit */
