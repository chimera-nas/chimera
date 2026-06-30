// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"
#include "client_txn.h"

static void
chimera_rename_vfs_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_rename_vfs_complete */

static void
chimera_rename_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_rename(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->rename.source_path,
        request->rename.source_path_len,
        request->rename.dest_path,
        request->rename.dest_path_len,
        chimera_rename_vfs_complete,
        request);
} /* chimera_rename_start */

static void
chimera_rename_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_rename_callback_t callback     = request->rename.callback;
    void                     *callback_arg = request->rename.private_data;
    enum chimera_vfs_error    status       = request->txn_op_status;

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_rename_reply */

static inline void
chimera_dispatch_rename(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->rename.source_name_offset == -1 || request->rename.dest_name_offset == -1)) {
        chimera_dispatch_error_rename(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_rename_start, chimera_rename_reply);
} /* chimera_dispatch_rename */
