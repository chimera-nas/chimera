// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"
#include "client_txn.h"

static void
chimera_symlink_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_symlink_vfs_complete */

static void
chimera_symlink_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_symlink(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->symlink.path,
        request->symlink.path_len,
        request->symlink.target,
        request->symlink.target_len,
        &request->symlink.set_attr,
        CHIMERA_VFS_ATTR_FH,
        chimera_symlink_vfs_complete,
        request);
} /* chimera_symlink_start */

static void
chimera_symlink_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_symlink_callback_t callback     = request->symlink.callback;
    void                      *callback_arg = request->symlink.private_data;
    enum chimera_vfs_error     status       = request->txn_op_status;

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_symlink_reply */

static inline void
chimera_dispatch_symlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->symlink.name_offset == -1)) {
        chimera_dispatch_error_symlink(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    request->symlink.set_attr.va_req_mask = 0;
    request->symlink.set_attr.va_set_mask = 0;

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_symlink_start, chimera_symlink_reply);
} /* chimera_dispatch_symlink */
