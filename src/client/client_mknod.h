// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"
#include "client_txn.h"

static void
chimera_mknod_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_mknod_vfs_complete */

static void
chimera_mknod_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_mknod(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->mknod.path,
        request->mknod.path_len,
        &request->mknod.set_attr,
        0,
        chimera_mknod_vfs_complete,
        request);
} /* chimera_mknod_start */

static void
chimera_mknod_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_mknod_callback_t callback     = request->mknod.callback;
    void                    *callback_arg = request->mknod.private_data;
    enum chimera_vfs_error   status       = request->txn_op_status;

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_mknod_reply */

static inline void
chimera_dispatch_mknod(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->mknod.name_offset == -1)) {
        chimera_dispatch_error_mknod(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_mknod_start, chimera_mknod_reply);
} /* chimera_dispatch_mknod */
