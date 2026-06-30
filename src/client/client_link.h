// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"
#include "client_txn.h"

static void
chimera_link_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_link_vfs_complete */

static void
chimera_link_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_link(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->link.source_path,
        request->link.source_path_len,
        request->link.dest_path,
        request->link.dest_path_len,
        0,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_vfs_complete,
        request);
} /* chimera_link_start */

static void
chimera_link_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_link_callback_t callback     = request->link.callback;
    void                   *callback_arg = request->link.private_data;
    enum chimera_vfs_error  status       = request->txn_op_status;

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_link_reply */

static inline void
chimera_dispatch_link(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->link.dest_name_offset == -1)) {
        chimera_dispatch_error_link(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_link_start, chimera_link_reply);
} /* chimera_dispatch_link */
