// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"
#include "client_txn.h"

/* Shared reply for both the path and _at mkdir transactions. */
static void
chimera_mkdir_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_mkdir_callback_t callback     = request->mkdir.callback;
    void                    *callback_arg = request->mkdir.private_data;
    enum chimera_vfs_error   status       = request->txn_op_status;

    /* Note: parent handle (for the _at variant) is NOT released - caller owns it */
    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_mkdir_reply */

static void
chimera_mkdir_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_mkdir_vfs_complete */

static void
chimera_mkdir_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_mkdir(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->mkdir.path,
        request->mkdir.path_len,
        &request->mkdir.set_attr,
        0,
        chimera_mkdir_vfs_complete,
        request);
} /* chimera_mkdir_start */

static inline void
chimera_dispatch_mkdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->mkdir.name_offset == -1)) {
        /* Caller is trying to mkdir the root directory, which always exists already */
        chimera_dispatch_error_mkdir(thread, request, CHIMERA_VFS_EEXIST);
        return;
    }

    chimera_client_txn_run(thread, request,
                           thread->client->root_fh,
                           thread->client->root_fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_mkdir_start, chimera_mkdir_reply);
} /* chimera_dispatch_mkdir */

static void
chimera_mkdir_dispatch_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_txn_finish(request->thread, request, error_code);
} /* chimera_mkdir_dispatch_at_complete */

static void
chimera_mkdir_at_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_mkdir_at(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->txn,
        request->mkdir.parent_handle,
        request->mkdir.path,
        request->mkdir.path_len,
        &request->mkdir.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_mkdir_dispatch_at_complete,
        request);
} /* chimera_mkdir_at_start */

static inline void
chimera_dispatch_mkdir_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    request->mkdir.parent_handle = parent_handle;

    chimera_client_txn_run(thread, request,
                           parent_handle->fh, parent_handle->fh_len,
                           CHIMERA_VFS_TXN_WRITE,
                           chimera_mkdir_at_start, chimera_mkdir_reply);
} /* chimera_dispatch_mkdir_at */
