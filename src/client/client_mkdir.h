// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_mkdir_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_mkdir_callback_t       callback     = request->mkdir.callback;
    void                          *callback_arg = request->mkdir.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, callback_arg);
} /* chimera_mkdir_vfs_complete */

static void
chimera_mkdir_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        attr = op->attr;
    }

    /* Taken out before the free: a freed sequence is recycled and reset. */
    chimera_vfs_compound_free(compound);

    chimera_mkdir_vfs_complete(status, &attr, private_data);
} /* chimera_mkdir_sequence_complete */

static inline void
chimera_dispatch_mkdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    if (unlikely(request->mkdir.name_offset == -1)) {
        /* Caller is trying to mkdir the root directory, which always exists already */
        chimera_dispatch_error_mkdir(thread, request, CHIMERA_VFS_EEXIST);
        return;
    }

    compound = chimera_client_compound_at_root(thread, request);

    /* set_attr (creation mode) is initialized by the caller. */
    chimera_vfs_compound_add_create_path(compound,
                                         CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                         request->mkdir.path,
                                         request->mkdir.path_len,
                                         NULL, 0,
                                         &request->mkdir.set_attr, 0, 0);

    chimera_vfs_compound_submit(compound, chimera_mkdir_sequence_complete,
                                request);
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
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_mkdir_callback_t       callback       = request->mkdir.callback;
    void                          *callback_arg   = request->mkdir.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    /* Note: parent handle is NOT released - caller owns it */
    callback(client_thread, error_code, callback_arg);
} /* chimera_mkdir_dispatch_at_complete */

static inline void
chimera_dispatch_mkdir_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    chimera_vfs_mkdir_at(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        parent_handle,
        request->mkdir.path,
        request->mkdir.path_len,
        &request->mkdir.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_mkdir_dispatch_at_complete,
        request);
} /* chimera_dispatch_mkdir_at */
