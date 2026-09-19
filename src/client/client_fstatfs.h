// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_statfs.h"

static void
chimera_fstatfs_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_fstatfs_callback_t     callback      = request->fstatfs.callback;
    void                          *callback_arg  = request->fstatfs.private_data;
    struct chimera_statvfs         st;

    chimera_client_request_free(client_thread, request);

    if (error_code != CHIMERA_VFS_OK) {
        callback(client_thread, error_code, NULL, callback_arg);
        return;
    }

    chimera_attrs_to_statvfs(attr, &st);

    callback(client_thread, CHIMERA_VFS_OK, &st, callback_arg);
} /* chimera_fstatfs_getattr_complete */

static void
chimera_fstatfs_sequence_complete(
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

    chimera_vfs_compound_free(compound);

    chimera_fstatfs_getattr_complete(status, &attr, private_data);
} /* chimera_fstatfs_sequence_complete */

static inline void
chimera_dispatch_fstatfs(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                                   chimera_client_req_cred(request));

    /* See chimera_dispatch_fstat on the flags. */
    chimera_vfs_compound_add_puthandle(request->compound,
                                       request->fstatfs.handle,
                                       request->fstatfs.open_flags);
    chimera_vfs_compound_add_getattr(request->compound,
                                     CHIMERA_VFS_ATTR_MASK_STATFS);

    chimera_vfs_compound_submit(request->compound,
                                chimera_fstatfs_sequence_complete, request);
} /* chimera_dispatch_fstatfs */
