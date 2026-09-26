// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif // ifdef _WIN32

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_remove_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_remove_callback_t      callback     = request->remove.callback;
    void                          *callback_arg = request->remove.private_data;
    enum chimera_vfs_error         status       = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_remove_sequence_complete */

static inline void
chimera_dispatch_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound;

    if (unlikely(request->remove.name_offset == -1)) {
        chimera_dispatch_error_remove(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    compound = chimera_client_compound_at_root(thread, request);

    chimera_vfs_compound_add_remove_path(compound,
                                         request->remove.path,
                                         request->remove.path_len,
                                         request->remove.flags);

    chimera_frontend_compound_submit(compound, chimera_remove_sequence_complete,
                                     request);
} /* chimera_dispatch_remove */

static void
chimera_remove_at_check(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_client_request  *request = private_data;
    const struct chimera_vfs_attrs *attr    = &chimera_vfs_compound_op(compound, index)->attr;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        bool directory = S_ISDIR(attr->va_mode);
        if (((request->remove.flags & CHIMERA_VFS_REMOVE_ISDIR) && !directory) ||
            ((request->remove.flags & CHIMERA_VFS_REMOVE_ISNOTDIR) && directory)) {
            *status = directory ? CHIMERA_VFS_EISDIR : CHIMERA_VFS_ENOTDIR;
            return;
        }
    }
    request->remove.child_fh_len = attr->va_fh_len;
    memcpy(request->remove.child_fh, attr->va_fh, attr->va_fh_len);
} // chimera_remove_at_check

static void
chimera_remove_at_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct chimera_client_request  *request = private_data;
    struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op_args(compound, index);

    op->arg_fh_len = request->remove.child_fh_len;
    memcpy(op->arg_fh, request->remove.child_fh, op->arg_fh_len);
} // chimera_remove_at_prepare

static inline void
chimera_dispatch_remove_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, chimera_client_req_cred(request));

    request->compound = compound;
    chimera_vfs_compound_add_puthandle(compound, parent_handle, CHIMERA_VFS_OPEN_INFERRED);
    int                          lookup = chimera_vfs_compound_add_lookup(compound, request->remove.path, request->
                                                                          remove.path_len, CHIMERA_VFS_ATTR_FH
                                                                          | CHIMERA_VFS_ATTR_MODE, 0);
    if (lookup >= 0) {
        chimera_vfs_compound_op_set_handle(compound, lookup, parent_handle);
        chimera_vfs_compound_set_op_callbacks(compound, lookup, NULL,
                                              chimera_remove_at_check, request);
    }
    chimera_vfs_compound_add_puthandle(compound, parent_handle, CHIMERA_VFS_OPEN_INFERRED);
    int remove = chimera_vfs_compound_add_remove(compound, request->remove.path, request->
                                                 remove.path_len, request->remove.
                                                 flags, 0, 0);
    if (remove >= 0) {
        chimera_vfs_compound_op_set_handle(compound, remove, parent_handle);
        chimera_vfs_compound_set_op_callbacks(compound, remove,
                                              chimera_remove_at_prepare, NULL, request);
    }
    chimera_frontend_compound_submit(compound, chimera_remove_sequence_complete, request);
} /* chimera_dispatch_remove_at */
