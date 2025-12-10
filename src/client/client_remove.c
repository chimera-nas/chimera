// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_remove_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    request->remove.callback(request->thread, error_code, request->remove.private_data);

    chimera_vfs_release(request->thread->vfs_thread, request->remove.parent_handle);

    chimera_client_request_free(request->thread, request);

} /* chimera_remove_at_complete */

static void
chimera_remove_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->remove.callback(request->thread, error_code, request->remove.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->remove.parent_handle = oh;

    chimera_vfs_remove(
        request->thread->vfs_thread,
        oh,
        request->remove.path + request->remove.name_offset,
        request->remove.path_len - request->remove.name_offset,
        0,
        0,
        chimera_remove_at_complete,
        request);

} /* chimera_remove_parent_complete */

static void
chimera_remove_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->remove.callback(request->thread, error_code, request->remove.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_remove_parent_complete,
        request);


} /* chimera_remove_parent_lookup_complete */

static inline void
chimera_dispatch_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->remove.name_offset == -1)) {
        request->remove.callback(request->thread, CHIMERA_VFS_EINVAL, request->remove.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->remove.path,
        request->remove.parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_remove_parent_lookup_complete,
        request);
} /* chimera_dispatch_remove */

SYMBOL_EXPORT void
chimera_remove(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_remove_callback_t     callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode              = CHIMERA_CLIENT_OP_REMOVE;
    request->remove.callback     = callback;
    request->remove.private_data = private_data;
    request->remove.path_len     = path_len;
    request->remove.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->remove.name_offset = slash ? slash - path : -1;

    memcpy(request->remove.path, path, path_len);

    chimera_dispatch_remove(thread, request);
} /* chimera_remove */

