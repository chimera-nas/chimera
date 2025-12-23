// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_symlink_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    request->symlink.callback(request->thread, error_code, request->symlink.private_data);

    chimera_vfs_release(request->thread->vfs_thread, request->symlink.parent_handle);

    chimera_client_request_free(request->thread, request);

} /* chimera_symlink_at_complete */

static void
chimera_symlink_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->symlink.callback(request->thread, error_code, request->symlink.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->symlink.parent_handle = oh;

    request->symlink.set_attr.va_req_mask = 0;
    request->symlink.set_attr.va_set_mask = 0;

    chimera_vfs_symlink(
        request->thread->vfs_thread,
        oh,
        request->symlink.path + request->symlink.name_offset,
        request->symlink.path_len - request->symlink.name_offset,
        request->symlink.target,
        request->symlink.target_len,
        &request->symlink.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_symlink_at_complete,
        request);

} /* chimera_symlink_parent_complete */

static void
chimera_symlink_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->symlink.callback(request->thread, error_code, request->symlink.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_symlink_parent_complete,
        request);

} /* chimera_symlink_parent_lookup_complete */

void
chimera_dispatch_symlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->symlink.name_offset == -1)) {
        request->symlink.callback(request->thread, CHIMERA_VFS_EINVAL, request->symlink.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->symlink.path,
        request->symlink.parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_symlink_parent_lookup_complete,
        request);
} /* chimera_dispatch_symlink */

SYMBOL_EXPORT void
chimera_symlink(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    const char                   *target,
    int                           target_len,
    chimera_symlink_callback_t    callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    if (unlikely(target_len >= CHIMERA_VFS_PATH_MAX)) {
        callback(thread, CHIMERA_VFS_ENAMETOOLONG, private_data);
        return;
    }

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode               = CHIMERA_CLIENT_OP_SYMLINK;
    request->symlink.callback     = callback;
    request->symlink.private_data = private_data;
    request->symlink.path_len     = path_len;
    request->symlink.parent_len   = slash ? slash - path : path_len;
    request->symlink.target_len   = target_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->symlink.name_offset = slash ? slash - path : -1;

    memcpy(request->symlink.path, path, path_len);
    memcpy(request->symlink.target, target, target_len);

    chimera_dispatch_symlink(thread, request);
} /* chimera_symlink */

