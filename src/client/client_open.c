// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_open_path_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    request->open.callback(request->thread, error_code, oh, request->open.private_data);

    chimera_vfs_release(request->thread->vfs_thread, request->open.parent_handle);

    chimera_client_request_free(request->thread, request);

} /* chimera_open_path_complete */

static void
chimera_open_path_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    request->open.callback(request->thread, error_code, oh, request->open.private_data);

    chimera_client_request_free(request->thread, request);
} /* chimera_open_path_complete */

static void
chimera_open_path_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->open.callback(request->thread, error_code, NULL, request->open.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }
    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        request->open.flags,
        chimera_open_path_complete,
        request);


} /* chimera_open_path_complete */

static void
chimera_open_path_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->open.callback(request->thread, error_code, NULL, request->open.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->open.parent_handle = oh;

    request->open.set_attr.va_req_mask = 0;
    request->open.set_attr.va_set_mask = 0;

    chimera_vfs_open_at(
        request->thread->vfs_thread,
        oh,
        request->open.path + request->open.name_offset,
        request->open.path_len - request->open.name_offset,
        request->open.flags,
        &request->open.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_open_path_at_complete,
        request);

} /* chimera_client_open_path_parent_complete */

static void
chimera_open_path_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->open.callback(request->thread, error_code, NULL, request->open.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_open_path_parent_complete,
        request);


} /* chimera_client_open_path_complete */

void
chimera_dispatch_open(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (request->open.flags & CHIMERA_VFS_OPEN_CREATE) {

        chimera_vfs_lookup_path(
            thread->vfs_thread,
            root_fh,
            sizeof(root_fh),
            request->open.path,
            request->open.parent_len,
            CHIMERA_VFS_ATTR_FH,
            chimera_open_path_parent_lookup_complete,
            request);
    } else {
        chimera_vfs_lookup_path(
            thread->vfs_thread,
            root_fh,
            sizeof(root_fh),
            request->open.path,
            request->open.path_len,
            CHIMERA_VFS_ATTR_FH,
            chimera_open_path_lookup_complete,
            request);

    }

} /* chimera_client_open_path */


SYMBOL_EXPORT void
chimera_open(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    unsigned int                  flags,
    chimera_open_callback_t       callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode            = CHIMERA_CLIENT_OP_OPEN;
    request->open.callback     = callback;
    request->open.private_data = private_data;
    request->open.flags        = flags;
    request->open.path_len     = path_len;
    request->open.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->open.name_offset = slash ? slash - path : -1;

    memcpy(request->open.path, path, path_len);

    chimera_dispatch_open(thread, request);
} /* chimera_open */