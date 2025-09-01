// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"


static void
chimera_mkdir_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    request->mkdir.callback(request->thread, error_code, request->mkdir.private_data);

    chimera_vfs_release(request->thread->vfs_thread, request->mkdir.parent_handle);

    chimera_client_request_free(request->thread, request);

} /* chimera_mkdir_at_complete */

static void
chimera_mkdir_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_vfs_attrs       set_attr;

    if (error_code != CHIMERA_VFS_OK) {
        request->mkdir.callback(request->thread, error_code, request->mkdir.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->mkdir.parent_handle = oh;

    set_attr.va_req_mask = 0;
    set_attr.va_set_mask = 0;

    chimera_vfs_mkdir(
        request->thread->vfs_thread,
        oh,
        request->mkdir.path + request->mkdir.name_offset,
        request->mkdir.path_len - request->mkdir.name_offset,
        &set_attr,
        0,
        0,
        0,
        chimera_mkdir_at_complete,
        request);

} /* chimera_mkdir_parent_complete */

static void
chimera_mkdir_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->mkdir.callback(request->thread, error_code, request->mkdir.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_mkdir_parent_complete,
        request);


} /* chimera_mkdir_parent_lookup_complete */

static inline void
chimera_dispatch_mkdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->mkdir.name_offset == -1)) {
        /* Caller is trying to mkdir the root directory, which always exists already */
        request->mkdir.callback(request->thread, CHIMERA_VFS_EEXIST, request->mkdir.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->mkdir.path,
        request->mkdir.parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_mkdir_parent_lookup_complete,
        request);
} /* chimera_mkdir */

SYMBOL_EXPORT void
chimera_mkdir(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_mkdir_callback_t      callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode             = CHIMERA_CLIENT_OP_MKDIR;
    request->mkdir.callback     = callback;
    request->mkdir.private_data = private_data;
    request->mkdir.path_len     = path_len;
    request->mkdir.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->mkdir.name_offset = slash ? slash - path : -1;

    memcpy(request->mkdir.path, path, path_len);

    chimera_dispatch_mkdir(thread, request);
} /* chimera_mkdir */