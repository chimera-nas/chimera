// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_rename_at_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    request->rename.callback(request->thread, error_code, request->rename.private_data);

    chimera_vfs_release(request->thread->vfs_thread, request->rename.source_parent_handle);
    chimera_vfs_release(request->thread->vfs_thread, request->rename.dest_parent_handle);

    chimera_client_request_free(request->thread, request);

} /* chimera_rename_at_complete */

static void
chimera_rename_dest_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->rename.callback(request->thread, error_code, request->rename.private_data);
        chimera_vfs_release(request->thread->vfs_thread, request->rename.source_parent_handle);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->rename.dest_parent_handle = oh;
    memcpy(request->rename.dest_fh, oh->fh, oh->fh_len);
    request->rename.dest_fh_len = oh->fh_len;

    chimera_vfs_rename(
        request->thread->vfs_thread,
        request->rename.source_fh,
        request->rename.source_fh_len,
        request->rename.source_path + request->rename.source_name_offset,
        request->rename.source_path_len - request->rename.source_name_offset,
        request->rename.dest_fh,
        request->rename.dest_fh_len,
        request->rename.dest_path + request->rename.dest_name_offset,
        request->rename.dest_path_len - request->rename.dest_name_offset,
        chimera_rename_at_complete,
        request);

} /* chimera_rename_dest_parent_complete */

static void
chimera_rename_dest_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->rename.callback(request->thread, error_code, request->rename.private_data);
        chimera_vfs_release(request->thread->vfs_thread, request->rename.source_parent_handle);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_rename_dest_parent_complete,
        request);

} /* chimera_rename_dest_parent_lookup_complete */

static void
chimera_rename_source_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->rename.callback(request->thread, error_code, request->rename.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->rename.source_parent_handle = oh;
    memcpy(request->rename.source_fh, oh->fh, oh->fh_len);
    request->rename.source_fh_len = oh->fh_len;

    chimera_vfs_lookup_path(
        request->thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->rename.dest_path,
        request->rename.dest_parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_rename_dest_parent_lookup_complete,
        request);

} /* chimera_rename_source_parent_complete */

static void
chimera_rename_source_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->rename.callback(request->thread, error_code, request->rename.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_rename_source_parent_complete,
        request);

} /* chimera_rename_source_parent_lookup_complete */

static inline void
chimera_dispatch_rename(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->rename.source_name_offset == -1 || request->rename.dest_name_offset == -1)) {
        request->rename.callback(request->thread, CHIMERA_VFS_EINVAL, request->rename.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->rename.source_path,
        request->rename.source_parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_rename_source_parent_lookup_complete,
        request);
} /* chimera_dispatch_rename */

SYMBOL_EXPORT void
chimera_rename(
    struct chimera_client_thread *thread,
    const char                   *source_path,
    int                           source_path_len,
    const char                   *dest_path,
    int                           dest_path_len,
    chimera_rename_callback_t     callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *source_slash;
    const char                    *dest_slash;

    source_slash = rindex(source_path, '/');
    dest_slash   = rindex(dest_path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode                   = CHIMERA_CLIENT_OP_RENAME;
    request->rename.callback          = callback;
    request->rename.private_data      = private_data;
    request->rename.source_path_len   = source_path_len;
    request->rename.source_parent_len = source_slash ? source_slash - source_path : source_path_len;
    request->rename.dest_path_len     = dest_path_len;
    request->rename.dest_parent_len   = dest_slash ? dest_slash - dest_path : dest_path_len;

    while (source_slash && *source_slash == '/') {
        source_slash++;
    }

    while (dest_slash && *dest_slash == '/') {
        dest_slash++;
    }

    request->rename.source_name_offset = source_slash ? source_slash - source_path : -1;
    request->rename.dest_name_offset   = dest_slash ? dest_slash - dest_path : -1;

    memcpy(request->rename.source_path, source_path, source_path_len);
    memcpy(request->rename.dest_path, dest_path, dest_path_len);

    chimera_dispatch_rename(thread, request);
} /* chimera_rename */

