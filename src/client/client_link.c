// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_link_at_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    request->link.callback(request->thread, error_code, request->link.private_data);

    chimera_vfs_release(request->thread->vfs_thread, request->link.dest_parent_handle);

    chimera_client_request_free(request->thread, request);

} /* chimera_link_at_complete */

static void
chimera_link_dest_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->link.callback(request->thread, error_code, request->link.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    request->link.dest_parent_handle = oh;
    memcpy(request->link.dest_fh, oh->fh, oh->fh_len);
    request->link.dest_fh_len = oh->fh_len;

    chimera_vfs_link(
        request->thread->vfs_thread,
        request->link.source_fh,
        request->link.source_fh_len,
        request->link.dest_fh,
        request->link.dest_fh_len,
        request->link.dest_path + request->link.dest_name_offset,
        request->link.dest_path_len - request->link.dest_name_offset,
        0,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_link_at_complete,
        request);

} /* chimera_link_dest_parent_complete */

static void
chimera_link_dest_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->link.callback(request->thread, error_code, request->link.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_link_dest_parent_complete,
        request);

} /* chimera_link_dest_parent_lookup_complete */

static void
chimera_link_source_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->link.callback(request->thread, error_code, request->link.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    memcpy(request->link.source_fh, attr->va_fh, attr->va_fh_len);
    request->link.source_fh_len = attr->va_fh_len;

    chimera_vfs_lookup_path(
        request->thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->link.dest_path,
        request->link.dest_parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_dest_parent_lookup_complete,
        request);

} /* chimera_link_source_complete */

static void
chimera_link_source_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        request->link.callback(request->thread, error_code, request->link.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_lookup_path(
        request->thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->link.source_path,
        request->link.source_path_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_source_complete,
        request);

} /* chimera_link_source_lookup_complete */

static inline void
chimera_dispatch_link(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->link.dest_name_offset == -1)) {
        request->link.callback(request->thread, CHIMERA_VFS_EINVAL, request->link.private_data);
        chimera_client_request_free(request->thread, request);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->link.source_path,
        request->link.source_path_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_source_lookup_complete,
        request);
} /* chimera_dispatch_link */

SYMBOL_EXPORT void
chimera_link(
    struct chimera_client_thread *thread,
    const char                   *source_path,
    int                           source_path_len,
    const char                   *dest_path,
    int                           dest_path_len,
    chimera_link_callback_t       callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *source_slash;
    const char                    *dest_slash;

    source_slash = rindex(source_path, '/');
    dest_slash   = rindex(dest_path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode                 = CHIMERA_CLIENT_OP_LINK;
    request->link.callback          = callback;
    request->link.private_data      = private_data;
    request->link.source_path_len   = source_path_len;
    request->link.source_parent_len = source_slash ? source_slash - source_path : source_path_len;
    request->link.dest_path_len     = dest_path_len;
    request->link.dest_parent_len   = dest_slash ? dest_slash - dest_path : dest_path_len;

    while (dest_slash && *dest_slash == '/') {
        dest_slash++;
    }

    request->link.dest_name_offset = dest_slash ? dest_slash - dest_path : -1;

    memcpy(request->link.source_path, source_path, source_path_len);
    memcpy(request->link.dest_path, dest_path, dest_path_len);

    chimera_dispatch_link(thread, request);
} /* chimera_link */

