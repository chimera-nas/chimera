// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>

#include "client_internal.h"
#include "client_dispatch.h"
#include "client_compound.h"

/* Shared reply for both the path and _at remove compounds. */
static void
chimera_remove_reply(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_remove_callback_t callback     = request->remove.callback;
    void                     *callback_arg = request->remove.private_data;
    enum chimera_vfs_error    status       = request->compound_op_status;

    /* Note: parent handle (for the _at variant) is NOT released - caller owns it */
    chimera_client_request_free(thread, request);

    callback(thread, status, callback_arg);
} /* chimera_remove_reply */

static void
chimera_remove_vfs_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_compound_finish(request->thread, request, error_code);
} /* chimera_remove_vfs_complete */

static void
chimera_remove_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_remove(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->compound,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->remove.path,
        request->remove.path_len,
        request->remove.flags,
        chimera_remove_vfs_complete,
        request);
} /* chimera_remove_start */

static inline void
chimera_dispatch_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->remove.name_offset == -1)) {
        chimera_dispatch_error_remove(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_client_compound_run(thread, request,
                                thread->client->root_fh,
                                thread->client->root_fh_len,
                                CHIMERA_VFS_COMPOUND_WRITE,
                                chimera_remove_start, chimera_remove_reply);
} /* chimera_dispatch_remove */

static void
chimera_remove_dispatch_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    chimera_client_compound_finish(request->thread, request, error_code);
} /* chimera_remove_dispatch_at_complete */

static void
chimera_remove_at_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;
    struct chimera_client_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        /* Child doesn't exist or other error - fail the compound. */
        chimera_client_compound_finish(thread, request, error_code);
        return;
    }

    /* Enforce the caller's type assertion (rmdir vs unlink) before the
     * backend remove, exactly as the path-based chimera_vfs_remove does at
     * its own child resolve: NFSv4 REMOVE is type-agnostic on the wire and
     * NFSv3 would silly-rename an open directory, so by the time the backend
     * answers, the wrong-type removal has already happened.  The resolve
     * above fetched the mode, so this adds no round trip. */
    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        int is_dir = S_ISDIR(attr->va_mode);

        if (((request->remove.flags & CHIMERA_VFS_REMOVE_ISDIR) && !is_dir) ||
            ((request->remove.flags & CHIMERA_VFS_REMOVE_ISNOTDIR) && is_dir)) {
            /* Fail the operation through the compound driver like any other op
             * error: finish aborts the begun backend compound (dropping
             * its locks) and chimera_remove_reply then delivers this same
             * status to the user callback and frees the request. */
            chimera_client_compound_finish(thread, request,
                                           is_dir ? CHIMERA_VFS_EISDIR :
                                           CHIMERA_VFS_ENOTDIR);
            return;
        }
    }

    /* Save the child FH for the remove call */
    request->remove.child_fh_len = attr->va_fh_len;
    memcpy(request->remove.child_fh, attr->va_fh, attr->va_fh_len);

    /* Now call remove with the child FH */
    chimera_vfs_remove_at(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->compound,
        request->remove.parent_handle,
        request->remove.path,
        request->remove.path_len,
        request->remove.child_fh,
        request->remove.child_fh_len,
        request->remove.flags,
        0,
        0,
        NULL,
        chimera_remove_dispatch_at_complete,
        request);
} /* chimera_remove_at_lookup_complete */

static void
chimera_remove_at_start(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* First lookup the child to get its FH for silly rename optimization.
     * Use NOFOLLOW (0) because we want the FH of the symlink itself,
     * not the target it points to. */
    chimera_vfs_lookup_at(
        thread->vfs_thread,
        chimera_client_req_cred(request), request->compound,
        request->remove.parent_handle,
        request->remove.path,
        request->remove.path_len,
        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
        0,
        chimera_remove_at_lookup_complete,
        request);
} /* chimera_remove_at_start */

static inline void
chimera_dispatch_remove_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    /* Save parent handle for use in the start/lookup callbacks */
    request->remove.parent_handle = parent_handle;

    chimera_client_compound_run(thread, request,
                                parent_handle->fh, parent_handle->fh_len,
                                CHIMERA_VFS_COMPOUND_WRITE,
                                chimera_remove_at_start, chimera_remove_reply);
} /* chimera_dispatch_remove_at */
