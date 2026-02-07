// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_remove_op_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request   *request  = private_data;
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_remove_callback_t callback = request->remove.callback;
    void                         *priv     = request->remove.private_data;

    chimera_vfs_release(thread, request->remove.parent_handle);
    chimera_vfs_request_free(thread, request);

    callback(error_code, priv);
} /* chimera_vfs_remove_op_complete */

static void
chimera_vfs_remove_child_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code == CHIMERA_VFS_OK) {
        memcpy(request->remove.child_fh, attr->va_fh, attr->va_fh_len);
        request->remove.child_fh_len = attr->va_fh_len;
    } else if (error_code == CHIMERA_VFS_ENOENT) {
        /* Child doesn't exist - proceed with no child FH, remove_at will
         * return the appropriate error */
        request->remove.child_fh_len = 0;
    } else {
        chimera_vfs_remove_callback_t callback = request->remove.callback;
        void                         *priv     = request->remove.private_data;

        chimera_vfs_release(thread, request->remove.parent_handle);
        chimera_vfs_request_free(thread, request);
        callback(error_code, priv);
        return;
    }

    chimera_vfs_remove_at(
        thread,
        request->cred,
        request->remove.parent_handle,
        request->remove.path + request->remove.name_offset,
        request->remove.pathlen - request->remove.name_offset,
        request->remove.child_fh_len ? request->remove.child_fh : NULL,
        request->remove.child_fh_len,
        0,
        0,
        chimera_vfs_remove_op_complete,
        request);
} /* chimera_vfs_remove_child_lookup_complete */

static void
chimera_vfs_remove_parent_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_remove_callback_t callback = request->remove.callback;
        void                         *priv     = request->remove.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, priv);
        return;
    }

    request->remove.parent_handle = oh;

    chimera_vfs_lookup_at(
        thread,
        request->cred,
        oh,
        request->remove.path + request->remove.name_offset,
        request->remove.pathlen - request->remove.name_offset,
        CHIMERA_VFS_ATTR_FH,
        0,
        chimera_vfs_remove_child_lookup_complete,
        request);
} /* chimera_vfs_remove_parent_open_complete */

static void
chimera_vfs_remove_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_remove_callback_t callback = request->remove.callback;
        void                         *priv     = request->remove.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, priv);
        return;
    }

    memcpy(request->remove.parent_fh, attr->va_fh, attr->va_fh_len);
    request->remove.parent_fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(
        thread,
        request->cred,
        request->remove.parent_fh,
        request->remove.parent_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_vfs_remove_parent_open_complete,
        request);
} /* chimera_vfs_remove_parent_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_remove(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    chimera_vfs_remove_callback_t  callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;
    const char                 *slash;

    while (pathlen > 0 && *path == '/') {
        path++;
        pathlen--;
    }

    while (pathlen > 0 && path[pathlen - 1] == '/') {
        pathlen--;
    }

    if (pathlen == 0) {
        callback(CHIMERA_VFS_EINVAL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    memcpy(request->plugin_data, path, pathlen);
    ((char *) request->plugin_data)[pathlen] = '\0';

    request->remove.path         = request->plugin_data;
    request->remove.pathlen      = pathlen;
    request->remove.callback     = callback;
    request->remove.private_data = private_data;

    if (request->module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
        request->remove.name_offset = 0;

        memcpy(request->remove.parent_fh, fh, fhlen);
        request->remove.parent_fh_len = fhlen;

        chimera_vfs_open_fh(
            thread,
            cred,
            request->remove.parent_fh,
            request->remove.parent_fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_vfs_remove_parent_open_complete,
            request);
    } else {
        slash = strrchr(request->remove.path, '/');

        if (slash) {
            request->remove.parent_len  = slash - request->remove.path;
            request->remove.name_offset = (slash + 1) - request->remove.path;
        } else {
            request->remove.parent_len  = 0;
            request->remove.name_offset = 0;
        }

        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->remove.path,
            request->remove.parent_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_vfs_remove_parent_lookup_complete,
            request);
    }
} /* chimera_vfs_remove */
