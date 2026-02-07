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
chimera_vfs_link_op_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request  = private_data;
    struct chimera_vfs_thread  *thread   = request->thread;
    chimera_vfs_link_callback_t callback = request->link.callback;
    void                       *priv     = request->link.private_data;

    chimera_vfs_request_free(thread, request);

    callback(error_code, r_attr, priv);
} /* chimera_vfs_link_op_complete */

static void
chimera_vfs_link_dest_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_link_callback_t callback = request->link.callback;
        void                       *priv     = request->link.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, priv);
        return;
    }

    memcpy(request->link.dest_parent_fh, attr->va_fh, attr->va_fh_len);
    request->link.dest_parent_fh_len = attr->va_fh_len;

    chimera_vfs_link_at(
        thread,
        request->cred,
        request->link.source_fh,
        request->link.source_fh_len,
        request->link.dest_parent_fh,
        request->link.dest_parent_fh_len,
        request->link.new_path + request->link.new_name_offset,
        request->link.new_pathlen - request->link.new_name_offset,
        request->link.replace,
        request->link.attr_mask,
        0,
        0,
        chimera_vfs_link_op_complete,
        request);
} /* chimera_vfs_link_dest_parent_lookup_complete */

static void
chimera_vfs_link_source_lookup_fast_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_link_callback_t callback = request->link.callback;
        void                       *priv     = request->link.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, priv);
        return;
    }

    memcpy(request->link.source_fh, attr->va_fh, attr->va_fh_len);
    request->link.source_fh_len = attr->va_fh_len;

    /* Dest parent FH and full path already set up; skip dest lookup */
    chimera_vfs_link_at(
        thread,
        request->cred,
        request->link.source_fh,
        request->link.source_fh_len,
        request->link.dest_parent_fh,
        request->link.dest_parent_fh_len,
        request->link.new_path + request->link.new_name_offset,
        request->link.new_pathlen - request->link.new_name_offset,
        request->link.replace,
        request->link.attr_mask,
        0,
        0,
        chimera_vfs_link_op_complete,
        request);
} /* chimera_vfs_link_source_lookup_fast_complete */

static void
chimera_vfs_link_source_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_link_callback_t callback = request->link.callback;
        void                       *priv     = request->link.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, priv);
        return;
    }

    memcpy(request->link.source_fh, attr->va_fh, attr->va_fh_len);
    request->link.source_fh_len = attr->va_fh_len;

    chimera_vfs_lookup(
        thread,
        request->cred,
        request->fh,
        request->fh_len,
        request->link.new_path,
        request->link.new_parent_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_vfs_link_dest_parent_lookup_complete,
        request);
} /* chimera_vfs_link_source_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_link(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *old_path,
    int                            old_pathlen,
    const char                    *new_path,
    int                            new_pathlen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    chimera_vfs_link_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;
    const char                 *slash;
    char                       *buf;

    while (old_pathlen > 0 && *old_path == '/') {
        old_path++;
        old_pathlen--;
    }

    while (new_pathlen > 0 && *new_path == '/') {
        new_path++;
        new_pathlen--;
    }

    while (new_pathlen > 0 && new_path[new_pathlen - 1] == '/') {
        new_pathlen--;
    }

    if (old_pathlen == 0 || new_pathlen == 0) {
        callback(CHIMERA_VFS_EINVAL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, private_data);
        return;
    }

    /* Pack both paths into plugin_data: old_path \0 new_path \0 */
    buf = request->plugin_data;
    memcpy(buf, old_path, old_pathlen);
    buf[old_pathlen] = '\0';
    memcpy(buf + old_pathlen + 1, new_path, new_pathlen);
    buf[old_pathlen + 1 + new_pathlen] = '\0';

    request->link.path         = buf;
    request->link.pathlen      = old_pathlen;
    request->link.new_path     = buf + old_pathlen + 1;
    request->link.new_pathlen  = new_pathlen;
    request->link.replace      = replace;
    request->link.attr_mask    = attr_mask;
    request->link.callback     = callback;
    request->link.private_data = private_data;

    if (request->module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
        /* Fast path: pass full dest path directly, kernel resolves */
        request->link.new_name_offset = 0;

        memcpy(request->link.dest_parent_fh, fh, fhlen);
        request->link.dest_parent_fh_len = fhlen;

        /* Still need to resolve source path to get source FH */
        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->link.path,
            request->link.pathlen,
            CHIMERA_VFS_ATTR_FH,
            0,
            chimera_vfs_link_source_lookup_fast_complete,
            request);
    } else {
        /* Fallback: resolve both paths component-by-component */

        /* Split new path for dest parent/name */
        slash = strrchr(request->link.new_path, '/');

        if (slash) {
            request->link.new_parent_len  = slash - request->link.new_path;
            request->link.new_name_offset = (slash + 1) - request->link.new_path;
        } else {
            request->link.new_parent_len  = 0;
            request->link.new_name_offset = 0;
        }

        /* Resolve source (full path) to get source FH */
        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->link.path,
            request->link.pathlen,
            CHIMERA_VFS_ATTR_FH,
            0,
            chimera_vfs_link_source_lookup_complete,
            request);
    }
} /* chimera_vfs_link */
