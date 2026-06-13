// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "vfs_mount_table.h"
#include "common/misc.h"
#include "common/macros.h"

/*
 * See vfs_proc_open.c for the rationale.  When `path` (slash-stripped, relative
 * to the global vfs root) resolves into a path-only mount, copy out the mount's
 * re-openable root fh and return the offset of the in-mount remainder; the
 * caller opens the mount root and hands the whole sub-path to remove_at (the
 * path-only branch in chimera_vfs_remove_parent_open_complete then removes by
 * path).  Returns -1 when the target is not under a path-only mount.
 */
static int
chimera_vfs_pathonly_rebase(
    struct chimera_vfs_thread *thread,
    const char                *path,
    int                        pathlen,
    uint8_t                   *r_root_fh,
    int                       *r_root_fh_len)
{
    struct chimera_vfs_mount_table       *table = thread->vfs->mount_table;
    struct chimera_vfs_mount_table_entry *entry;
    uint32_t                              i;
    int                                   offset = -1;

    urcu_qsbr_read_lock();

    for (i = 0; i < table->num_buckets && offset < 0; i++) {
        entry = rcu_dereference(table->buckets[i]);
        while (entry) {
            struct chimera_vfs_mount *mount = entry->mount;

            if (mount->pathlen <= (uint32_t) pathlen &&
                memcmp(mount->path, path, mount->pathlen) == 0 &&
                (mount->pathlen == (uint32_t) pathlen ||
                 path[mount->pathlen] == '/') &&
                chimera_vfs_module_is_path_only(mount->module)) {

                memcpy(r_root_fh, mount->root_fh, mount->root_fh_len);
                *r_root_fh_len = mount->root_fh_len;

                offset = mount->pathlen;
                while (offset < pathlen && path[offset] == '/') {
                    offset++;
                }
                break;
            }
            entry = rcu_dereference(entry->next);
        }
    }

    urcu_qsbr_read_unlock();

    return offset;
} /* chimera_vfs_pathonly_rebase */

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

    /* Path-only backends have no child fh; remove by path directly (the backend
     * deletes by path) -- skip the child lookup that yields nothing usable. */
    if (chimera_vfs_module_is_path_only(request->module)) {
        request->remove.child_fh_len = 0;
        chimera_vfs_remove_at(
            thread,
            request->cred,
            oh,
            request->remove.path + request->remove.name_offset,
            request->remove.pathlen - request->remove.name_offset,
            NULL,
            0,
            0,
            0,
            chimera_vfs_remove_op_complete,
            request);
        return;
    }

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

    if (pathlen > CHIMERA_VFS_PATH_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, private_data);
        return;
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
        return;
    }

    /* Deep path crossing into a path-only mount: rebase onto the mount root and
     * dispatch remove_at with the whole in-mount sub-path as the name. */
    {
        int rebase = chimera_vfs_pathonly_rebase(thread, request->remove.path,
                                                 request->remove.pathlen,
                                                 request->remove.parent_fh,
                                                 &request->remove.parent_fh_len);

        if (rebase >= 0 && rebase < request->remove.pathlen) {
            request->remove.name_offset = rebase;

            chimera_vfs_open_fh(
                thread,
                cred,
                request->remove.parent_fh,
                request->remove.parent_fh_len,
                CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                chimera_vfs_remove_parent_open_complete,
                request);
            return;
        }
    }

    {
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
