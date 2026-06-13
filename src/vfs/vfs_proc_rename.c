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
 * re-openable root fh and return the offset of the in-mount remainder.  Returns
 * -1 when the target is not under a path-only mount.
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
chimera_vfs_rename_op_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_request   *request  = private_data;
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_rename_callback_t callback = request->rename.callback;
    void                         *priv     = request->rename.private_data;

    chimera_vfs_request_free(thread, request);

    callback(error_code, priv);
} /* chimera_vfs_rename_op_complete */

static void
chimera_vfs_rename_target_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code == CHIMERA_VFS_OK) {
        memcpy(request->rename.target_fh, attr->va_fh, attr->va_fh_len);
        request->rename.target_fh_len = attr->va_fh_len;
    } else if (error_code == CHIMERA_VFS_ENOENT) {
        request->rename.target_fh_len = 0;
    } else {
        chimera_vfs_rename_callback_t callback = request->rename.callback;
        void                         *priv     = request->rename.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, priv);
        return;
    }

    chimera_vfs_rename_at(
        thread,
        request->cred,
        request->rename.old_parent_fh,
        request->rename.old_parent_fh_len,
        request->rename.path + request->rename.name_offset,
        request->rename.pathlen - request->rename.name_offset,
        request->rename.new_parent_fh,
        request->rename.new_parent_fh_len,
        request->rename.new_path + request->rename.new_name_offset,
        request->rename.new_pathlen - request->rename.new_name_offset,
        request->rename.target_fh_len ? request->rename.target_fh : NULL,
        request->rename.target_fh_len,
        0,
        0,
        chimera_vfs_rename_op_complete,
        request);
} /* chimera_vfs_rename_target_lookup_complete */

static void
chimera_vfs_rename_new_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_rename_callback_t callback = request->rename.callback;
        void                         *priv     = request->rename.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, priv);
        return;
    }

    memcpy(request->rename.new_parent_fh, attr->va_fh, attr->va_fh_len);
    request->rename.new_parent_fh_len = attr->va_fh_len;

    /* Lookup the target to get its FH for silly rename optimization */
    chimera_vfs_lookup(
        thread,
        request->cred,
        request->rename.new_parent_fh,
        request->rename.new_parent_fh_len,
        request->rename.new_path + request->rename.new_name_offset,
        request->rename.new_pathlen - request->rename.new_name_offset,
        CHIMERA_VFS_ATTR_FH,
        0,
        chimera_vfs_rename_target_lookup_complete,
        request);
} /* chimera_vfs_rename_new_parent_lookup_complete */

static void
chimera_vfs_rename_fast_target_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code == CHIMERA_VFS_OK) {
        memcpy(request->rename.target_fh, attr->va_fh, attr->va_fh_len);
        request->rename.target_fh_len = attr->va_fh_len;
    } else if (error_code == CHIMERA_VFS_ENOENT) {
        request->rename.target_fh_len = 0;
    } else {
        chimera_vfs_rename_callback_t callback = request->rename.callback;
        void                         *priv     = request->rename.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, priv);
        return;
    }

    chimera_vfs_rename_at(
        thread,
        request->cred,
        request->rename.old_parent_fh,
        request->rename.old_parent_fh_len,
        request->rename.path,
        request->rename.pathlen,
        request->rename.new_parent_fh,
        request->rename.new_parent_fh_len,
        request->rename.new_path,
        request->rename.new_pathlen,
        request->rename.target_fh_len ? request->rename.target_fh : NULL,
        request->rename.target_fh_len,
        0,
        0,
        chimera_vfs_rename_op_complete,
        request);
} /* chimera_vfs_rename_fast_target_lookup_complete */

static void
chimera_vfs_rename_old_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_rename_callback_t callback = request->rename.callback;
        void                         *priv     = request->rename.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, priv);
        return;
    }

    memcpy(request->rename.old_parent_fh, attr->va_fh, attr->va_fh_len);
    request->rename.old_parent_fh_len = attr->va_fh_len;

    chimera_vfs_lookup(
        thread,
        request->cred,
        request->fh,
        request->fh_len,
        request->rename.new_path,
        request->rename.new_parent_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_vfs_rename_new_parent_lookup_complete,
        request);
} /* chimera_vfs_rename_old_parent_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_rename(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *old_path,
    int                            old_pathlen,
    const char                    *new_path,
    int                            new_pathlen,
    chimera_vfs_rename_callback_t  callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;
    const char                 *slash;
    char                       *buf;

    while (old_pathlen > 0 && *old_path == '/') {
        old_path++;
        old_pathlen--;
    }

    while (old_pathlen > 0 && old_path[old_pathlen - 1] == '/') {
        old_pathlen--;
    }

    while (new_pathlen > 0 && *new_path == '/') {
        new_path++;
        new_pathlen--;
    }

    while (new_pathlen > 0 && new_path[new_pathlen - 1] == '/') {
        new_pathlen--;
    }

    if (old_pathlen > CHIMERA_VFS_PATH_MAX || new_pathlen > CHIMERA_VFS_PATH_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, private_data);
        return;
    }

    if (old_pathlen + 1 + new_pathlen + 1 > CHIMERA_VFS_PLUGIN_DATA_SIZE) {
        callback(CHIMERA_VFS_ENAMETOOLONG, private_data);
        return;
    }

    if (old_pathlen == 0 || new_pathlen == 0) {
        callback(CHIMERA_VFS_EINVAL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    /* Pack both paths into plugin_data: old_path \0 new_path \0 */
    buf = request->plugin_data;
    memcpy(buf, old_path, old_pathlen);
    buf[old_pathlen] = '\0';
    memcpy(buf + old_pathlen + 1, new_path, new_pathlen);
    buf[old_pathlen + 1 + new_pathlen] = '\0';

    request->rename.path         = buf;
    request->rename.pathlen      = old_pathlen;
    request->rename.new_path     = buf + old_pathlen + 1;
    request->rename.new_pathlen  = new_pathlen;
    request->rename.callback     = callback;
    request->rename.private_data = private_data;

    if (request->module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
        /* Fast path: pass full paths directly, kernel resolves */
        request->rename.name_offset     = 0;
        request->rename.new_name_offset = 0;

        memcpy(request->rename.old_parent_fh, fh, fhlen);
        request->rename.old_parent_fh_len = fhlen;
        memcpy(request->rename.new_parent_fh, fh, fhlen);
        request->rename.new_parent_fh_len = fhlen;

        /* Path-only backends have no target fh (no silly-rename); rename by full
         * paths directly.  The backend returns EXDEV if the two parents are on
         * different mounts. */
        if (chimera_vfs_module_is_path_only(request->module)) {
            request->rename.target_fh_len = 0;
            chimera_vfs_rename_at(
                thread,
                cred,
                request->rename.old_parent_fh,
                request->rename.old_parent_fh_len,
                request->rename.path,
                request->rename.pathlen,
                request->rename.new_parent_fh,
                request->rename.new_parent_fh_len,
                request->rename.new_path,
                request->rename.new_pathlen,
                NULL,
                0,
                0,
                0,
                chimera_vfs_rename_op_complete,
                request);
            return;
        }

        /* Lookup the target to get its FH for silly rename optimization */
        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->rename.new_path,
            request->rename.new_pathlen,
            CHIMERA_VFS_ATTR_FH,
            0,
            chimera_vfs_rename_fast_target_lookup_complete,
            request);
        return;
    }

    /* Deep path crossing into a path-only mount: both paths must land in the
     * SAME path-only mount.  Rebase onto the (shared) mount root and dispatch
     * rename_at with the in-mount sub-paths; the path-only backend resolves the
     * whole sub-paths in one operation (no child fh, no silly-rename). */
    {
        uint8_t old_root_fh[CHIMERA_VFS_FH_SIZE + 16];
        uint8_t new_root_fh[CHIMERA_VFS_FH_SIZE + 16];
        int     old_root_fh_len = 0;
        int     new_root_fh_len = 0;
        int     old_rebase, new_rebase;

        old_rebase = chimera_vfs_pathonly_rebase(thread, request->rename.path,
                                                 request->rename.pathlen,
                                                 old_root_fh, &old_root_fh_len);

        if (old_rebase >= 0 && old_rebase < request->rename.pathlen) {
            new_rebase = chimera_vfs_pathonly_rebase(thread, request->rename.new_path,
                                                     request->rename.new_pathlen,
                                                     new_root_fh, &new_root_fh_len);

            if (new_rebase >= 0 && new_rebase < request->rename.new_pathlen &&
                old_root_fh_len == new_root_fh_len &&
                memcmp(old_root_fh, new_root_fh, old_root_fh_len) == 0) {

                request->rename.name_offset     = old_rebase;
                request->rename.new_name_offset = new_rebase;

                memcpy(request->rename.old_parent_fh, old_root_fh, old_root_fh_len);
                request->rename.old_parent_fh_len = old_root_fh_len;
                memcpy(request->rename.new_parent_fh, new_root_fh, new_root_fh_len);
                request->rename.new_parent_fh_len = new_root_fh_len;

                request->rename.target_fh_len = 0;

                chimera_vfs_rename_at(
                    thread,
                    cred,
                    request->rename.old_parent_fh,
                    request->rename.old_parent_fh_len,
                    request->rename.path + request->rename.name_offset,
                    request->rename.pathlen - request->rename.name_offset,
                    request->rename.new_parent_fh,
                    request->rename.new_parent_fh_len,
                    request->rename.new_path + request->rename.new_name_offset,
                    request->rename.new_pathlen - request->rename.new_name_offset,
                    NULL,
                    0,
                    0,
                    0,
                    chimera_vfs_rename_op_complete,
                    request);
                return;
            }
            /* TODO: old path is under a path-only mount but new path is not in
             * the same path-only mount (cross-mount rename).  Fall through to
             * the FH-relative fallback below, which will fail to resolve the
             * intermediate path-only directories and surface an error; a proper
             * cross-mount/EXDEV path-only rename is out of scope here. */
        }
    }

    {
        /* Fallback: resolve both parent paths component-by-component */

        /* Split old path */
        slash = strrchr(request->rename.path, '/');

        if (slash) {
            request->rename.parent_len  = slash - request->rename.path;
            request->rename.name_offset = (slash + 1) - request->rename.path;
        } else {
            request->rename.parent_len  = 0;
            request->rename.name_offset = 0;
        }

        /* Split new path */
        slash = strrchr(request->rename.new_path, '/');

        if (slash) {
            request->rename.new_parent_len  = slash - request->rename.new_path;
            request->rename.new_name_offset = (slash + 1) - request->rename.new_path;
        } else {
            request->rename.new_parent_len  = 0;
            request->rename.new_name_offset = 0;
        }

        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->rename.path,
            request->rename.parent_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_vfs_rename_old_parent_lookup_complete,
            request);
    }
} /* chimera_vfs_rename */
