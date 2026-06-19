// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "vfs_mount_table.h"
#include "common/misc.h"
#include "common/macros.h"

/* Canonical attr+ACL mask needed to authorize an open against a resolved file. */
#define CHIMERA_VFS_OPEN_GATE_MASK (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL)

/* Map an open's access mode + O_TRUNC to the ACE rights the caller must hold on
 * the target file. */
static inline uint32_t
chimera_vfs_open_required_access(unsigned int flags)
{
    uint32_t required = 0;

    /* Access intent is signalled positively: O_RDONLY -> READ_ONLY,
     * O_WRONLY -> WRITE_ONLY, O_RDWR -> both.  A raw open with neither bit
     * (e.g. an O_PATH-style handle open) requests no data access and is not
     * gated here. */
    if (flags & CHIMERA_VFS_OPEN_READ_ONLY) {
        required |= CHIMERA_ACE_READ_DATA;
    }
    if (flags & CHIMERA_VFS_OPEN_WRITE_ONLY) {
        required |= CHIMERA_ACE_WRITE_DATA;
    }
    if (flags & CHIMERA_VFS_OPEN_TRUNCATE) {
        required |= CHIMERA_ACE_WRITE_DATA;
    }
    return required;
} /* chimera_vfs_open_required_access */

/*
 * Path-only deep-path rebasing.
 *
 * Operations arrive against the GLOBAL vfs-root fh with a full path (e.g.
 * "share/a/b/file").  When that path resolves into a path-only mount, the
 * intermediate directories (share/a/b) have NO re-openable file handles, so the
 * usual "resolve immediate parent dir + dispatch _at on the leaf" scheme breaks
 * for anything deeper than a single level.
 *
 * This helper detects that case: if `path` (already slash-stripped, relative to
 * the global vfs root) falls under a path-only mount, it copies out that mount's
 * re-openable root fh and returns the byte offset of the in-mount remainder
 * within `path`.  The caller then opens the mount root as a directory handle and
 * dispatches the _at op with the entire in-mount sub-path as the name; the
 * path-only backend resolves the whole sub-path in one operation.
 *
 * Returns the in-mount offset (>= 0) on a path-only match, or -1 otherwise (in
 * which case the caller keeps its existing FH-relative behavior unchanged).
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
                /* Skip the separating slash to land on the in-mount remainder. */
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
chimera_vfs_open_root_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *request  = private_data;
    struct chimera_vfs_thread  *thread   = request->thread;
    chimera_vfs_open_callback_t callback = request->open.callback;
    void                       *priv     = request->open.private_data;

    chimera_vfs_request_free(thread, request);

    callback(error_code, oh, NULL, priv);
} /* chimera_vfs_open_root_complete */

static void
chimera_vfs_open_op_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_vfs_request *request  = private_data;
    struct chimera_vfs_thread  *thread   = request->thread;
    chimera_vfs_open_callback_t callback = request->open.callback;
    void                       *priv     = request->open.private_data;

    chimera_vfs_release(thread, request->open.parent_handle);
    chimera_vfs_request_free(thread, request);

    callback(error_code, oh, attr, priv);
} /* chimera_vfs_open_op_complete */

static void
chimera_vfs_open_parent_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_open_callback_t callback = request->open.callback;
        void                       *priv     = request->open.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, NULL, priv);
        return;
    }

    request->open.parent_handle = oh;

    chimera_vfs_open_at(
        thread,
        request->cred,
        oh,
        request->open.path + request->open.name_offset,
        request->open.pathlen - request->open.name_offset,
        request->open.flags,
        request->open.set_attr,
        request->open.attr_mask,
        0,
        0,
        chimera_vfs_open_op_complete,
        request);
} /* chimera_vfs_open_parent_open_complete */

static void
chimera_vfs_open_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_open_callback_t callback = request->open.callback;
        void                       *priv     = request->open.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, NULL, priv);
        return;
    }

    memcpy(request->open.parent_fh, attr->va_fh, attr->va_fh_len);
    request->open.parent_fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(
        thread,
        request->cred,
        request->open.parent_fh,
        request->open.parent_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_vfs_open_parent_open_complete,
        request);
} /* chimera_vfs_open_parent_lookup_complete */

static void
chimera_vfs_open_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_vfs_thread  *thread  = request->thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_open_callback_t callback = request->open.callback;
        void                       *priv     = request->open.private_data;

        chimera_vfs_request_free(thread, request);
        callback(error_code, NULL, NULL, priv);
        return;
    }

    /* POSIX open(2) semantics on the resolved final object: */
    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        chimera_vfs_open_callback_t callback = request->open.callback;
        void                       *priv     = request->open.private_data;
        unsigned int                f        = request->open.flags;

        /* O_NOFOLLOW and the final component is a symlink -> ELOOP, EXCEPT for an
         * O_PATH-style open (CHIMERA_VFS_OPEN_PATH): open(O_PATH | O_NOFOLLOW) on
         * a symlink returns a handle to the link itself, which is how lstat /
         * readlink / lchown operate on the link rather than its target. */
        if ((f & CHIMERA_VFS_OPEN_NOFOLLOW) && !(f & CHIMERA_VFS_OPEN_PATH) &&
            S_ISLNK(attr->va_mode)) {
            chimera_vfs_request_free(thread, request);
            callback(CHIMERA_VFS_ELOOP, NULL, NULL, priv);
            return;
        }

        /* Opening a directory for writing (or with O_TRUNC) -> EISDIR.  Write
         * intent is signalled positively (WRITE_ONLY for O_WRONLY/O_RDWR, or
         * TRUNCATE); a read-only or accessless handle open of a directory is
         * allowed. */
        if (S_ISDIR(attr->va_mode) &&
            ((f & CHIMERA_VFS_OPEN_WRITE_ONLY) ||
             (f & CHIMERA_VFS_OPEN_TRUNCATE))) {
            chimera_vfs_request_free(thread, request);
            callback(CHIMERA_VFS_EISDIR, NULL, NULL, priv);
            return;
        }

        /* Authorize the requested read/write access against the file. */
        if (chimera_vfs_gate_needed(request->module->capabilities, request->cred) &&
            chimera_vfs_gate(attr, request->cred,
                             chimera_vfs_open_required_access(f)) != CHIMERA_VFS_OK) {
            chimera_vfs_request_free(thread, request);
            callback(CHIMERA_VFS_EACCES, NULL, NULL, priv);
            return;
        }
    }

    memcpy(request->open.parent_fh, attr->va_fh, attr->va_fh_len);
    request->open.parent_fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(
        thread,
        request->cred,
        request->open.parent_fh,
        request->open.parent_fh_len,
        request->open.flags,
        chimera_vfs_open_root_complete,
        request);
} /* chimera_vfs_open_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_open(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    unsigned int                   flags,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_open_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;
    const char                 *slash;

    while (pathlen > 0 && *path == '/') {
        path++;
        pathlen--;
    }

    if (pathlen == 0) {
        /* Path is root "/" - open the provided FH directly */
        request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

        if (CHIMERA_VFS_IS_ERR(request)) {
            callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
            return;
        }

        request->open.callback     = callback;
        request->open.private_data = private_data;

        chimera_vfs_open_fh(
            thread,
            cred,
            fh,
            fhlen,
            flags,
            chimera_vfs_open_root_complete,
            request);
        return;
    }

    while (pathlen > 0 && path[pathlen - 1] == '/') {
        pathlen--;
    }

    if (pathlen > CHIMERA_VFS_PATH_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    memcpy(request->plugin_data, path, pathlen);
    ((char *) request->plugin_data)[pathlen] = '\0';

    request->open.path         = request->plugin_data;
    request->open.pathlen      = pathlen;
    request->open.flags        = flags;
    request->open.attr_mask    = attr_mask;
    request->open.callback     = callback;
    request->open.private_data = private_data;

    /* A non-create open may pass no set_attr, but open_at (the path-op / deep
     * path-only dispatch) requires a non-NULL one; hand it a zeroed stand-in. */
    if (set_attr) {
        request->open.set_attr = set_attr;
    } else {
        request->open.scratch_set_attr.va_set_mask = 0;
        request->open.set_attr                     = &request->open.scratch_set_attr;
    }

    if (request->module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) {
        request->open.name_offset = 0;

        memcpy(request->open.parent_fh, fh, fhlen);
        request->open.parent_fh_len = fhlen;

        chimera_vfs_open_fh(
            thread,
            cred,
            request->open.parent_fh,
            request->open.parent_fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_vfs_open_parent_open_complete,
            request);
        return;
    }

    /* Deep path crossing into a path-only mount: rebase onto the mount root and
     * dispatch open_at with the whole in-mount sub-path as the name. */
    {
        int rebase = chimera_vfs_pathonly_rebase(thread, request->open.path,
                                                 request->open.pathlen,
                                                 request->open.parent_fh,
                                                 &request->open.parent_fh_len);

        if (rebase >= 0 && rebase < request->open.pathlen) {
            request->open.name_offset = rebase;

            chimera_vfs_open_fh(
                thread,
                cred,
                request->open.parent_fh,
                request->open.parent_fh_len,
                CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                chimera_vfs_open_parent_open_complete,
                request);
            return;
        }
    }

    if (flags & CHIMERA_VFS_OPEN_CREATE) {
        /* Create needs parent handle + name for open_at */
        slash = strrchr(request->open.path, '/');

        if (slash) {
            request->open.parent_len  = slash - request->open.path;
            request->open.name_offset = (slash + 1) - request->open.path;
        } else {
            request->open.parent_len  = 0;
            request->open.name_offset = 0;
        }

        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->open.path,
            request->open.parent_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_vfs_open_parent_lookup_complete,
            request);
    } else {
        /* Non-create: resolve full path via lookup, then open the result.  The
         * mode is needed to enforce O_NOFOLLOW (ELOOP on a symlink) and EISDIR
         * (writing a directory); O_NOFOLLOW means the final component is not
         * followed. */
        uint32_t lookup_flags = (flags & CHIMERA_VFS_OPEN_NOFOLLOW) ?
            0 : CHIMERA_VFS_LOOKUP_FOLLOW;

        chimera_vfs_lookup(
            thread,
            cred,
            fh,
            fhlen,
            request->open.path,
            request->open.pathlen,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_OPEN_GATE_MASK,
            lookup_flags,
            chimera_vfs_open_lookup_complete,
            request);
    }
} /* chimera_vfs_open */
