// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "sdk/vfs_access.h"
#include "sdk/vfs_acl.h"
#include "vfs_mount_table.h"
#include "common/misc.h"
#include "common/macros.h"

/* Canonical attr+ACL mask needed to authorize an open against a resolved file. */
#define CHIMERA_VFS_OPEN_GATE_MASK (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL)

/* Map an open's access mode + O_TRUNC to the ACE rights the caller must hold on
 * the target file (moved to vfs_internal.h as
 * chimera_vfs_open_required_access, shared with the open_at wrapper). */

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

    /* Bind the open-time access grant computed by the lookup gate (POSIX
     * rights retention; only set on the gated non-create path). */
    if (error_code == CHIMERA_VFS_OK && oh && request->open.granted_valid) {
        chimera_vfs_handle_stamp_access(oh, request->open.granted_access);
    }

    chimera_vfs_request_free(thread, request);

    callback(error_code, oh, NULL, priv);
} /* chimera_vfs_open_root_complete */

/* ----------------------------------------------------------------------------
 * Final-symlink follow on the create (parent + open_at) leg.
 *
 * open_at opens the named entry itself; when that entry is a symlink and the
 * open wants to follow (no O_NOFOLLOW, no O_PATH), POSIX resolution must
 * continue through the link: an existing target is opened, a dangling target
 * is created (O_CREAT), and a loop is ELOOP.  Implemented by reading the
 * link and restarting chimera_vfs_open() on the rewritten path; the hop
 * count rides the high byte of the (internal) open flags so a chain of
 * links terminates at SYMLOOP_MAX with ELOOP.
 * ------------------------------------------------------------------------- */

#define CHIMERA_VFS_OPEN_HOPS_SHIFT 24
#define CHIMERA_VFS_OPEN_HOPS_MASK  (0xffu << CHIMERA_VFS_OPEN_HOPS_SHIFT)
#define CHIMERA_VFS_OPEN_HOPS(f) (((f)&CHIMERA_VFS_OPEN_HOPS_MASK) >> \
                                  CHIMERA_VFS_OPEN_HOPS_SHIFT)
#define CHIMERA_VFS_OPEN_SYMLOOP    8

struct chimera_vfs_open_follow_ctx {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *oh;         /* handle on the symlink */
    chimera_vfs_open_callback_t     callback;
    void                           *private_data;
    unsigned int                    flags;      /* original + bumped hops */
    uint64_t                        attr_mask;
    struct chimera_vfs_attrs        set_attr;   /* copy: the original may
                                                 * live in the request */
    int                             root_fh_len;
    int                             parent_len;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    char                            parent[CHIMERA_VFS_PATH_MAX];
    char                            target[CHIMERA_VFS_PATH_MAX];
};

static void
chimera_vfs_open_follow_done(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_vfs_open_follow_ctx *ctx = private_data;

    ctx->callback(error_code, oh, attr, ctx->private_data);
    free(ctx);
} /* chimera_vfs_open_follow_done */

static void
chimera_vfs_open_follow_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_open_follow_ctx *ctx = private_data;
    char                                newpath[CHIMERA_VFS_PATH_MAX];
    int                                 newlen;

    chimera_vfs_release(ctx->thread, ctx->oh);

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, NULL, NULL, ctx->private_data);
        free(ctx);
        return;
    }

    ctx->target[targetlen] = '\0';

    if (ctx->target[0] == '/' || ctx->parent_len == 0) {
        newlen = snprintf(newpath, sizeof(newpath), "%s", ctx->target);
    } else {
        newlen = snprintf(newpath, sizeof(newpath), "%.*s/%s",
                          ctx->parent_len, ctx->parent, ctx->target);
    }

    if (newlen >= (int) sizeof(newpath)) {
        ctx->callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL,
                      ctx->private_data);
        free(ctx);
        return;
    }

    chimera_vfs_open(ctx->thread,
                     ctx->cred,
                     ctx->root_fh,
                     ctx->root_fh_len,
                     newpath,
                     newlen,
                     ctx->flags,
                     &ctx->set_attr,
                     ctx->attr_mask,
                     chimera_vfs_open_follow_done,
                     ctx);
} /* chimera_vfs_open_follow_readlink_complete */

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

    /* The named entry is a symlink and the open follows: resolve through
     * it (see the block comment above).  SMB (AUTH_ATTR) handles reparse
     * points in its own model and is exempt. */
    if (error_code == CHIMERA_VFS_OK && oh && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        S_ISLNK(attr->va_mode) &&
        !(request->open.flags & (CHIMERA_VFS_OPEN_NOFOLLOW |
                                 CHIMERA_VFS_OPEN_PATH)) &&
        request->cred->flavor != CHIMERA_VFS_AUTH_ATTR) {

        unsigned int hops = CHIMERA_VFS_OPEN_HOPS(request->open.flags);

        if (hops >= CHIMERA_VFS_OPEN_SYMLOOP) {
            chimera_vfs_release(thread, oh);
            chimera_vfs_release(thread, request->open.parent_handle);
            chimera_vfs_request_free(thread, request);
            callback(CHIMERA_VFS_ELOOP, NULL, NULL, priv);
            return;
        }

        struct chimera_vfs_open_follow_ctx *ctx = malloc(sizeof(*ctx));

        ctx->thread       = thread;
        ctx->cred         = request->cred;
        ctx->oh           = oh;
        ctx->callback     = callback;
        ctx->private_data = priv;
        ctx->flags        =
            (request->open.flags & ~(unsigned int)
             CHIMERA_VFS_OPEN_HOPS_MASK) |
            ((hops + 1) << CHIMERA_VFS_OPEN_HOPS_SHIFT);
        ctx->attr_mask = request->open.attr_mask;
        /* Copy: the original may live inside the request being freed. */
        ctx->set_attr = *request->open.set_attr;

        memcpy(ctx->root_fh, request->fh, request->fh_len);
        ctx->root_fh_len = request->fh_len;

        ctx->parent_len = request->open.parent_len;
        memcpy(ctx->parent, request->open.path, request->open.parent_len);

        chimera_vfs_release(thread, request->open.parent_handle);
        chimera_vfs_request_free(thread, request);

        chimera_vfs_readlink(thread, ctx->cred, ctx->oh,
                             ctx->target, sizeof(ctx->target) - 1,
                             0,
                             chimera_vfs_open_follow_readlink_complete,
                             ctx);
        return;
    }

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

        /* FIFOs/sockets/devices have no blocking-open or device semantics
         * behind a NAS client; opening one with data access is ENXIO.
         * Accessless (O_PATH-style / INFERRED) opens still work so path
         * metadata operations on such nodes are unaffected. */
        if (!S_ISREG(attr->va_mode) && !S_ISDIR(attr->va_mode) &&
            !S_ISLNK(attr->va_mode) &&
            (f & (CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY))) {
            chimera_vfs_request_free(thread, request);
            callback(CHIMERA_VFS_ENXIO, NULL, NULL, priv);
            return;
        }

        /* Authorize the requested read/write access against the file. */
        if (chimera_vfs_open_gate_needed(request->module->capabilities,
                                         request->cred)) {
            if (chimera_vfs_gate(attr, request->cred,
                                 chimera_vfs_open_required_access(f)) != CHIMERA_VFS_OK) {
                chimera_vfs_request_free(thread, request);
                callback(CHIMERA_VFS_EACCES, NULL, NULL, priv);
                return;
            }

            /* POSIX binds I/O rights at open: capture the effective grant
             * now and stamp it on the handle once it exists (see
             * chimera_vfs_open_root_complete), so later I/O through the
             * descriptor is immune to subsequent mode changes. */
            request->open.granted_access =
                chimera_vfs_access_check(attr, request->cred,
                                         CHIMERA_ACE_MASK_ALL);
            request->open.granted_valid = 1;
        } else {
            /* Gate-exempt credential (root, AUTH_NONE): DAC grants this open
             * everything; bind the full grant to the descriptor so a later
             * setuid() does not revoke I/O on it. */
            request->open.granted_access = CHIMERA_ACE_MASK_ALL;
            request->open.granted_valid  = 1;
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

        request->open.callback      = callback;
        request->open.private_data  = private_data;
        request->open.granted_valid = 0;

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

    request->open.path          = request->plugin_data;
    request->open.pathlen       = pathlen;
    request->open.flags         = flags;
    request->open.attr_mask     = attr_mask;
    request->open.callback      = callback;
    request->open.private_data  = private_data;
    request->open.granted_valid = 0;

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

/*
 * Non-inline export of chimera_vfs_release() (defined inline in the internal
 * vfs_release.h).  Lets out-of-tree consumers that link libchimera_vfs -- e.g.
 * downstream in-process VFS module tests -- release open handles without
 * pulling in the internal open-cache headers.  Declared in vfs_procs.h.
 */
SYMBOL_EXPORT void
chimera_vfs_release_handle(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_release(thread, handle);
} /* chimera_vfs_release_handle */
