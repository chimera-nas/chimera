// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_release.h"
#include "vfs_mount_table.h"
#include "vfs_rootfs.h"
#include <sys/stat.h>
#include "common/macros.h"

int
chimera_vfs_parse_mount_options(
    const char                       *options,
    struct chimera_vfs_mount_options *mount_options,
    char                             *buffer,
    int                               buffer_size,
    char                             *errbuf,
    size_t                            errbuf_len)
{
    const char *p, *end, *eq;
    int         opt_idx    = 0;
    int         buf_offset = 0;
    int         key_len, value_len;

    mount_options->num_options = 0;

    if (!options || !options[0]) {
        return 0;
    }

    p = options;

    while (*p) {
        /* Skip leading whitespace */
        while (*p == ' ' || *p == '\t') {
            p++;
        }

        if (!*p) {
            break;
        }

        /* Find end of this option (comma or end of string) */
        end = p;
        while (*end && *end != ',') {
            end++;
        }

        /* Check for too many options */
        if (opt_idx >= CHIMERA_VFS_MOUNT_OPT_MAX) {
            if (errbuf) {
                snprintf(errbuf, errbuf_len, "too many options (max %d)",
                         CHIMERA_VFS_MOUNT_OPT_MAX);
            }
            return -EINVAL;
        }

        /* Find '=' if present */
        eq = p;
        while (eq < end && *eq != '=') {
            eq++;
        }

        if (eq == p) {
            /* Empty key */
            if (errbuf) {
                snprintf(errbuf, errbuf_len, "empty option key");
            }
            return -EINVAL;
        }

        key_len = eq - p;

        /* Check buffer space for key + null terminator */
        if (buf_offset + key_len + 1 > buffer_size) {
            if (errbuf) {
                snprintf(errbuf, errbuf_len, "options string too long");
            }
            return -EINVAL;
        }

        /* Copy key to buffer */
        memcpy(buffer + buf_offset, p, key_len);
        buffer[buf_offset + key_len]        = '\0';
        mount_options->options[opt_idx].key = buffer + buf_offset;
        buf_offset                         += key_len + 1;

        /* Check for value */
        if (eq < end && *eq == '=') {
            eq++;             /* Skip '=' */
            value_len = end - eq;

            /* Check buffer space for value + null terminator */
            if (buf_offset + value_len + 1 > buffer_size) {
                if (errbuf) {
                    snprintf(errbuf, errbuf_len, "options string too long");
                }
                return -EINVAL;
            }

            memcpy(buffer + buf_offset, eq, value_len);
            buffer[buf_offset + value_len]        = '\0';
            mount_options->options[opt_idx].value = buffer + buf_offset;
            buf_offset                           += value_len + 1;
        } else {
            mount_options->options[opt_idx].value = NULL;
        }

        opt_idx++;

        /* Move past comma if present */
        p = end;
        if (*p == ',') {
            p++;
        }
    }

    mount_options->num_options = opt_idx;
    return 0;
} /* chimera_vfs_parse_mount_options */

SYMBOL_EXPORT int
chimera_vfs_mount_options_valid(
    const char *options,
    char       *errbuf,
    size_t      errbuf_len)
{
    struct chimera_vfs_mount_options tmp_opts;
    char                             tmp_buf[CHIMERA_VFS_MOUNT_OPT_BUFFER_MAX];
    int                              rc;

    rc = chimera_vfs_parse_mount_options(options, &tmp_opts, tmp_buf,
                                         sizeof(tmp_buf), errbuf, errbuf_len);
    return rc == 0;
} /* chimera_vfs_mount_options_valid */


/*
 * A mount in progress.  Placing a mount in the namespace first resolves its
 * mount point -- walking the path from the namespace root, crossing any mounts
 * on the way, and creating missing directories only where the path runs
 * through the built-in rootfs -- and only then asks the backend to mount.
 */
struct chimera_vfs_mount_ctx {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_module      *module;
    chimera_vfs_mount_callback_t    callback;
    void                           *private_data;
    int                             detached;
    char                           *path;         /* normalized namespace path */
    int                             pathlen;
    char                           *module_path;
    char                           *options;

    /* Walk state */
    const char                     *cursor;
    uint8_t                         cur_fh[CHIMERA_VFS_FH_SIZE];
    int                             cur_fh_len;
    uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
    int                             parent_fh_len;
    const char                     *name;
    int                             namelen;
    int                             final;
    struct chimera_vfs_open_handle *handle;
};

static void
chimera_vfs_mount_ctx_free(struct chimera_vfs_mount_ctx *ctx)
{
    free(ctx->path);
    free(ctx->module_path);
    free(ctx->options);
    free(ctx);
} /* chimera_vfs_mount_ctx_free */

static void
chimera_vfs_mount_fail(
    struct chimera_vfs_mount_ctx *ctx,
    enum chimera_vfs_error        status)
{
    /* Take back any rootfs directories the walk made for this mount point.
     * A walk that stopped at an existing mount point (EBUSY) left cur_fh on a
     * directory holding it, which is not empty and stays. */
    if (ctx->cur_fh_len > 0) {
        chimera_vfs_rootfs_prune(ctx->thread->vfs, ctx->cur_fh, ctx->cur_fh_len);
    }

    ctx->callback(ctx->thread, status, ctx->private_data);
    chimera_vfs_mount_ctx_free(ctx);
} /* chimera_vfs_mount_fail */

static void
chimera_vfs_mount_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread = request->thread;
    struct chimera_vfs           *vfs    = thread->vfs;
    struct chimera_vfs_mount_ctx *ctx    = request->proto_private_data;
    struct chimera_vfs_mount     *mount;

    chimera_vfs_complete(request);

    if (request->status != CHIMERA_VFS_OK) {
        chimera_vfs_mount_fail(ctx, request->status);
        chimera_vfs_request_free(thread, request);
        return;
    }

    mount = calloc(1, sizeof(*mount));

    mount->module        = request->mount.module;
    mount->path          = strdup(ctx->path);
    mount->module_path   = strdup(request->mount.path ? request->mount.path : "");
    mount->options       = request->mount.raw_options ? strdup(request->mount.raw_options) : NULL;
    mount->pathlen       = ctx->pathlen;
    mount->mount_private = request->mount.r_mount_private;
    mount->detached      = ctx->detached;

    /* Generic, module-independent mount options handled by the VFS core.  "ro"
     * (alias "readonly") marks the mount read-only; chimera_vfs_dispatch then
     * rejects any mutating op targeting it with CHIMERA_VFS_EROFS. */
    for (int i = 0; i < request->mount.options.num_options; i++) {
        const char *key = request->mount.options.options[i].key;

        if (key && (strcmp(key, "ro") == 0 || strcmp(key, "readonly") == 0)) {
            mount->attrs.flags |= CHIMERA_VFS_MOUNT_ATTR_READONLY;
        }
    }

    /* The mounted filesystem's, as the module reported it. */
    mount->attrs.case_policy = request->mount.r_case_policy;

    /* Store the root FH (first 16 bytes is the mount_id) */
    memcpy(mount->root_fh, request->mount.r_attr.va_fh, request->mount.r_attr.va_fh_len);
    mount->root_fh_len = request->mount.r_attr.va_fh_len;

    /* Keep the root's attributes for the directory entry naming the mount
     * point; the pointer-valued ones only live for this callback. */
    mount->root_attrs              = request->mount.r_attr;
    mount->root_attrs.va_set_mask &= ~(CHIMERA_VFS_ATTR_ACL |
                                       CHIMERA_VFS_ATTR_OWNER_SID |
                                       CHIMERA_VFS_ATTR_GROUP_SID);
    mount->root_attrs.va_acl       = NULL;
    mount->root_attrs.va_owner_sid = NULL;
    mount->root_attrs.va_group_sid = NULL;

    if (!ctx->detached && ctx->pathlen > 0) {
        memcpy(mount->cover_parent_fh, ctx->parent_fh, ctx->parent_fh_len);
        mount->cover_parent_fh_len = ctx->parent_fh_len;
        memcpy(mount->cover_fh, ctx->cur_fh, ctx->cur_fh_len);
        mount->cover_fh_len  = ctx->cur_fh_len;
        mount->cover_name    = strndup(ctx->name, ctx->namelen);
        mount->cover_namelen = ctx->namelen;
        mount->cover_hash    = chimera_vfs_mount_cover_hash(ctx->parent_fh,
                                                            ctx->parent_fh_len,
                                                            ctx->name,
                                                            ctx->namelen);
    }

    chimera_vfs_mount_table_insert(vfs->mount_table, mount);

    ctx->callback(thread, CHIMERA_VFS_OK, ctx->private_data);
    chimera_vfs_mount_ctx_free(ctx);
    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_mount_complete */

/* The mount point is resolved (or none is needed): hand the backend its MOUNT. */
static void
chimera_vfs_mount_dispatch(struct chimera_vfs_mount_ctx *ctx)
{
    struct chimera_vfs_thread  *thread = ctx->thread;
    struct chimera_vfs_module  *module = ctx->module;
    struct chimera_vfs_request *request;
    int                         rc;

    /* For mount operations, the module is already known - use alloc_with_module
     * since chimera_vfs_get_module returns NULL (no mount exists yet) */
    request = chimera_vfs_request_alloc_with_module(thread, ctx->cred,
                                                    &module->fh_magic, 1,
                                                    chimera_vfs_hash(&module->fh_magic, 1),
                                                    module);

    if (CHIMERA_VFS_IS_ERR(request)) {
        chimera_vfs_mount_fail(ctx, CHIMERA_VFS_PTR_ERR(request));
        return;
    }

    /* Parse mount options directly into request buffer */
    rc = chimera_vfs_parse_mount_options(ctx->options,
                                         &request->mount.options,
                                         request->mount.options_buffer,
                                         sizeof(request->mount.options_buffer),
                                         NULL, 0);

    if (rc) {
        chimera_vfs_error("chimera_vfs_mount: invalid mount options: %s",
                          ctx->options ? ctx->options : "(null)");
        chimera_vfs_request_free(thread, request);
        chimera_vfs_mount_fail(ctx, CHIMERA_VFS_EINVAL);
        return;
    }


    request->opcode                   = CHIMERA_VFS_OP_MOUNT;
    request->complete                 = chimera_vfs_mount_complete;
    request->mount.path               = ctx->module_path;
    request->mount.pathlen            = strlen(ctx->module_path);
    request->mount.module             = module;
    request->mount.mount_path         = ctx->path;
    request->mount.mount_pathlen      = ctx->pathlen;
    request->mount.raw_options        = ctx->options;
    request->mount.r_case_policy      = CHIMERA_VFS_CASE_SENSITIVE;
    request->mount.r_attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_CACHEABLE | CHIMERA_VFS_ATTR_FH;
    request->mount.r_attr.va_set_mask = 0;
    request->proto_callback           = NULL;
    request->proto_private_data       = ctx;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mount_dispatch */

/* The walk reached the mount point: ctx->cur_fh is the directory to cover,
 * ctx->parent_fh the directory holding it. */
static void
chimera_vfs_mount_point_resolved(struct chimera_vfs_mount_ctx *ctx)
{
    /* A path-only filesystem resolves whole paths in one step and never
     * looks up the mount point by name, so nothing could be mounted on it. */
    if (chimera_vfs_fh_is_path_only(ctx->thread, ctx->parent_fh, ctx->parent_fh_len)) {
        chimera_vfs_mount_fail(ctx, CHIMERA_VFS_ENOTSUP);
        return;
    }

    chimera_vfs_mount_dispatch(ctx);
} /* chimera_vfs_mount_point_resolved */

static void chimera_vfs_mount_walk(
    struct chimera_vfs_mount_ctx *ctx);

static void
chimera_vfs_mount_walk_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_mount_ctx *ctx = private_data;

    (void) dir_attr;

    chimera_vfs_release(ctx->thread, ctx->handle);
    ctx->handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_mount_fail(ctx, error_code);
        return;
    }

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_vfs_mount_fail(ctx, CHIMERA_VFS_ENOTSUP);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISDIR(attr->va_mode)) {
        chimera_vfs_mount_fail(ctx, CHIMERA_VFS_ENOTDIR);
        return;
    }

    memcpy(ctx->parent_fh, ctx->cur_fh, ctx->cur_fh_len);
    ctx->parent_fh_len = ctx->cur_fh_len;
    memcpy(ctx->cur_fh, attr->va_fh, attr->va_fh_len);
    ctx->cur_fh_len = attr->va_fh_len;

    if (ctx->final) {
        chimera_vfs_mount_point_resolved(ctx);
    } else {
        chimera_vfs_mount_walk(ctx);
    }
} /* chimera_vfs_mount_walk_lookup_cb */

static void
chimera_vfs_mount_walk_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_mount_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_mount_fail(ctx, error_code);
        return;
    }

    ctx->handle = handle;

    chimera_vfs_lookup_at(ctx->thread, chimera_vfs_get_server_cred(), handle,
                          ctx->name, ctx->namelen,
                          CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE, 0,
                          chimera_vfs_mount_walk_lookup_cb, ctx);
} /* chimera_vfs_mount_walk_open_cb */

/* Resolve the next component of the mount path. */
static void
chimera_vfs_mount_walk(struct chimera_vfs_mount_ctx *ctx)
{
    struct chimera_vfs *vfs = ctx->thread->vfs;
    uint8_t             child_fh[CHIMERA_VFS_FH_SIZE + 16];
    int                 child_fh_len;
    int                 rc;

    while (1) {
        ctx->name = ctx->cursor;
        while (*ctx->cursor && *ctx->cursor != '/') {
            ctx->cursor++;
        }
        ctx->namelen = ctx->cursor - ctx->name;
        if (*ctx->cursor == '/') {
            ctx->cursor++;
        }
        ctx->final = (*ctx->cursor == '\0');

        /* Already a mount point: cross it, or refuse to mount on top of it. */
        if (chimera_vfs_mount_table_cover_root(vfs->mount_table,
                                               ctx->cur_fh, ctx->cur_fh_len,
                                               ctx->name, ctx->namelen,
                                               child_fh, &child_fh_len, NULL) == 0) {
            if (ctx->final) {
                chimera_vfs_mount_fail(ctx, CHIMERA_VFS_EBUSY);
                return;
            }
            memcpy(ctx->cur_fh, child_fh, child_fh_len);
            ctx->cur_fh_len = child_fh_len;
            continue;
        }

        if (!chimera_vfs_rootfs_owns(ctx->cur_fh, ctx->cur_fh_len)) {
            break;
        }

        /* The rootfs exists to hold mount points: make the directory. */
        rc = chimera_vfs_rootfs_mkdir(vfs, ctx->cur_fh, ctx->cur_fh_len,
                                      ctx->name, ctx->namelen,
                                      child_fh, &child_fh_len);

        if (rc != CHIMERA_VFS_OK) {
            chimera_vfs_mount_fail(ctx, rc);
            return;
        }

        memcpy(ctx->parent_fh, ctx->cur_fh, ctx->cur_fh_len);
        ctx->parent_fh_len = ctx->cur_fh_len;
        memcpy(ctx->cur_fh, child_fh, child_fh_len);
        ctx->cur_fh_len = child_fh_len;

        if (ctx->final) {
            chimera_vfs_mount_point_resolved(ctx);
            return;
        }
    }

    /* A real filesystem: the mount point must already exist, as on Linux. */
    chimera_vfs_open_fh(ctx->thread, chimera_vfs_get_server_cred(),
                        ctx->cur_fh, ctx->cur_fh_len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_mount_walk_open_cb, ctx);
} /* chimera_vfs_mount_walk */

static void
chimera_vfs_mount_start(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *mount_path,
    const char                    *module_name,
    const char                    *module_path,
    const char                    *options,
    int                            detached,
    chimera_vfs_mount_callback_t   callback,
    void                          *private_data)
{
    struct chimera_vfs           *vfs    = thread->vfs;
    struct chimera_vfs_module    *module = NULL;
    struct chimera_vfs_mount_ctx *ctx;
    struct chimera_vfs_mount     *root;
    uint32_t                      root_fh_len;
    int                           i;

    for (i = 0; i < CHIMERA_VFS_MAX_MODULES; i++) {
        module = vfs->modules[i];
        if (module && module != &chimera_vfs_rootfs_module &&
            strcmp(module->name, module_name) == 0) {
            break;
        }
    }

    if (i == CHIMERA_VFS_MAX_MODULES) {
        chimera_vfs_error("chimera_vfs_mount: module %s not found",
                          module_name);
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    ctx               = calloc(1, sizeof(*ctx));
    ctx->thread       = thread;
    ctx->cred         = cred;
    ctx->module       = module;
    ctx->callback     = callback;
    ctx->private_data = private_data;
    ctx->detached     = detached;
    ctx->path         = malloc(CHIMERA_VFS_PATH_MAX);
    ctx->module_path  = strdup(module_path ? module_path : "");
    ctx->options      = options ? strdup(options) : NULL;
    ctx->pathlen      = chimera_vfs_mount_path_normalize(mount_path, ctx->path,
                                                         CHIMERA_VFS_PATH_MAX);

    if (ctx->pathlen < 0 || (detached && ctx->pathlen == 0)) {
        chimera_vfs_mount_fail(ctx, CHIMERA_VFS_EINVAL);
        return;
    }

    if (detached) {
        if (chimera_vfs_mount_table_find_exact(vfs->mount_table, ctx->path, ctx->pathlen)) {
            chimera_vfs_mount_fail(ctx, CHIMERA_VFS_EEXIST);
            return;
        }
        chimera_vfs_mount_dispatch(ctx);
        return;
    }

    if (ctx->pathlen == 0) {
        /* A mount at "/" goes over the built-in rootfs, as a mount over any
         * directory does on Linux: what the rootfs showed, mounts on its
         * directories included, is hidden until it is unmounted.  One mount
         * someone placed at "/" is the limit. */
        root = chimera_vfs_mount_table_find_exact(vfs->mount_table, "", 0);

        if (root && !root->builtin) {
            chimera_vfs_mount_fail(ctx, CHIMERA_VFS_EBUSY);
            return;
        }

        chimera_vfs_mount_dispatch(ctx);
        return;
    }

    chimera_vfs_get_root_fh(vfs, ctx->cur_fh, &root_fh_len);
    ctx->cur_fh_len = root_fh_len;
    ctx->cursor     = ctx->path;

    chimera_vfs_mount_walk(ctx);
} /* chimera_vfs_mount_start */

SYMBOL_EXPORT void
chimera_vfs_mount(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *mount_path,
    const char                    *module_name,
    const char                    *module_path,
    const char                    *options,
    chimera_vfs_mount_callback_t   callback,
    void                          *private_data)
{
    chimera_vfs_mount_start(thread, cred, mount_path, module_name, module_path,
                            options, 0, callback, private_data);
} /* chimera_vfs_mount */

SYMBOL_EXPORT void
chimera_vfs_mount_detached(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *name,
    const char                    *module_name,
    const char                    *module_path,
    const char                    *options,
    chimera_vfs_mount_callback_t   callback,
    void                          *private_data)
{
    chimera_vfs_mount_start(thread, cred, name, module_name, module_path,
                            options, 1, callback, private_data);
} /* chimera_vfs_mount_detached */

SYMBOL_EXPORT enum chimera_vfs_case_policy
chimera_vfs_case_policy(
    struct chimera_vfs *vfs,
    const void         *fh,
    int                 fh_len)
{
    return chimera_vfs_fh_case_policy(vfs, fh, fh_len);
} /* chimera_vfs_case_policy */
