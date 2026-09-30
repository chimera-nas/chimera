// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "common/macros.h"

/*
 * Mount point crossing.
 *
 * A name lookup that lands on a mount point, or ".." taken from a mount's
 * root, resolves to a directory on another filesystem than the one the
 * lookup was sent to.  The mount table says which handle; this finishes the
 * lookup against it: open the handle, fetch the attributes the caller asked
 * for, and answer as lookup_at would.  The directory's own attributes are not
 * reported, since they belong to the filesystem the lookup left.
 */

struct chimera_vfs_cross_ctx {
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    struct chimera_vfs_open_handle  *handle;
    uint64_t                         attr_mask;
    chimera_vfs_lookup_at_callback_t callback;
    void                            *private_data;
    struct chimera_vfs_attrs         dir_attr;
};

static void
chimera_vfs_cross_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_cross_ctx   *ctx    = private_data;
    struct chimera_vfs_open_handle *handle = ctx->handle;

    if (error_code == CHIMERA_VFS_OK) {
        memcpy(attr->va_fh, handle->fh, handle->fh_len);
        attr->va_fh_len    = handle->fh_len;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
    }

    ctx->callback(error_code, error_code == CHIMERA_VFS_OK ? attr : NULL,
                  &ctx->dir_attr, ctx->private_data);

    chimera_vfs_release(ctx->thread, handle);
    free(ctx);
} /* chimera_vfs_cross_getattr_cb */

static void
chimera_vfs_cross_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_cross_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, NULL, &ctx->dir_attr, ctx->private_data);
        free(ctx);
        return;
    }

    ctx->handle = handle;

    chimera_vfs_getattr(ctx->thread, ctx->cred, handle,
                        ctx->attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE,
                        chimera_vfs_cross_getattr_cb, ctx);
} /* chimera_vfs_cross_open_cb */

void
chimera_vfs_lookup_at_redirect(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const uint8_t                   *fh,
    int                              fhlen,
    uint64_t                         attr_mask,
    chimera_vfs_lookup_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_cross_ctx *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        callback(CHIMERA_VFS_EIO, NULL, NULL, private_data);
        return;
    }

    ctx->thread               = thread;
    ctx->cred                 = cred;
    ctx->attr_mask            = attr_mask;
    ctx->callback             = callback;
    ctx->private_data         = private_data;
    ctx->dir_attr.va_req_mask = 0;
    ctx->dir_attr.va_set_mask = 0;

    chimera_vfs_open_fh(thread, cred, fh, fhlen,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
                        chimera_vfs_cross_open_cb, ctx);
} /* chimera_vfs_lookup_at_redirect */

struct chimera_vfs_cross_open_ctx {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        attr_mask;
    struct chimera_vfs_attrs       *set_attr;
    chimera_vfs_open_at_callback_t  callback;
    void                           *private_data;
    struct chimera_vfs_attrs        dir_pre_attr;
    struct chimera_vfs_attrs        dir_post_attr;
};

static void
chimera_vfs_cross_open_at_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_cross_open_ctx *ctx    = private_data;
    struct chimera_vfs_open_handle    *handle = ctx->handle;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(ctx->thread, handle);
        ctx->callback(error_code, NULL, NULL, NULL, NULL, NULL, ctx->private_data);
        free(ctx);
        return;
    }

    memcpy(attr->va_fh, handle->fh, handle->fh_len);
    attr->va_fh_len    = handle->fh_len;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;

    /* The caller now owns the handle, as with any open_at. */
    ctx->callback(CHIMERA_VFS_OK, handle, ctx->set_attr, attr,
                  &ctx->dir_pre_attr, &ctx->dir_post_attr, ctx->private_data);
    free(ctx);
} /* chimera_vfs_cross_open_at_getattr_cb */

static void
chimera_vfs_cross_open_at_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_cross_open_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, NULL, NULL, NULL, NULL, NULL, ctx->private_data);
        free(ctx);
        return;
    }

    ctx->handle = handle;

    chimera_vfs_getattr(ctx->thread, ctx->cred, handle,
                        ctx->attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE,
                        chimera_vfs_cross_open_at_getattr_cb, ctx);
} /* chimera_vfs_cross_open_at_open_cb */

void
chimera_vfs_open_at_redirect(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    int                            fhlen,
    unsigned int                   flags,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_open_at_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_cross_open_ctx *ctx;

    /* The name exists -- it is a mount point -- so an exclusive create fails
     * as it would on the directory underneath, and nothing is created or
     * truncated: what opens is the mounted filesystem's root directory. */
    if ((flags & CHIMERA_VFS_OPEN_CREATE) && (flags & CHIMERA_VFS_OPEN_EXCLUSIVE)) {
        callback(CHIMERA_VFS_EEXIST, NULL, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        callback(CHIMERA_VFS_EIO, NULL, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    ctx->thread       = thread;
    ctx->cred         = cred;
    ctx->attr_mask    = attr_mask;
    ctx->set_attr     = set_attr;
    ctx->callback     = callback;
    ctx->private_data = private_data;

    chimera_vfs_open_fh(thread, cred, fh, fhlen,
                        flags & ~(CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                                  CHIMERA_VFS_OPEN_TRUNCATE),
                        chimera_vfs_cross_open_at_open_cb, ctx);
} /* chimera_vfs_open_at_redirect */
