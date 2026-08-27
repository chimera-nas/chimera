// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Async enforcement pre-steps shared by the namespace wrappers.
 *
 * The setattr/open wrappers gate against an open handle they already hold; the
 * namespace wrappers (remove/rename/mkdir/...) instead need the attrs+ACL of a
 * directory (and sometimes a child) identified only by file handle.  These
 * helpers encapsulate the open -> getattr+ACL -> evaluate -> release dance so a
 * wrapper inserts a single call before dispatching its real operation.  Each is
 * a true no-op (no backend I/O) when chimera_vfs_gate_needed() is false, so the
 * delegated/AUTH_NONE/root fast paths are unchanged.
 */

#include <stdlib.h>
#include <sys/stat.h>

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "sdk/vfs_access.h"
#include "sdk/vfs_acl.h"
#include "common/macros.h"

#define CHIMERA_VFS_GATE_ATTR_MASK (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL)

/* ----------------------------------------------------------------------------
 * chimera_vfs_gate_fh: require a fixed mask on a single object.
 * ------------------------------------------------------------------------- */
static void
chimera_vfs_gate_fh_getattr(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_gate_ctx *ctx = private_data;
    enum chimera_vfs_error       status;

    if (error_code != CHIMERA_VFS_OK) {
        status = error_code;
    } else if (!ctx->any_type &&
               (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
               !S_ISDIR(attr->va_mode)) {
        /* Every gate_fh caller gates a directory (the parent of a create/
         * remove, a rename/link destination, a lookup dir); reaching a
         * non-directory means the caller resolved through a non-directory
         * descriptor or handle.  Report POSIX's ENOTDIR ahead of the
         * access evaluation, so the type error is not masked as EACCES
         * for callers the backend's own type check never reaches. */
        status = CHIMERA_VFS_ENOTDIR;
    } else {
        status = chimera_vfs_gate(attr, ctx->cred, ctx->required);
    }

    chimera_vfs_release(ctx->thread, ctx->handle);

    ctx->callback(status, ctx->private_data);
} /* chimera_vfs_gate_fh_getattr */

static void
chimera_vfs_gate_fh_open(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_gate_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, ctx->private_data);
        return;
    }

    ctx->handle = handle;

    chimera_vfs_getattr(ctx->thread, ctx->cred, handle,
                        CHIMERA_VFS_GATE_ATTR_MASK,
                        chimera_vfs_gate_fh_getattr, ctx);
} /* chimera_vfs_gate_fh_open */

static void
chimera_vfs_gate_fh_impl(
    struct chimera_vfs_gate_ctx   *ctx,
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint32_t                       required,
    int                            needed,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data)
{
    if (!needed) {
        callback(CHIMERA_VFS_OK, private_data);
        return;
    }

    ctx->thread       = thread;
    ctx->cred         = cred;
    ctx->required     = required;
    ctx->callback     = callback;
    ctx->private_data = private_data;
    ctx->handle       = NULL;

    chimera_vfs_open_fh(thread, cred, fh, fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_vfs_gate_fh_open, ctx);
} /* chimera_vfs_gate_fh_impl */

SYMBOL_EXPORT void
chimera_vfs_gate_fh_obj(
    struct chimera_vfs_gate_ctx   *ctx,
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint32_t                       required,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_module *module;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    if (!module) {
        callback(CHIMERA_VFS_ESTALE, private_data);
        return;
    }

    ctx->any_type = 1;

    chimera_vfs_gate_fh_impl(ctx, thread, cred, fh, fhlen, required,
                             chimera_vfs_gate_needed(module->capabilities, cred),
                             callback, private_data);
} /* chimera_vfs_gate_fh_obj */

SYMBOL_EXPORT void
chimera_vfs_gate_fh(
    struct chimera_vfs_gate_ctx   *ctx,
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint32_t                       required,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_module *module;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    if (!module) {
        callback(CHIMERA_VFS_ESTALE, private_data);
        return;
    }

    /* The caller owns this context and may be reusing storage; the directory
    * assertion is the default and must not be inherited from a prior use. */
    ctx->any_type = 0;

    chimera_vfs_gate_fh_impl(ctx, thread, cred, fh, fhlen, required,
                             chimera_vfs_gate_needed(module->capabilities, cred),
                             callback, private_data);
} /* chimera_vfs_gate_fh */

/*
 * Variant of chimera_vfs_gate_fh() that also enforces DAC the kernel cannot see
 * on a handle-based passthrough backend for POSIX callers (path-prefix search,
 * link/rename destination-directory write).  See chimera_vfs_gate_needed_dac().
 */
SYMBOL_EXPORT void
chimera_vfs_gate_fh_dac(
    struct chimera_vfs_gate_ctx   *ctx,
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint32_t                       required,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_module *module;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    if (!module) {
        callback(CHIMERA_VFS_ESTALE, private_data);
        return;
    }

    ctx->any_type = 0;

    chimera_vfs_gate_fh_impl(ctx, thread, cred, fh, fhlen, required,
                             chimera_vfs_gate_needed_dac(module->capabilities, cred),
                             callback, private_data);
} /* chimera_vfs_gate_fh_dac */

/* ----------------------------------------------------------------------------
 * chimera_vfs_gate_delete: delete_allowed across parent dir + child.
 *
 * Fetch the parent first; if it grants DELETE_CHILD and is not sticky we are
 * done with no child fetch (the common case).  Otherwise fetch the child too so
 * the per-object DELETE grant and the sticky-bit owner check can be evaluated.
 * ------------------------------------------------------------------------- */
static void
chimera_vfs_gate_delete_child_getattr(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_gate_ctx *ctx = private_data;
    enum chimera_vfs_error       status;
    int                          allow;
    uint64_t                     child_uid;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(ctx->thread, ctx->handle);
        ctx->callback(error_code, ctx->private_data);
        return;
    }

    /* The parent's DELETE_CHILD grant always authorizes removal.  A per-object
     * DELETE grant on the child is an NFSv4/NT concept; under POSIX (AUTH_UNIX /
     * AUTH_NONE) deletion is governed solely by the containing directory's
     * write+search permission, so the child-DELETE fallback applies only to
     * ACL-flavored (AUTH_ATTR) callers. */
    allow = ctx->dc ||
        (ctx->cred->flavor == CHIMERA_VFS_AUTH_ATTR &&
         chimera_vfs_access_allowed(attr, ctx->cred, CHIMERA_ACE_DELETE));

    if (allow && ctx->sticky) {
        child_uid = (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ?
            attr->va_uid : (uint64_t) -1;

        if (ctx->cred->uid != 0 &&
            (uint64_t) ctx->cred->uid != child_uid &&
            (uint64_t) ctx->cred->uid != ctx->parent_uid) {
            allow = 0;
        }
    }

    chimera_vfs_release(ctx->thread, ctx->handle);

    status = allow ? CHIMERA_VFS_OK : CHIMERA_VFS_EACCES;
    ctx->callback(status, ctx->private_data);
} /* chimera_vfs_gate_delete_child_getattr */

static void
chimera_vfs_gate_delete_child_open(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_gate_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, ctx->private_data);
        return;
    }

    ctx->handle = handle;

    chimera_vfs_getattr(ctx->thread, ctx->cred, handle,
                        CHIMERA_VFS_GATE_ATTR_MASK,
                        chimera_vfs_gate_delete_child_getattr, ctx);
} /* chimera_vfs_gate_delete_child_open */

static void
chimera_vfs_gate_delete_parent_getattr(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_gate_ctx *ctx = private_data;
    uint32_t                     mode;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(ctx->thread, ctx->handle);
        ctx->callback(error_code, ctx->private_data);
        return;
    }

    ctx->dc = chimera_vfs_access_allowed(attr, ctx->cred,
                                         CHIMERA_ACE_DELETE_CHILD);
    mode            = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ? attr->va_mode : 0;
    ctx->sticky     = !!(mode & S_ISVTX);
    ctx->parent_uid = (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ?
        attr->va_uid : (uint64_t) -1;

    chimera_vfs_release(ctx->thread, ctx->handle);
    ctx->handle = NULL;

    /* Fast path: parent grants DELETE_CHILD and is not sticky -- allowed
     * without fetching the child at all. */
    if (ctx->dc && !ctx->sticky) {
        ctx->callback(CHIMERA_VFS_OK, ctx->private_data);
        return;
    }

    chimera_vfs_open_fh(ctx->thread, ctx->cred, ctx->child_fh, ctx->child_fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_vfs_gate_delete_child_open, ctx);
} /* chimera_vfs_gate_delete_parent_getattr */

static void
chimera_vfs_gate_delete_parent_open(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_gate_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, ctx->private_data);
        return;
    }

    ctx->handle = handle;

    chimera_vfs_getattr(ctx->thread, ctx->cred, handle,
                        CHIMERA_VFS_GATE_ATTR_MASK,
                        chimera_vfs_gate_delete_parent_getattr, ctx);
} /* chimera_vfs_gate_delete_parent_open */

SYMBOL_EXPORT void
chimera_vfs_gate_delete(
    struct chimera_vfs_gate_ctx   *ctx,
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *parent_fh,
    int                            parent_fhlen,
    const void                    *child_fh,
    int                            child_fhlen,
    chimera_vfs_gate_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_module *module;

    module = chimera_vfs_get_module(thread, parent_fh, parent_fhlen);

    if (!module) {
        callback(CHIMERA_VFS_ESTALE, private_data);
        return;
    }

    if (!chimera_vfs_gate_needed(module->capabilities, cred)) {
        callback(CHIMERA_VFS_OK, private_data);
        return;
    }

    ctx->thread       = thread;
    ctx->cred         = cred;
    ctx->child_fh     = child_fh;
    ctx->child_fhlen  = child_fhlen;
    ctx->callback     = callback;
    ctx->private_data = private_data;
    ctx->handle       = NULL;
    ctx->dc           = 0;
    ctx->sticky       = 0;
    ctx->parent_uid   = (uint64_t) -1;

    chimera_vfs_open_fh(thread, cred, parent_fh, parent_fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_vfs_gate_delete_parent_open, ctx);
} /* chimera_vfs_gate_delete */
