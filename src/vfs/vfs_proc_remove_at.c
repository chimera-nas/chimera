// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#include <stdlib.h>

#include "vfs/vfs_procs.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_name_cache.h"
#include "vfs/vfs_attr_cache.h"
#include "vfs/vfs_notify.h"
#include "vfs/sdk/vfs_access.h"
#include "vfs/sdk/vfs_acl.h"
#include "common/macros.h"

static void
chimera_vfs_remove_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread       *thread     = request->thread;
    struct chimera_vfs_attr_cache   *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_name_cache   *name_cache = thread->vfs->vfs_name_cache;
    chimera_vfs_remove_at_callback_t callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK && !request->remove_at.r_unmatched) {
        /* Pick FILE_REMOVED vs DIR_REMOVED based on the removed
         * object's mode.  Clients filtering only SMB2_NOTIFY_CHANGE_DIR_NAME
         * would otherwise miss rmdir entirely, since the SMB
         * filter→VFS mapping routes DIR_NAME only to DIR_ADDED /
         * DIR_REMOVED / RENAMED.  va_mode is in MASK_STAT which is
         * included in MASK_CACHEABLE requested below. */
        uint32_t action = CHIMERA_VFS_NOTIFY_FILE_REMOVED;
        if ((request->remove_at.r_removed_attr.va_set_mask &
             CHIMERA_VFS_ATTR_MODE) &&
            S_ISDIR(request->remove_at.r_removed_attr.va_mode)) {
            action = CHIMERA_VFS_NOTIFY_DIR_REMOVED;
        }

        /* Strip STAT from the removed-object attrs before the cache
         * insert below.  We requested STAT from the backend purely to
         * learn va_mode for the notify dispatch — but inserting the
         * pre-unlink STAT into the attr cache pollutes hardlink
         * survivors: file.0 and newfile.0 share an inode (and thus an
         * FH), so a cached pre-unlink nlink=2 entry on newfile.0's FH
         * is also returned for file.0 lookups even though file.0's
         * actual nlink is now 1.  The attr_cache_insert path skips
         * insertion when STAT bits are not all present, so clearing
         * them effectively invalidates any prior entry for this FH —
         * which is exactly what we want post-remove. */
        request->remove_at.r_removed_attr.va_set_mask &=
            ~CHIMERA_VFS_ATTR_MASK_STAT;

        uint64_t skip_lo = 0, skip_hi = 0;

        if (request->remove_at.parent_lease_skip_valid) {
            memcpy(&skip_lo, request->remove_at.parent_lease_skip, 8);
            memcpy(&skip_hi, request->remove_at.parent_lease_skip + 8, 8);
        }
        chimera_vfs_notify_emit_lease(thread->vfs->vfs_notify,
                                      request->remove_at.handle->fh,
                                      request->remove_at.handle->fh_len,
                                      action,
                                      request->remove_at.name,
                                      request->remove_at.namelen,
                                      NULL, 0,
                                      skip_lo, skip_hi,
                                      request->remove_at.parent_lease_skip_valid);

        /* Signal any CHANGE_NOTIFY armed on a handle to the object that was
         * just removed (its own FH, distinct from the parent emit above) so
         * that pending request completes with STATUS_DELETE_PENDING rather
         * than parking forever.  Prefer the caller-supplied child FH; fall
         * back to the FH the backend reported for the removed entry. */
        if (request->remove_at.child_fh && request->remove_at.child_fh_len > 0) {
            chimera_vfs_notify_emit_delete(thread->vfs->vfs_notify,
                                           request->remove_at.child_fh,
                                           request->remove_at.child_fh_len);
        } else if (request->remove_at.r_removed_attr.va_set_mask &
                   CHIMERA_VFS_ATTR_FH) {
            chimera_vfs_notify_emit_delete(thread->vfs->vfs_notify,
                                           request->remove_at.r_removed_attr.va_fh,
                                           request->remove_at.r_removed_attr.va_fh_len);
        }

        chimera_vfs_name_cache_insert(thread, name_cache,
                                      request->remove_at.handle->fh_hash,
                                      request->remove_at.handle->fh,
                                      request->remove_at.handle->fh_len,
                                      request->remove_at.name_hash,
                                      request->remove_at.name,
                                      request->remove_at.namelen,
                                      NULL,
                                      0);

        chimera_vfs_attr_cache_insert(thread, attr_cache,
                                      request->remove_at.handle->fh_hash,
                                      request->remove_at.handle->fh,
                                      request->remove_at.handle->fh_len,
                                      &request->remove_at.r_dir_post_attr);

        if (request->remove_at.r_removed_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            chimera_vfs_attr_cache_insert(thread, attr_cache,
                                          chimera_vfs_hash(request->remove_at.r_removed_attr.va_fh, request->remove_at.
                                                           r_removed_attr.va_fh_len),
                                          request->remove_at.r_removed_attr.va_fh,
                                          request->remove_at.r_removed_attr.va_fh_len,
                                          &request->remove_at.r_removed_attr);
        } else if (request->remove_at.child_fh &&
                   request->remove_at.child_fh_len > 0) {
            /* The backend did not report the removed object's attrs (the NFS
             * proxies have no post-op attrs for the victim), but the caller
             * resolved the child before the remove -- invalidate its attr
             * cache entry by that handle, or a hardlink survivor keeps
             * serving the pre-unlink nlink. */
            struct chimera_vfs_attrs inval;

            inval.va_set_mask = 0;
            inval.va_req_mask = 0;

            chimera_vfs_attr_cache_insert(thread, attr_cache,
                                          chimera_vfs_hash(request->remove_at.child_fh,
                                                           request->remove_at.child_fh_len),
                                          request->remove_at.child_fh,
                                          request->remove_at.child_fh_len,
                                          &inval);
        }
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->remove_at.r_dir_pre_attr,
             &request->remove_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_remove_at_complete */

static void
chimera_vfs_remove_at_dispatch(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    int                              match_child_fh,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode              = CHIMERA_VFS_OP_REMOVE_AT;
    request->complete            = chimera_vfs_remove_at_complete;
    request->remove_at.handle    = handle;
    request->remove_at.name      = name;
    request->remove_at.namelen   = namelen;
    request->remove_at.name_hash = chimera_vfs_hash(name, namelen);
    request->remove_at.flags     = flags;
    /* Copy the child FH into request-owned storage: the caller's or
     * gate-resolved source may be freed before this async op completes. */
    if (child_fh && child_fh_len > 0) {
        memcpy(request->remove_at.child_fh_store, child_fh, child_fh_len);
        request->remove_at.child_fh = request->remove_at.child_fh_store;
    } else {
        request->remove_at.child_fh = NULL;
    }
    request->remove_at.child_fh_len   = child_fh_len;
    request->remove_at.match_child_fh = match_child_fh ? 1 : 0;
    request->remove_at.r_unmatched    = 0;
    if (parent_lease_skip) {
        memcpy(request->remove_at.parent_lease_skip, parent_lease_skip, 16);
        request->remove_at.parent_lease_skip_valid = 1;
    } else {
        request->remove_at.parent_lease_skip_valid = 0;
    }
    request->remove_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->remove_at.r_dir_pre_attr.va_set_mask  = 0;
    request->remove_at.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->remove_at.r_dir_post_attr.va_set_mask = 0;
    request->remove_at.r_removed_attr.va_req_mask  = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->remove_at.r_removed_attr.va_set_mask  = 0;
    request->proto_callback                        = callback;
    request->proto_private_data                    = private_data;

    /* Recall any delegation/oplock on the file being removed before unlinking
     * it (the caller supplies its FH when known). */
    chimera_vfs_io_recall(request, child_fh, child_fh_len,
                          child_fh_len ? chimera_vfs_hash(child_fh, child_fh_len) : 0,
                          0 /* namespace recall: revoke fully */,
                          chimera_vfs_dispatch);

} /* chimera_vfs_remove_at_dispatch */

/*
 * Enforcement pre-step context: removing a name is authorized by
 * chimera_vfs_delete_allowed (DELETE_CHILD on the parent or DELETE on the
 * child, plus the POSIX sticky-bit owner rule).  When the child FH is known we
 * run the full two-object check; otherwise we fall back to DELETE_CHILD on the
 * parent alone.
 */
struct chimera_vfs_remove_at_gate {
    struct chimera_vfs_gate_ctx      gate_ctx;
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    struct chimera_vfs_open_handle  *handle;
    const char                      *name;
    int                              namelen;
    const uint8_t                   *child_fh;
    int                              child_fh_len;
    int                              match_child_fh;
    unsigned int                     flags;
    uint64_t                         pre_attr_mask;
    uint64_t                         post_attr_mask;
    uint8_t                          parent_lease_skip[16];
    uint8_t                          parent_lease_skip_valid;
    chimera_vfs_remove_at_callback_t callback;
    void                            *private_data;
    /* Storage for a child FH resolved by name (sticky-dir owner check) when
     * the caller did not supply one. */
    uint8_t                          child_fh_buf[CHIMERA_VFS_FH_SIZE];
    /* The child FH was resolved by our sticky-lookup, not supplied by the
     * caller.  It authorizes the delete gate, but must NOT be handed to the
     * dispatch's io_recall: a name-based delete (the caller passed no FH --
     * e.g. SMB delete-on-close) recalls via the post-unlink FILE_REMOVED
     * notify, and also recalling the resolved FH pre-unlink would break the
     * holder's lease twice (smbtorture smb2.lease.unlink: count 0x2 vs 0x1). */
    uint8_t                          child_fh_resolved;
};

_Static_assert(sizeof(struct chimera_vfs_remove_at_gate) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "remove_at gate context outgrew the request gate scratch area");

static void chimera_vfs_remove_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data);

/* When a gate is needed but the caller gave no child FH (e.g. an NFS
 * name-based REMOVE), resolve the child by name so the sticky-directory owner
 * rule and any per-object DELETE grant can be evaluated -- otherwise a sticky
 * directory's owner check is silently skipped. */
static void
chimera_vfs_remove_at_sticky_lookup(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_remove_at_gate *gate = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        /* Name absent/unreadable: surface the natural removal error (ENOENT). */
        gate->callback(error_code, NULL, NULL, gate->private_data);
        chimera_vfs_gate_scratch_free(gate->thread, gate);
        return;
    }

    memcpy(gate->child_fh_buf, attr->va_fh, attr->va_fh_len);
    gate->child_fh          = gate->child_fh_buf;
    gate->child_fh_len      = attr->va_fh_len;
    gate->child_fh_resolved = 1;

    chimera_vfs_gate_delete(&gate->gate_ctx, gate->thread, gate->cred,
                            gate->handle->fh, gate->handle->fh_len,
                            gate->child_fh, gate->child_fh_len,
                            chimera_vfs_remove_at_gate_complete, gate);
} /* chimera_vfs_remove_at_sticky_lookup */

static void
chimera_vfs_remove_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_remove_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        gate->callback(status, NULL, NULL, gate->private_data);
        chimera_vfs_gate_scratch_free(gate->thread, gate);
        return;
    }

    /* Only a caller-supplied FH drives the dispatch's io_recall.  A
     * sticky-resolved FH authorized the gate above but is withheld here so the
     * name-based delete recalls once (via the post-unlink notify), matching the
     * pre-sticky-lookup behavior. */
    chimera_vfs_remove_at_dispatch(gate->thread, gate->cred, gate->handle,
                                   gate->name, gate->namelen,
                                   gate->child_fh_resolved ? NULL :
                                   gate->child_fh,
                                   gate->child_fh_resolved ? 0 :
                                   gate->child_fh_len, gate->match_child_fh,
                                   gate->flags,
                                   gate->pre_attr_mask,
                                   gate->post_attr_mask,
                                   gate->parent_lease_skip_valid ?
                                   gate->parent_lease_skip : NULL,
                                   gate->callback,
                                   gate->private_data);
    chimera_vfs_gate_scratch_free(gate->thread, gate);
} /* chimera_vfs_remove_at_gate_complete */

static void
chimera_vfs_remove_at_common(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    int                              match_child_fh,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_remove_at_gate *gate;

    if (namelen >= CHIMERA_VFS_NAME_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, private_data);
        return;
    }

    /* POSIX: a final path component of "." cannot be removed (EINVAL); ".."
     * names the (non-empty) parent directory (ENOTEMPTY). */
    if (namelen == 1 && name[0] == '.') {
        callback(CHIMERA_VFS_EINVAL, NULL, NULL, private_data);
        return;
    }
    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        callback(CHIMERA_VFS_ENOTEMPTY, NULL, NULL, private_data);
        return;
    }

    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        gate                    = chimera_vfs_gate_scratch_alloc(thread);
        gate->thread            = thread;
        gate->cred              = cred;
        gate->handle            = handle;
        gate->name              = name;
        gate->namelen           = namelen;
        gate->child_fh          = child_fh;
        gate->child_fh_len      = child_fh_len;
        gate->child_fh_resolved = 0;
        gate->match_child_fh    = match_child_fh;
        gate->flags             = flags;
        gate->pre_attr_mask     = pre_attr_mask;
        gate->post_attr_mask    = post_attr_mask;
        if (parent_lease_skip) {
            memcpy(gate->parent_lease_skip, parent_lease_skip, 16);
            gate->parent_lease_skip_valid = 1;
        } else {
            gate->parent_lease_skip_valid = 0;
        }
        gate->callback     = callback;
        gate->private_data = private_data;

        if (child_fh && child_fh_len > 0) {
            chimera_vfs_gate_delete(&gate->gate_ctx, thread, cred,
                                    handle->fh, handle->fh_len,
                                    child_fh, child_fh_len,
                                    chimera_vfs_remove_at_gate_complete, gate);
        } else {
            /* No child FH from the caller (e.g. an NFS name-based REMOVE):
            * resolve it by name so the sticky-directory owner rule and any
            * per-object DELETE grant are honored, not silently skipped. */
            chimera_vfs_lookup(thread, cred, handle->fh, handle->fh_len,
                               name, namelen, CHIMERA_VFS_ATTR_FH, 0,
                               chimera_vfs_remove_at_sticky_lookup, gate);
        }
        return;
    }

    chimera_vfs_remove_at_dispatch(thread, cred, handle, name, namelen,
                                   child_fh, child_fh_len, match_child_fh,
                                   flags, pre_attr_mask,
                                   post_attr_mask, parent_lease_skip,
                                   callback, private_data);
} /* chimera_vfs_remove_at_common */

/*
 * Recall pre-step for a by-name remove.  An NFSv3/NFSv4 REMOVE/RMDIR (and the
 * SMB namespace deletes) arrive with a parent handle and a name but no handle
 * for the victim, so the VFS cannot recall a delegation/oplock/lease held on
 * it before the unlink -- io_recall keys on the FH.  When a caching protocol
 * is enabled we resolve the name to its FH here, once, in the VFS, rather than
 * making every by-name caller do it.  A failed lookup is not fatal: the by-name
 * remove then produces the authoritative error.
 */
struct chimera_vfs_remove_recall_ctx {
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    struct chimera_vfs_open_handle  *handle;
    const char                      *name;
    int                              namelen;
    unsigned int                     flags;
    uint64_t                         pre_attr_mask;
    uint64_t                         post_attr_mask;
    const uint8_t                   *parent_lease_skip;
    chimera_vfs_remove_at_callback_t callback;
    void                            *private_data;
    uint8_t                          child_fh[CHIMERA_VFS_FH_SIZE];
    int                              child_fh_len;
};

_Static_assert(sizeof(struct chimera_vfs_remove_recall_ctx) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "remove recall context outgrew the request gate scratch area");

static void
chimera_vfs_remove_recall_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_remove_recall_ctx *ctx          = private_data;
    const uint8_t                        *child_fh     = NULL;
    int                                   child_fh_len = 0;

    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->child_fh, attr->va_fh, attr->va_fh_len);
        ctx->child_fh_len = attr->va_fh_len;
        child_fh          = ctx->child_fh;
        child_fh_len      = ctx->child_fh_len;
    }

    chimera_vfs_remove_at_common(ctx->thread, ctx->cred, ctx->handle,
                                 ctx->name, ctx->namelen, child_fh, child_fh_len,
                                 0 /* match_child_fh */, ctx->flags,
                                 ctx->pre_attr_mask, ctx->post_attr_mask,
                                 ctx->parent_lease_skip, ctx->callback,
                                 ctx->private_data);
    chimera_vfs_gate_scratch_free(ctx->thread, ctx);
} /* chimera_vfs_remove_recall_lookup_complete */

/* Remove a name in a directory (unconditional by-name unlink).  child_fh, when
 * supplied, is used for delegation/oplock recall and change-notify, NOT to guard
 * the unlink.  When it is NOT supplied and a caching protocol is enabled, the
 * VFS resolves it first (above) so a cross-protocol holder is recalled before
 * the unlink. */
SYMBOL_EXPORT void
chimera_vfs_remove_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data)
{
    /* Resolve the victim's FH first so a cross-protocol caching holder is
     * recalled before the unlink.  Only when the caller opted in
     * (CHIMERA_VFS_REMOVE_RECALL), a caching protocol is enabled, the caller
     * has not already supplied the FH, and the name is a real component
     * ("." / ".." are rejected by _common). */
    if ((flags & CHIMERA_VFS_REMOVE_RECALL) && thread->vfs->caching_enabled &&
        !child_fh && namelen >= 1 && namelen < CHIMERA_VFS_NAME_MAX &&
        !(namelen == 1 && name[0] == '.') &&
        !(namelen == 2 && name[0] == '.' && name[1] == '.')) {
        struct chimera_vfs_remove_recall_ctx *ctx = chimera_vfs_gate_scratch_alloc(thread);

        ctx->thread            = thread;
        ctx->cred              = cred;
        ctx->handle            = handle;
        ctx->name              = name;
        ctx->namelen           = namelen;
        ctx->flags             = flags;
        ctx->pre_attr_mask     = pre_attr_mask;
        ctx->post_attr_mask    = post_attr_mask;
        ctx->parent_lease_skip = parent_lease_skip;
        ctx->callback          = callback;
        ctx->private_data      = private_data;
        ctx->child_fh_len      = 0;

        chimera_vfs_lookup_at(thread, cred, handle, name, namelen,
                              CHIMERA_VFS_ATTR_FH, 0,
                              chimera_vfs_remove_recall_lookup_complete, ctx);
        return;
    }

    chimera_vfs_remove_at_common(thread, cred, handle, name, namelen,
                                 child_fh, child_fh_len, 0 /* match_child_fh */,
                                 flags, pre_attr_mask, post_attr_mask, parent_lease_skip,
                                 callback, private_data);
} /* chimera_vfs_remove_at */

/* Inode-scoped variant: only unlink the name while it STILL resolves to
 * child_fh.  If the original object was removed and a different one created at
 * the same name in the meantime, the name is left intact and the callback
 * reports success (the caller's object is already gone).  Used by delete-on-
 * close so an async unlink cannot destroy an unrelated file (see
 * remove_at.match_child_fh).  Requires child_fh; a backend that does not honor
 * the flag falls back to an unconditional remove. */
SYMBOL_EXPORT void
chimera_vfs_remove_at_match_fh(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data)
{
    chimera_vfs_remove_at_common(thread, cred, handle, name, namelen,
                                 child_fh, child_fh_len, 1 /* match_child_fh */,
                                 0 /* flags */, pre_attr_mask, post_attr_mask, parent_lease_skip,
                                 callback, private_data);
} /* chimera_vfs_remove_at_match_fh */
