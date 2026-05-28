// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#include <stdlib.h>

#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_name_cache.h"
#include "vfs/vfs_attr_cache.h"
#include "vfs/vfs_notify.h"
#include "vfs/vfs_access.h"
#include "vfs/vfs_acl.h"
#include "common/macros.h"

static void
chimera_vfs_remove_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread       *thread     = request->thread;
    struct chimera_vfs_attr_cache   *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_name_cache   *name_cache = thread->vfs->vfs_name_cache;
    chimera_vfs_remove_at_callback_t callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
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

        chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                request->remove_at.handle->fh,
                                request->remove_at.handle->fh_len,
                                action,
                                request->remove_at.name,
                                request->remove_at.namelen,
                                NULL, 0);

        chimera_vfs_name_cache_insert(name_cache,
                                      request->remove_at.handle->fh_hash,
                                      request->remove_at.handle->fh,
                                      request->remove_at.handle->fh_len,
                                      request->remove_at.name_hash,
                                      request->remove_at.name,
                                      request->remove_at.namelen,
                                      NULL,
                                      0);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->remove_at.handle->fh_hash,
                                      request->remove_at.handle->fh,
                                      request->remove_at.handle->fh_len,
                                      &request->remove_at.r_dir_post_attr);

        if (request->remove_at.r_removed_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            chimera_vfs_attr_cache_insert(attr_cache,
                                          chimera_vfs_hash(request->remove_at.r_removed_attr.va_fh, request->remove_at.
                                                           r_removed_attr.va_fh_len),
                                          request->remove_at.r_removed_attr.va_fh,
                                          request->remove_at.r_removed_attr.va_fh_len,
                                          &request->remove_at.r_removed_attr);
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
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                                = CHIMERA_VFS_OP_REMOVE_AT;
    request->complete                              = chimera_vfs_remove_at_complete;
    request->remove_at.handle                      = handle;
    request->remove_at.name                        = name;
    request->remove_at.namelen                     = namelen;
    request->remove_at.name_hash                   = chimera_vfs_hash(name, namelen);
    request->remove_at.child_fh                    = child_fh;
    request->remove_at.child_fh_len                = child_fh_len;
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
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    struct chimera_vfs_open_handle  *handle;
    const char                      *name;
    int                              namelen;
    const uint8_t                   *child_fh;
    int                              child_fh_len;
    uint64_t                         pre_attr_mask;
    uint64_t                         post_attr_mask;
    chimera_vfs_remove_at_callback_t callback;
    void                            *private_data;
};

static void
chimera_vfs_remove_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_remove_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        gate->callback(status, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_remove_at_dispatch(gate->thread, gate->cred, gate->handle,
                                   gate->name, gate->namelen, gate->child_fh,
                                   gate->child_fh_len, gate->pre_attr_mask,
                                   gate->post_attr_mask, gate->callback,
                                   gate->private_data);
    free(gate);
} /* chimera_vfs_remove_at_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_remove_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_remove_at_gate *gate;

    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        gate                 = malloc(sizeof(*gate));
        gate->thread         = thread;
        gate->cred           = cred;
        gate->handle         = handle;
        gate->name           = name;
        gate->namelen        = namelen;
        gate->child_fh       = child_fh;
        gate->child_fh_len   = child_fh_len;
        gate->pre_attr_mask  = pre_attr_mask;
        gate->post_attr_mask = post_attr_mask;
        gate->callback       = callback;
        gate->private_data   = private_data;

        if (child_fh && child_fh_len > 0) {
            chimera_vfs_gate_delete(thread, cred, handle->fh, handle->fh_len,
                                    child_fh, child_fh_len,
                                    chimera_vfs_remove_at_gate_complete, gate);
        } else {
            /* No child FH available: authorize on the parent's DELETE_CHILD
             * grant alone (the per-object DELETE and sticky owner checks need
             * the child's attrs). */
            chimera_vfs_gate_fh(thread, cred, handle->fh, handle->fh_len,
                                CHIMERA_ACE_DELETE_CHILD,
                                chimera_vfs_remove_at_gate_complete, gate);
        }
        return;
    }

    chimera_vfs_remove_at_dispatch(thread, cred, handle, name, namelen,
                                   child_fh, child_fh_len, pre_attr_mask,
                                   post_attr_mask, callback, private_data);
} /* chimera_vfs_remove_at */
