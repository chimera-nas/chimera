// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "vfs/vfs_procs.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_name_cache.h"
#include "vfs/vfs_attr_cache.h"
#include "vfs/vfs_notify.h"
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

    chimera_vfs_dispatch(request);

} /* chimera_vfs_remove_at */
