// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs_procs.h"
#include "vfs_state.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_notify.h"
#include "common/macros.h"

static void
chimera_vfs_rename_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread       *thread     = request->thread;
    struct chimera_vfs_name_cache   *name_cache = thread->vfs->vfs_name_cache;
    chimera_vfs_rename_at_callback_t callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        int cross_dir = (request->fh_len != request->rename_at.new_fhlen) ||
            memcmp(request->fh, request->rename_at.new_fh,
                   request->fh_len) != 0;

        if (!cross_dir) {
            /* Intra-directory rename: a single RENAMED event on the
             * directory carrying both old and new names.  The SMB
             * serializer expands this to FILE_ACTION_RENAMED_OLD_NAME
             * followed by FILE_ACTION_RENAMED_NEW_NAME per MS-FSCC. */
            chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                    request->fh,
                                    request->fh_len,
                                    CHIMERA_VFS_NOTIFY_RENAMED,
                                    request->rename_at.new_name,
                                    request->rename_at.new_namelen,
                                    request->rename_at.name,
                                    request->rename_at.namelen);
        } else {
            /* Cross-directory rename: source dir sees the OLD name
             * only (RENAMED_OLD_NAME record), destination sees the
             * NEW name only (RENAMED_NEW_NAME record).  Matches
             * Windows behavior and avoids reporting a name on the
             * source dir that exists in a different directory. */
            chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                    request->fh,
                                    request->fh_len,
                                    CHIMERA_VFS_NOTIFY_RENAMED,
                                    NULL, 0,
                                    request->rename_at.name,
                                    request->rename_at.namelen);
            chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                    request->rename_at.new_fh,
                                    request->rename_at.new_fhlen,
                                    CHIMERA_VFS_NOTIFY_RENAMED,
                                    request->rename_at.new_name,
                                    request->rename_at.new_namelen,
                                    NULL, 0);
        }

        /* Remove cache entries for both old and new paths.
         * We don't insert a negative entry for the old path because
         * if the source and destination are hard links to the same inode,
         * the backend may treat the rename as a no-op and leave both
         * paths valid. Inserting a negative entry would incorrectly
         * mark the old path as deleted. */

        chimera_vfs_name_cache_remove(name_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      request->rename_at.name_hash,
                                      request->rename_at.name,
                                      request->rename_at.namelen);

        chimera_vfs_name_cache_remove(name_cache,
                                      request->rename_at.new_fh_hash,
                                      request->rename_at.new_fh,
                                      request->rename_at.new_fhlen,
                                      request->rename_at.new_name_hash,
                                      request->rename_at.new_name,
                                      request->rename_at.new_namelen);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->rename_at.r_fromdir_pre_attr,
             &request->rename_at.r_fromdir_post_attr,
             &request->rename_at.r_todir_pre_attr,
             &request->rename_at.r_todir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_rename_at_complete */

/*
 * Recall any delegation/oplock on the doomed destination (the file being
 * overwritten, if any) before it is replaced, then perform the rename.
 */
static void
chimera_vfs_rename_at_recall_target(struct chimera_vfs_request *request)
{
    chimera_vfs_io_recall(request,
                          request->rename_at.target_fh,
                          request->rename_at.target_fh_len,
                          request->rename_at.target_fh_len ?
                          chimera_vfs_hash(request->rename_at.target_fh,
                                           request->rename_at.target_fh_len) : 0,
                          chimera_vfs_dispatch);
} /* chimera_vfs_rename_at_recall_target */

/*
 * The source file is being moved: renaming it changes its ctime and directory
 * linkage, which invalidates a delegation holder's cached state, so recall any
 * delegation on it first (matching the Linux VFS, which breaks the source
 * lease on rename).  Then fall through to the destination recall.
 */
static void
chimera_vfs_rename_at_source_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (error_code == CHIMERA_VFS_OK && attr->va_fh_len) {
        memcpy(request->rename_at.source_fh, attr->va_fh, attr->va_fh_len);
        request->rename_at.source_fh_len = attr->va_fh_len;

        chimera_vfs_io_recall(request,
                              request->rename_at.source_fh,
                              request->rename_at.source_fh_len,
                              chimera_vfs_hash(request->rename_at.source_fh,
                                               request->rename_at.source_fh_len),
                              chimera_vfs_rename_at_recall_target);
    } else {
        /* Source not resolvable (e.g. ENOENT); the backend rename will return
         * the appropriate error.  Just recall the destination and proceed. */
        request->rename_at.source_fh_len = 0;
        chimera_vfs_rename_at_recall_target(request);
    }
} /* chimera_vfs_rename_at_source_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_rename_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    const char                      *name,
    int                              namelen,
    const void                      *new_fh,
    int                              new_fhlen,
    const char                      *new_name,
    int                              new_namelen,
    const uint8_t                   *target_fh,
    int                              target_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, NULL, private_data);
        return;
    }

    request->opcode                                    = CHIMERA_VFS_OP_RENAME_AT;
    request->complete                                  = chimera_vfs_rename_at_complete;
    request->rename_at.name                            = name;
    request->rename_at.namelen                         = namelen;
    request->rename_at.name_hash                       = chimera_vfs_hash(name, namelen);
    request->rename_at.new_fh                          = new_fh;
    request->rename_at.new_fhlen                       = new_fhlen;
    request->rename_at.new_fh_hash                     = chimera_vfs_hash(new_fh, new_fhlen);
    request->rename_at.new_name                        = new_name;
    request->rename_at.new_namelen                     = new_namelen;
    request->rename_at.new_name_hash                   = chimera_vfs_hash(new_name, new_namelen);
    request->rename_at.target_fh                       = target_fh;
    request->rename_at.target_fh_len                   = target_fh_len;
    request->rename_at.r_fromdir_pre_attr.va_req_mask  = pre_attr_mask;
    request->rename_at.r_fromdir_pre_attr.va_set_mask  = 0;
    request->rename_at.r_fromdir_post_attr.va_req_mask = post_attr_mask;
    request->rename_at.r_fromdir_post_attr.va_set_mask = 0;
    request->rename_at.r_todir_pre_attr.va_req_mask    = pre_attr_mask;
    request->rename_at.r_todir_pre_attr.va_set_mask    = 0;
    request->rename_at.r_todir_post_attr.va_req_mask   = post_attr_mask;
    request->rename_at.r_todir_post_attr.va_set_mask   = 0;
    request->proto_callback                            = callback;
    request->proto_private_data                        = private_data;
    request->rename_at.source_fh_len                   = 0;

    /* Recall delegations before the directory change: first on the source file
     * being moved (its ctime/linkage changes invalidate cached state), then on
     * any file overwritten at the destination.  Resolve the source FH only when
     * the lease subsystem is active; otherwise go straight to the destination
     * recall (which fast-paths to dispatch when there is nothing to break). */
    if (thread->vfs->vfs_state) {
        chimera_vfs_lookup(thread, cred, fh, fhlen, name, namelen,
                           CHIMERA_VFS_ATTR_FH, 0,
                           chimera_vfs_rename_at_source_lookup_complete,
                           request);
    } else {
        chimera_vfs_rename_at_recall_target(request);
    }
} /* chimera_vfs_rename_at */