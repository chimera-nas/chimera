// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_namespace.h"

/* Membership is weak with respect to open->refcnt. Registry locking and the
 * mandatory detach-before-recycle rule keep its context alive. This avoids a
 * permanent participant/open reference cycle during durable and tree teardown.
 * No production namespace edit dispatch is enabled in this initial integration. */

static void
smb_namespace_open_publish(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot)
{
    struct chimera_smb_open_file            *open = context;
    const struct chimera_smb_namespace_path *path = &snapshot->path;
    struct chimera_vfs_file_state           *file = open->share_file_state;

    if (file) {
        evpl_mutex_lock(&file->lock);
    }
    open->parent_fh_len = path->parent_fh_len;
    memcpy(open->parent_fh, path->parent_fh, path->parent_fh_len);
    open->name_len = path->name_len;
    memcpy(open->name, path->name, path->name_len);
    open->full_path_len = path->full_path_len;
    memcpy(open->full_path, path->full_path, path->full_path_len);
    open->full_path[path->full_path_len] = 0;
    if (file) {
        evpl_mutex_unlock(&file->lock);
    }
} /* smb_namespace_open_publish */

bool
chimera_smb_open_namespace_prepare(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open,
    bool                              base)
{
    (void) thread;
    if (!open->namespace_participant) {
        open->namespace_participant = chimera_smb_namespace_participant_reserve(
            open, NULL, NULL, smb_namespace_open_publish);
        if (!open->namespace_participant) {
            return false;
        }
    }
    if (base && !open->base_namespace_participant) {
        open->base_namespace_participant = chimera_smb_namespace_participant_reserve(
            open, NULL, NULL, smb_namespace_open_publish);
        if (!open->base_namespace_participant) {
            return false;
        }
    }
    return true;
} /* chimera_smb_open_namespace_prepare */

bool
chimera_smb_open_namespace_bind(
    struct chimera_smb_open_file   *open,
    struct chimera_vfs_open_handle *handle,
    const uint8_t                  *root_fh,
    uint32_t                        root_fh_len)
{
    struct chimera_smb_namespace_snapshot snapshot = { 0 };

    if (!handle || !root_fh_len || root_fh_len > CHIMERA_VFS_FH_SIZE ||
        open->parent_fh_len > CHIMERA_VFS_FH_SIZE || open->name_len > SMB_FILENAME_MAX ||
        open->full_path_len >= SMB_PATH_MAX) {
        return false;
    }
    snapshot.path.view_fh_len = root_fh_len;
    memcpy(snapshot.path.view_fh, root_fh, root_fh_len);
    snapshot.path.parent_fh_len = open->parent_fh_len;
    memcpy(snapshot.path.parent_fh, open->parent_fh, open->parent_fh_len);
    snapshot.path.name_len = open->name_len;
    memcpy(snapshot.path.name, open->name, open->name_len);
    snapshot.path.full_path_len = open->full_path_len;
    memcpy(snapshot.path.full_path, open->full_path, open->full_path_len);
    snapshot.delete_on_close = !!(open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE);
    snapshot.doc_posix       = open->doc_posix;
    snapshot.doc_cred        = open->stream_delete_cred;
    if (!open->namespace_registered && !chimera_smb_namespace_attached(open->namespace_participant) &&
        !chimera_smb_namespace_participant_bind(
            open->namespace_participant, handle->fh, handle->fh_len, &snapshot)) {
        return false;
    }
    if (open->base_namespace_participant && !open->base_namespace_registered && open->base_fh_len &&
        !chimera_smb_namespace_attached(open->base_namespace_participant) &&
        !chimera_smb_namespace_participant_bind(open->base_namespace_participant,
                                                open->base_fh, open->base_fh_len, &snapshot)) {
        return false;
    }
    return true;
} /* chimera_smb_open_namespace_bind */

void
chimera_smb_open_namespace_attach(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open)
{
    /* A private named-stream producer tracks renames through its base token.
     * Promote both identities under one registry lock: a previously admitted
     * rename may still publish while the CREATE is accepting its result. */
    struct chimera_smb_namespace_snapshot pending;

    if (open->base_namespace_participant && !open->base_namespace_registered &&
        chimera_smb_namespace_pending_snapshot(open->base_namespace_participant, &pending)) {
        chimera_smb_abort_if(!chimera_smb_namespace_promote_stream_pair(
                                 &thread->shared->namespace_registry, open->base_namespace_participant,
                                 open->namespace_participant, open->handle->fh, open->handle->fh_len, NULL),
                             "stream namespace pair publication");
        open->base_namespace_registered = open->namespace_registered = true;
    }
    if (open->namespace_participant && !open->namespace_registered) {
        /* Dispatch remains disabled until CREATE owns namespace admission.
         * Therefore this attach is allocation-free and cannot meet an edit. */
        chimera_smb_abort_if(!chimera_smb_namespace_attach(&thread->shared->namespace_registry,
                                                           open->namespace_participant, NULL),
                             "namespace publication without admission");
        open->namespace_registered = true;
    }
    if (open->base_namespace_participant && !open->base_namespace_registered && open->base_fh_len) {
        chimera_smb_abort_if(!chimera_smb_namespace_attach(&thread->shared->namespace_registry,
                                                           open->base_namespace_participant, NULL),
                             "base namespace publication without admission");
        open->base_namespace_registered = true;
    }
} /* chimera_smb_open_namespace_attach */

SYMBOL_EXPORT void
chimera_smb_open_namespace_detach(struct chimera_smb_open_file *open)
{
    if (open->namespace_participant) {
        if (open->namespace_registered || chimera_smb_namespace_attached(open->namespace_participant)) {
            chimera_smb_abort_if(!chimera_smb_namespace_detach(open->namespace_participant, NULL),
                                 "namespace recycle before edit drain");
        } else {
            chimera_smb_namespace_participant_free(open->namespace_participant);
        }
        open->namespace_participant = NULL;
        open->namespace_registered  = false;
    }
    if (open->base_namespace_participant) {
        if (open->base_namespace_registered || chimera_smb_namespace_attached(open->base_namespace_participant)) {
            chimera_smb_abort_if(!chimera_smb_namespace_detach(open->base_namespace_participant, NULL),
                                 "base namespace recycle before edit drain");
        } else {
            chimera_smb_namespace_participant_free(open->base_namespace_participant);
        }
        open->base_namespace_participant = NULL;
        open->base_namespace_registered  = false;
    }
} /* chimera_smb_open_namespace_detach */

static void
smb_namespace_open_final_retired(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open,
    void                             *private_data)
{
    (void) private_data;
    if (open->durable_flags || open->resilient) {
        chimera_smb_durable_forget(thread->shared, open->file_id.pid);
    }
    chimera_smb_open_file_drain_locks(thread, open);
    atomic_store(&open->refcnt, 0);
    chimera_smb_open_file_free(thread, open);
} /* smb_namespace_open_final_retired */

SYMBOL_EXPORT void
chimera_smb_open_file_finalize(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open)
{
    /* Transfer the final reference into asynchronous retirement. No registry
     * lookup can resurrect this logically closed/unhashed object. */
    atomic_store(&open->refcnt, 1);
    chimera_smb_open_file_retire_async(thread, open, smb_namespace_open_final_retired, NULL);
} /* chimera_smb_open_file_finalize */

static void
smb_namespace_open_doc_peer(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    const struct chimera_smb_open_file *open    = context;
    bool                               *has_doc = private_data;

    (void) snapshot;
    if ((open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) || open->doc_from_create) {
        *has_doc = true;
    }
} /* smb_namespace_open_doc_peer */

bool
chimera_smb_open_namespace_has_doc_locked(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len)
{
    bool has_doc = false;

    chimera_smb_namespace_foreach_locked(registry, fh, fh_len,
                                         smb_namespace_open_doc_peer, &has_doc);
    return has_doc;
} /* chimera_smb_open_namespace_has_doc_locked */

uint32_t
chimera_smb_namespace_legacy_begin(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open)
{
    struct chimera_vfs_file_state *file = open->share_file_state;

    if (!file) {
        return SMB2_STATUS_FILE_CLOSED;
    }
    struct chimera_smb_doc_fence  *fence = calloc(1, sizeof(*fence));
    if (!fence) {
        return SMB2_STATUS_INSUFFICIENT_RESOURCES;
    }
    if (!chimera_smb_doc_fence_acquire(fence,
                                       &request->compound->thread->shared->namespace_registry,
                                       file->fh, file->fh_len, request)) {
        free(fence);
        return SMB2_STATUS_FILE_NOT_AVAILABLE;
    }
    chimera_smb_abort_if(request->namespace_fence, "legacy namespace fence already acquired");
    request->namespace_fence = fence;
    return SMB2_STATUS_SUCCESS;
} /* chimera_smb_namespace_legacy_begin */

void
chimera_smb_namespace_legacy_end(struct chimera_smb_request *request)
{
    struct chimera_smb_doc_fence *fence = request->namespace_fence;

    if (!fence) {
        return;
    }
    request->namespace_fence = NULL;
    chimera_smb_doc_fence_release(fence);
    free(fence);
} /* chimera_smb_namespace_legacy_end */

bool
chimera_smb_open_namespace_pending_begin(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open,
    const uint8_t                    *root_fh,
    uint32_t                          root_fh_len)
{
    if (!open->namespace_participant || !root_fh_len || root_fh_len > CHIMERA_VFS_FH_SIZE ||
        open->parent_fh_len > CHIMERA_VFS_FH_SIZE || open->name_len > SMB_FILENAME_MAX ||
        open->full_path_len >= SMB_PATH_MAX) {
        return false;
    }
    struct chimera_smb_namespace_snapshot initial = { 0 };
    initial.path.view_fh_len = root_fh_len;
    memcpy(initial.path.view_fh, root_fh, root_fh_len);
    initial.path.parent_fh_len = open->parent_fh_len;
    memcpy(initial.path.parent_fh, open->parent_fh, open->parent_fh_len);
    initial.path.name_len = open->name_len;
    memcpy(initial.path.name, open->name, open->name_len);
    initial.path.full_path_len = open->full_path_len;
    memcpy(initial.path.full_path, open->full_path, open->full_path_len);
    initial.doc_cred        = open->stream_delete_cred;
    initial.delete_on_close = !!(open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) || open->doc_from_create;
    initial.doc_posix       = open->doc_posix;
    return chimera_smb_namespace_pending_begin(&thread->shared->namespace_registry,
                                               open->base_namespace_participant ? open->base_namespace_participant :
                                               open->namespace_participant, &initial);
} /* chimera_smb_open_namespace_pending_begin */

bool
chimera_smb_open_namespace_pending_bind(
    struct chimera_smb_open_file   *open,
    struct chimera_vfs_open_handle *handle)
{
    return handle && chimera_smb_namespace_pending_bind(
        open->base_namespace_participant ? open->base_namespace_participant : open->namespace_participant,
        handle->fh, handle->fh_len);
} /* chimera_smb_open_namespace_pending_bind */

bool
chimera_smb_open_namespace_pending_path(
    struct chimera_smb_open_file      *open,
    struct chimera_smb_namespace_path *path)
{
    if (open->namespace_registered) {
        return false;
    }
    struct chimera_smb_namespace_snapshot snapshot;
    if (!chimera_smb_namespace_pending_snapshot(
            open->base_namespace_participant ? open->base_namespace_participant : open->namespace_participant,
            &snapshot)) {
        return false;
    }
    *path = snapshot.path;
    return true;
} /* chimera_smb_open_namespace_pending_path */
