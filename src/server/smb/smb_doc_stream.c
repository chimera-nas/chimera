// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_doc_stream.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_notify.h"

/* Pending metadata owns no open/session/handle pointers. Only the final
 * retiring opener fills execution fields and acquires the state pin. */
struct chimera_smb_stream_delete {
    struct chimera_vfs_cred        cred;
    uint8_t                        base_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                        target_fh[CHIMERA_VFS_FH_SIZE];
    char                           name[CHIMERA_VFS_NAME_MAX];
    uint8_t                        parent_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                        parent_lease_key[16];
    uint64_t                       client_key;
    char                           base_name[SMB_FILENAME_MAX];
    uint32_t                       parent_fh_len, base_name_len;
    uint32_t                       base_fh_len;
    uint32_t                       target_fh_len;
    uint32_t                       name_len;
    struct chimera_vfs_thread     *thread;
    struct chimera_vfs_file_state *file;
    void                           (*done)(
        enum chimera_vfs_error,
        void *);
    void                          *private_data;
};

void
chimera_smb_stream_doc_repath_locked(
    struct chimera_vfs_file_state           *file,
    const struct chimera_vfs_file_state     *base_file,
    const struct chimera_smb_namespace_path *before,
    const struct chimera_smb_namespace_path *after)
{
    struct chimera_smb_stream_delete *pending = file ? file->smb_pending_delete : NULL;

    if (!pending || !base_file || file->smb_delete_started ||
        pending->base_fh_len != base_file->fh_len ||
        memcmp(pending->base_fh, base_file->fh, base_file->fh_len) ||
        pending->parent_fh_len != before->parent_fh_len ||
        pending->base_name_len != before->name_len ||
        memcmp(pending->parent_fh, before->parent_fh, before->parent_fh_len) ||
        memcmp(pending->base_name, before->name, before->name_len)) {
        return;
    }
    /* Identity-based stream removal needs no change. Its deferred namespace
     * event must describe the accepted link, independently of surviving peers. */
    chimera_smb_abort_if(after->parent_fh_len > sizeof(pending->parent_fh) ||
                         after->name_len > sizeof(pending->base_name), "invalid stream delete repath");
    pending->parent_fh_len = after->parent_fh_len;
    pending->base_name_len = after->name_len;
    memcpy(pending->parent_fh, after->parent_fh, after->parent_fh_len);
    memcpy(pending->base_name, after->name, after->name_len);
} /* chimera_smb_stream_doc_repath_locked */

struct stream_doc_peer_scan {
    struct chimera_smb_open_file *open;
    bool                          other;
};

static void
stream_doc_namespace_peer(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    struct stream_doc_peer_scan  *scan = private_data;
    struct chimera_smb_open_file *peer = context;

    (void) snapshot;
    if (peer != scan->open && !peer->doc_stream_close_started) {
        scan->other = true;
    }
} /* stream_doc_namespace_peer */

struct chimera_smb_stream_delete *
chimera_smb_stream_doc_retire(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    struct chimera_vfs_file_state    *file = open_file->share_file_state;
    struct chimera_vfs_file_state    *pin;
    struct chimera_smb_stream_delete *action = NULL;
    bool                              other  = false;

    if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) ||
        !file || !open_file->handle) {
        return NULL;
    }
    pin = chimera_vfs_state_get(file->state, file->fh, file->fh_len,
                                file->fh_hash, false);
    chimera_smb_abort_if(!pin, "stream delete state disappeared");
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    chimera_smb_namespace_lock(registry);
    evpl_mutex_lock(&file->lock);
    if (open_file->doc_stream_close_started) {
        evpl_mutex_unlock(&file->lock);
        chimera_smb_namespace_unlock(registry);
        chimera_vfs_state_put(file->state, pin);
        return NULL;
    }
    if (!file->smb_delete_started &&
        (open_file->doc_from_create ||
         ((open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) &&
          file->delete_pending)) &&
        !file->smb_pending_delete) {
        struct chimera_smb_stream_delete *pending = calloc(1, sizeof(*pending));

        chimera_smb_abort_if(!pending, "stream delete metadata allocation failed");
        pending->cred          = open_file->stream_delete_cred;
        pending->base_fh_len   = open_file->base_fh_len;
        pending->target_fh_len = open_file->handle->fh_len;
        pending->name_len      = open_file->stream_name_len;
        pending->parent_fh_len = open_file->parent_fh_len;
        pending->base_name_len = open_file->name_len;
        chimera_smb_abort_if(pending->name_len > sizeof(pending->name) ||
                             pending->base_fh_len > sizeof(pending->base_fh) ||
                             pending->target_fh_len > sizeof(pending->target_fh) ||
                             pending->parent_fh_len > sizeof(pending->parent_fh) ||
                             pending->base_name_len > sizeof(pending->base_name),
                             "invalid stream delete identity");
        memcpy(pending->base_fh, open_file->base_fh, pending->base_fh_len);
        memcpy(pending->target_fh, open_file->handle->fh, pending->target_fh_len);
        memcpy(pending->name, open_file->stream_name, pending->name_len);
        memcpy(pending->parent_fh, open_file->parent_fh, pending->parent_fh_len);
        memcpy(pending->base_name, open_file->name, pending->base_name_len);
        memcpy(pending->parent_lease_key, open_file->parent_lease_key, 16);
        pending->client_key      = chimera_smb_share_claim(open_file)->owner.client_key;
        file->smb_pending_delete = pending;
        file->delete_pending     = 1;
    }
    open_file->doc_stream_close_started = 1;
    struct stream_doc_peer_scan peers = { .open = open_file };
    chimera_smb_namespace_foreach_locked(registry, file->fh, file->fh_len,
                                         stream_doc_namespace_peer, &peers);
    other = peers.other;

    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if (claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
            claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) {
            struct chimera_smb_open_file *peer = claim->cb_private;

            if (peer && peer != open_file && !peer->doc_stream_close_started) {
                other = true;
                break;
            }
        }
    }
    /* POSIX disposition consumes this stream link at the deleting open's
     * close. Existing handles keep their unlinked backend stream alive. */
    if ((!other || (open_file->doc_posix &&
                    (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE))) &&
        !file->smb_delete_started && file->delete_pending && file->smb_pending_delete) {
        action                   = file->smb_pending_delete;
        file->smb_pending_delete = NULL;
        file->smb_delete_started = 1;
        action->thread           = thread->vfs_thread;
        action->file             = pin;
    }
    evpl_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
    if (!action) {
        chimera_vfs_state_put(file->state, pin);
    }
    return action;
} /* chimera_smb_stream_doc_retire */

static void
stream_doc_finish(
    struct chimera_smb_stream_delete *action,
    enum chimera_vfs_error            status)
{
    void  (*done)(
        enum chimera_vfs_error,
        void *) = action->done;
    void *private_data = action->private_data;

    if (status != CHIMERA_VFS_OK && status != CHIMERA_VFS_ENOENT) {
        chimera_smb_debug("stream delete-on-close failed for '%.*s': %d",
                          action->name_len, action->name, status);
    }
    chimera_vfs_state_clear_delete_pending(action->file);
    if (status != CHIMERA_VFS_OK && status != CHIMERA_VFS_ENOENT &&
        status != CHIMERA_VFS_ESTALE) {
        evpl_mutex_lock(&action->file->lock);
        action->file->smb_delete_started = 0;
        evpl_mutex_unlock(&action->file->lock);
    }
    chimera_vfs_state_put(action->file->state, action->file);
    free(action);
    done(status, private_data);
} /* stream_doc_finish */

static void
stream_doc_compound_done(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    enum chimera_vfs_error            status = chimera_vfs_compound_status(compound);
    struct chimera_smb_stream_delete *action = private_data;

    /* STREAM_NAME describes a fork change on the existing base entry; do not
     * wake FILE_NAME watchers by pretending the base entry was removed. */
    if (status == CHIMERA_VFS_OK && action->parent_fh_len && action->base_name_len) {
        struct chimera_claim_actor actor = { .owner     = {
                                                 .proto = CHIMERA_CLAIM_PROTO_SMB2, .client_key = action->client_key,
                                             } };
        memcpy(actor.owner.key, action->parent_lease_key, 16);
        chimera_vfs_notify_emit_actor(action->thread->vfs->vfs_notify,
                                      action->parent_fh, action->parent_fh_len, CHIMERA_VFS_NOTIFY_STREAM_NAME,
                                      action->base_name, action->base_name_len, NULL, 0,
                                      chimera_claim_owner_has_key(&actor.owner) ? &actor : NULL);
    }
    chimera_vfs_compound_free(compound);
    stream_doc_finish(private_data, status);
} /* stream_doc_compound_done */

void
chimera_smb_stream_doc_run(
    struct chimera_smb_stream_delete *action,
    void ( *done )(enum chimera_vfs_error, void *),
    void *private_data)
{
    if (!action) {
        done(CHIMERA_VFS_OK, private_data);
        return;
    }
    action->done         = done;
    action->private_data = private_data;
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(action->thread, &action->cred);
    chimera_vfs_compound_add_putfh(compound, action->base_fh, action->base_fh_len);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_remove_stream_checked(compound, action->name, action->name_len,
                                                   action->target_fh, action->target_fh_len);
    chimera_vfs_compound_submit(compound, stream_doc_compound_done, action);
} /* chimera_smb_stream_doc_run */
