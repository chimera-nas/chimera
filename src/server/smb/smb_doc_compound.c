// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_compound.h"
#include "smb_doc_compound.h"
#include "smb_doc_stream.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim_access.h"

struct smb_doc_file {
    struct chimera_vfs_file_state        *file;
    struct chimera_vfs_state             *vfs_state;
    struct smb_vfs_open_state            *producer;
    bool                                  dynamic;
    struct chimera_smb_doc_fence          fence;
    struct chimera_vfs_claim_access_fence admission_fence;
    struct chimera_vfs_doc_info           pending;
    bool                                  seeded, delete_pending, have_pending;
};

struct smb_doc_path {
    struct chimera_smb_namespace_path       before, after;
    struct chimera_smb_namespace_view_fence view_fence;
    bool                                    staged;
};

struct smb_doc_event {
    struct smb_vfs_command      *command;
    struct smb_doc_file         *file, *initial_file;
    struct smb_doc_path         *path;
    /* Allocated before execution, transferred only by accepted CLOSE. */
    struct chimera_vfs_doc_info *publication;
    struct chimera_vfs_doc_info  intent;
    struct chimera_vfs_compound *compound;
    uint64_t                     token;
    uint32_t                     close_status;
    uint8_t                      parent_lease_key[16];
    bool                         setter, staged, committed, remove, removed;
    bool                         after_delete_pending, after_have_pending;
};

struct smb_doc_batch {
    unsigned int          capacity, num_files, num_events;
    struct smb_doc_file  *files;
    struct smb_doc_event *events;
    struct evpl_timer     preflight_timer;
    struct evpl          *evpl;
    void                  (*preflight_done)(
        void *);
    void                 *preflight_private;
};

struct smb_doc_batch *
smb_doc_batch_alloc(unsigned int capacity)
{
    struct smb_doc_batch *batch = calloc(1, sizeof(*batch));

    if (!batch) {
        return NULL;
    }
    batch->capacity = capacity;
    batch->files    = calloc(capacity, sizeof(*batch->files));
    batch->events   = calloc(capacity, sizeof(*batch->events));
    if (!batch->files || !batch->events) {
        smb_doc_batch_free(batch); return NULL;
    }
    return batch;
} /* smb_doc_batch_alloc */

/* Dynamic namespace admission happens after a producer's cache recalls.
 * Drop only those attempt-owned coordination resources before replay can wait
 * on recalls again. This is cleanup, never publication of tentative paths. */
static void
smb_doc_dynamic_cleanup(struct smb_doc_batch *batch)
{
    for (unsigned int i = 0; i < batch->num_events; i++) {
        if (batch->events[i].path) {
            chimera_smb_namespace_view_fence_release(&batch->events[i].path->view_fence);
        }
    }
    for (unsigned int i = 0; i < batch->num_files; i++) {
        if (batch->files[i].dynamic) {
            chimera_smb_doc_fence_release(&batch->files[i].fence);
        }
    }
    for (unsigned int i = 0; i < batch->num_files; i++) {
        struct smb_doc_file *slot = &batch->files[i];
        if (!slot->dynamic) {
            continue;
        }
        chimera_vfs_claim_access_fence_release(&slot->admission_fence);
        if (slot->file) {
            chimera_vfs_state_put(slot->vfs_state, slot->file);
        }
        slot->file      = NULL;
        slot->vfs_state = NULL;
    }
} /* smb_doc_dynamic_cleanup */

void
smb_doc_batch_reset(struct smb_doc_batch *batch)
{
    if (!batch) {
        return;
    }
    smb_doc_dynamic_cleanup(batch);
    for (unsigned int i = 0; i < batch->num_files; i++) {
        batch->files[i].seeded = false;
    }
    for (unsigned int i = 0; i < batch->num_events; i++) {
        struct smb_doc_event *event = &batch->events[i];
        event->file = event->initial_file;
        if (event->path) {
            event->path->staged = false;
        }
        event->staged               = event->committed = event->remove = event->removed = false;
        event->close_status         = SMB2_STATUS_SUCCESS;
        event->after_delete_pending = event->after_have_pending = false;
        memset(&event->intent, 0, sizeof(event->intent));
    }
} /* smb_doc_batch_reset */

/* A waiting batch owns no namespace or ACCESS fences. In particular, never
 * wait for file B while holding file A: the other close may name both files in
 * the opposite wire order. Registry and ACCESS locks are never nested. */
static void
smb_doc_batch_unfence(struct smb_doc_batch *batch)
{
    for (unsigned int i = 0; i < batch->num_files; i++) {
        chimera_smb_doc_fence_release(&batch->files[i].fence);
    }
    for (unsigned int i = 0; i < batch->num_files; i++) {
        chimera_vfs_claim_access_fence_release(&batch->files[i].admission_fence);
    }
} /* smb_doc_batch_unfence */

static void
smb_doc_preflight_tick(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct smb_doc_batch        *batch = (struct smb_doc_batch *)
        ((char *) timer - offsetof(struct smb_doc_batch, preflight_timer));
    struct smb_vfs_command      *command = batch->events[0].command;
    struct chimera_smb_request  *request = command->request;
    struct chimera_smb_compound *wire    = request->compound;
    bool                         invalid = wire->conn->generation != wire->conn_generation ||
        wire->conn->disconnecting || request->tree->compound_tearing_down ||
        (request->session_handle->session->flags & CHIMERA_SMB_SESSION_DELETED);

    if (!invalid) {
        for (unsigned int i = 0; i < batch->num_files; i++) {
            struct smb_doc_file *slot = &batch->files[i];
            if (slot->dynamic) {
                continue;
            }
            if (!chimera_smb_doc_fence_acquire(&slot->fence,
                                               &wire->thread->shared->namespace_registry,
                                               slot->file->fh, slot->file->fh_len, command->batch) ||
                !chimera_vfs_claim_access_fence_acquire(&slot->admission_fence,
                                                        slot->file, batch)) {
                smb_doc_batch_unfence(batch);
                evpl_add_oneshot_timer(evpl, timer, smb_doc_preflight_tick, 1000);
                return;
            }
        }
    } else {
        smb_doc_batch_unfence(batch);
    }
    void  (*done)(
        void *) = batch->preflight_done;
    void *private_data = batch->preflight_private;
    batch->preflight_done    = NULL;
    batch->preflight_private = NULL;
    /* May synchronously finish/free the batch. Invalidated requests are sent
     * through the ordinary group preambles, which reject before any effect. */
    done(private_data);
} /* smb_doc_preflight_tick */

void
smb_doc_batch_preflight(
    struct smb_doc_batch *batch,
    void (               *done )(void *),
    void                 *private_data)
{
    if (!batch || !batch->num_events) {
        done(private_data);
        return;
    }
    batch->evpl              = batch->events[0].command->request->compound->thread->evpl;
    batch->preflight_done    = done;
    batch->preflight_private = private_data;
    smb_doc_preflight_tick(batch->evpl, &batch->preflight_timer);
} /* smb_doc_batch_preflight */

void
smb_doc_batch_free(struct smb_doc_batch *batch)
{
    if (!batch) {
        return;
    }
    if (batch->evpl) {
        evpl_remove_timer(batch->evpl, &batch->preflight_timer);
    }
    for (unsigned int i = 0; i < batch->num_events; i++) {
        if (batch->events[i].path) {
            chimera_smb_namespace_view_fence_release(&batch->events[i].path->view_fence);
        }
    }
    smb_doc_dynamic_cleanup(batch);
    smb_doc_batch_unfence(batch);
    for (unsigned int i = 0; i < batch->num_events; i++) {
        free(batch->events[i].publication);
        if (batch->events[i].path) {
            free(batch->events[i].path);
        }
    }
    free(batch->files);
    free(batch->events);
    free(batch);
} /* smb_doc_batch_free */

static struct smb_doc_event *
smb_doc_event(
    struct smb_vfs_command *command,
    bool                    allocate)
{
    struct smb_doc_batch *batch = (allocate ? chimera_smb_compound_doc_batch_ensure(command) :
                                   chimera_smb_compound_doc_batch(command));

    if (!batch) {
        return NULL;
    }
    for (unsigned int i = 0; i < batch->num_events; i++) {
        if (batch->events[i].command == command) {
            return &batch->events[i];
        }
    }
    if (!allocate || batch->num_events == batch->capacity) {
        return NULL;
    }
    bool                           dynamic = command->state && command->state->producer;
    struct chimera_vfs_file_state *file    = dynamic ? NULL : command->open->share_file_state;
    struct smb_doc_file           *slot    = NULL;
    for (unsigned int i = 0; i < batch->num_files; i++) {
        if ((dynamic && batch->files[i].producer == command->state) ||
            (!dynamic && !batch->files[i].dynamic && batch->files[i].file == file)) {
            slot = &batch->files[i]; break;
        }
    }
    if (!slot) {
        if (batch->num_files == batch->capacity) {
            return NULL;
        }
        slot           = &batch->files[batch->num_files++];
        slot->file     = file;
        slot->dynamic  = dynamic;
        slot->producer = dynamic ? command->state : NULL;
    }
    struct smb_doc_event *event = &batch->events[batch->num_events++];
    event->command     = command;
    event->file        = event->initial_file = slot;
    event->setter      = command->request->smb2_hdr.command == SMB2_SET_INFO;
    event->publication = calloc(1, sizeof(*event->publication));
    return event->publication ? event : NULL;
} /* smb_doc_event */

static bool
smb_doc_path_match(
    const struct chimera_smb_namespace_path *path,
    const struct chimera_smb_namespace_path *before)
{
    return path->parent_fh_len == before->parent_fh_len &&
           path->name_len == before->name_len &&
           !memcmp(path->parent_fh, before->parent_fh, path->parent_fh_len) &&
           !memcmp(path->name, before->name, path->name_len);
} /* smb_doc_path_match */

static bool
smb_doc_path_replace(
    struct chimera_smb_namespace_path *path,
    const char                        *name,
    uint32_t                           name_len)
{
    /* Same-parent renames preserve each share's independent path prefix. */
    if (path->full_path_len < path->name_len ||
        memcmp(path->full_path + path->full_path_len - path->name_len,
               path->name, path->name_len)) {
        return false;
    }
    uint32_t prefix = path->full_path_len - path->name_len;
    if (prefix + name_len >= SMB_PATH_MAX) {
        return false;
    }
    memcpy(path->full_path + prefix, name, name_len);
    path->full_path_len                  = prefix + name_len;
    path->full_path[path->full_path_len] = 0;
    path->name_len                       = name_len;
    memcpy(path->name, name, name_len);
    path->name[name_len] = 0;
    return true;
} /* smb_doc_path_replace */

static bool
smb_doc_path_apply(
    struct chimera_smb_namespace_path *path,
    const struct smb_doc_path         *record)
{
    const struct chimera_smb_namespace_path *after       = &record->after;
    bool                                     same_parent = path->parent_fh_len == after->parent_fh_len &&
        !memcmp(path->parent_fh, after->parent_fh, path->parent_fh_len);

    if (same_parent) {
        return smb_doc_path_replace(path, after->name, after->name_len);
    }
    if (path->view_fh_len != after->view_fh_len ||
        memcmp(path->view_fh, after->view_fh, path->view_fh_len)) {
        return false;
    }
    *path = *after;
    return true;
} /* smb_doc_path_apply */

static void
smb_doc_open_path(
    struct smb_vfs_command            *command,
    struct chimera_smb_open_file      *open,
    struct chimera_smb_namespace_path *path)
{
    struct smb_doc_batch *batch = chimera_smb_compound_doc_batch(command);

    memset(path, 0, sizeof(*path));
    path->parent_fh_len = open->parent_fh_len;
    memcpy(path->parent_fh, open->parent_fh, path->parent_fh_len);
    path->name_len = open->name_len;
    memcpy(path->name, open->name, path->name_len);
    path->full_path_len = open->full_path_len;
    memcpy(path->full_path, open->full_path, path->full_path_len);
    path->view_fh_len = open->tree->fh_len;
    memcpy(path->view_fh, open->tree->fh, path->view_fh_len);
    /* Only the owning producer reads an invisible token; a foreign accepted
     * rename never writes this private open's fields. */
    if (open == command->open && command->state && command->state->producer) {
        chimera_smb_open_namespace_pending_path(open, path);
    }
    if (!batch) {
        return;
    }
    for (unsigned int i = 0; i < batch->num_events; i++) {
        struct smb_doc_event *event = &batch->events[i];
        if (event->command->group >= command->group) {
            break;
        }
        if (event->path && event->path->staged && event->file->file &&
            (event->file->file == open->share_file_state ||
             ((open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) &&
              (event->file->file == open->base_share_file_state ||
               (open->base_fh_len && event->file->file->fh_len == open->base_fh_len &&
                !memcmp(event->file->file->fh, open->base_fh, open->base_fh_len)))) ||
             (open == command->open && command->handle &&
              event->file->file->fh_len == command->handle->fh_len &&
              !memcmp(event->file->file->fh, command->handle->fh, command->handle->fh_len))) &&
            smb_doc_path_match(path, &event->path->before)) {
            bool ok = smb_doc_path_apply(path, event->path);
            chimera_smb_abort_if(!ok, "reserved rename path overflow");
        }
    }
} /* smb_doc_open_path */

void
smb_doc_command_path(
    struct smb_vfs_command            *command,
    struct chimera_smb_namespace_path *path)
{
    smb_doc_open_path(command, command->open, path);
} /* smb_doc_command_path */

bool
smb_doc_path_reserve(struct smb_vfs_command *command)
{
    struct smb_doc_event *event = smb_doc_event(command, true);

    if (!event) {
        return false;
    }
    event->setter = false;
    if (!event->path) {
        event->path = calloc(1, sizeof(*event->path));
    }
    return event->path != NULL;
} /* smb_doc_path_reserve */

struct smb_doc_path_check {
    const struct smb_doc_path *path;
    struct smb_vfs_command    *command;
    struct chimera_vfs_file_state *locked_file;
    bool                       valid;
};
static void
smb_doc_path_check_peer(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    struct chimera_smb_open_file     *open  = context;
    struct smb_doc_path_check        *check = private_data;
    struct chimera_smb_namespace_path path;

    (void) snapshot;
    /* A base rename visits stream peers through their base participant. The
     * registry pins that participant, while the stream file lock protects its
     * public path fields. Namespace writers acquire registry before file locks. */
    struct chimera_vfs_file_state *peer_file = open->share_file_state;
    bool lock_peer = peer_file && peer_file != check->locked_file;
    if (lock_peer) { pthread_mutex_lock(&peer_file->lock); }
    smb_doc_open_path(check->command, open, &path);
    if (smb_doc_path_match(&path, &check->path->before) &&
        !smb_doc_path_apply(&path, check->path)) {
        check->valid = false;
    }
    if (lock_peer) { pthread_mutex_unlock(&peer_file->lock); }
} /* smb_doc_path_check_peer */

bool
smb_doc_path_prepare_move(
    struct smb_vfs_command                  *command,
    const struct chimera_smb_namespace_path *after)
{
    struct smb_doc_event             *event = smb_doc_event(command, false);

    if (!event || !event->path || !after->name_len || after->name_len > SMB_FILENAME_MAX ||
        after->full_path_len >= SMB_PATH_MAX) {
        return false;
    }
    struct smb_doc_path              *record = event->path;
    smb_doc_command_path(command, &record->before);
    record->after = *after;
    struct chimera_smb_namespace_path next = record->before;
    if (!smb_doc_path_apply(&next, record)) {
        return false;
    }
    bool                              same_parent = record->before.parent_fh_len == after->parent_fh_len &&
        !memcmp(record->before.parent_fh, after->parent_fh, after->parent_fh_len);
    /* Called only from explicit COORD. Prevent a late foreign-view producer
     * from entering after validation but before accepted publication. */
    if (!same_parent) {
        struct smb_doc_batch             *batch   = chimera_smb_compound_doc_batch(command);
        struct chimera_smb_namespace_path visible = record->before;
        /* Pending tokens still carry the public pre-batch spelling. Reserve
         * every earlier link in this private A->B->C chain before checking the
         * current link; otherwise a foreign-view token at A escapes B's gate. */
        for (unsigned int i = batch->num_events; i > 0; i--) {
            struct smb_doc_event *prior = &batch->events[i - 1];
            if (prior->command->group >= command->group || prior->file != event->file ||
                !prior->path || !prior->path->staged || !smb_doc_path_match(&visible, &prior->path->after)) {
                continue;
            }
            if (!chimera_smb_namespace_view_fence_acquire(&prior->path->view_fence,
                                                          event->file->fence.registry, event->file->file->fh, event->
                                                          file->file->fh_len,
                                                          &prior->path->before)) {
                return false;
            }
            visible = prior->path->before;
        }
        if (!chimera_smb_namespace_view_fence_acquire(&record->view_fence,
                                                      event->file->fence.registry, event->file->file->fh, event->file->
                                                      file->fh_len,
                                                      &record->before)) {
            return false;
        }
    }
    struct smb_doc_path_check              check    = { .path = record, .command = command,
        .locked_file = event->file->file, .valid = true };
    struct chimera_smb_namespace_registry *registry = event->file->fence.registry;
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&event->file->file->lock);
    chimera_smb_namespace_foreach_locked(registry, event->file->file->fh,
                                         event->file->file->fh_len, smb_doc_path_check_peer, &check);
    /* Legacy admission binds its open before inserting ACCESS, but namespace
     * attachment follows later. Include those already-admitted path anchors. */
    for (struct chimera_vfs_claim *claim = event->file->file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if ((claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
             claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) && claim->cb_private) {
            struct chimera_smb_namespace_snapshot snapshot = { 0 };
            smb_doc_path_check_peer(claim->cb_private, &snapshot, &check);
        }
    }
    if (same_parent) {
        check.valid &= chimera_smb_namespace_pending_rename_check_locked(registry,
                                                                         event->file->file->fh, event->file->file->
                                                                         fh_len, &
                                                                         record->before, after->name_len);
    }
    pthread_mutex_unlock(&event->file->file->lock);
    chimera_smb_namespace_unlock(registry);
    return check.valid;
} /* smb_doc_path_prepare_move */

void
smb_doc_path_complete(
    struct smb_vfs_command *command,
    bool                    success)
{
    struct smb_doc_event *event = smb_doc_event(command, false);

    if (event && event->path) {
        event->path->staged = success;
    }
} /* smb_doc_path_complete */

struct smb_doc_path_publish {
    struct smb_doc_event      *event;
    struct chimera_vfs_thread *thread;
};
static void
smb_doc_path_publish_peer(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    struct chimera_smb_open_file     *open    = context;
    struct smb_doc_path_publish      *publish = private_data;
    const struct smb_doc_path        *record  = publish->event->path;
    struct chimera_smb_namespace_path path    = snapshot->path;

    struct chimera_vfs_file_state *peer_file = open->share_file_state;
    bool lock_peer = peer_file && peer_file != publish->event->file->file;
    if (lock_peer) { pthread_mutex_lock(&peer_file->lock); }
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) {
        chimera_smb_stream_doc_repath_locked(peer_file, publish->event->file->file,
            &record->before, &record->after);
    }
    path.parent_fh_len = open->parent_fh_len;
    memcpy(path.parent_fh, open->parent_fh, path.parent_fh_len);
    path.name_len = open->name_len;
    memcpy(path.name, open->name, path.name_len);
    path.full_path_len = open->full_path_len;
    memcpy(path.full_path, open->full_path, path.full_path_len);
    if (!smb_doc_path_match(&path, &record->before)) {
        if (lock_peer) { pthread_mutex_unlock(&peer_file->lock); }
        return;
    }
    bool ok = smb_doc_path_apply(&path, record);
    chimera_smb_abort_if(!ok, "accepted rename path was not reserved");
    open->parent_fh_len = path.parent_fh_len;
    memcpy(open->parent_fh, path.parent_fh, path.parent_fh_len);
    open->name_len = path.name_len;
    memcpy(open->name, path.name, path.name_len);
    open->full_path_len = path.full_path_len;
    memcpy(open->full_path, path.full_path, path.full_path_len + 1);
    chimera_smb_namespace_set_path_locked(open->namespace_participant, &path);
    chimera_smb_namespace_set_path_locked(open->base_namespace_participant, &path);
    struct chimera_vfs_open_handle *handle = open->handle;
    struct vfs_open_cache          *cache  = handle ? chimera_vfs_get_cache_for_handle(publish->thread, handle) : NULL;
    if (cache) {
        struct vfs_open_cache_shard *shard = &cache->shards[handle->fh_hash & cache->shard_mask];
        pthread_mutex_lock(&shard->lock);
        if (handle->doc_delete_on_close && handle->doc_parent_fh_len == record->before.parent_fh_len &&
            handle->doc_name_len == record->before.name_len &&
            !memcmp(handle->doc_parent_fh, record->before.parent_fh, handle->doc_parent_fh_len) &&
            !memcmp(handle->doc_name, record->before.name, handle->doc_name_len)) {
            handle->doc_parent_fh_len = record->after.parent_fh_len;
            memcpy(handle->doc_parent_fh, record->after.parent_fh, record->after.parent_fh_len);
            handle->doc_name_len = record->after.name_len;
            memcpy(handle->doc_name, record->after.name, record->after.name_len);
        }
        pthread_mutex_unlock(&shard->lock);
    }
    if (lock_peer) { pthread_mutex_unlock(&peer_file->lock); }
} /* smb_doc_path_publish_peer */

void
smb_doc_path_publish(struct smb_vfs_command *command)
{
    struct smb_doc_event                  *event = smb_doc_event(command, false);

    if (!event || !event->path || !event->path->staged) {
        return;
    }
    struct chimera_vfs_file_state         *file     = event->file->file;
    struct chimera_server_smb_thread      *thread   = command->request->compound->thread;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    struct smb_doc_path_publish            publish  = { event, thread->vfs_thread };
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&file->lock);
    chimera_smb_namespace_foreach_locked(registry, file->fh, file->fh_len,
                                         smb_doc_path_publish_peer, &publish);
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if ((claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
             claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) && claim->cb_private) {
            struct chimera_smb_open_file         *peer     = claim->cb_private;
            struct chimera_smb_namespace_snapshot snapshot = { 0 };
            snapshot.path.view_fh_len = peer->tree->fh_len;
            memcpy(snapshot.path.view_fh, peer->tree->fh, peer->tree->fh_len);
            smb_doc_path_publish_peer(peer, &snapshot, &publish);
        }
    }
    chimera_smb_namespace_pending_move_locked(registry, file->fh, file->fh_len,
                                              &event->path->before, &event->path->after);
    pthread_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
} /* smb_doc_path_publish */

static void
smb_doc_seed(struct smb_doc_event *event)
{
    struct smb_doc_file                   *slot = event->file;

    if (slot->seeded) {
        return;
    }
    struct chimera_smb_namespace_registry *registry = slot->fence.registry;
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&slot->file->lock);
    slot->delete_pending = slot->file->delete_pending;
    slot->have_pending   = slot->file->smb_pending_delete != NULL;
    if (slot->have_pending) {
        slot->pending = *(struct chimera_vfs_doc_info *) slot->file->smb_pending_delete;
    } else {
        memset(&slot->pending, 0, sizeof(slot->pending));
    }
    slot->seeded = true;
    pthread_mutex_unlock(&slot->file->lock);
    chimera_smb_namespace_unlock(registry);
} /* smb_doc_seed */

int
smb_doc_query_pending(
    struct smb_vfs_command *command,
    int                     public_value)
{
    struct smb_doc_batch *batch = chimera_smb_compound_doc_batch(command);

    if (!batch || !command->open) {
        return public_value;
    }
    for (unsigned int i = 0; i < batch->num_files; i++) {
        if (batch->files[i].file == command->open->share_file_state && batch->files[i].seeded) {
            return batch->files[i].delete_pending;
        }
    }
    return public_value;
} /* smb_doc_query_pending */

bool
smb_doc_close_allowed(struct smb_vfs_command *command)
{
    struct chimera_smb_open_file   *open   = command->open;
    struct chimera_vfs_open_handle *handle = command->handle;
    struct chimera_vfs_file_state  *file   = open ? open->share_file_state : NULL;

    return open && handle && file && !command->state->producer && !open->doc_posix &&
           !(open->flags & (CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY | CHIMERA_SMB_OPEN_FILE_FLAG_STREAM)) &&
           file->fh_len == handle->fh_len && !memcmp(file->fh, handle->fh, file->fh_len) &&
           (handle->vfs_module->capabilities & CHIMERA_VFS_CAP_REMOVE_MATCH_FH);
} /* smb_doc_close_allowed */

static void
smb_doc_recall_done(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct smb_doc_event *event = private_data;

    /* The legacy disposition contract waits for recall but does not translate
     * a recall error into failure after validating the delete intent. */
    (void) status;
    chimera_vfs_compound_coordinate_done(event->compound, event->token, CHIMERA_VFS_OK);
} /* smb_doc_recall_done */

static bool
smb_doc_bind_dynamic(
    struct smb_vfs_command *command,
    struct smb_doc_event   *event)
{
    struct smb_doc_batch           *batch  = chimera_smb_compound_doc_batch(command);
    struct chimera_vfs_open_handle *handle = command->handle;

    if (!event->initial_file->dynamic) {
        return true;
    }
    if (!handle || !command->state->access_owner) {
        return false;
    }
    for (unsigned int i = 0; i < batch->num_files; i++) {
        struct smb_doc_file *slot = &batch->files[i];
        if (slot->file && slot->file->fh_len == handle->fh_len &&
            !memcmp(slot->file->fh, handle->fh, handle->fh_len)) {
            event->file = slot; return true;
        }
    }
    struct smb_doc_file *slot = event->initial_file;
    slot->vfs_state = command->request->compound->thread->vfs_thread->vfs->vfs_state;
    slot->file      = chimera_vfs_state_get(slot->vfs_state, handle->fh, handle->fh_len,
                                            handle->fh_hash, false);
    event->file = slot;
    return slot->file != NULL;
} /* smb_doc_bind_dynamic */

static void
smb_doc_coordinate_busy(
    struct chimera_vfs_compound *compound,
    uint64_t                     token,
    struct smb_vfs_command      *command,
    struct smb_doc_event        *event)
{
    if (event->initial_file->dynamic) {
        /* Never wait while holding a producer's provisional share admission.
         * Accept its prefix and redispatch this still-unexecuted operation. */
        chimera_smb_compound_defer(command);
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINTR);
    } else {
        command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EAGAIN);
    }
} /* smb_doc_coordinate_busy */

static void
smb_doc_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct smb_vfs_command           *command = private_data;
    struct smb_doc_event             *event   = smb_doc_event(command, false);
    struct chimera_server_smb_thread *thread  = command->request->compound->thread;

    (void) index;
    if (!event || !event->publication) {
        command->status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    if (!smb_doc_bind_dynamic(command, event)) {
        command->status = SMB2_STATUS_FILE_CLOSED;
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINVAL);
        return;
    }
    bool had_namespace_fence = event->file->fence.acquired;
    if (!chimera_smb_doc_fence_acquire(&event->file->fence,
                                       &thread->shared->namespace_registry, fh, fh_len, command->batch)) {
        smb_doc_coordinate_busy(compound, token, command, event);
        return;
    }
    if (!chimera_vfs_claim_access_fence_acquire(&event->file->admission_fence,
                                                event->file->file, chimera_smb_compound_doc_batch(command))) {
        if (!had_namespace_fence) {
            chimera_smb_doc_fence_release(&event->file->fence);
        }
        smb_doc_coordinate_busy(compound, token, command, event);
        return;
    }
    pthread_mutex_lock(&event->file->file->lock);
    bool already_deleting = event->file->file->smb_delete_started;
    bool pending          = event->file->file->delete_pending;
    pthread_mutex_unlock(&event->file->file->lock);
    if (already_deleting) {
        command->status = SMB2_STATUS_FILE_CLOSED;
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINVAL);
        return;
    }
    if (event->path) {
        if (smb_doc_fh_pending(command, fh, fh_len, pending)) {
            command->status = SMB2_STATUS_ACCESS_DENIED;
            chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EACCES); return;
        }
    }
    event->compound = compound;
    event->token    = token;
    if (event->setter && command->request->set_info.attrs.smb_disposition) {
        chimera_vfs_recall_handle_lease(thread->vfs_thread,
                                        &command->request->session_handle->session->cred, command->handle,
                                        smb_doc_recall_done, event);
    } else {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
    }
} /* smb_doc_coordinate */

static void
smb_doc_coordinate_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    (void) compound; (void) index;
    /* Coordination failures are memoized across finish retry, so restore the
     * protocol status on every replay rather than only in the callout. */
    if (*status == CHIMERA_VFS_EAGAIN) {
        command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
    } else if (*status == CHIMERA_VFS_ENOSPC) {
        command->status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
    } else if (*status == CHIMERA_VFS_EINVAL) {
        command->status = SMB2_STATUS_FILE_CLOSED;
    }
} /* smb_doc_coordinate_complete */

static void
smb_doc_intent(
    struct smb_doc_event        *event,
    struct chimera_vfs_doc_info *intent)
{
    struct smb_vfs_command           *command = event->command;
    struct chimera_smb_namespace_path path;

    smb_doc_command_path(command, &path);
    memset(intent, 0, sizeof(*intent));
    intent->parent_fh_len = path.parent_fh_len;
    memcpy(intent->parent_fh, path.parent_fh, intent->parent_fh_len);
    intent->name_len = path.name_len;
    memcpy(intent->name, path.name, intent->name_len);
    intent->target_fh_len = command->handle->fh_len;
    memcpy(intent->target_fh, command->handle->fh, intent->target_fh_len);
    intent->cred = command->request->session_handle->session->cred;
} /* smb_doc_intent */

static int
smb_disposition_eligible(struct chimera_smb_request *request)
{
    return request->set_info.info_type == SMB2_INFO_FILE &&
           (request->set_info.info_class == SMB2_FILE_DISPOSITION_INFO ||
            request->set_info.info_class == SMB2_FILE_DISPOSITION_INFO_EX) &&
           !(request->set_info.attrs.smb_disposition_flags & ~0x11U);
} /* smb_disposition_eligible */

static int smb_disposition_bound(struct smb_vfs_command *command) { return smb_doc_close_allowed(command); }
static struct chimera_smb_file_id
smb_disposition_file_id(struct chimera_smb_request *request)
{
    return request->set_info.file_id;
} /* smb_disposition_file_id */

static void
smb_disposition_validate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command         *command = private_data;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    const struct chimera_vfs_attrs *attrs = &chimera_vfs_compound_op(compound, index)->attr;
    if (command->request->set_info.attrs.smb_disposition &&
        !(command->request->set_info.attrs.smb_disposition_flags & 0x10) &&
        (attrs->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
        (attrs->va_dos_attributes & SMB2_FILE_ATTRIBUTE_READONLY)) {
        command->status = SMB2_STATUS_CANNOT_DELETE;
        *status         = CHIMERA_VFS_EACCES;
    }
} /* smb_disposition_validate */

static int
smb_disposition_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    if (!smb_doc_event(command, true)) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES; return -1;
    }
    if (!chimera_vfs_compound_set_admission_cookie(compound,
                                                   chimera_smb_compound_doc_batch(command))) {
        command->input_status = SMB2_STATUS_INTERNAL_ERROR;
        return -1;
    }
    if (command->request->set_info.attrs.smb_disposition) {
        int attrs = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_DOS_ATTRIBUTES);
        chimera_vfs_compound_set_op_callbacks(compound, attrs, NULL, smb_disposition_validate, command);
    }
    int coord = chimera_vfs_compound_add_coordinate(compound, smb_doc_coordinate, command);
    chimera_vfs_compound_set_op_callbacks(compound, coord, NULL, smb_doc_coordinate_complete, command);
    return chimera_vfs_compound_add_checkpoint(compound);
} /* smb_disposition_build */

static void
smb_disposition_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    (void) compound; (void) index; (void) status;
    if (!(command->open->granted_access & SMB2_DELETE)) {
        command->status = SMB2_STATUS_ACCESS_DENIED; return;
    }
    if (command->state->channel_sequence_valid &&
        (uint16_t) (command->request->channel_sequence - command->state->channel_sequence) >= 0x8000) {
        command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
        return;
    }
    command->state->channel_sequence       = command->request->channel_sequence;
    command->state->channel_sequence_valid = command->state->sequence_dirty = 1;
} /* smb_disposition_prepare */

static void
smb_disposition_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_doc_event   *event   = smb_doc_event(command, false);

    (void) compound; (void) index;
    if (*status != CHIMERA_VFS_OK || !event) {
        return;
    }
    smb_doc_seed(event);
    event->staged = true;
    bool                    deleting = command->request->set_info.attrs.smb_disposition;
    event->file->delete_pending = deleting;
    if (deleting) {
        command->state->flags |= CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
        smb_doc_intent(event, &event->intent);
    } else {
        command->state->flags    &= ~CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
        event->file->have_pending = false;
    }
} /* smb_disposition_complete */

static void
smb_disposition_publish(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct smb_doc_event                  *event = smb_doc_event(command, false);

    if (!event || !event->staged || command->status != SMB2_STATUS_SUCCESS) {
        return;
    }
    struct chimera_smb_open_file          *open     = command->open;
    struct chimera_vfs_file_state         *file     = event->file->file;
    struct chimera_server_smb_thread      *thread   = command->request->compound->thread;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    bool                                   deleting = command->request->set_info.attrs.smb_disposition;
    (void) compound;
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&file->lock);
    if (deleting) {
        open->flags             |= CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
        open->doc_posix          = 0;
        open->stream_delete_cred = event->intent.cred;
        file->delete_pending     = 1;
        if (open->handle) {
            chimera_vfs_set_delete_on_close(thread->vfs_thread, open->handle,
                                            event->intent.parent_fh, event->intent.parent_fh_len,
                                            event->intent.name, event->intent.name_len, &event->intent.cred);
        }
    } else {
        open->flags         &= ~CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
        open->doc_posix      = 0;
        file->delete_pending = 0;
        free(file->smb_pending_delete);
        file->smb_pending_delete = NULL;
        if (open->handle) {
            chimera_vfs_clear_delete_on_close(thread->vfs_thread, open->handle);
        }
    }
    pthread_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
} /* smb_disposition_publish */

const struct smb_vfs_command_ops chimera_smb_disposition_compound_ops = {
    .eligible       = smb_disposition_eligible,
    .bound_eligible = smb_disposition_bound,
    .file_id        = smb_disposition_file_id,
    .build          = smb_disposition_build,
    .prepare        = smb_disposition_prepare,
    .complete       = smb_disposition_complete,
    .publish        = smb_disposition_publish,
};

int
smb_doc_fh_pending(
    struct smb_vfs_command *command,
    const uint8_t          *fh,
    uint32_t                fh_len,
    int                     public_value)
{
    struct smb_doc_batch *batch = chimera_smb_compound_doc_batch(command);

    if (!batch) {
        return public_value;
    }
    for (unsigned int i = 0; i < batch->num_files; i++) {
        struct smb_doc_file *slot = &batch->files[i];
        if (slot->seeded && slot->file && slot->file->fh_len == fh_len && !memcmp(slot->file->fh, fh, fh_len)) {
            return slot->delete_pending;
        }
    }
    return public_value;
} /* smb_doc_fh_pending */

int
smb_doc_close_add_coordinate(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    if (!smb_doc_event(command, true)) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        return -1;
    }
    if (!chimera_vfs_compound_set_admission_cookie(compound,
                                                   chimera_smb_compound_doc_batch(command))) {
        command->input_status = SMB2_STATUS_INTERNAL_ERROR;
        return -1;
    }
    int coord = chimera_vfs_compound_add_coordinate(compound, smb_doc_coordinate, command);
    chimera_vfs_compound_set_op_callbacks(compound, coord, NULL, smb_doc_coordinate_complete, command);
    if (coord >= 0 && command->state && command->state->producer) {
        chimera_vfs_compound_op_args(compound, coord)->coordinate_each_attempt = 1;
    }
    return coord;
} /* smb_doc_close_add_coordinate */

static void smb_doc_close_select_complete(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);
static void smb_doc_parent_fh_prepare(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);
static void smb_doc_parent_prepare(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);
static void smb_doc_parent_complete(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);
static void smb_doc_remove_prepare(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);
static void smb_doc_remove_complete(
    struct chimera_vfs_compound *,
    uint32_t,
    enum chimera_vfs_error *,
    void *);

int
smb_doc_close_build_tail(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct smb_doc_event *event = smb_doc_event(command, false);

    if (!event) {
        return -1;
    }
    /* Allocate every possible descriptor before claim retirement executes.
     * Selection only rewrites fixed inputs and skips inactive operations. */
    int                   select = chimera_vfs_compound_add_checkpoint(compound);
    chimera_vfs_compound_set_op_callbacks(compound, select, NULL,
                                          smb_doc_close_select_complete, command);
    int                   put = chimera_vfs_compound_add_putfh(compound, command->handle->fh, command->handle->fh_len);
    chimera_vfs_compound_set_op_prepare(compound, put, smb_doc_parent_fh_prepare, event);
    int                   parent = chimera_vfs_compound_add_open_current(compound,
                                                                         CHIMERA_VFS_OPEN_INFERRED |
                                                                         CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_set_op_callbacks(compound, parent,
                                          smb_doc_parent_prepare, smb_doc_parent_complete, event);
    int                   remove = chimera_vfs_compound_add_remove(compound, "doc", 3, CHIMERA_VFS_REMOVE_NO_NOTIFY);
    chimera_vfs_compound_set_op_callbacks(compound, remove,
                                          smb_doc_remove_prepare, smb_doc_remove_complete, event);
    chimera_vfs_compound_add_puthandle(compound, command->handle, CHIMERA_VFS_OPEN_INFERRED);
    return chimera_vfs_compound_add_close(compound);
} /* smb_doc_close_build_tail */

struct smb_doc_peers {
    struct smb_vfs_command *command;
    bool                    other;
};

static bool
smb_doc_other_peer(
    struct smb_vfs_command       *command,
    struct chimera_smb_open_file *peer)
{
    return !peer || (peer != command->open && !peer->doc_close_started &&
                     !chimera_smb_compound_open_closed(command, peer));
} /* smb_doc_other_peer */

static void
smb_doc_count_peer(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    struct smb_doc_peers *peers = private_data;

    (void) snapshot;
    if (smb_doc_other_peer(peers->command, context)) {
        peers->other = true;
    }
} /* smb_doc_count_peer */

/* Pure snapshot under the file's DOC fence. An earlier setter in this batch
 * supplies its credentials, even when a different opener performs last close. */
static bool
smb_doc_close_intent(
    struct smb_doc_event        *event,
    struct chimera_vfs_doc_info *intent)
{
    struct smb_vfs_command *command     = event->command;
    struct smb_doc_batch   *batch       = chimera_smb_compound_doc_batch(command);
    struct smb_doc_event   *latest      = NULL;
    bool                    from_create = command->open->doc_from_create;

    /* CREATE's per-open FILE_DELETE_ON_CLOSE mode survives clearing the link's
     * disposition. It must not depend on VFS cache entry sharing. */
    if (!from_create &&
        (!(command->state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) ||
         !event->file->delete_pending)) {
        return false;
    }
    for (unsigned int i = 0; i < batch->num_events; i++) {
        struct smb_doc_event *prior = &batch->events[i];
        if (prior == event) {
            break;
        }
        if (prior->setter && prior->staged && prior->command->open == command->open &&
            prior->command->request->set_info.attrs.smb_disposition) {
            latest = prior;
        }
    }
    smb_doc_intent(event, intent);
    intent->cred = latest ? latest->intent.cred : command->open->stream_delete_cred;
    return intent->parent_fh_len != 0;

} /* smb_doc_close_intent */

static uint32_t
smb_doc_close_error(enum chimera_vfs_error status)
{
    switch (status) {
        case CHIMERA_VFS_OK: return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOENT:
        case CHIMERA_VFS_ESTALE: return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM: return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_ENOTEMPTY: return SMB2_STATUS_DIRECTORY_NOT_EMPTY;
        default: return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* smb_doc_close_error */

static void
smb_doc_parent_fh_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_doc_event           *event = private_data;

    (void) status;
    if (!event->remove) {
        chimera_vfs_compound_op_skip(compound, index); return;
    }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->arg_fh_len = event->intent.parent_fh_len;
    memcpy(op->arg_fh, event->intent.parent_fh, op->arg_fh_len);
} /* smb_doc_parent_fh_prepare */

static void
smb_doc_parent_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_doc_event *event = private_data;

    (void) status;
    if (!event->remove) {
        chimera_vfs_compound_op_skip(compound, index); return;
    }
    chimera_vfs_compound_op_args(compound, index)->namespace_cred = &event->intent.cred;
} /* smb_doc_parent_prepare */

static void
smb_doc_parent_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_doc_event *event = private_data;

    (void) compound; (void) index;
    if (*status != CHIMERA_VFS_OK) {
        event->close_status = smb_doc_close_error(*status);
    }
    /* Parent failure suppresses unlink, but still retires the SMB open. */
    if (*status != CHIMERA_VFS_OK) {
        event->remove = false;
    }
    *status = CHIMERA_VFS_OK;
} /* smb_doc_parent_complete */

static void
smb_doc_remove_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_doc_event           *event = private_data;
    struct chimera_vfs_compound_op *op    = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    if (!event->remove) {
        chimera_vfs_compound_op_skip(compound, index); return;
    }
    op->name_len = event->intent.name_len;
    memcpy(op->name, event->intent.name, op->name_len);
    op->name[op->name_len]    = 0;
    op->namespace_cred        = &event->intent.cred;
    op->remove_match_child_fh = 1;
    op->arg_fh_len            = event->intent.target_fh_len;
    memcpy(op->arg_fh, event->intent.target_fh, op->arg_fh_len);
    memcpy(op->namespace_parent_lease_key, event->parent_lease_key, 16);
    op->namespace_parent_lease_key_valid = 1;
    op->io_owner                         = event->command->actor;
    op->have_io_owner                    = 1;
} /* smb_doc_remove_prepare */

static void
smb_doc_remove_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_doc_event *event = private_data;

    if (event->remove) {
        event->close_status = smb_doc_close_error(*status);
        event->removed      = *status == CHIMERA_VFS_OK &&
            !chimera_vfs_compound_op(compound, index)->remove_unmatched;
    }
    *status = CHIMERA_VFS_OK;
} /* smb_doc_remove_complete */

static void
smb_doc_close_tail_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_doc_event *event = private_data;

    (void) compound; (void) index;
    if (*status == CHIMERA_VFS_OK) {
        event->committed              = true;
        event->command->state->closed = 1;
        event->command->status        = event->close_status;
    }
} /* smb_doc_close_tail_complete */

static void
smb_doc_close_select_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command                *command = private_data;
    struct smb_doc_event                  *event   = smb_doc_event(command, false);

    (void) index;
    if (*status != CHIMERA_VFS_OK || !event) {
        return;
    }
    smb_doc_seed(event);
    struct smb_doc_file                   *slot     = event->file;
    struct chimera_smb_namespace_registry *registry = slot->fence.registry;
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&slot->file->lock);
    struct chimera_vfs_doc_info            own_intent;
    if (!slot->have_pending && smb_doc_close_intent(event, &own_intent)) {
        slot->pending      = own_intent;
        slot->have_pending = slot->delete_pending = true;
    }
    struct smb_doc_peers                   peers = { .command = command };
    chimera_smb_namespace_foreach_locked(registry, slot->file->fh, slot->file->fh_len,
                                         smb_doc_count_peer, &peers);
    for (struct chimera_vfs_claim *claim = slot->file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if ((claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
             claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) &&
            !chimera_smb_compound_claim_closed(command, claim) &&
            smb_doc_other_peer(command, claim->cb_private)) {
            peers.other = true;
        }
    }
    event->remove = !peers.other && slot->delete_pending && slot->have_pending;
    if (slot->have_pending) {
        event->intent = slot->pending;
    }
    if (event->remove) {
        slot->delete_pending = slot->have_pending = false;
    }
    event->after_delete_pending = slot->delete_pending;
    event->after_have_pending   = slot->have_pending;
    if (slot->have_pending) {
        *event->publication = slot->pending;
    }
    memset(event->parent_lease_key, 0, sizeof(event->parent_lease_key));
    if ((command->state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) ||
        command->open->doc_from_create) {
        memcpy(event->parent_lease_key, command->open->parent_lease_key, 16);
    }
    pthread_mutex_unlock(&slot->file->lock);
    chimera_smb_namespace_unlock(registry);

    (void) compound;
} /* smb_doc_close_complete */

void
smb_doc_close_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_doc_event   *event   = smb_doc_event(command, false);

    if (event) {
        smb_doc_close_tail_complete(compound, index, status, event);
    }
} /* smb_doc_close_complete */

bool
smb_doc_close_committed(struct smb_vfs_command *command)
{
    struct smb_doc_event *event = smb_doc_event(command, false);

    return event && event->committed;
} /* smb_doc_close_committed */

static void
smb_doc_consume_peer(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *private_data)
{
    struct chimera_smb_open_file *peer   = context;
    struct chimera_vfs_doc_info  *intent = private_data;

    (void) snapshot;
    if (peer->parent_fh_len == intent->parent_fh_len && peer->name_len == intent->name_len &&
        !memcmp(peer->parent_fh, intent->parent_fh, peer->parent_fh_len) &&
        !memcmp(peer->name, intent->name, peer->name_len)) {
        peer->doc_from_create = 0;
    }
} /* smb_doc_consume_peer */

void
smb_doc_close_publish(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct smb_doc_event                  *event = smb_doc_event(command, false);

    if (!event || !event->committed) {
        return;
    }
    struct chimera_smb_open_file          *open     = command->open;
    struct chimera_vfs_file_state         *file     = event->file->file;
    struct chimera_server_smb_thread      *thread   = command->request->compound->thread;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    (void) compound;
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&file->lock);
    open->doc_close_started = 1;
    file->delete_pending    = event->after_delete_pending;
    free(file->smb_pending_delete);
    file->smb_pending_delete = NULL;
    if (event->after_have_pending) {
        file->smb_pending_delete = event->publication;
        event->publication       = NULL;
    }
    /* A selected deletion consumes that intent even when the name disappeared
     * or removal failed, matching legacy CLOSE's finalization semantics. */
    if (!event->after_have_pending && event->intent.parent_fh_len) {
        chimera_smb_namespace_foreach_locked(registry, file->fh, file->fh_len,
                                             smb_doc_consume_peer, &event->intent);
    }
    /* Preserve another live peer's cache intent when no close has yet copied
     * it into shared pending metadata. Clearing a plain peer's shared handle
     * here would silently discard that future deletion. */
    if (open->handle && event->intent.parent_fh_len) {
        chimera_vfs_clear_delete_on_close(thread->vfs_thread, open->handle);
    }
    file->smb_delete_started = 0;
    pthread_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
    if (event->removed) {
        struct chimera_claim_actor actor = { .owner = {
            .proto = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = command->request->session_handle->session->client_key,
        } };
        memcpy(actor.owner.key, event->parent_lease_key, 16);
        chimera_vfs_notify_emit_actor(thread->shared->vfs->vfs_notify,
                                      event->intent.parent_fh, event->intent.parent_fh_len,
                                      CHIMERA_VFS_NOTIFY_FILE_REMOVED,
                                      event->intent.name, event->intent.name_len, NULL, 0,
                                      chimera_claim_owner_has_key(&actor.owner) ? &actor : NULL);
        chimera_vfs_notify_emit_delete(thread->shared->vfs->vfs_notify,
                                       event->intent.target_fh, event->intent.target_fh_len);
    }
} /* smb_doc_close_publish */
