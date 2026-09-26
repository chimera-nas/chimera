// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_compound.h"
#include "smb_doc_compound.h"
#include "common/compound_retry.h"

/* Each command owns an execution group. Inputs and open references survive
 * finish rejection; only this journal is reset. Further families add builders
 * here without reintroducing the legacy completion callback chain. */
struct smb_vfs_binding {
    struct smb_vfs_binding *next;
    struct smb_vfs_command *command;
    chimera_vfs_compound_op_callback_t prepare;
    void *private_data;
};

struct chimera_smb_vfs_batch {
    struct chimera_smb_vfs_batch *next;
    struct chimera_smb_vfs_batch *cancel_next;
    bool cancel_linked;
    struct chimera_smb_compound *wire;
    struct chimera_smb_session_handle session_handle;
    struct chimera_smb_tree *tree;
    unsigned int count;
    unsigned int complete_count;
    unsigned int defer_at;
    unsigned int num_states;
    bool has_doc;
    bool has_dynamic_doc;
    bool has_waiting_lock;
    bool has_other_close_fence;
    struct smb_vfs_open_state states[CHIMERA_SMB_COMPOUND_MAX_REQUESTS];
    unsigned int max_read;
    struct chimera_smb_file_id initial_saved;
    struct chimera_smb_file_id attempt_saved;
    struct chimera_vfs_compound *compound;
    unsigned int next_command;
    int accepted;
    int advancing;
    int pending;
    struct smb_vfs_binding *bindings;
    struct smb_vfs_file *files;
    struct smb_doc_batch *doc;
    struct smb_vfs_command commands[CHIMERA_SMB_COMPOUND_MAX_REQUESTS];
};

static void
smb_vfs_cancel_untrack(struct chimera_smb_vfs_batch *batch)
{
    if (!batch->cancel_linked) return;
    struct chimera_smb_conn *conn = batch->wire->conn;
    chimera_smb_abort_if(conn->generation != batch->wire->conn_generation,
        "native cancellation registry survived connection recycling");
    struct chimera_smb_vfs_batch **link = &conn->cancel_batches;
    while (*link && *link != batch) link = &(*link)->cancel_next;
    chimera_smb_abort_if(!*link, "native cancellation registration lost");
    *link = batch->cancel_next;
    batch->cancel_next = NULL;
    batch->cancel_linked = false;
}

static void
smb_vfs_cancel_track(struct chimera_smb_vfs_batch *batch)
{
    struct chimera_smb_conn *conn = batch->wire->conn;
    /* Cancellation stops an entire VFS batch. Register only an isolated CLOSE
     * so a CANCEL cannot discard a different command's accepted prefix/suffix.
     * The batch's existing pins retain all context until normal wire cleanup. */
    if (batch->count != 1 || batch->commands[0].ops != &chimera_smb_close_compound_ops ||
        conn->generation != batch->wire->conn_generation || conn->disconnecting) return;
    chimera_smb_abort_if(batch->cancel_linked, "duplicate native cancellation registration");
    batch->cancel_next = conn->cancel_batches;
    conn->cancel_batches = batch;
    batch->cancel_linked = true;
}

bool
chimera_smb_vfs_cancel(struct chimera_smb_conn *conn, uint64_t session_id,
    uint64_t target_id, bool async)
{
    for (struct chimera_smb_vfs_batch *batch = conn->cancel_batches;
         batch; batch = batch->cancel_next) {
        const struct chimera_smb_request *request = batch->commands[0].request;
        if (batch->wire->conn_generation != conn->generation ||
            batch->session_handle.session_id != session_id ||
            (async ? (!request->async_id || request->async_id != target_id) :
                     request->smb2_hdr.message_id != target_id)) continue;
        /* At/after finish, cancel returns false: retain accepted CLOSE outcome.
         * During execution this can complete synchronously and destroy batch;
         * do not inspect it, the request, or its link after this call. */
        chimera_vfs_compound_cancel(batch->compound);
        return true;
    }
    return false;
}

void
chimera_smb_vfs_cancel_drain(struct chimera_smb_conn *conn)
{
    /* Called on the owning loop before conn recycling. Unlink before cancel:
     * callbacks may complete inline or later, but neither can retain a list
     * link into a connection reused by another session/generation. */
    while (conn->cancel_batches) {
        struct chimera_smb_vfs_batch *batch = conn->cancel_batches;
        conn->cancel_batches = batch->cancel_next;
        batch->cancel_next = NULL;
        batch->cancel_linked = false;
        chimera_vfs_compound_cancel(batch->compound);
    }
}

struct smb_doc_batch *
chimera_smb_compound_doc_batch(struct smb_vfs_command *command)
{
    return command->batch->doc;
}

void
chimera_smb_compound_defer(struct smb_vfs_command *command)
{
    unsigned int index = command - command->batch->commands;
    if (index < command->batch->defer_at) { command->batch->defer_at = index; }
}

struct smb_doc_batch *
chimera_smb_compound_doc_batch_ensure(struct smb_vfs_command *command)
{
    struct chimera_smb_vfs_batch *batch = command->batch;
    if (!batch->doc) { batch->doc = smb_doc_batch_alloc(batch->count); }
    return batch->doc;
}

bool
chimera_smb_compound_open_closed(struct smb_vfs_command *command,
                                const struct chimera_smb_open_file *open)
{
    struct chimera_smb_vfs_batch *batch = command->batch;
    for (unsigned int i = 0; i < batch->num_states; i++) {
        if (batch->states[i].open == open) { return batch->states[i].closed; }
    }
    return false;
}

bool
chimera_smb_compound_claim_closed(struct smb_vfs_command *command,
                                 const struct chimera_vfs_claim *claim)
{
    struct chimera_smb_vfs_batch *batch = command->batch;
    for (unsigned int i = 0; i < batch->num_states; i++) {
        struct smb_vfs_open_state *state = &batch->states[i];
        if ((state->access_owner &&
             chimera_vfs_claim_access_owner_claim(state->access_owner) == claim) ||
            (state->base_access_owner &&
             chimera_vfs_claim_access_owner_claim(state->base_access_owner) == claim)) {
            return state->closed;
        }
    }
    return false;
}

bool
chimera_smb_compound_directory_isolated(struct smb_vfs_command *command)
{
    struct chimera_smb_vfs_batch *batch = command->batch;
    if (!command->handle) { return false; }
    struct chimera_smb_namespace_path source_path;
    smb_doc_command_path(command, &source_path);
    for (unsigned int i = 0; i < batch->num_states; i++) {
        struct smb_vfs_open_state *state = &batch->states[i];
        struct chimera_vfs_open_handle *handle = NULL;
        if (state->producer) {
            if (state->producer >= command || state->handle_from < 0) { continue; }
            handle = chimera_vfs_compound_op(batch->compound, state->handle_from)->out_handle;
        } else {
            for (unsigned int j = 0; j < batch->count; j++) {
                struct smb_vfs_command *peer = &batch->commands[j];
                if (peer->initial_state == state && peer->owned_handle) {
                    handle = peer->owned_handle;
                    break;
                }
            }
        }
        if (!handle) {
            if (state->available) { return false; }
            continue;
        }
        /* Earlier same-wire moves are still private. Registry snapshots alone
         * can misclassify a root sibling that already moved into this subtree. */
        struct smb_vfs_command view = *command;
        struct chimera_smb_namespace_path peer_path;
        view.open = state->open;
        view.state = state;
        view.handle = handle;
        smb_doc_command_path(&view, &peer_path);
        if (!chimera_smb_namespace_directory_peer_allowed(command->handle->fh,
                command->handle->fh_len, &source_path, handle->fh, handle->fh_len,
                &peer_path)) { return false; }
    }
    return true;
}

static struct chimera_claim_owner
smb_vfs_actor_owner(struct smb_vfs_open_state *state, struct chimera_smb_open_file *open)
{
    if (state && state->producer && state->producer->ops->private_actor_owner) {
        return state->producer->ops->private_actor_owner(state->producer);
    }
    return chimera_smb_open_actor_owner(open);
}

static const struct smb_vfs_command_ops *
smb_vfs_ops(struct chimera_smb_request *request)
{
    const struct smb_vfs_command_ops *ops;
    switch (request->smb2_hdr.command) {
        case SMB2_QUERY_INFO:
            ops = request->query_info.info_type == SMB2_INFO_SECURITY ?
                  &chimera_smb_query_security_compound_ops : &chimera_smb_query_info_compound_ops;
            break;
        case SMB2_SET_INFO:
            if (request->set_info.info_type == SMB2_INFO_FILE &&
                request->set_info.info_class == SMB2_FILE_RENAME_INFO) {
                ops = &chimera_smb_rename_compound_ops;
            } else if (request->set_info.info_type == SMB2_INFO_FILE &&
                (request->set_info.info_class == SMB2_FILE_DISPOSITION_INFO ||
                 request->set_info.info_class == SMB2_FILE_DISPOSITION_INFO_EX)) {
                ops = &chimera_smb_disposition_compound_ops;
            } else {
                ops = request->set_info.info_type == SMB2_INFO_SECURITY ?
                      &chimera_smb_set_security_compound_ops : &chimera_smb_set_info_compound_ops;
            }
            break;
        case SMB2_IOCTL: ops = chimera_smb_ioctl_compound_ops_for(request); break;
        case SMB2_CREATE: ops = &chimera_smb_create_compound_ops; break;
        case SMB2_CLOSE: ops = &chimera_smb_close_compound_ops; break;
        case SMB2_READ: ops = &chimera_smb_read_compound_ops; break;
        case SMB2_WRITE: ops = &chimera_smb_write_compound_ops; break;
        case SMB2_QUERY_DIRECTORY: ops = &chimera_smb_query_directory_compound_ops; break;
        case SMB2_FLUSH: ops = &chimera_smb_flush_compound_ops; break;
        case SMB2_LOCK: ops = &chimera_smb_lock_compound_ops; break;
        default: return NULL;
    }
    return ops && ops->eligible(request) ? ops : NULL;
}

/* Resolve without consuming replay eligibility or publishing related state. */
static struct chimera_smb_open_file *
smb_vfs_pin_open(struct chimera_smb_vfs_batch *batch, struct smb_vfs_command *command,
                 struct chimera_smb_file_id id)
{
    struct chimera_smb_open_file *open;
    struct chimera_smb_tree *tree = batch->tree;
    int bucket = id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    HASH_FIND(hh, tree->open_files[bucket], &id, sizeof(id), open);
    if (open && !(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) && open->identity_rebind) {
        command->input_status = SMB2_STATUS_FILE_NOT_AVAILABLE;
        open = NULL;
    }
    if (open && !(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
        open->refcnt++;
        struct chimera_vfs_file_state *file = open->share_file_state;
        bool stream = open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM;
        if (file) {
            pthread_mutex_lock(&file->lock);
        }
        if (!(stream ? open->doc_stream_close_started : open->doc_close_started) && open->handle) {
            /* Identity-only pointer: claims compare it but never dereference
             * it. A retained synthetic handle has a different address. */
            command->actor.op_handle = open->handle;
            command->handle = chimera_smb_retain_vfs_handle(batch->wire->thread->vfs_thread,
                                                            open->handle);
        }
        if (file) {
            pthread_mutex_unlock(&file->lock);
        }
    } else {
        open = NULL;
    }
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);
    return open;
}

struct smb_vfs_file *
chimera_smb_compound_pin_file(struct smb_vfs_command *command,
                              struct chimera_smb_file_id id)
{
    struct chimera_smb_vfs_batch *batch = command->batch;
    struct smb_vfs_file *file = calloc(1, sizeof(*file));
    struct smb_vfs_command pin = { 0 };
    if (!file) { return NULL; }
    file->command = command;
    for (unsigned int i = 0; i < batch->num_states; i++) {
        struct smb_vfs_open_state *state = &batch->states[i];
        if (state->producer && state->producer < command && state->open &&
            state->open->file_id.pid == id.pid && state->open->file_id.vid == id.vid) {
            file->state = state;
            file->open = state->open;
            break;
        }
    }
    if (!file->open) {
        file->open = file->owned_open = smb_vfs_pin_open(batch, &pin, id);
        if (pin.input_status) { command->input_status = pin.input_status; }
        file->handle = file->owned_handle = pin.handle;
        file->actor = pin.actor;
        if (!file->open || !file->handle) {
            if (file->owned_open) { chimera_smb_open_file_release(command->request, file->owned_open); }
            free(file);
            return NULL;
        }
    }
    file->next = batch->files;
    batch->files = file;
    return file;
}

unsigned int
chimera_smb_compound_file_prepare(struct chimera_vfs_compound *compound,
                                  struct smb_vfs_file *file)
{
    struct chimera_smb_open_file *open;
    if (!file) { return SMB2_STATUS_FILE_CLOSED; }
    open = file->open;
    if (file->state) {
        const struct chimera_vfs_compound_op *producer;
        if (!file->state->available || file->state->closed) { return SMB2_STATUS_FILE_CLOSED; }
        producer = chimera_vfs_compound_op(compound, file->state->handle_from);
        file->handle = producer->out_handle;
        file->actor.op_handle = file->handle;
    }
    if (!file->handle || (open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) { return SMB2_STATUS_FILE_CLOSED; }
    file->actor.owner = smb_vfs_actor_owner(file->state, open);
    return SMB2_STATUS_SUCCESS;
}

static unsigned int
smb_vfs_error(struct smb_vfs_command *command, enum chimera_vfs_error error)
{
    if (error == CHIMERA_VFS_OK) {
        return SMB2_STATUS_SUCCESS;
    }
    if (command->ops->map_error) {
        return command->ops->map_error(error);
    }
    return SMB2_STATUS_INTERNAL_ERROR;
}

static unsigned int
smb_vfs_command_status(struct chimera_vfs_compound *compound,
                       struct smb_vfs_command *command)
{
    if (command->status != SMB2_STATUS_SUCCESS) {
        return command->status;
    }
    return smb_vfs_error(command, chimera_vfs_compound_group_status(compound,
                                                                   command->group));
}

static void
smb_vfs_reset(struct chimera_vfs_compound *compound, void *private_data)
{
    struct chimera_smb_vfs_batch *batch = private_data;
    (void) compound;
    batch->attempt_saved = batch->initial_saved;
    batch->defer_at = batch->count;
    smb_doc_batch_reset(batch->doc);
    for (unsigned int i = 0; i < batch->num_states; i++) {
        struct smb_vfs_open_state *state = &batch->states[i];
        struct chimera_smb_open_file *open = state->open;
        if (!open) { continue; }
        int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        pthread_mutex_lock(&batch->tree->open_files_lock[bucket]);
        state->available = state->producer ? 0 : 1;
        state->closed = 0;
        state->access_owner = state->producer ? NULL : open->access_owner;
        state->base_access_owner = state->producer ? NULL : open->base_access_owner;
        state->range_owner = state->producer ? NULL : open->range_owner;
        if (state->producer) { state->range_owner_from = -1; }
        state->position = state->producer ? 0 : open->position;
        state->flags = open->flags;
        state->flags_dirty = 0;
        state->integrity_algo = open->integrity_algo;
        state->integrity_flags = open->integrity_flags;
        state->integrity_dirty = 0;
        memcpy(state->lock_seq_valid, open->lock_seq_valid, sizeof(state->lock_seq_valid));
        memcpy(state->lock_seq_index, open->lock_seq_index, sizeof(state->lock_seq_index));
        memcpy(state->lock_seq_status, open->lock_seq_status, sizeof(state->lock_seq_status));
        state->lock_seq_dirty = 0;
        state->channel_sequence = open->channel_sequence;
        state->channel_sequence_valid = open->channel_sequence_valid;
        pthread_mutex_unlock(&batch->tree->open_files_lock[bucket]);
        state->position_dirty = state->sequence_dirty = state->end_replay = 0;
    }
    for (unsigned int i = 0; i < batch->count; i++) {
        struct smb_vfs_command *command = &batch->commands[i];
        chimera_smb_namespace_open_end(&command->mutation_token);
        command->state = command->initial_state;
        command->open = command->owned_open;
        command->handle = command->owned_handle;
        if (command->state && command->state->producer) {
            command->handle = NULL;
            command->open = command->state->open;
        }
        batch->commands[i].status = SMB2_STATUS_SUCCESS;
        if (batch->commands[i].ops->reset) {
            batch->commands[i].ops->reset(&batch->commands[i]);
        }
    }
}

void
chimera_smb_compound_open_available(struct smb_vfs_command *command)
{
    command->state->available = 1;
    command->batch->attempt_saved = command->open->file_id;
    command->saved_pid = command->open->file_id.pid;
    command->saved_vid = command->open->file_id.vid;
}

static void
smb_vfs_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct chimera_smb_vfs_batch *batch = command->batch;
    struct chimera_smb_compound *wire = batch->wire;
    struct chimera_smb_request *request = command->request;
    struct chimera_smb_open_file *open = command->open;
    unsigned int result = SMB2_STATUS_SUCCESS;
    (void) index;

    if ((unsigned int) (command - batch->commands) >= batch->defer_at) {
        *status = CHIMERA_VFS_EINTR;
        return;
    }

    if (wire->conn->generation != wire->conn_generation || wire->conn->disconnecting) {
        result = SMB2_STATUS_FILE_CLOSED;
    } else if (batch->session_handle.session->flags & CHIMERA_SMB_SESSION_DELETED) {
        result = SMB2_STATUS_USER_SESSION_DELETED;
    } else if (batch->tree->compound_tearing_down) {
        result = SMB2_STATUS_NETWORK_NAME_DELETED;
    } else if ((request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) &&
               command != batch->commands && batch->attempt_saved.pid == UINT64_MAX) {
        result = smb_vfs_command_status(compound, command - 1);
    }
    if (!(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS)) {
        batch->attempt_saved.pid = UINT64_MAX;
        batch->attempt_saved.vid = UINT64_MAX;
    }
    if (command->inherits_file) {
        command->state = NULL;
        command->open = NULL;
        if (command->initial_state && command->initial_state->producer) { command->handle = NULL; }
        for (unsigned int i = 0; i < batch->num_states; i++) {
            struct smb_vfs_open_state *state = &batch->states[i];
            if (state->open && state->open->file_id.pid == batch->attempt_saved.pid &&
                state->open->file_id.vid == batch->attempt_saved.vid) {
                command->state = state;
                command->open = state->open;
                break;
            }
        }
    }
    open = command->open;
    if (command->state && command->state->producer && command->state->available && !command->state->closed) {
        command->handle = chimera_vfs_compound_op(compound, command->state->handle_from)->out_handle;
        command->actor.op_handle = command->handle;
    } else if (command->inherits_file && !command->handle && command->state && !command->state->producer) {
        /* Find the original request's independently pinned handle. */
        for (unsigned int i = 0; i < batch->count; i++) {
            struct smb_vfs_command *source = &batch->commands[i];
            if (source != command && source->initial_state == command->state && source->handle) {
                command->handle = source->handle;
                command->actor.op_handle = source->actor.op_handle;
                break;
            }
        }
    }
    if (result == SMB2_STATUS_SUCCESS &&
        (request->flags & CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED)) {
        result = request->status;
    }
    if (result == SMB2_STATUS_SUCCESS && !wire->received_encrypted &&
        ((batch->session_handle.session->flags & CHIMERA_SMB_SESSION_ENCRYPT_DATA) ||
         (batch->tree->share && batch->tree->share->encrypt_data))) {
        result = SMB2_STATUS_ACCESS_DENIED;
    }
    if (result == SMB2_STATUS_SUCCESS && command->input_status) {
        result = command->input_status;
    }
    if (result == SMB2_STATUS_SUCCESS && !command->ops->creates_open &&
        (!open || !command->handle || !command->state || command->state->closed ||
         (open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED))) {
        result = SMB2_STATUS_FILE_CLOSED;
    }
    if (result != SMB2_STATUS_SUCCESS) {
        goto reject;
    }

    if (!command->ops->creates_open) {
        command->actor.owner = smb_vfs_actor_owner(command->state, open);
        batch->attempt_saved = open->file_id;
        command->state->end_replay |= !request->is_replay;
    }
    if (command->ops->prepare) {
        command->ops->prepare(compound, index, status, command);
        result = command->status;
    }
reject:
    command->saved_pid = batch->attempt_saved.pid;
    command->saved_vid = batch->attempt_saved.vid;
    command->status = result;
    if (result != SMB2_STATUS_SUCCESS) {
        *status = CHIMERA_VFS_EINVAL;
    }
}

static void
smb_vfs_terminal(struct chimera_smb_vfs_batch *batch)
{
    struct chimera_smb_compound *wire = batch->wire;
    struct chimera_vfs_compound *compound = batch->compound;
    if (batch->accepted) {
        for (unsigned int i = 0; i < batch->num_states; i++) {
            struct smb_vfs_open_state *state = &batch->states[i];
            struct chimera_smb_open_file *open = state->open;
            if (!open) { continue; }
            int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
            pthread_mutex_lock(&batch->tree->open_files_lock[bucket]);
            if (!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
                open->flags = (open->flags & ~state->flags_dirty) |
                              (state->flags & state->flags_dirty);
                if (state->end_replay) {
                    open->flags &= ~CHIMERA_SMB_OPEN_FILE_REPLAY_ELIGIBLE;
                }
                for (unsigned int seq = 0; seq < 64; seq++) {
                    if (state->lock_seq_dirty & (UINT64_C(1) << seq)) {
                        open->lock_seq_valid[seq] = state->lock_seq_valid[seq];
                        open->lock_seq_index[seq] = state->lock_seq_index[seq];
                        open->lock_seq_status[seq] = state->lock_seq_status[seq];
                    }
                }
                if (state->integrity_dirty) {
                    open->integrity_algo = state->integrity_algo;
                    open->integrity_flags = state->integrity_flags;
                }
                if (state->position_dirty) {
                    open->position = state->position;
                }
                if (state->sequence_dirty) {
                    chimera_smb_channel_sequence_stale(open, state->channel_sequence, 0);
                }
            }
            pthread_mutex_unlock(&batch->tree->open_files_lock[bucket]);
        }
    }
    chimera_vfs_compound_free(compound);
    smb_doc_batch_free(batch->doc);
    batch->doc = NULL;
    /* Preserve CLOSE/lock retirement semantics in a following legacy command;
     * only context snapshots, not open references, survive through the reply. */
    /* Consumers can retain pointers into an earlier private CREATE slot (LOCK
     * cleanup in particular). Destroy them before destroying their producer. */
    for (unsigned int remaining = batch->count; remaining; remaining--) {
        unsigned int i = remaining - 1;
        batch->commands[i].deferred = batch->accepted && i >= batch->complete_count;
        if (batch->commands[i].ops->release) {
            batch->commands[i].ops->release(&batch->commands[i]);
        }
        chimera_smb_namespace_open_end(&batch->commands[i].mutation_token);
        if (batch->commands[i].owned_handle) {
            chimera_vfs_release(wire->thread->vfs_thread, batch->commands[i].owned_handle);
            batch->commands[i].owned_handle = NULL;
        }
        if (batch->commands[i].owned_open) {
            chimera_smb_open_file_release(batch->commands[i].request, batch->commands[i].owned_open);
            batch->commands[i].owned_open = NULL;
        }
        batch->commands[i].handle = NULL;
        batch->commands[i].open = NULL;
    }
    while (batch->files) {
        struct smb_vfs_file *file = batch->files;
        batch->files = file->next;
        if (file->owned_handle) { chimera_vfs_release(wire->thread->vfs_thread, file->owned_handle); }
        if (file->owned_open) { chimera_smb_open_file_release(file->command->request, file->owned_open); }
        free(file);
    }
    while (batch->bindings) {
        struct smb_vfs_binding *binding = batch->bindings;
        batch->bindings = binding->next;
        free(binding);
    }

    if (batch->accepted && batch->complete_count < batch->count) {
        batch->commands[batch->complete_count].request->compound_legacy_once = true;
    }
    chimera_smb_compound_complete_batch(wire, batch->complete_count);
}

static void smb_vfs_publish_next(struct chimera_smb_vfs_batch *batch);

static void
smb_vfs_publish_done(struct smb_vfs_command *command, unsigned int status)
{
    struct chimera_smb_vfs_batch *batch = command->batch;
    command->status = command->request->status = status;
    batch->pending = 0;
    if (!batch->advancing) {
        smb_vfs_publish_next(batch);
    }
}

static void
smb_vfs_publish_next(struct chimera_smb_vfs_batch *batch)
{
    struct chimera_smb_compound *wire = batch->wire;
    batch->advancing = 1;
    while (batch->next_command < batch->complete_count) {
        struct smb_vfs_command *command = &batch->commands[batch->next_command++];
        command->publish_live = wire->conn->generation == wire->conn_generation &&
                                !wire->conn->disconnecting;
        if (batch->accepted && command->ops->publish) {
            command->ops->publish(batch->compound, command);
        }
        command->request->status = command->status;
        if (batch->accepted && command->publish_live &&
            command->status == SMB2_STATUS_SUCCESS && command->ops->publish_async) {
            batch->pending = 1;
            command->ops->publish_async(command, smb_vfs_publish_done);
            if (batch->pending) {
                batch->advancing = 0;
                return;
            }
        }
    }
    batch->advancing = 0;
    smb_vfs_terminal(batch);
}

static void
smb_vfs_complete(struct chimera_vfs_compound *compound, void *private_data)
{
    struct chimera_smb_vfs_batch *batch = private_data;
    struct chimera_smb_compound *wire = batch->wire;
    /* Accepted publication may dispatch more work or recycle the wire. Stop
     * exposing this execution before entering any of those completion paths. */
    smb_vfs_cancel_untrack(batch);
    batch->compound = compound;
    batch->accepted = chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK;
    batch->complete_count = batch->accepted && !chimera_vfs_compound_is_canceled(compound) ?
                            batch->defer_at : batch->count;
    batch->next_command = 0;
    for (unsigned int i = 0; i < batch->complete_count; i++) {
        struct smb_vfs_command *command = &batch->commands[i];
        struct chimera_smb_request *request = command->request;
        command->status = batch->accepted ? smb_vfs_command_status(compound, command) :
                                          SMB2_STATUS_INTERNAL_ERROR;
        request->session_handle = &batch->session_handle;
        request->tree = batch->tree;
        request->status = command->status;
        wire->saved_file_id.pid = batch->accepted ? command->saved_pid : UINT64_MAX;
        wire->saved_file_id.vid = batch->accepted ? command->saved_vid : UINT64_MAX;
        wire->saved_session_handle = &batch->session_handle;
        wire->saved_session_id = batch->session_handle.session_id;
        wire->saved_tree = batch->tree;
        wire->saved_tree_id = batch->tree->tree_id;
    }
    smb_vfs_publish_next(batch);
}

static void
smb_vfs_session_unpin(struct chimera_server_smb_shared *shared,
                      struct chimera_smb_session *session)
{
    pthread_mutex_lock(&shared->sessions_lock);
    session->compound_pins--;
    if (!session->compound_pins && session->compound_retired) {
        LL_PREPEND(shared->free_sessions, session);
    }
    pthread_mutex_unlock(&shared->sessions_lock);
}

void
chimera_smb_vfs_compound_cleanup(struct chimera_smb_compound *wire)
{
    struct chimera_smb_vfs_batch *batch;
    struct chimera_server_smb_thread *thread = wire->thread;
    struct chimera_server_smb_shared *shared = thread->shared;

    while ((batch = wire->vfs_batches)) {
        chimera_smb_abort_if(batch->cancel_linked, "freeing registered native compound");
        wire->vfs_batches = batch->next;
        for (unsigned int i = 0; i < batch->count; i++) {
            if (batch->commands[i].ops->reply_release) {
                batch->commands[i].ops->reply_release(&batch->commands[i]);
            }
            if (batch->commands[i].owned_open) {
                chimera_smb_open_file_release(batch->commands[i].request, batch->commands[i].owned_open);
            }
        }
        pthread_mutex_lock(&shared->trees_lock);
        batch->tree->compound_pins--;
        if (!batch->tree->compound_pins && batch->tree->compound_retired) {
            if (batch->tree->share) {
                chimera_smb_share_release(batch->tree->share);
                batch->tree->share = NULL;
            }
            LL_PREPEND(shared->free_trees, batch->tree);
        }
        pthread_mutex_unlock(&shared->trees_lock);
        smb_vfs_session_unpin(shared, batch->session_handle.session);
        smb_doc_batch_free(batch->doc);
        free(batch);
    }
}

/* Bind after the command checkpoint, when a preceding CREATE has resolved
 * the slot. Preserve each handler's operation-specific prepare callback. */
static void
smb_vfs_bind_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                     enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_binding *binding = private_data;
    struct smb_vfs_command *command = binding->command;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    if (op->type == CHIMERA_VFS_COMPOUND_OP_PUTHANDLE) {
        if (command->state->producer) {
            op->in_handle = NULL;
            chimera_vfs_compound_op_use_handle(compound, index, command->state->handle_from);
        } else {
            op->handle_from = -1;
            op->in_handle = command->handle;
        }
    } else {
        /* Producer-dependent I/O was built before its data handle existed.
         * Bind it explicitly once resolved, just like a public FileId: cursor
         * I/O otherwise reopens via PATH and checks the base inode's type,
         * which incorrectly rejects a directory's named data stream. */
        if (op->in_handle ||
            (command->state && command->state->producer &&
             (op->type == CHIMERA_VFS_COMPOUND_OP_READ ||
              op->type == CHIMERA_VFS_COMPOUND_OP_WRITE))) {
            op->in_handle = command->handle;
        }
        if (op->have_io_owner) { op->io_owner = command->actor; }
    }
    if (binding->prepare) {
        binding->prepare(compound, index, status, binding->private_data);
    }
}

static void
smb_vfs_submit_ready(void *private_data)
{
    struct chimera_smb_vfs_batch *batch = private_data;
    smb_vfs_cancel_track(batch);
    chimera_frontend_compound_submit(batch->compound, smb_vfs_complete, batch);
}

static void
smb_vfs_mutation_coordinate(struct chimera_vfs_compound *compound, uint32_t index,
    uint64_t token, const uint8_t *fh, uint32_t fh_len, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) index; (void) fh; (void) fh_len;
    if (!chimera_smb_namespace_open_begin(&command->mutation_token,
            &command->request->compound->thread->shared->namespace_registry,
            command->request->compound)) {
        /* No waiting while a static DOC preflight might hold source fences.
         * Accept/drain the prefix, then legacy admission can wait without them. */
        chimera_smb_compound_defer(command);
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINTR);
        return;
    }
    chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
}

static bool
smb_vfs_namespace_mutator(struct chimera_smb_request *request)
{
    return request->smb2_hdr.command == SMB2_SET_INFO &&
        request->set_info.info_type == SMB2_INFO_FILE &&
        (request->set_info.info_class == SMB2_FILE_RENAME_INFO ||
         request->set_info.info_class == SMB2_FILE_LINK_INFO);
}

static void
smb_vfs_build(struct chimera_smb_vfs_batch *batch)
{
    struct chimera_smb_compound *wire = batch->wire;
    struct chimera_smb_session *session = batch->session_handle.session;
    struct chimera_vfs_compound *compound;
    compound = chimera_vfs_compound_alloc(wire->thread->vfs_thread, &session->cred);
    for (unsigned int i = 0; i < batch->count; i++) {
        struct smb_vfs_command *command = &batch->commands[i];
        int checkpoint = chimera_vfs_compound_add_checkpoint(compound);
        int start = checkpoint;
        struct chimera_vfs_compound_group_config group = {
            .first_op = start,
            .cred = &session->cred,
            .context = command,
            .dependency = -1,
            .continue_on_error = true,
        };
        chimera_vfs_compound_set_op_prepare(compound, checkpoint, smb_vfs_prepare, command);
        command->result = checkpoint;
        if ((command->handle || (command->state && command->state->producer) || command->ops->creates_open) &&
            command->input_status == SMB2_STATUS_SUCCESS) {
            if (!command->ops->creates_open) {
                if (command->state && command->state->producer) {
                    chimera_vfs_compound_add_puthandle_from(compound, command->state->handle_from,
                                                          CHIMERA_VFS_OPEN_INFERRED);
                } else {
                    chimera_vfs_compound_add_puthandle(compound, command->handle,
                                                     CHIMERA_VFS_OPEN_INFERRED);
                }
            }
            if (smb_vfs_namespace_mutator(command->request)) {
                /* COORDINATE resolves its object from the cursor. Pure command
                 * validation and PUTHANDLE precede admission; all namespace
                 * discovery/mutation and source DOC coordinates follow it. */
                int coordinate = chimera_vfs_compound_add_coordinate(compound,
                    smb_vfs_mutation_coordinate, command);
                if (coordinate >= 0) {
                    chimera_vfs_compound_op_args(compound, coordinate)->coordinate_each_attempt = 1;
                }
            }
            command->result = command->ops->build(compound, command);
            if (command->result >= 0 && command->ops->complete) {
                const struct chimera_vfs_compound_op *op =
                    chimera_vfs_compound_op(compound, command->result);
                chimera_vfs_compound_op_callback_t prepare = op->prepare;
                void *prepare_private = op->prepare_private;
                chimera_vfs_compound_set_op_callbacks(compound, command->result,
                    prepare, command->ops->complete, command);
                chimera_vfs_compound_set_op_prepare(compound, command->result,
                    prepare, prepare_private);
            }
        }
        if (!command->ops->creates_open) {
            for (uint32_t oi = checkpoint + 1; oi < chimera_vfs_compound_num_ops(compound); oi++) {
                const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, oi);
                struct smb_vfs_binding *binding = calloc(1, sizeof(*binding));
                if (!binding) { command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES; break; }
                binding->command = command;
                binding->prepare = op->prepare;
                binding->private_data = op->prepare_private;
                binding->next = batch->bindings;
                batch->bindings = binding;
                chimera_vfs_compound_set_op_prepare(compound, oi, smb_vfs_bind_prepare, binding);
            }
        }
        group.num_ops = chimera_vfs_compound_num_ops(compound) - start;
        command->group = chimera_vfs_compound_add_group(compound, &group);
    }
    chimera_vfs_compound_set_attempt_reset(compound, smb_vfs_reset, batch);
    batch->compound = compound;
    smb_doc_batch_preflight(batch->doc, smb_vfs_submit_ready, batch);
}

static void smb_vfs_gather_next(struct chimera_smb_vfs_batch *batch);

static void
smb_vfs_gather_done(struct smb_vfs_command *command, unsigned int status)
{
    struct chimera_smb_vfs_batch *batch = command->batch;
    command->input_status = status;
    command->request->compound_input_gathered = true;
    command->request->compound_input_status = status;
    batch->pending = 0;
    if (!batch->advancing) {
        smb_vfs_gather_next(batch);
    }
}

static void
smb_vfs_gather_next(struct chimera_smb_vfs_batch *batch)
{
    batch->advancing = 1;
    while (batch->next_command < batch->count) {
        struct smb_vfs_command *command = &batch->commands[batch->next_command++];
        /* A prior input snapshot cannot override failed identity admission. */
        if (command->input_status != SMB2_STATUS_SUCCESS) { continue; }
        if (command->request->compound_input_gathered) {
            command->input_status = command->request->compound_input_status;
            continue;
        }
        if (batch->wire->conn->generation != batch->wire->conn_generation ||
            batch->wire->conn->disconnecting || !batch->wire->conn->bind) {
            command->input_status = SMB2_STATUS_FILE_CLOSED;
            continue;
        }
        if ((command->handle || (command->state && command->state->producer)) && command->ops->gather_inputs) {
            batch->pending = 1;
            command->ops->gather_inputs(command, smb_vfs_gather_done);
            if (batch->pending) {
                batch->advancing = 0;
                return;
            }
        }
    }
    batch->advancing = 0;
    batch->next_command = 0;
    smb_vfs_build(batch);
}

SYMBOL_EXPORT int
chimera_smb_vfs_compound_try(struct chimera_smb_compound *wire)
{
    struct chimera_smb_request *first = wire->requests[wire->complete_requests];
    struct chimera_smb_vfs_batch *batch;
    struct chimera_smb_file_id saved = wire->saved_file_id;
    struct chimera_server_smb_shared *shared = wire->thread->shared;
    struct chimera_smb_session *session;
    struct smb_vfs_command *private_producer = NULL;

    if (first->compound_legacy_once) {
        first->compound_legacy_once = false;
        return 0;
    }

    if (!smb_vfs_ops(first) || !first->session_handle || !first->tree ||
        first->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE) {
        return 0;
    }
    batch = calloc(1, sizeof(*batch));
    if (!batch) {
        return 0;
    }
    batch->wire = wire;
    batch->tree = first->tree;
    batch->session_handle = *first->session_handle;
    batch->max_read = chimera_smb_max_rw_size(wire->conn);
    batch->initial_saved = wire->saved_file_id;
    session = batch->session_handle.session;
    pthread_mutex_lock(&shared->sessions_lock);
    session->compound_pins++;
    pthread_mutex_unlock(&shared->sessions_lock);
    pthread_mutex_lock(&session->lock);
    if (batch->tree->tree_id >= session->max_trees ||
        session->trees[batch->tree->tree_id] != batch->tree) {
        pthread_mutex_unlock(&session->lock);
        smb_vfs_session_unpin(shared, session);
        smb_doc_batch_free(batch->doc);
        free(batch);
        return 0;
    }
    pthread_mutex_lock(&shared->trees_lock);
    batch->tree->compound_pins++;
    pthread_mutex_unlock(&shared->trees_lock);
    pthread_mutex_unlock(&session->lock);

    for (int n = wire->complete_requests; n < wire->num_requests; n++) {
        struct chimera_smb_request *request = wire->requests[n];
        struct smb_vfs_command *command = &batch->commands[batch->count];
        struct chimera_smb_file_id id;
        unsigned int states_before = batch->num_states;
        int related = request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS;

        if (private_producer && private_producer->ops->private_suffix_eligible &&
            !private_producer->ops->private_suffix_eligible(private_producer, request)) {
            break;
        }

        /* Eligibility may inspect share capabilities (CREATE in particular).
         * Related commands inherit this effective context before construction. */
        if (related) {
            request->session_handle = &batch->session_handle;
            request->tree = batch->tree;
        }
        if (!smb_vfs_ops(request) ||
            (request->flags & CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED) ||
            (!related && (request->tree != batch->tree ||
                          !request->session_handle || request->session_handle->session != session))) {
            break;
        }
        if (!related) {
            saved.pid = UINT64_MAX;
            saved.vid = UINT64_MAX;
        }
        command->request = request;
        command->batch = batch;
        command->ops = smb_vfs_ops(request);
        command->max_read = batch->max_read;
        request->session_handle = &batch->session_handle;
        request->tree = batch->tree;
        command->inherit_error = related && batch->count && saved.pid == UINT64_MAX;
        if (command->ops->creates_open) {
            command->state = &batch->states[batch->num_states++];
            command->state->producer = command;
            command->state->handle_from = -1;
            command->state->range_owner_from = -1;
            if (!command->ops->initialize || command->ops->initialize(command)) {
                command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
            }
            command->state->open = command->open;
        } else {
            id = command->ops->file_id(request);
            command->inherits_file = related && (id.pid == UINT64_MAX || id.vid == UINT64_MAX);
            if (command->inherits_file) {
                id = saved;
                for (unsigned int i = 0; i < batch->num_states; i++) {
                    if (batch->states[i].open &&
                        batch->states[i].open->file_id.pid == id.pid &&
                        batch->states[i].open->file_id.vid == id.vid) {
                        command->state = &batch->states[i];
                        break;
                    }
                }
            }
            if (command->state && command->state->producer) {
                command->open = command->state->open;
            } else if (!command->inherit_error) {
                command->open = smb_vfs_pin_open(batch, command, id);
            }
            if (command->open && command->open->type == CHIMERA_SMB_OPEN_FILE_TYPE_PIPE) {
                if (command->handle) { chimera_vfs_release(wire->thread->vfs_thread, command->handle); }
                chimera_smb_open_file_release(request, command->open);
                command->handle = NULL;
                command->open = NULL;
                break;
            }
            if (command->open && !command->state) {
                unsigned int j;
                for (j = 0; j < batch->num_states; j++) {
                    if (batch->states[j].open == command->open) { break; }
                }
                if (j == batch->num_states) {
                    batch->states[batch->num_states].range_owner_from = -1;
                    batch->states[batch->num_states++].open = command->open;
                }
                command->state = &batch->states[j];
            }
        }
        bool wants_dynamic_doc = command->ops == &chimera_smb_rename_compound_ops &&
            command->state && command->state->producer;
        bool wants_doc = command->ops == &chimera_smb_disposition_compound_ops ||
            (command->ops == &chimera_smb_rename_compound_ops && !wants_dynamic_doc) ||
            (command->ops == &chimera_smb_close_compound_ops && chimera_smb_close_compound_uses_doc(command));
        bool wants_wait = request->smb2_hdr.command == SMB2_LOCK && request->lock.lock_count == 1 &&
            !(request->lock.l_flags & (0x00000004U /* UNLOCK */ | 0x00000010U /* FAIL_IMMEDIATELY */));
        wants_wait |= request->smb2_hdr.command == SMB2_CREATE &&
            ((request->create.create_disposition == SMB2_FILE_OVERWRITE ||
              request->create.create_disposition == SMB2_FILE_OVERWRITE_IF ||
              request->create.create_disposition == SMB2_FILE_SUPERSEDE) ||
             ((request->create.create_disposition == SMB2_FILE_OPEN ||
               request->create.create_disposition == SMB2_FILE_OPEN_IF) &&
              (request->create.desired_access & ~(SMB2_FILE_READ_ATTRIBUTES |
                  SMB2_FILE_WRITE_ATTRIBUTES | SMB2_READ_CONTROL | SMB2_SYNCHRONIZE))));
        bool wants_other_close = command->ops == &chimera_smb_close_compound_ops &&
            command->open && command->state && !command->state->producer && !wants_doc;
        /* Never hold DOC fences while LOCK or an OPEN cache recall waits for a
         * peer CLOSE needing that fence. Both runs retain native compounds. */
        if ((wants_doc && (batch->has_waiting_lock || batch->has_other_close_fence)) ||
            ((wants_wait || wants_other_close) && batch->has_doc) ||
            (wants_wait && (batch->has_dynamic_doc || batch->has_other_close_fence)) ||
            (wants_other_close && batch->has_waiting_lock) ||
            (command->ops->bound_eligible && !command->ops->bound_eligible(command))) {
            if (command->ops->creates_open) {
                if (command->ops->release) { command->ops->release(command); }
                if (command->ops->reply_release) { command->ops->reply_release(command); }
            } else if (!(command->state && command->state->producer)) {
                if (command->handle) { chimera_vfs_release(wire->thread->vfs_thread, command->handle); }
                if (command->open) { chimera_smb_open_file_release(request, command->open); }
            }
            command->handle = NULL;
            command->open = NULL;
            batch->num_states = states_before;
            break;
        }
        batch->has_doc |= wants_doc;
        batch->has_dynamic_doc |= wants_dynamic_doc;
        batch->has_waiting_lock |= wants_wait;
        batch->has_other_close_fence |= wants_other_close;
        if (command->open) { saved = command->open->file_id; }
        command->saved_pid = saved.pid;
        command->saved_vid = saved.vid;
        command->initial_state = command->state;
        if (!(command->state && command->state->producer)) {
            command->owned_open = command->open;
            command->owned_handle = command->handle;
        }
        batch->count++;
        if (command->ops->creates_open) { private_producer = command; }
        if (command->ops->ends_batch && command->ops->ends_batch(command)) { break; }
    }
    batch->next = wire->vfs_batches;
    wire->vfs_batches = batch;
    if (!batch->count) {
        /* No eligible file-backed command. Keep the harmless context pin until
         * the legacy request drains, avoiding a separate lifetime path. */
        return 0;
    }

    smb_vfs_gather_next(batch);
    return 1;
}
