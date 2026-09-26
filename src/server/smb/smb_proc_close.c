// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_doc_stream.h"
#include "smb_sharemode.h"
#include "smb_notify.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim_journal.h"

/* An embedded one-shot timer cannot fail allocation after unhashing an open.
 * The caller's logical reference is retained; no new lookup may acquire one.
 * A tree memory pin keeps its bucket locks valid even after TDIS/LOGOFF. */
static void chimera_smb_teardown_doc_start(void *private_data);
static void chimera_smb_teardown_stream_done(enum chimera_vfs_error, void *);

static void
smb_open_retire_finish(struct chimera_smb_open_file *open)
{
    struct chimera_server_smb_thread *thread = open->retire_thread;
    struct chimera_smb_tree *tree = open->tree;
    void (*done)(struct chimera_server_smb_thread *, struct chimera_smb_open_file *, void *) = open->retire_done;
    void *private_data = open->retire_private;
    LL_DELETE2(thread->retiring_opens, open, retire_next);
    open->retire_next = NULL;
    open->retire_done = NULL;
    done(thread, open, private_data);
    chimera_smb_tree_memory_unpin(thread->shared, tree);
}

static void
smb_open_retire_tick(struct evpl *evpl, struct evpl_timer *timer)
{
    struct chimera_smb_open_file *open = (struct chimera_smb_open_file *)
        ((char *) timer - offsetof(struct chimera_smb_open_file, retire_timer));
    if (open->retire_doc_running) { return; }
    if (atomic_load(&open->refcnt) != 1 ||
        !chimera_vfs_claim_access_owner_is_retired(open->access_owner) ||
        !chimera_vfs_claim_access_owner_is_retired(open->base_access_owner) ||
        (open->range_owner && !chimera_vfs_claim_owner_is_retired(open->range_owner))) {
        evpl_add_oneshot_timer(evpl, timer, smb_open_retire_tick, 1000);
        return;
    }
    struct chimera_server_smb_thread *thread = open->retire_thread;
    chimera_smb_open_file_drain_cache(thread, open);
    struct chimera_vfs_file_state *file = open->base_share_file_state ?
        open->base_share_file_state : open->share_file_state;
    if (!open->retire_skip_doc && open->handle && file) {
        if (!chimera_smb_doc_fence_acquire(&open->retire_doc_fence,
                &thread->shared->namespace_registry, file->fh, file->fh_len, open) ||
            !chimera_vfs_claim_access_fence_acquire(&open->retire_access_fence, file, open)) {
            chimera_smb_doc_fence_release(&open->retire_doc_fence);
            chimera_vfs_claim_access_fence_release(&open->retire_access_fence);
            evpl_add_oneshot_timer(evpl, timer, smb_open_retire_tick, 1000);
            return;
        }
    }
    if (!open->retire_skip_doc && open->handle) {
        struct chimera_smb_teardown_doc_ctx *ctx = &open->retire_doc_ctx;
        memset(ctx, 0, sizeof(*ctx));
        ctx->vfs_thread = thread->vfs_thread;
        ctx->retiring_open = open;
        if (chimera_smb_release_doc(thread, open, &ctx->doc_info)) {
            open->retire_doc_running = true;
            if (ctx->doc_info.smb_stream_delete) {
                struct chimera_smb_stream_delete *action = ctx->doc_info.smb_stream_delete;
                ctx->doc_info.smb_stream_delete = NULL;
                chimera_smb_stream_doc_run(action, chimera_smb_teardown_stream_done, ctx);
            } else {
                chimera_smb_teardown_doc_start(ctx);
            }
            return;
        }
    }
    smb_open_retire_finish(open);
}

void
chimera_smb_open_file_retire_async(
    struct chimera_server_smb_thread *thread, struct chimera_smb_open_file *open,
    void (*done)(struct chimera_server_smb_thread *, struct chimera_smb_open_file *, void *),
    void *private_data)
{
    chimera_smb_abort_if(open->retire_done != NULL, "duplicate logical open retirement");
    /* Live callers pin under their bucket lock before unhashing. Parked
     * durable opens retain a separate tree pin through their registry life. */
    open->tree->compound_pins++;
    open->retire_thread = thread;
    open->retire_done = done;
    open->retire_private = private_data;
    LL_PREPEND2(thread->retiring_opens, open, retire_next);
    memset(&open->retire_timer, 0, sizeof(open->retire_timer));
    chimera_smb_open_file_revoke_cache(thread, open);
    chimera_vfs_claim_access_owner_retire(open->access_owner);
    chimera_vfs_claim_access_owner_retire(open->base_access_owner);
    if (open->range_owner) { chimera_vfs_claim_owner_retire(open->range_owner, NULL, NULL); }
    smb_open_retire_tick(thread->evpl, &open->retire_timer);
}

static void
smb_retire_drain_wakeup(struct evpl *evpl, struct evpl_timer *timer)
{
    (void) evpl;
    (void) timer;
}

void
chimera_smb_open_file_retire_drain(struct chimera_server_smb_thread *thread)
{
    /* libevpl closes this worker's binds before its shutdown callback. Their
     * disconnect handlers cancel parked requests; other workers still run or
     * have already drained all of their compounds before exiting. Never stop
     * this loop while a callback can retain another worker's open/claim pin. */
    if (!thread->live_compounds && !thread->maintenance_compounds && !thread->retiring_opens) { return; }
    /* evpl_continue may poll after processing its last callback. Keep a
     * periodic no-op timer alive so that poll can never become unbounded. */
    struct evpl_timer wakeup = { 0 };
    evpl_add_timer(thread->evpl, &wakeup, smb_retire_drain_wakeup, 1000);
    while (thread->live_compounds || thread->maintenance_compounds || thread->retiring_opens) {
        while (thread->retiring_opens) {
            struct chimera_smb_open_file *open = thread->retiring_opens;
            evpl_remove_timer(thread->evpl, &open->retire_timer);
            smb_open_retire_tick(thread->evpl, &open->retire_timer);
            /* Restart after callbacks: cleanup may also retire another open.
             * A blocked head rearms its timer; the loop pumps peers below. */
            if (thread->retiring_opens == open) { break; }
        }
        if (thread->live_compounds || thread->maintenance_compounds || thread->retiring_opens) {
            evpl_continue(thread->evpl);
        }
    }
    evpl_remove_timer(thread->evpl, &wakeup);
}

static void chimera_smb_close_release(
    struct chimera_smb_request *request);
static void chimera_smb_close_range_tick(struct evpl *, struct evpl_timer *);

void
chimera_smb_break_caching_for_namespace(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_vfs_thread      *vfs_thread =
        request->compound->thread->vfs_thread;
    struct chimera_vfs_state       *vfs_state = vfs_thread->vfs->vfs_state;
    struct chimera_vfs_open_handle *oh        = open_file->handle;
    struct chimera_claim_actor      actor     = {
        .owner          = {
            .proto      = CHIMERA_CLAIM_PROTO_SMB2,
            .client_key = request->session_handle->session->client_key,
            .owner_lo   = open_file->file_id.pid,
            .owner_hi   = open_file->file_id.vid,
        },
        .op_handle      = open_file->handle,
    };

    if (!oh) {
        return;
    }

    /* An RqLs open is owned by its lease key, not its file_id: identify the
    * operating open by the key so its OWN lease is spared by the recall. */
    if (open_file->oplock_level == SMB2_OPLOCK_LEVEL_LEASE) {
        memcpy(actor.owner.key, open_file->lease_key, 16);
        memcpy(&actor.owner.owner_lo, open_file->lease_key, 8);
        memcpy(&actor.owner.owner_hi, open_file->lease_key + 8, 8);
    }

    chimera_vfs_claim_invalidate(
        vfs_state, oh->fh, oh->fh_len, oh->fh_hash,
        CHIMERA_TRIGGER_NS_UNLINK, &actor,
        CHIMERA_CLAIM_CR /* strip beyond-R holders to a read cache */);
} /* chimera_smb_break_caching_for_namespace */

/* Fire-and-forget completion for the persistent-handle record delete. */
static void
chimera_smb_close_durable_delete_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    (void) private_data;
} /* chimera_smb_close_durable_delete_callback */

/* Map a delete-on-close remove_at failure to the SMB CLOSE response status.
 * MS-FSA close semantics: the close itself succeeds, but the delete failure
 * (ENOTEMPTY on a non-empty directory most commonly, also EACCES) must be
 * reported to the client so it knows the object survived.  Silently turning
 * every failure into SUCCESS makes smb2_deltree believe its recursive teardown
 * worked, leaving a stale BASEDIR / leftover entries that wreck the next
 * subtest's setup. */
static inline uint32_t
chimera_smb_close_doc_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:        return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTEMPTY: return SMB2_STATUS_DIRECTORY_NOT_EMPTY;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:     return SMB2_STATUS_ACCESS_DENIED;
        case CHIMERA_VFS_ENOENT:
        /* A handle whose object is gone reports ESTALE now that the backends
         * distinguish it from a name that was never there; SMB has one status
         * for both. */
        case CHIMERA_VFS_ESTALE:    return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        default:                    return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_close_doc_status */

static void
chimera_smb_close_doc_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code) {
        chimera_smb_debug("delete-on-close: remove_at failed for '%.*s' (error %d)",
                          request->close.doc_info.name_len,
                          request->close.doc_info.name,
                          error_code);
    }

    chimera_vfs_release(vfs_thread, request->close.parent_handle);

    /* Close the backend VFS module handle that was detached from the cache */
    chimera_smb_doc_finish(vfs_thread, &request->close.doc_info);

    chimera_smb_open_file_release(request, request->close.open_file);

    chimera_smb_complete_request(request, chimera_smb_close_doc_status(
                                     request->close.stream_delete_status != CHIMERA_VFS_OK ?
                                     request->close.stream_delete_status : error_code));
} /* chimera_smb_close_doc_remove_callback */

static void
chimera_smb_close_doc_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        /* Cannot open parent directory — skip deletion, but still close backend */
        chimera_smb_debug("delete-on-close: failed to open parent dir for '%.*s' (error %d)",
                          request->close.doc_info.name_len,
                          request->close.doc_info.name,
                          error_code);

        chimera_smb_doc_finish(vfs_thread, &request->close.doc_info);

        chimera_smb_open_file_release(request, request->close.open_file);
        chimera_smb_complete_request(request, chimera_smb_close_doc_status(
                                         request->close.stream_delete_status != CHIMERA_VFS_OK ?
                                         request->close.stream_delete_status : error_code));
        return;
    }

    request->close.parent_handle = oh;

    /* Self-exempt a directory lease ONLY when the handle being closed here is the
     * one that carried delete-on-close: its ParentLeaseKey names the directory
     * lease whose cached view is coherent with the removal it caused, so spare it
     * (dirlease.unlink_same_*).  When the last handle to close is NOT the one that
     * set delete-on-close (a different open triggers the actual removal), the set
     * and closing parent keys differ, so no lease is spared and ALL directory
     * leases break (MS-SMB2; dirlease.unlink_different_*). */
    const uint8_t *unlink_skip = NULL;

    if (request->close.doc_skip_parent) {
        unlink_skip = request->close.doc_parent_lease_key;
    }

    struct chimera_claim_actor actor = {
        .owner = chimera_smb_open_actor_owner(request->close.open_file),
        .op_handle = request->close.open_file->handle,
    };
    chimera_vfs_remove_at_match_fh_actor(
        vfs_thread,
        &request->close.doc_info.cred,
        oh,
        request->close.doc_info.name,
        request->close.doc_info.name_len,
        request->close.doc_info.target_fh,
        request->close.doc_info.target_fh_len,
        0,
        0,
        unlink_skip,
        &actor,
        chimera_smb_close_doc_remove_callback,
        request);
} /* chimera_smb_close_doc_open_parent_callback */

/*
 * Request-less delete-on-close unlink for the abrupt-teardown path
 * (connection disconnect / session logoff / tree disconnect).
 *
 * The clean-CLOSE DOC path above threads parent_handle, doc_info and the
 * VFS-module close state through the live chimera_smb_request.  Teardown frees
 * open files with no request in hand, so this self-contained context carries a
 * heap-owned copy of the doc_info returned by chimera_vfs_release_doc and runs
 * the same open-parent -> remove_at -> close-backend sequence as a
 * fire-and-forget operation on the SMB thread's VFS thread.
 *
 * Only invoked for opens teardown is genuinely DISCARDING: a preserved
 * (courtesy-held / parked) durable handle never reaches release_doc in the
 * teardown path, so its file is never deleted on disconnect.
 */
struct smb_doc_peer_scan {
    struct chimera_smb_open_file *open;
    struct chimera_vfs_doc_info *consume;
    bool other;
};

static void
smb_doc_namespace_peer(void *context,
    const struct chimera_smb_namespace_snapshot *snapshot, void *private_data)
{
    struct smb_doc_peer_scan *scan = private_data;
    struct chimera_smb_open_file *peer = context;
    (void) snapshot;
    if (scan->consume) {
        const struct chimera_vfs_doc_info *doc = scan->consume;
        if (peer->parent_fh_len == doc->parent_fh_len && peer->name_len == doc->name_len &&
            !memcmp(peer->parent_fh, doc->parent_fh, peer->parent_fh_len) &&
            !memcmp(peer->name, doc->name, peer->name_len)) { peer->doc_from_create = 0; }
    } else if (peer != scan->open && !peer->doc_close_started) {
        scan->other = true;
    }
}

/* Cache handles are not SMB opens: RO/RW and detached handles may differ for
 * one inode. Serialize close retirement and delete ownership at the file's
 * SMB share reservations instead. This runs before those claims are drained. */
int
chimera_smb_release_doc(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file,
    struct chimera_vfs_doc_info      *doc_out)
{
    struct chimera_vfs_open_handle   *handle = open_file->handle;
    struct chimera_vfs_file_state    *file   = open_file->base_share_file_state ?
        open_file->base_share_file_state : open_file->share_file_state;
    struct chimera_vfs_doc_info       snapshot = { 0 }, backend = { 0 };
    struct chimera_vfs_doc_info      *pending = NULL;
    bool                              stream  = open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM;
    bool                              other   = false;
    int                               remove = 0, backend_doc;
    struct chimera_smb_stream_delete *stream_delete;

    memset(doc_out, 0, sizeof(*doc_out));
    if (!handle) {
        return 0;
    }
    stream_delete = chimera_smb_stream_doc_retire(thread, open_file);
    if (!file) {
        backend_doc                = chimera_vfs_release_doc(thread->vfs_thread, handle, doc_out);
        open_file->handle          = NULL;
        doc_out->smb_stream_delete = stream_delete;
        return backend_doc || stream_delete != NULL;
    }
    struct chimera_vfs_file_state *pin = chimera_vfs_state_get(file->state,
                                                               file->fh, file->fh_len, file->fh_hash, false);
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&file->lock);
    if (!stream && !file->smb_delete_started &&
        (open_file->doc_from_create ||
         ((open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) && file->delete_pending))) {
        /* Logical open identity is authoritative: a cached handle can be
         * shared by aliases and ordinary clear cannot cancel CREATE mode. */
        snapshot.parent_fh_len = open_file->parent_fh_len;
        snapshot.name_len = open_file->name_len;
        snapshot.cred = open_file->stream_delete_cred;
        memcpy(snapshot.parent_fh, open_file->parent_fh, snapshot.parent_fh_len);
        memcpy(snapshot.name, open_file->name, snapshot.name_len);
        snapshot.target_fh_len = handle->fh_len;
        memcpy(snapshot.target_fh, handle->fh, handle->fh_len);
        if (snapshot.parent_fh_len) {
            pending = malloc(sizeof(*pending));
            chimera_smb_abort_if(!pending, "delete metadata allocation failed");
            *pending = snapshot;
        }
    }

    if (pending) {
        file->delete_pending = 1;
        if (!file->smb_pending_delete) {
            file->smb_pending_delete = pending;
            pending                  = NULL;
        }
    }
    open_file->doc_close_started = 1;
    struct smb_doc_peer_scan peers = { .open = open_file };
    chimera_smb_namespace_foreach_locked(registry, file->fh, file->fh_len,
        smb_doc_namespace_peer, &peers);
    other = peers.other;
    /* Pending legacy admission can precede namespace publication. Retain this
     * admission scan in addition to independent retired-but-live membership. */

    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         claim; claim = claim->next) {
        if (claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
            claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) {
            struct chimera_smb_open_file *peer = claim->cb_private;
            if (!peer || (peer != open_file && !peer->doc_close_started)) {
                other = true;
                break;
            }
        }
    }
    /* POSIX disposition removes the link when its deleting open closes,
     * while ordinary SMB disposition waits for the final logical open. */
    if ((!other || (!stream && open_file->doc_posix &&
                    (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE))) &&
        !file->smb_delete_started && file->delete_pending && file->smb_pending_delete) {
        *doc_out               = *(struct chimera_vfs_doc_info *) file->smb_pending_delete;
        doc_out->pending_state = pin;
        free(file->smb_pending_delete);
        file->smb_pending_delete = NULL;
        file->smb_delete_started = 1;
        /* Consume CREATE-DOC on this link, including surviving POSIX peers.
         * Other hardlinks to the inode retain their independent close intent. */
        for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
             claim; claim = claim->next) {
            if (claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
                claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) {
                struct chimera_smb_open_file *peer = claim->cb_private;
                if (peer && peer->parent_fh_len == doc_out->parent_fh_len &&
                    peer->name_len == doc_out->name_len &&
                    !memcmp(peer->parent_fh, doc_out->parent_fh, peer->parent_fh_len) &&
                    !memcmp(peer->name, doc_out->name, peer->name_len)) {
                    peer->doc_from_create = 0;
                }
            }
        }
        peers.consume = doc_out;
        chimera_smb_namespace_foreach_locked(registry, file->fh, file->fh_len,
            smb_doc_namespace_peer, &peers);
        remove = 1;
    }
    pthread_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
    if (!remove) {
        chimera_vfs_state_put(file->state, pin);
    }
    free(pending);

    backend_doc       = chimera_vfs_release_doc(thread->vfs_thread, handle, &backend);
    open_file->handle = NULL;
    if (backend_doc) {
        if (remove) {
            doc_out->close_ref = backend.close_ref;
        } else {
            chimera_vfs_close_ref_dispatch(thread->vfs_thread, &backend.close_ref, NULL, NULL);
        }
    }
    doc_out->smb_stream_delete = stream_delete;
    return remove || stream_delete != NULL;
} /* chimera_smb_release_doc */

void
chimera_smb_doc_finish(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_doc_info *doc)
{
    chimera_vfs_close_ref_dispatch(thread, &doc->close_ref, NULL, NULL);
    if (doc->pending_state) {
        /* Base-file identities can have another live hardlink. Consume this
         * link's intent, but permit a later explicit delete of another link. */
        pthread_mutex_lock(&doc->pending_state->lock);
        doc->pending_state->delete_pending     = 0;
        doc->pending_state->smb_delete_started = 0;
        free(doc->pending_state->smb_pending_delete);
        doc->pending_state->smb_pending_delete = NULL;
        pthread_mutex_unlock(&doc->pending_state->lock);
        chimera_vfs_state_put(doc->pending_state->state, doc->pending_state);
    }
} /* chimera_smb_doc_finish */

static void
chimera_smb_teardown_doc_close_backend(struct chimera_smb_teardown_doc_ctx *ctx)
{
    chimera_smb_doc_finish(ctx->vfs_thread, &ctx->doc_info);
    if (ctx->retiring_open) {
        struct chimera_smb_open_file *open = ctx->retiring_open;
        open->retire_doc_running = false;
        smb_open_retire_finish(open);
    } else {
        free(ctx);
    }
} /* chimera_smb_teardown_doc_close_backend */

static void
chimera_smb_teardown_doc_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_teardown_doc_ctx *ctx = private_data;

    (void) pre_attr;
    (void) post_attr;

    if (error_code) {
        chimera_smb_debug("teardown delete-on-close: remove_at failed for "
                          "'%.*s' (error %d)",
                          ctx->doc_info.name_len,
                          ctx->doc_info.name,
                          error_code);
    }

    chimera_vfs_release(ctx->vfs_thread, ctx->parent_handle);
    chimera_smb_teardown_doc_close_backend(ctx);
} /* chimera_smb_teardown_doc_remove_callback */

static void
chimera_smb_teardown_doc_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_teardown_doc_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        /* Cannot open parent — skip the unlink but still close the backend
         * handle that release_doc detached from the cache. */
        chimera_smb_debug("teardown delete-on-close: failed to open parent dir "
                          "for '%.*s' (error %d)",
                          ctx->doc_info.name_len,
                          ctx->doc_info.name,
                          error_code);
        chimera_smb_teardown_doc_close_backend(ctx);
        return;
    }

    ctx->parent_handle = oh;

    /* Teardown has no parent-lease exemption. Match the saved object identity
    * so a replacement at the deferred path cannot be removed accidentally. */
    chimera_vfs_remove_at_match_fh(
        ctx->vfs_thread,
        &ctx->doc_info.cred,
        oh,
        ctx->doc_info.name,
        ctx->doc_info.name_len,
        ctx->doc_info.target_fh,
        ctx->doc_info.target_fh_len,
        0,
        0,
        NULL,
        chimera_smb_teardown_doc_remove_callback,
        ctx);
} /* chimera_smb_teardown_doc_open_parent_callback */

static void
chimera_smb_teardown_doc_start(void *private_data)
{
    struct chimera_smb_teardown_doc_ctx *ctx = private_data;

    if (!ctx->doc_info.parent_fh_len) {
        chimera_smb_teardown_doc_close_backend(ctx);
        return;
    }
    chimera_vfs_open_fh(ctx->vfs_thread, &ctx->doc_info.cred,
                        ctx->doc_info.parent_fh, ctx->doc_info.parent_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_smb_teardown_doc_open_parent_callback, ctx);
} /* chimera_smb_teardown_doc_start */

static void
chimera_smb_teardown_stream_done(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    /* Teardown has no reply channel, but must still process any base intent. */
    chimera_smb_teardown_doc_start(private_data);
} /* chimera_smb_teardown_stream_done */

void
chimera_smb_teardown_doc_unlink(
    struct chimera_server_smb_thread  *thread,
    const struct chimera_vfs_doc_info *doc_info)
{
    struct chimera_smb_teardown_doc_ctx *ctx = malloc(sizeof(*ctx));

    ctx->vfs_thread    = thread->vfs_thread;
    ctx->doc_info      = *doc_info;
    ctx->parent_handle = NULL;
    ctx->retiring_open = NULL;
    if (ctx->doc_info.smb_stream_delete) {
        struct chimera_smb_stream_delete *action = ctx->doc_info.smb_stream_delete;
        ctx->doc_info.smb_stream_delete = NULL;
        chimera_smb_stream_doc_run(action, chimera_smb_teardown_stream_done, ctx);
    } else {
        chimera_smb_teardown_doc_start(ctx);
    }
} /* chimera_smb_teardown_doc_unlink */

static void chimera_smb_close_finish_doc(
    void *private_data);

static void
chimera_smb_close_stream_done(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;

    request->close.stream_delete_status = status;
    chimera_smb_close_finish_doc(private_data);
} /* chimera_smb_close_stream_done */

static void
chimera_smb_close_finish_doc(void *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (request->close.doc_info.smb_stream_delete) {
        struct chimera_smb_stream_delete *action = request->close.doc_info.smb_stream_delete;
        request->close.doc_info.smb_stream_delete = NULL;
        chimera_smb_stream_doc_run(action, chimera_smb_close_stream_done, request);
        return;
    }
    if (request->close.doc_info.parent_fh_len) {
        chimera_vfs_open_fh(request->compound->thread->vfs_thread,
                            &request->close.doc_info.cred, request->close.doc_info.parent_fh,
                            request->close.doc_info.parent_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                            chimera_smb_close_doc_open_parent_callback, request);
    } else {
        chimera_smb_doc_finish(request->compound->thread->vfs_thread,
                               &request->close.doc_info);
        chimera_smb_open_file_release(request, request->close.open_file);
        chimera_smb_complete_request(request,
                                     chimera_smb_close_doc_status(request->close.stream_delete_status));
    }
} /* chimera_smb_close_finish_doc */

/*
 * Release the VFS handle and check for delete-on-close.
 *
 * If this was the last reference and DOC was set on the VFS handle,
 * perform the unlink synchronously before completing the close request.
 * Otherwise just release and complete.
 */
static void
chimera_smb_close_after_range(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->close.open_file;
    struct chimera_server_smb_thread *thread = request->compound->thread;

    /* A compound holding the DOC fence may be recalling this cache grant.
     * Settle that recall before waiting for the guarded namespace phase. */
    chimera_smb_open_file_drain_cache(thread, open_file);

    request->close.stream_delete_status = CHIMERA_VFS_OK;

    if (!open_file->handle) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    struct chimera_vfs_file_state *file = open_file->base_share_file_state ?
        open_file->base_share_file_state : open_file->share_file_state;
    if (file &&
        (!chimera_smb_doc_fence_acquire(request->namespace_fence,
            &thread->shared->namespace_registry, file->fh, file->fh_len, request) ||
         !chimera_vfs_claim_access_fence_acquire(&request->namespace_access_fence, file, request))) {
        chimera_smb_doc_fence_release(request->namespace_fence);
        chimera_vfs_claim_access_fence_release(&request->namespace_access_fence);
        evpl_add_oneshot_timer(thread->evpl, &request->close.range_retire_timer,
                              chimera_smb_close_range_tick, 1000);
        return;
    }

    request->close.doc_skip_parent = open_file->doc_from_create ||
        (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE);
    memcpy(request->close.doc_parent_lease_key, open_file->parent_lease_key, 16);
    chimera_smb_release_doc(request->compound->thread, open_file,
                            &request->close.doc_info);
    chimera_smb_close_finish_doc(request);
} /* chimera_smb_close_release */

/* Owner retirement may be completed by another worker publishing or
 * rejecting its compound. Poll only a pinned owner from this request's loop:
 * no cross-thread callback retains a raw request or can complete it twice. */
static void
chimera_smb_close_range_tick(struct evpl *evpl, struct evpl_timer *timer)
{
    struct chimera_smb_request *request = (struct chimera_smb_request *)
        ((char *) timer - offsetof(struct chimera_smb_request, close.range_retire_timer));
    struct chimera_smb_open_file *open = request->close.open_file;
    if (!chimera_vfs_claim_access_owner_is_retired(open->access_owner) ||
        !chimera_vfs_claim_access_owner_is_retired(open->base_access_owner) ||
        (request->close.range_retire_owner &&
         !chimera_vfs_claim_owner_is_retired(request->close.range_retire_owner))) {
        evpl_add_oneshot_timer(evpl, timer, chimera_smb_close_range_tick, 1000);
        return;
    }
    if (request->close.range_retire_owner) {
        chimera_vfs_claim_owner_put(request->close.range_retire_owner);
        request->close.range_retire_owner = NULL;
    }
    chimera_smb_close_after_range(request);
}

static void
chimera_smb_close_release(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open = request->close.open_file;
    chimera_smb_open_file_revoke_cache(request->compound->thread, open);
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
    struct chimera_vfs_claim_owner *owner = open->range_owner;
    if (owner) { chimera_vfs_claim_owner_ref(owner); }
    pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
    request->close.range_retire_owner = owner;
    chimera_vfs_claim_access_owner_retire(open->access_owner);
    chimera_vfs_claim_access_owner_retire(open->base_access_owner);
    if (owner) { chimera_vfs_claim_owner_retire(owner, NULL, NULL); }
    if (!chimera_vfs_claim_access_owner_is_retired(open->access_owner) ||
        !chimera_vfs_claim_access_owner_is_retired(open->base_access_owner) ||
        (owner && !chimera_vfs_claim_owner_is_retired(owner))) {
        memset(&request->close.range_retire_timer, 0, sizeof(request->close.range_retire_timer));
        evpl_add_oneshot_timer(request->compound->thread->evpl, &request->close.range_retire_timer,
                              chimera_smb_close_range_tick, 1000);
        return;
    }
    if (owner) {
        chimera_vfs_claim_owner_put(owner);
        request->close.range_retire_owner = NULL;
    }
    chimera_smb_close_after_range(request);
}

static void
chimera_smb_close_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (unlikely(error_code)) {
        memset(&request->close.r_attrs, 0, sizeof(request->close.r_attrs));
    } else {
        chimera_smb_marshal_open_attrs(attr,
            request->close.open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM,
            &request->close.r_attrs);
    }

    /* Always go through close_release so DOC fires even if getattr failed */
    chimera_smb_close_release(request);
} /* chimera_smb_close_getattr_callback */


void
chimera_smb_close(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;

    /* Reserve the guard before irreversible unhashing. */
    request->namespace_fence = calloc(1, sizeof(*request->namespace_fence));
    if (!request->namespace_fence) {
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    memset(&request->close.range_retire_timer, 0, sizeof(request->close.range_retire_timer));
    request->close.open_file = chimera_smb_open_file_close(request, &request->close.file_id);

    if (unlikely(!request->close.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* A blocking byte-range LOCK still parked on this handle is aborted by the
     * close and completed with RANGE_NOT_LOCKED (MS-SMB2; smb2.lock.cancel
     * "cancel by close").  The open is already unhashed + marked CLOSED above, so
     * the compound wait observes the cutoff and completes on its worker. */
    unsigned bucket = request->close.open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&request->close.open_file->tree->open_files_lock[bucket]);
    chimera_smb_lock_abort_parked(request->close.open_file);
    pthread_mutex_unlock(&request->close.open_file->tree->open_files_lock[bucket]);

    /* Clean up any notify watches on this open file */
    chimera_smb_notify_close(thread->shared->vfs->vfs_notify,
        chimera_smb_notify_detach(request->close.open_file));

    /* Deferred directory-lease content break for a modified file: a write does
     * not break the parent dir lease at write time (the file's directory-visible
     * size/mtime settle only at close), so emit the break now.  Self-exempt the
     * directory lease named by this handle's ParentLeaseKey -- a writer that
     * holds the parent's lease keeps its cached view coherent (MS-SMB2;
     * dirlease.v2_request "only the close on the modified file break[s] the
     * directory lease", and the valid-parent-key write closes without a break). */
    if ((request->close.open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_MODIFIED) &&
        request->close.open_file->parent_fh_len > 0) {
        struct chimera_claim_actor actor = {
            .owner = chimera_smb_open_actor_owner(request->close.open_file),
        };
        memcpy(actor.owner.key, request->close.open_file->parent_lease_key, 16);

        chimera_vfs_notify_emit_actor(thread->shared->vfs->vfs_notify,
                                      request->close.open_file->parent_fh,
                                      request->close.open_file->parent_fh_len,
                                      CHIMERA_VFS_NOTIFY_FILE_MODIFIED,
                                      request->close.open_file->name,
                                      request->close.open_file->name_len,
                                      NULL, 0,
                                      chimera_claim_owner_has_key(&actor.owner) ? &actor : NULL);
    }

    /* Release share mode entry for regular file opens */
    if (request->close.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
        request->tree->share) {
        chimera_smb_sharemode_release(&request->tree->share->sharemode,
                                      request->close.open_file);
    }

    /* Persistent handle: delete its backend record so a server restart does
    * not resurrect a handle the client explicitly closed.  Best-effort and
    * fire-and-forget; routed to the share's backend via the file handle. */
    if ((request->close.open_file->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) &&
        request->close.open_file->handle) {
        uint8_t  dkey[CHIMERA_SMB_DURABLE_KEY_LEN];
        uint32_t dkey_len = chimera_smb_durable_key(dkey, request->close.open_file->file_id.pid);

        chimera_vfs_delete_key_at(thread->vfs_thread,
                                  &request->session_handle->session->cred,
                                  request->close.open_file->handle->fh,
                                  request->close.open_file->handle->fh_len,
                                  dkey, dkey_len,
                                  chimera_smb_close_durable_delete_callback, NULL);
    }

    if ((request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) &&
        request->close.open_file->handle) {

        /* Named-pipe FIDs carry handle==NULL; fall through to the zero-attrs
         * path rather than dereferencing NULL in getattr (MS-SMB2 3.3.5.10). */
        /* The POSTQUERY response is FILE_NETWORK_OPEN_INFORMATION whose first
         * field is CreationTime (BTIME).  MASK_STAT deliberately omits BTIME, so
         * request it explicitly or CreationTime is emitted as 0 (issue #1117). */
        chimera_vfs_getattr(thread->vfs_thread,
                            &request->session_handle->session->cred,
                            request->close.open_file->handle,
                            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME,
                            chimera_smb_close_getattr_callback,
                            request);

    } else {
        memset(&request->close.r_attrs, 0, sizeof(request->close.r_attrs));
        chimera_smb_close_release(request);
    }

} /* chimera_smb_close */

void
chimera_smb_close_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_CLOSE_REPLY_SIZE);
    /* Emit only the defined POSTQUERY_ATTRIB bit; do not reflect reserved client
     * Flags bits (MS-SMB2 3.3.5.10 / 2.2.16). */
    evpl_iovec_cursor_append_uint16(reply_cursor,
                                    request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB);

    if (request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {
        chimera_smb_append_network_open_info(reply_cursor, &request->close.r_attrs);
    } else {
        chimera_smb_append_null_network_open_info_null(reply_cursor);
    }

} /* chimera_smb_close_reply */

int
chimera_smb_parse_close(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{

    if (unlikely(request->request_struct_size != SMB2_CLOSE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CLOSE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_CLOSE_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->close.flags);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->close.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->close.file_id.vid);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 CLOSE request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    return 0;
} /* chimera_smb_parse_close */

#include "smb_compound.h"
#include "smb_doc_compound.h"

struct smb_close_attempt {
    struct chimera_smb_attrs attrs;
    struct chimera_smb_namespace_path path;
    struct chimera_smb_doc_fence fence;
    struct chimera_smb_doc_fence base_fence;
    int modified;
    bool committed;
    bool consumed;
    bool doc_mode;
    bool cache_close;
    bool cache_detached;
    struct chimera_vfs_claim_grant *cache_grant;
    struct chimera_vfs_file_state *cache_file;
    bool persisted;
    enum chimera_vfs_error record_delete_status;
};

static int
smb_close_compound_eligible(struct chimera_smb_request *request)
{
    (void) request;
    return 1;
}

/* Cache CLOSE ends the run. Atomic member acquisition keeps a concurrent
 * same-key CREATE visible to last-member retirement, even before it replies. */
static bool
smb_close_cache_construct(struct chimera_smb_open_file *open,
                          enum chimera_claim_construct construct)
{
    if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) {
        return construct == CHIMERA_CONSTRUCT_DIR_LEASE;
    }
    return construct == CHIMERA_CONSTRUCT_OPLOCK_II ||
           construct == CHIMERA_CONSTRUCT_OPLOCK_EX ||
           construct == CHIMERA_CONSTRUCT_OPLOCK_BATCH ||
           construct == CHIMERA_CONSTRUCT_RQLS;
}

static bool
smb_close_cache_present(struct smb_vfs_command *command)
{
    struct chimera_smb_open_file *open = command->open;
    if (!open || (command->state && command->state->producer)) { return false; }
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
    bool present = !(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) && open->grant;
    pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
    return present;
}

static bool
smb_close_cache_eligible(struct smb_vfs_command *command)
{
    struct chimera_smb_open_file *open = command->open;
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    bool eligible = true;
    pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
    if (!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) && open->grant) {
        struct chimera_vfs_file_state *file = open->caching_file_state;
        /* Named streams cache their own data identity; the separate base
         * ACCESS owner is retired by the same journal. Directory leases use
         * this identity too, with a DIR_LEASE rather than RqLs construct. */
        eligible = file && file == open->grant->file && file == open->share_file_state;
        if (eligible) {
            pthread_mutex_lock(&file->lock);
            eligible = smb_close_cache_construct(open, open->grant->claim.construct);
            pthread_mutex_unlock(&file->lock);
        }
    }
    pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
    return eligible;
}

/* Construction-only ownership, not an execution-time protocol side effect.
 * The bucket cutoff prevents a legacy CLOSE from freeing grant storage while
 * we take a grant/file pin. No member, mode, epoch, or break state is changed. */
static bool
smb_close_cache_pin(struct smb_vfs_command *command)
{
    struct chimera_smb_open_file *open = command->open;
    struct smb_close_attempt *attempt = command->private_data;
    struct chimera_vfs_state *state = command->request->compound->thread->vfs_thread->vfs->vfs_state;
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    bool valid = true;
    pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
    if (!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) && open->grant) {
        struct chimera_vfs_file_state *file = open->caching_file_state;
        attempt->cache_close = true;
        valid = file && file == open->share_file_state && file == open->grant->file;
        if (valid) {
            attempt->cache_file = chimera_vfs_state_get(state, file->fh, file->fh_len,
                                                         file->fh_hash, false);
            valid = attempt->cache_file == file;
        }
        if (valid) {
            pthread_mutex_lock(&file->lock);
            valid = smb_close_cache_construct(open, open->grant->claim.construct);
            if (valid) { attempt->cache_grant = chimera_vfs_claim_pin_grant(&open->grant->claim); }
            pthread_mutex_unlock(&file->lock);
            valid &= attempt->cache_grant != NULL;
        }
    }
    pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
    return valid;
}

bool
chimera_smb_close_compound_uses_doc(struct smb_vfs_command *command)
{
    return command->open && !command->open->durable_flags &&
        !(command->open->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED) &&
        !smb_close_cache_present(command) && smb_doc_close_allowed(command);
}

static bool smb_close_has_doc(struct smb_vfs_command *command);

/* A stream's primary handle does not anchor the file-level DELETE claim.
 * Keep and validate its separate base handle through both journal retirements,
 * including a durable open returned by warm reconnect. Request references keep
 * these identities alive; the bucket excludes concurrent logical CLOSE while
 * we inspect them. The base lock protects the canonical claim fields. */
static bool
smb_close_stream_identity(struct chimera_smb_open_file *open)
{
    if (!(open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM)) { return true; }
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
    struct chimera_vfs_file_state *base = open->base_share_file_state;
    struct chimera_vfs_open_handle *handle = open->base_handle;
    bool valid = !(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) &&
        open->base_access_owner && base && handle &&
        base != open->share_file_state && open->base_fh_len == base->fh_len &&
        handle->fh_len == base->fh_len &&
        !memcmp(open->base_fh, base->fh, base->fh_len) &&
        !memcmp(handle->fh, base->fh, base->fh_len);
    if (valid) {
        pthread_mutex_lock(&base->lock);
        struct chimera_vfs_claim *claim = chimera_smb_base_share_claim(open);
        valid = claim->file == base && claim->op_handle == handle;
        pthread_mutex_unlock(&base->lock);
    }
    pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
    return valid;
}

static int
smb_close_compound_bound_eligible(struct smb_vfs_command *command)
{
    struct chimera_smb_open_file *open = command->open;
    if (!open || (command->state && command->state->producer)) { return 1; }
    struct chimera_vfs_file_state *file = open->share_file_state;
    return command->handle && file && open->access_owner &&
        open->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
        smb_close_stream_identity(open) &&
        smb_close_cache_eligible(command) &&
        file->fh_len == command->handle->fh_len &&
        !memcmp(file->fh, command->handle->fh, file->fh_len) &&
        (chimera_smb_close_compound_uses_doc(command) || !smb_close_has_doc(command));
}

/* The fence prevents new intent/path changes between this check and accepted
 * retirement. Existing peer intent remains a deliberate legacy boundary. */
static bool
smb_close_file_has_doc(struct smb_vfs_command *command,
                        struct chimera_vfs_file_state *file,
                        struct chimera_vfs_open_handle *handle)
{
    struct chimera_server_smb_thread *thread = command->request->compound->thread;
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    bool pending;
    chimera_smb_namespace_lock(registry);
    pthread_mutex_lock(&file->lock);
    pending = file->delete_pending || file->smb_pending_delete || file->smb_delete_started ||
        chimera_smb_open_namespace_has_doc_locked(registry, file->fh, file->fh_len);
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_ACCESS];
         !pending && claim; claim = claim->next) {
        if (claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN ||
            claim->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) {
            struct chimera_smb_open_file *peer = claim->cb_private;
            pending = peer && (peer->doc_from_create ||
                (peer->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE));
        }
    }
    struct vfs_open_cache *cache = handle ? chimera_vfs_get_cache_for_handle(thread->vfs_thread, handle) : NULL;
    if (cache) {
        struct vfs_open_cache_shard *shard = &cache->shards[handle->fh_hash & cache->shard_mask];
        pthread_mutex_lock(&shard->lock);
        pending |= handle->doc_delete_on_close;
        pthread_mutex_unlock(&shard->lock);
    }
    pthread_mutex_unlock(&file->lock);
    chimera_smb_namespace_unlock(registry);
    return pending;
}

static bool
smb_close_has_doc(struct smb_vfs_command *command)
{
    struct chimera_smb_open_file *open = command->open;
    return smb_close_file_has_doc(command, open->share_file_state, command->handle) ||
        ((open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) &&
         (!open->base_share_file_state ||
          smb_close_file_has_doc(command, open->base_share_file_state, NULL)));
}

static void
smb_close_coordinate(struct chimera_vfs_compound *compound, uint32_t index,
                     uint64_t token, const uint8_t *fh, uint32_t fh_len, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_close_attempt *attempt = command->private_data;
    (void) index;
    bool acquired = chimera_smb_doc_fence_acquire(&attempt->fence,
        &command->request->compound->thread->shared->namespace_registry,
        fh, fh_len, command->batch);
    if (acquired && (command->open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM)) {
        /* A stream owns an ACCESS reservation on its base too. A peer's base
         * delete intent must neither appear nor become last-close work while
         * this ordinary CLOSE is awaiting acceptance. Acquire both without
         * waiting, and leave no partial fence set on conflict. */
        acquired = chimera_smb_doc_fence_acquire(&attempt->base_fence,
            &command->request->compound->thread->shared->namespace_registry,
            command->open->base_fh, command->open->base_fh_len, command->batch);
        if (!acquired) { chimera_smb_doc_fence_release(&attempt->fence); }
    }
    if (!acquired && attempt->cache_close) {
        /* A peer can satisfy a cache recall by CLOSE. Waiting here while its
         * namespace fence is held would prevent that CLOSE dropping rights.
         * Legacy CLOSE revokes rights before waiting for namespace admission. */
        chimera_smb_compound_defer(command);
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EINTR);
        return;
    }
    chimera_vfs_compound_coordinate_done(compound, token,
        acquired ? CHIMERA_VFS_OK : CHIMERA_VFS_EBUSY);
}

static void
smb_close_coordinate_complete(struct chimera_vfs_compound *compound, uint32_t index,
    enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_close_attempt *attempt = command->private_data;
    (void) compound; (void) index;
    if (attempt->cache_close && *status == CHIMERA_VFS_EINTR) {
        chimera_smb_compound_defer(command);
    }
}

static struct chimera_smb_file_id
smb_close_compound_file_id(struct chimera_smb_request *request)
{
    return request->close.file_id;
}

static void
smb_close_attrs_complete(struct chimera_vfs_compound *compound, uint32_t index,
                         enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_close_attempt *attempt = command->private_data;
    memset(&attempt->attrs, 0, sizeof(attempt->attrs));
    if (*status == CHIMERA_VFS_OK) {
        chimera_smb_marshal_open_attrs(&chimera_vfs_compound_op(compound, index)->attr,
            command->open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM, &attempt->attrs);
    }
    /* POSTQUERY failure has never prevented CLOSE itself. */
    *status = CHIMERA_VFS_OK;
}

static void
smb_close_access_prepare(struct chimera_vfs_compound *compound, uint32_t index,
                          enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_close_attempt *attempt = command->private_data;
    smb_doc_command_path(command, &attempt->path);
    /* Watch admission uses this same DOC fence. Existing attachments remain
     * public and live until accepted publication; replay never closes a watch. */
    if (attempt->cache_grant) {
        struct chimera_smb_open_file *open = command->open;
        unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
        pthread_mutex_lock(&open->tree->open_files_lock[bucket]);
        bool closed = !!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED);
        bool same = open->grant == attempt->cache_grant &&
            open->caching_file_state == attempt->cache_file;
        pthread_mutex_unlock(&open->tree->open_files_lock[bucket]);
        if (closed || !same) {
            if (closed) { command->status = SMB2_STATUS_FILE_CLOSED; }
            else { chimera_smb_compound_defer(command); }
            *status = closed ? CHIMERA_VFS_EINVAL : CHIMERA_VFS_EINTR;
            return;
        }
    }
    if (!command->state->producer && !smb_close_stream_identity(command->open)) {
        command->status = SMB2_STATUS_FILE_CLOSED;
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    if (!command->state->producer && !attempt->doc_mode && smb_close_has_doc(command)) {
        if (attempt->cache_close) {
            chimera_smb_compound_defer(command);
            *status = CHIMERA_VFS_EINTR;
        } else {
            command->status = SMB2_STATUS_FILE_NOT_AVAILABLE;
            *status = CHIMERA_VFS_EBUSY;
        }
        return;
    }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    op->access_retire_owner = command->state->access_owner;
    op->range_retire_owner = command->state->range_owner;
    op->base_access_retire_owner = command->state->base_access_owner;
}

static void
smb_close_retire_complete(struct chimera_vfs_compound *compound, uint32_t index,
                          enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_close_attempt *attempt = command->private_data;
    (void) compound; (void) index;
    if (*status == CHIMERA_VFS_OK) {
        /* An accepted prefix which retired the claims is already a logical
         * CLOSE, even if cancellation skips its resource-only final op. */
        attempt->committed = true;
        attempt->modified = !!(command->state->flags & CHIMERA_SMB_OPEN_FILE_FLAG_MODIFIED);
        command->state->closed = 1;
    }
}

/* Preserve CLOSE's existing best-effort record deletion policy, but keep the
 * backend operation inside the accepted compound. No retryable callback logs,
 * unhashes a FileId, or starts an independent maintenance request. The retained
 * record on backend failure still needs a repair queue/tombstone guarantee. */
static void
smb_close_record_deleted(struct chimera_vfs_compound *compound, uint32_t index,
                         enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    struct smb_close_attempt *attempt = command->private_data;
    (void) compound; (void) index;
    attempt->record_delete_status = *status;
    *status = CHIMERA_VFS_OK;
}

static int
smb_close_compound_build(struct chimera_vfs_compound *compound,
                         struct smb_vfs_command *command)
{
    command->private_data = calloc(1, sizeof(struct smb_close_attempt));
    if (!command->private_data) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        return -1;
    }
    struct smb_close_attempt *attempt = command->private_data;
    attempt->persisted = !command->state->producer &&
        (command->open->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED);
    if (!command->state->producer && !smb_close_cache_pin(command)) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        return -1;
    }
    attempt->doc_mode = !command->state->producer && !attempt->cache_close &&
        chimera_smb_close_compound_uses_doc(command);
    if (attempt->doc_mode) {
        if (smb_doc_close_add_coordinate(compound, command) < 0) { return -1; }
    } else if (!command->state->producer) {
        int coord = chimera_vfs_compound_add_coordinate(compound, smb_close_coordinate, command);
        chimera_vfs_compound_set_op_callbacks(compound, coord, NULL,
                                              smb_close_coordinate_complete, command);
    }
    if (command->request->close.flags & SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB) {
        int attrs = chimera_vfs_compound_add_getattr(compound,
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_BTIME);
        chimera_vfs_compound_set_op_callbacks(compound, attrs, NULL,
                                              smb_close_attrs_complete, command);
    }
    int retire = chimera_vfs_compound_add_retire_open_claims(compound, NULL, NULL, NULL);
    chimera_vfs_compound_set_op_callbacks(compound, retire, smb_close_access_prepare,
                                         smb_close_retire_complete, command);
    if (attempt->doc_mode) {
        int close = smb_doc_close_build_tail(compound, command);
        if (close < 0 || !chimera_vfs_compound_set_cancel_scope(compound, retire, close)) {
            command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
            return -1;
        }
        return close;
    }
    /* POSTQUERY may have upgraded the cursor into an executor-owned PATH
     * handle. CLOSE must consume this command's precise retained reference. */
    if (command->state->producer) {
        chimera_vfs_compound_add_puthandle_from(compound, command->state->handle_from,
                                               CHIMERA_VFS_OPEN_INFERRED);
    } else {
        chimera_vfs_compound_add_puthandle(compound, command->handle,
                                          CHIMERA_VFS_OPEN_INFERRED);
    }
    if (attempt->persisted) {
        uint8_t key[CHIMERA_SMB_DURABLE_KEY_LEN];
        uint32_t key_len = chimera_smb_durable_key(key, command->open->file_id.pid);
        int remove = chimera_vfs_compound_add_delete_key_at(compound, key, key_len);
        chimera_vfs_compound_set_op_callbacks(compound, remove, NULL,
                                              smb_close_record_deleted, command);
    }
    int close = chimera_vfs_compound_add_close(compound);
    /* Once retirement succeeds, cancellation must not skip record deletion.
     * Conversely retirement can fail, so delete must never run first and leave
     * a public durable open without its recovery record. CLOSE consumes only
     * our retained handle; protocol/registry publication still waits for finish. */
    if (attempt->persisted &&
        (close < 0 || !chimera_vfs_compound_set_cancel_scope(compound, retire, close))) {
        command->input_status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        return -1;
    }
    return close;
}

static void
smb_close_compound_complete(struct chimera_vfs_compound *compound, uint32_t index,
                            enum chimera_vfs_error *status, void *private_data)
{
    struct smb_vfs_command *command = private_data;
    (void) compound; (void) index;
    struct smb_close_attempt *attempt = command->private_data;
    if (attempt && attempt->doc_mode) { smb_doc_close_complete(compound, index, status, command); }
    if (*status == CHIMERA_VFS_OK) {
        attempt->consumed = true;
    }
}

/* A disconnect may park a durable/resilient open after CLOSE has resolved
 * its FileId, including while compound finish is pending. Transfer exactly the
 * one tree-or-registry owner under the same bucket -> registry order as parking.
 * The command's retained reference prevents reclaim from rehoming this open. A
 * sweeper/legacy CLOSE which already owns retirement keeps that responsibility. */
static bool
smb_close_unpublish(struct smb_vfs_command *command,
                    struct chimera_smb_notify_state **notify_state)
{
    struct chimera_smb_open_file *open = command->open;
    struct chimera_server_smb_shared *shared = command->request->compound->thread->shared;
    struct chimera_smb_tree *tree = open->tree;
    unsigned int bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    struct chimera_smb_open_file *live;
    struct chimera_smb_durable_entry *entry = NULL;
    bool owner_ref = false;

    pthread_mutex_lock(&tree->open_files_lock[bucket]);
    pthread_mutex_lock(&shared->durable.lock);
    if (!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED)) {
        HASH_FIND(hh, tree->open_files[bucket], &open->file_id, sizeof(open->file_id), live);
        HASH_FIND(hh, shared->durable.by_pid, &open->file_id.pid, sizeof(open->file_id.pid), entry);
        if (entry && entry->open_file != open) { entry = NULL; }
        if (live == open) {
            HASH_DELETE(hh, tree->open_files[bucket], open);
            owner_ref = true;
        } else if (entry && entry->parked) {
            owner_ref = true;
        }
        if (owner_ref) {
            open->flags |= CHIMERA_SMB_OPEN_FILE_CLOSED;
            *notify_state = open->notify_state;
            open->notify_state = NULL;
            if (entry) { HASH_DELETE(hh, shared->durable.by_pid, entry); }
        } else {
            entry = NULL;
        }
    }
    pthread_mutex_unlock(&shared->durable.lock);
    pthread_mutex_unlock(&tree->open_files_lock[bucket]);
    if (entry) {
        chimera_smb_share_release(entry->share);
        free(entry);
    }
    return owner_ref;
}

static bool
smb_close_compound_ends_batch(struct smb_vfs_command *command)
{
    return (command->open && (command->open->durable_flags ||
            (command->open->flags & CHIMERA_SMB_OPEN_FILE_PERSISTED))) ||
        smb_close_cache_present(command);
}

static void
smb_close_compound_publish(struct chimera_vfs_compound *compound,
                           struct smb_vfs_command *command)
{
    struct smb_close_attempt *attempt = command->private_data;
    (void) compound;
    if (attempt && attempt->committed) {
        struct chimera_smb_open_file *open = command->open;
        if (attempt->persisted && attempt->record_delete_status != CHIMERA_VFS_OK &&
            attempt->record_delete_status != CHIMERA_VFS_ENOENT) {
            chimera_smb_error("CLOSE durable record deletion failed: pid=%lx error=%d",
                open->file_id.pid, attempt->record_delete_status);
        }
        /* Detach under the same cutoff as FileId unpublication, then queue
         * cleanup outside registry/bucket locks. This accepted-only transfer
         * consumes the attachment exactly once even if teardown won the race.
         * Cleanup must precede DOC's delete event on this directory's watch. */
        struct chimera_smb_notify_state *notify_state = NULL;
        bool removed = !command->state->producer &&
            smb_close_unpublish(command, &notify_state);
        if (notify_state) {
            chimera_smb_notify_close(command->request->compound->thread->shared->vfs->vfs_notify,
                                    notify_state);
        }
        /* Preserve the legacy observable order: settle modified metadata
         * before publishing a following delete-on-close removal. */
        if (attempt->modified && attempt->path.parent_fh_len) {
            struct chimera_claim_actor actor = {
                .owner = chimera_smb_open_actor_owner(open),
            };
            memcpy(actor.owner.key, open->parent_lease_key, 16);
            chimera_vfs_notify_emit_actor(command->request->compound->thread->shared->vfs->vfs_notify,
                attempt->path.parent_fh, attempt->path.parent_fh_len,
                CHIMERA_VFS_NOTIFY_FILE_MODIFIED, attempt->path.name,
                attempt->path.name_len, NULL, 0,
                chimera_claim_owner_has_key(&actor.owner) ? &actor : NULL);
        }
        if (attempt->doc_mode) { smb_doc_close_publish(compound, command); }
        command->request->close.r_attrs = attempt->attrs;
        if (!command->state->producer) {
            struct chimera_server_smb_thread *thread = command->request->compound->thread;
            struct chimera_smb_tree *tree = open->tree;
            /* Typed CLOSE owns the independently retained input reference. */
            if (attempt->consumed) { command->owned_handle = NULL; }
            if (removed) {
                if (attempt->cache_grant) {
                    /* Membership publication is accepted-only. Keep rights
                     * conservative until all journal publication has finished;
                     * release() below runs after compound_free. */
                    chimera_smb_grant_remove_member(attempt->cache_grant, open);
                    attempt->cache_detached = true;
                }
                struct chimera_vfs_open_handle *handle;
                pthread_mutex_lock(&open->share_file_state->lock);
                handle = open->handle;
                open->handle = NULL;
                open->doc_close_started = 1;
                if (open->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) {
                    open->doc_stream_close_started = 1;
                }
                pthread_mutex_unlock(&open->share_file_state->lock);
                chimera_smb_open_namespace_detach(open);
                if (tree->share) { chimera_smb_sharemode_release(&tree->share->sharemode, open); }
                if (handle) { chimera_vfs_release(thread->vfs_thread, handle); }
                chimera_smb_open_file_release(command->request, open);
            }
        }
    }
}

static void
smb_close_compound_reset(struct smb_vfs_command *command)
{
    struct smb_close_attempt *attempt = command->private_data;
    if (!attempt) { return; }
    memset(&attempt->attrs, 0, sizeof(attempt->attrs));
    attempt->modified = 0;
    attempt->record_delete_status = CHIMERA_VFS_OK;
    attempt->committed = attempt->consumed = false;
}

static void
smb_close_compound_release(struct smb_vfs_command *command)
{
    struct smb_close_attempt *attempt = command->private_data;
    if (attempt) {
        struct chimera_server_smb_thread *thread = command->request->compound->thread;
        struct chimera_vfs_state *state = thread->vfs_thread->vfs->vfs_state;
        if (attempt->cache_detached) {
            chimera_vfs_claim_grant_revoke_empty(attempt->cache_grant);
            chimera_smb_open_file_drain_cache(thread, command->open);
        }
        if (attempt->cache_grant) {
            chimera_vfs_claim_grant_release(state, attempt->cache_grant, true);
        }
        if (attempt->cache_file) { chimera_vfs_state_put(state, attempt->cache_file); }
        chimera_smb_doc_fence_release(&attempt->base_fence);
        chimera_smb_doc_fence_release(&attempt->fence);
    }
    free(command->private_data);
    command->private_data = NULL;
}

static uint32_t
smb_close_compound_error(enum chimera_vfs_error error)
{
    return error == CHIMERA_VFS_EINTR ? SMB2_STATUS_CANCELLED : SMB2_STATUS_INTERNAL_ERROR;
}

const struct smb_vfs_command_ops chimera_smb_close_compound_ops = {
    .eligible = smb_close_compound_eligible,
    .bound_eligible = smb_close_compound_bound_eligible,
    .ends_batch = smb_close_compound_ends_batch,
    .file_id = smb_close_compound_file_id,
    .build = smb_close_compound_build,
    .complete = smb_close_compound_complete,
    .publish = smb_close_compound_publish,
    .reset = smb_close_compound_reset,
    .release = smb_close_compound_release,
    .map_error = smb_close_compound_error,
};
