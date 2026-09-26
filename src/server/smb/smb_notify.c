// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include "common/compiler.h"
#include <stdlib.h>
#include <string.h>

#include "smb_notify.h"
#include "smb_internal.h"
#include "smb_common/smb_signing.h"
#include "smb_string.h"
#include "smb_common/smb2.h"
#include "vfs/vfs_notify.h"

SYMBOL_EXPORT void
chimera_smb_notify_state_put(struct chimera_smb_notify_state *state)
{
    if (!state || atomic_fetch_sub(&state->refs, 1) != 1) {
        return;
    }
    /* Never hold state->lock here: VFS emits take its registry lock before
     * invoking our callback, which takes state->lock. Destroy waits for every
     * in-flight callback before freeing the final state reference. */
    if (state->watch) {
        chimera_vfs_notify_watch_destroy(state->vfs_notify, state->watch);
    }
    evpl_mutex_destroy(&state->lock);
    free(state);
} /* chimera_smb_notify_state_put */

struct chimera_smb_notify_state *
chimera_smb_notify_detach(struct chimera_smb_open_file *open)
{
    unsigned int                     bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
    struct chimera_smb_notify_state *state = open->notify_state;
    open->notify_state = NULL;
    evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
    return state; /* transfer the attachment reference */
} /* chimera_smb_notify_detach */

uint32_t
chimera_smb_notify_admit(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_session       *session,
    struct chimera_smb_open_file     *open,
    uint32_t                          mask,
    int                               watch_tree,
    struct chimera_smb_notify_state **result)
{
    struct chimera_smb_namespace_registry *registry = &thread->shared->namespace_registry;
    struct chimera_vfs_file_state         *file     = open->share_file_state;

    *result = NULL;
    if (!file) {
        return SMB2_STATUS_FILE_CLOSED;
    }
    /* Invalidation marks the session deleted under sessions_lock before its
     * notify flush. Serialize watch publication against that cutoff, including
     * admissions which began before PreviousSessionId parked a durable open. */
    evpl_mutex_lock(&thread->shared->sessions_lock);
    if (!session || !(session->flags & CHIMERA_SMB_SESSION_AUTHORIZED) ||
        (session->flags & CHIMERA_SMB_SESSION_DELETED)) {
        evpl_mutex_unlock(&thread->shared->sessions_lock);
        return SMB2_STATUS_NOTIFY_CLEANUP;
    }
    /* File state is retained by our open reference even if logical CLOSE has
     * detached its backend handle. Namespace -> bucket -> notify-state is the
     * admission order; no VFS watch call is made holding notify-state.lock. */
    if (!chimera_smb_doc_mutation_begin(registry, file->fh, file->fh_len, NULL)) {
        evpl_mutex_unlock(&thread->shared->sessions_lock);
        return SMB2_STATUS_PENDING;
    }
    unsigned int                     bucket = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;
    evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
    uint32_t                         status = SMB2_STATUS_SUCCESS;
    struct chimera_smb_notify_state *state  = open->notify_state;
    if ((open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED) || !open->handle) {
        status = SMB2_STATUS_FILE_CLOSED;
    } else if (state) {
        evpl_mutex_lock(&state->lock);
        bool closing = state->closing;
        evpl_mutex_unlock(&state->lock);
        if (closing) {
            status = SMB2_STATUS_FILE_CLOSED;
        } else {
            atomic_fetch_add(&state->refs, 1);
            chimera_vfs_notify_watch_update(state->vfs_notify, state->watch, mask, watch_tree);
            *result = state;
        }
    } else {
        state = calloc(1, sizeof(*state));
        if (!state) {
            status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
        } else {
            evpl_mutex_init(&state->lock, NULL);
            atomic_init(&state->refs, 2); /* attachment + caller */
            state->vfs_notify = thread->shared->vfs->vfs_notify;
            state->watch      = chimera_vfs_notify_watch_create(state->vfs_notify,
                                                                open->handle->fh, open->handle->fh_len, mask, watch_tree
                                                                ,
                                                                chimera_smb_notify_callback, state);
            if (!state->watch) {
                evpl_mutex_destroy(&state->lock);
                free(state);
                status = SMB2_STATUS_INSUFFICIENT_RESOURCES;
            } else {
                open->notify_state = state; *result = state;
            }
        }
    }
    evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
    chimera_smb_doc_mutation_end(registry);
    evpl_mutex_unlock(&thread->shared->sessions_lock);
    return status;
} /* chimera_smb_notify_admit */

/* Map a Windows-style SMB2 CompletionFilter to a chimera VFS event mask.
 * The mapping is conservative: any VFS event class that *could* be of
 * interest given the requested filter bits is included.  Acceptable
 * over-delivery is preferable to under-delivery (clients filter again
 * on their side from FILE_ACTION codes). */
uint32_t
chimera_smb_map_completion_filter(uint32_t cf)
{
    uint32_t m = 0;

    /* A rename is admitted by the name filter matching the renamed object's
     * TYPE (MS-FSCC 2.7.1): FILE_NAME covers file name changes "including
     * renaming", DIR_NAME the same for directories.  Both used to admit the
     * single RENAMED class, so a client watching directory names was woken
     * because a FILE was renamed. */
    if (cf & SMB2_NOTIFY_CHANGE_FILE_NAME) {
        m |= CHIMERA_VFS_NOTIFY_FILE_ADDED |
            CHIMERA_VFS_NOTIFY_FILE_REMOVED |
            CHIMERA_VFS_NOTIFY_RENAMED;
    }
    if (cf & SMB2_NOTIFY_CHANGE_DIR_NAME) {
        m |= CHIMERA_VFS_NOTIFY_DIR_ADDED |
            CHIMERA_VFS_NOTIFY_DIR_REMOVED |
            CHIMERA_VFS_NOTIFY_RENAMED_DIR;
    }
    if (cf & SMB2_NOTIFY_CHANGE_ATTRIBUTES) {
        m |= CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;
    }
    if (cf & SMB2_NOTIFY_CHANGE_SIZE) {
        m |= CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
            CHIMERA_VFS_NOTIFY_FILE_MODIFIED;
    }
    if (cf & SMB2_NOTIFY_CHANGE_LAST_WRITE) {
        m |= CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
            CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;
    }
    if (cf & (SMB2_NOTIFY_CHANGE_LAST_ACCESS |
              SMB2_NOTIFY_CHANGE_CREATION |
              SMB2_NOTIFY_CHANGE_EA |
              SMB2_NOTIFY_CHANGE_SECURITY)) {
        m |= CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;
    }
    /* Named-stream filters map to their dedicated VFS event classes so a
     * stream change (create/write/size) is delivered to a watcher that
     * requested only the stream filter, without conflating it with the
     * generic ATTRS_CHANGED class. */
    if (cf & SMB2_NOTIFY_CHANGE_STREAM_NAME) {
        m |= CHIMERA_VFS_NOTIFY_STREAM_NAME;
    }
    if (cf & SMB2_NOTIFY_CHANGE_STREAM_SIZE) {
        m |= CHIMERA_VFS_NOTIFY_STREAM_SIZE;
    }
    if (cf & SMB2_NOTIFY_CHANGE_STREAM_WRITE) {
        m |= CHIMERA_VFS_NOTIFY_STREAM_WRITE;
    }
    return m;
} /* chimera_smb_map_completion_filter */


/* ----------------------------------------------------------------
 * Helper: build an SMB2 header for a standalone async message.
 * Writes the 4-byte NetBIOS header + 64-byte SMB2 header.
 * Returns pointer past the SMB2 header.
 * ---------------------------------------------------------------- */
static uint8_t *
chimera_smb_notify_build_header(
    uint8_t                           *buf,
    struct chimera_smb_notify_request *nr,
    uint32_t                           status)
{
    struct smb2_header *hdr;

    /* Skip 4 bytes for NetBIOS header — filled in later */
    buf += 4;

    hdr = (struct smb2_header *) buf;
    memset(hdr, 0, sizeof(*hdr));

    hdr->protocol_id[0] = 0xFE;
    hdr->protocol_id[1] = 'S';
    hdr->protocol_id[2] = 'M';
    hdr->protocol_id[3] = 'B';
    hdr->struct_size    = 64;
    hdr->credit_charge  = nr->credit_charge;
    hdr->status         = status;
    hdr->command        = SMB2_CHANGE_NOTIFY;
    /* Grant credits (capped), debiting this request's CreditCharge exactly once
     * across the interim + final async responses so a client that keeps asking
     * for more cannot overflow its 16-bit credit window (the fault the
     * smb2.credits async flood otherwise hits).  The first response to be built
     * (the interim) consumes the charge; later ones pass 0.  Mirrors the read
     * path's interim/final handling -- see chimera_smb_grant_credits. */
    {
        uint32_t consume = 0;

        if (!nr->credits_charged) {
            nr->credits_charged = 1;
            consume             = nr->credit_charge ? nr->credit_charge : 1;
        }
        hdr->credit_request_response = chimera_smb_grant_credits(
            nr->conn, consume, nr->credit_request);
    }
    hdr->flags          = SMB2_FLAGS_SERVER_TO_REDIR | SMB2_FLAGS_ASYNC_COMMAND;
    hdr->next_command   = 0;
    hdr->message_id     = nr->message_id;
    hdr->async.async_id = nr->async_id;
    hdr->session_id     = nr->session_id;

    return buf + sizeof(struct smb2_header);
} /* chimera_smb_notify_build_header */

/* Fill in the 4-byte NetBIOS session header (big-endian length of SMB2 payload) */
static void
chimera_smb_notify_set_netbios_len(
    uint8_t *buf,
    int      smb2_payload_len)
{
    uint32_t nb = chimera_bswap32((uint32_t) smb2_payload_len);

    memcpy(buf, &nb, 4);
} /* chimera_smb_notify_set_netbios_len */

/* Unlink a parked notify request from the connection's list.
 *
 * This is the single chokepoint every un-park/terminal path funnels through
 * (event delivery, NOTIFY_CLEANUP, SMB2_CANCEL, drop-on-teardown), so it also
 * releases the request's async-credit slot.  The async_counted guard makes the
 * conn->async_outstanding decrement happen exactly once: a leaked count would
 * permanently shrink the connection's async ceiling, a double-decrement would
 * underflow the unsigned counter and effectively remove the cap. */
static void
chimera_smb_notify_unlink(struct chimera_smb_notify_request *nr)
{
    if (nr->prev) {
        nr->prev->next = nr->next;
    } else if (nr->conn && nr->conn->parked_notifies == nr) {
        nr->conn->parked_notifies = nr->next;
    }

    if (nr->next) {
        nr->next->prev = nr->prev;
    }

    nr->next = NULL;
    nr->prev = NULL;

    if (nr->async_counted) {
        nr->async_counted = 0;
        if (nr->conn && nr->conn->async_outstanding) {
            nr->conn->async_outstanding--;
        }
    }
} /* chimera_smb_notify_unlink */

/* ----------------------------------------------------------------
 * Write one FILE_NOTIFY_INFORMATION record.
 * Returns bytes written (4-byte aligned), or 0 if it doesn't fit.
 * ---------------------------------------------------------------- */
static int
chimera_smb_notify_write_record(
    struct chimera_smb_iconv_ctx *iconv_ctx,
    uint8_t                      *p,
    int                           remaining,
    uint32_t                      action,
    const char                   *name,
    int                           name_len)
{
    int      utf16_len;
    int      entry_len, aligned, i;
    uint8_t *name_start;

    /* 12-byte header must fit */
    if (remaining < 12) {
        return 0;
    }

    /* Skip header, convert name directly into output buffer */
    name_start = p + 12;
    utf16_len  = chimera_smb_utf8_to_utf16le(iconv_ctx,
                                             name, name_len,
                                             (uint16_t *) name_start,
                                             remaining - 12);

    if (utf16_len < 0) {
        return 0;
    }

    entry_len = 12 + utf16_len;
    aligned   = (entry_len + 3) & ~3;

    if (aligned > remaining) {
        return 0;
    }

    /* NextEntryOffset (0 = last, caller fixes up) */
    *(uint32_t *) p = 0;
    p              += 4;
    /* Action */
    *(uint32_t *) p = action;
    p              += 4;
    /* FileNameLength in bytes (UTF-16) */
    *(uint32_t *) p = utf16_len;

    /* Pad to 4-byte alignment */
    for (i = entry_len; i < aligned; i++) {
        p[i] = 0;
    }

    return aligned;
} /* chimera_smb_notify_write_record */

/* The FILE_ACTION_* a non-rename event serializes to.  Shared by the
 * serializer and the coalescer so they agree on a record's identity. */
static uint32_t
chimera_smb_notify_file_action(uint32_t a)
{
    if (a & (CHIMERA_VFS_NOTIFY_FILE_ADDED | CHIMERA_VFS_NOTIFY_DIR_ADDED)) {
        return FILE_ACTION_ADDED;
    } else if (a & (CHIMERA_VFS_NOTIFY_FILE_REMOVED | CHIMERA_VFS_NOTIFY_DIR_REMOVED)) {
        return FILE_ACTION_REMOVED;
    }
    return FILE_ACTION_MODIFIED;
} /* chimera_smb_notify_file_action */

int
chimera_smb_notify_coalesce_events(
    struct chimera_vfs_notify_event *events,
    int                              nevents)
{
    int i, j = 0;

    for (i = 0; i < nevents; i++) {
        /* Rename events carry a distinct OLD/NEW pair and are never merged. */
        if (j > 0 &&
            !(events[i].action & (CHIMERA_VFS_NOTIFY_RENAMED |
                                  CHIMERA_VFS_NOTIFY_RENAMED_DIR)) &&
            !(events[j - 1].action & (CHIMERA_VFS_NOTIFY_RENAMED |
                                      CHIMERA_VFS_NOTIFY_RENAMED_DIR)) &&
            chimera_smb_notify_file_action(events[i].action) ==
            chimera_smb_notify_file_action(events[j - 1].action) &&
            events[i].name_len == events[j - 1].name_len &&
            memcmp(events[i].name, events[j - 1].name, events[i].name_len) == 0) {
            /* Same record as the previous kept event — fold the class bits in
            * (harmless; matching already happened) and drop the duplicate. */
            events[j - 1].action |= events[i].action;
            continue;
        }

        if (i != j) {
            events[j] = events[i];
        }
        j++;
    }

    return j;
} /* chimera_smb_notify_coalesce_events */

/* ----------------------------------------------------------------
 * Serialize VFS notify events into FILE_NOTIFY_INFORMATION records.
 * RENAMED events produce two records (OLD_NAME + NEW_NAME).
 * Returns bytes written.
 * ---------------------------------------------------------------- */
int
chimera_smb_notify_serialize_events(
    struct chimera_smb_iconv_ctx    *iconv_ctx,
    struct chimera_vfs_notify_event *events,
    int                              nevents,
    uint8_t                         *out,
    int                              out_size,
    int                             *events_consumed)
{
    uint8_t *p          = out;
    uint8_t *prev_entry = NULL;
    int      remaining  = out_size;
    int      i, written;

    /* Snapshot the position before each event so we can roll back if a
     * record from a multi-part event (e.g. rename's old+new pair) fails
     * to fit.  Without this, a rename whose NEW_NAME record doesn't fit
     * after OLD_NAME has been written would leave a half-event in the
     * output and the caller could not tell which event was the last
     * fully-consumed one. */
    for (i = 0; i < nevents; i++) {
        struct chimera_vfs_notify_event *ev          = &events[i];
        uint8_t                         *event_start = p;
        uint8_t                         *event_prev  = prev_entry;
        int                              ok          = 1;

        if (ev->action & (CHIMERA_VFS_NOTIFY_RENAMED |
                          CHIMERA_VFS_NOTIFY_RENAMED_DIR)) {
            /* Produce two records: OLD_NAME + NEW_NAME */

            /* OLD_NAME record (using old_name) */
            if (ev->old_name_len > 0) {
                if (prev_entry) {
                    *(uint32_t *) prev_entry = (uint32_t) (p - prev_entry);
                }
                prev_entry = p;
                written    = chimera_smb_notify_write_record(
                    iconv_ctx, p, remaining, FILE_ACTION_RENAMED_OLD_NAME,
                    ev->old_name, ev->old_name_len);
                if (!written) {
                    ok = 0;
                } else {
                    p         += written;
                    remaining -= written;
                }
            }

            /* NEW_NAME record (using name) */
            if (ok && ev->name_len > 0) {
                if (prev_entry) {
                    *(uint32_t *) prev_entry = (uint32_t) (p - prev_entry);
                }
                prev_entry = p;
                written    = chimera_smb_notify_write_record(
                    iconv_ctx, p, remaining, FILE_ACTION_RENAMED_NEW_NAME,
                    ev->name, ev->name_len);
                if (!written) {
                    ok = 0;
                } else {
                    p         += written;
                    remaining -= written;
                }
            }
        } else {
            uint32_t action;

            /* Stream-class events (STREAM_NAME/SIZE/WRITE) are OR'd onto the
             * generic ADDED/MODIFIED action that carries them — a write to a
             * file's $DATA fork is reported as FILE_ACTION_MODIFIED and a
             * create as FILE_ACTION_ADDED, even when observed through a
             * FILE_NOTIFY_CHANGE_STREAM_* filter (matching Windows /
             * smbtorture smb2.notify.mask, which requires ACTION_MODIFIED for
             * a write under every filter bit).  So the stream bits only widen
             * which watchers are notified; they do not change the action. */
            action = chimera_smb_notify_file_action(ev->action);

            if (prev_entry) {
                *(uint32_t *) prev_entry = (uint32_t) (p - prev_entry);
            }
            prev_entry = p;
            written    = chimera_smb_notify_write_record(
                iconv_ctx, p, remaining, action, ev->name, ev->name_len);
            if (!written) {
                ok = 0;
            } else {
                p         += written;
                remaining -= written;
            }
        }

        if (!ok) {
            /* Roll back this event so the caller sees an atomic event count.
             * `remaining` does not need restoring because we break here. */
            p          = event_start;
            prev_entry = event_prev;
            if (prev_entry) {
                *(uint32_t *) prev_entry = 0;
            }
            break;
        }
    }

    if (events_consumed) {
        *events_consumed = i;
    }

    return (int) (p - out);
} /* chimera_smb_notify_serialize_events */

/* ----------------------------------------------------------------
 * Send an interim STATUS_PENDING response for a parked request.
 * Called from the CHANGE_NOTIFY handler on the SMB thread.
 * ---------------------------------------------------------------- */
void
chimera_smb_notify_send_interim(struct chimera_smb_notify_request *nr)
{
    struct evpl_iovec iov;
    uint8_t          *buf, *p;
    int               smb2_len;

    /* 4 (NetBIOS) + 64 (SMB2 header) + 9 (error body) = 77 bytes */
    evpl_iovec_alloc(nr->thread->evpl, 77, 8, 1, 0, &iov);
    buf = iov.data;
    memset(buf, 0, 77);

    p = chimera_smb_notify_build_header(buf, nr, SMB2_STATUS_PENDING);

    /* Error response body: StructureSize(2) + ErrorContextCount(1) + Reserved(1) + ByteCount(4) + pad(1) */
    p[0] = 9; /* StructureSize low */
    p[1] = 0; /* StructureSize high */
    /* remaining 7 bytes are 0 (already memset) */
    p += 9;

    smb2_len = (int) (p - buf - 4);
    chimera_smb_notify_set_netbios_len(buf, smb2_len);

    /* Sign, or wrap in a TRANSFORM header on an encrypting session (the interim
     * is a response and must be secured the same way -- MS-SMB2 §3.3.4.1.4).
     * Consumes iov. */
    iov.length = (int) (p - buf);
    chimera_smb_secure_send(nr->conn, &iov, smb2_len, &nr->secure);
} /* chimera_smb_notify_send_interim */

/* ----------------------------------------------------------------
 * VFS notify callback — called when events arrive on a watched directory.
 * May be called from any thread.  Grabs the pending request and queues
 * it to the SMB thread's doorbell for safe processing.
 * ---------------------------------------------------------------- */
/* ----------------------------------------------------------------
 * The watch's outstanding-request queue.  Both helpers require state->lock.
 * ---------------------------------------------------------------- */
static int
chimera_smb_notify_q_contains(
    const struct chimera_smb_notify_state   *state,
    const struct chimera_smb_notify_request *nr)
{
    const struct chimera_smb_notify_request *cur;

    for (cur = state->q_head; cur; cur = cur->q_next) {
        if (cur == nr) {
            return 1;
        }
    }
    return 0;
} /* chimera_smb_notify_q_contains */

SYMBOL_EXPORT void
chimera_smb_notify_q_push(
    struct chimera_smb_notify_state   *state,
    struct chimera_smb_notify_request *nr)
{
    nr->q_next = NULL;
    if (state->q_tail) {
        state->q_tail->q_next = nr;
    } else {
        state->q_head = nr;
    }
    state->q_tail = nr;
} /* chimera_smb_notify_q_push */

/* Unlink `nr` if it is queued.  Returns 1 when it was, so a caller can use it
 * as the CLAIM: exactly one path can take a request off the queue, and that
 * is the path that owns it from then on. */
static int
chimera_smb_notify_q_unlink(
    struct chimera_smb_notify_state   *state,
    struct chimera_smb_notify_request *nr)
{
    struct chimera_smb_notify_request **pp   = &state->q_head;
    struct chimera_smb_notify_request  *prev = NULL;

    while (*pp) {
        if (*pp == nr) {
            *pp = nr->q_next;
            if (state->q_tail == nr) {
                state->q_tail = prev;
            }
            nr->q_next = NULL;
            return 1;
        }
        prev = *pp;
        pp   = &(*pp)->q_next;
    }
    return 0;
} /* chimera_smb_notify_q_unlink */

SYMBOL_EXPORT void
chimera_smb_notify_callback(
    struct chimera_vfs_notify_watch *watch,
    void                            *private_data)
{
    struct chimera_smb_notify_state   *state = private_data;
    struct chimera_smb_notify_request *nr;
    struct chimera_server_smb_thread  *thread;
    int                                wake = 0;

    /* Hold state->lock for the entire transition so close/cancel can see
     * a consistent (state->pending, nr->on_ready_queue) pair.  The nested
     * thread->notify_ready_lock acquisition here matches the order taken
     * by close — state->lock outer, notify_ready_lock inner — so there
     * is no AB-BA deadlock between them. */
    evpl_mutex_lock(&state->lock);

    /* The OLDEST outstanding request is the one a change wakes; the rest wait
     * for the next one. */
    nr = state->q_head;
    if (nr && !state->closing && !nr->on_ready_queue) {
        thread             = nr->thread;
        nr->on_ready_queue = 1;

        evpl_mutex_lock(&thread->notify_ready_lock);
        nr->ready_next       = thread->notify_ready;
        thread->notify_ready = nr;
        evpl_mutex_unlock(&thread->notify_ready_lock);

        wake = 1;
    }

    evpl_mutex_unlock(&state->lock);

    if (wake) {
        evpl_ring_doorbell(&thread->notify_doorbell);
    }
} /* chimera_smb_notify_callback */

/* ----------------------------------------------------------------
 * Queue the open's parked CHANGE_NOTIFY for a NOTIFY_CLEANUP completion
 * on its owning thread.  Mirrors the callback's enqueue (state->lock outer,
 * notify_ready_lock inner) so close/cancel observe a consistent
 * (state->pending, on_ready_queue) pair, and rings the owning thread's
 * doorbell.  The doorbell handler honors nr->cleanup and completes the
 * request with STATUS_NOTIFY_CLEANUP rather than the event-delivery path.
 * ---------------------------------------------------------------- */
static int
smb_notify_state_cleanup(struct chimera_smb_notify_state *state)
{
    struct chimera_smb_notify_request *nr;
    struct chimera_server_smb_thread  *thread;
    int                                wake = 0;

    if (!state) {
        return 0;
    }

    evpl_mutex_lock(&state->lock);
    if (!state->closing) {
        state->cleanup_deleted = chimera_vfs_notify_watch_take_deleted(state->watch);
        state->closing         = 1;
    }

    /* EVERY outstanding request, not just the oldest: the handle is going
     * away, so there will never be another change, and one left queued would
     * never be answered at all. */
    for (nr = state->q_head; nr; nr = nr->q_next) {
        thread      = nr->thread;
        nr->cleanup = 1;
        if (!nr->on_ready_queue) {
            nr->on_ready_queue = 1;
            evpl_mutex_lock(&thread->notify_ready_lock);
            nr->ready_next       = thread->notify_ready;
            thread->notify_ready = nr;
            evpl_mutex_unlock(&thread->notify_ready_lock);
        }
        /* An already ready (possibly extracted) request must see cleanup too.
         * Ring every owner, not just the last worker visited in the list. */
        evpl_ring_doorbell(&thread->notify_doorbell);
        wake = 1;
    }

    evpl_mutex_unlock(&state->lock);

    return wake;
} /* smb_notify_state_cleanup */


/* ----------------------------------------------------------------
 * Build and send an async CHANGE_NOTIFY response with events.
 * Must be called on the SMB thread that owns the connection.
 *
 * cleanup == 0: normal event delivery.  A spurious wakeup (no matching
 *   events drained) leaves the request parked for the next event and
 *   returns without sending.  Status is SUCCESS (NOTIFY_ENUM_DIR on
 *   overflow).
 *
 * cleanup == 1: the watched open is being torn down (explicit CLOSE,
 *   TREE_DISCONNECT, LOGOFF).  The request is ALWAYS completed — never
 *   left parked.  Any buffered events are still drained and returned, but
 *   the status is STATUS_NOTIFY_CLEANUP (MS-SMB2 3.3.4.4) so the client
 *   learns the watch is gone.  Windows returns the buffered records
 *   alongside CLEANUP (smb2.notify.dir expects num_changes > 0 with
 *   NOTIFY_CLEANUP), so we serialize them exactly as the SUCCESS path does.
 * ---------------------------------------------------------------- */
static void
chimera_smb_notify_do_send_response(
    struct chimera_smb_notify_request *nr,
    struct chimera_smb_notify_state   *state,
    int                                cleanup)
{
    struct chimera_vfs_notify_event events[16];
    int                             overflowed = 0;
    int                             deleted    = 0;
    int                             nevents;
    struct evpl_iovec               iov;
    uint8_t                        *buf, *p;
    int                             total_alloc;
    int                             notify_data_len;
    uint32_t                        status;

    if (!state || !state->watch) {
        /* close already tore down the state behind us; nr was reaped
        * from the ready queue but we got dispatched first.  Drop. */
        chimera_smb_notify_unlink(nr);
        chimera_smb_notify_request_free(nr);
        return;
    }

    evpl_mutex_lock(&state->lock);
    cleanup |= state->closing || nr->cleanup;

    /* Bail if close/cancel claimed nr while it was sitting on the ready
     * queue: whoever took it off the watch's queue owns it now.
     *
     * An EVENT may only answer the oldest outstanding request; a cleanup may
     * answer any of them, because the handle is going away and all of them
     * have to be finished. */
    if (!chimera_smb_notify_q_contains(state, nr) ||
        (!cleanup && state->q_head != nr)) {
        nr->on_ready_queue = 0;
        evpl_mutex_unlock(&state->lock);
        return;
    }

    nevents = chimera_vfs_notify_drain(state->watch, events, 16, &overflowed);

    /* The watched directory may have been removed out from under this
     * request (delete-on-close on another handle).  That terminates the
     * notify with STATUS_DELETE_PENDING and, like the cleanup path, must
     * complete the request rather than leave it parked. */
    if (state->closing) {
        /* Preserve an independent deletion which preceded cleanup, exactly
         * once as with take_deleted. Ignore a subsequent own-DOC removal. */
        deleted                = state->cleanup_deleted;
        state->cleanup_deleted = 0;
    } else {
        deleted = chimera_vfs_notify_watch_take_deleted(state->watch);
    }

    if (nevents == 0 && !overflowed && !cleanup && !deleted) {
        /* Spurious wakeup — leave nr on the watch's queue so the next event
         * re-arms the ready queue via the callback.  Just clear the queued
         * flag.  On the cleanup/deleted paths we must complete the request. */
        nr->on_ready_queue = 0;
        evpl_mutex_unlock(&state->lock);
        return;
    }

    /* Re-filter drained events against THIS request's completion_filter.
     * The watch's filter mask may have been broader when these events
     * were enqueued (e.g. a previous request used a wider
     * CompletionFilter).  Without this, an event the client did not
     * ask for can leak through.  Non-matching events are silently
     * dropped: the client explicitly excluded them. */
    {
        uint32_t mask = chimera_smb_map_completion_filter(nr->completion_filter);
        int      j    = 0;
        for (int i = 0; i < nevents; i++) {
            if (events[i].action & mask) {
                if (i != j) {
                    events[j] = events[i];
                }
                j++;
            }
        }
        nevents = j;
    }

    nevents = chimera_smb_notify_coalesce_events(events, nevents);

    if (nevents == 0 && !overflowed && !cleanup && !deleted) {
        /* All drained events were filtered out — treat as a spurious
         * wakeup.  Leave nr on the watch's queue so the next matching
         * event (which by then will satisfy emit's enqueue-time filter,
         * since watch_update narrowed the mask) rewakes us.  On the
         * cleanup/deleted paths we must still complete the request. */
        nr->on_ready_queue = 0;
        evpl_mutex_unlock(&state->lock);
        return;
    }

    /* Claim the request before sending the response.  If a VFS callback
     * queued nr on the thread ready list (the cleanup path can reach here
     * with on_ready_queue still set, since it is not entered from the
     * doorbell), pluck it out so the doorbell handler never dispatches a
     * freed nr.  When we ARE called from the doorbell, nr has already been
     * removed from thread->notify_ready and the search is a harmless
     * no-op.  state->lock (outer) -> notify_ready_lock (inner) matches the
     * order taken by chimera_smb_notify_callback. */
    chimera_smb_notify_q_unlink(state, nr);
    if (nr->on_ready_queue) {
        struct chimera_server_smb_thread   *thread = nr->thread;
        struct chimera_smb_notify_request **pp;

        evpl_mutex_lock(&thread->notify_ready_lock);
        pp = &thread->notify_ready;
        while (*pp) {
            if (*pp == nr) {
                *pp            = nr->ready_next;
                nr->ready_next = NULL;
                break;
            }
            pp = &(*pp)->ready_next;
        }
        evpl_mutex_unlock(&thread->notify_ready_lock);
        nr->on_ready_queue = 0;
    }

    evpl_mutex_unlock(&state->lock);

    /* Size the response buffer to the client's OutputBufferLength plus
     * the SMB2 framing.  An undersize buffer would force a NOTIFY_ENUM_DIR
     * even when the client requested room for more events.  Cap at
     * CHIMERA_SMB_NOTIFY_MAX_RESP so a hostile client cannot force a
     * huge allocation; clients beyond that just see NOTIFY_ENUM_DIR.
     * Floor at the worst-case-1-event size so we never under-allocate
     * when output_buffer_length is small (the consumer treats 0/small
     * as "force overflow", but we still need a valid response body to
     * write the 9-byte CHANGE_NOTIFY reply structure + padding byte). */
    {
        uint32_t requested = nr->output_buffer_length;
        if (requested > CHIMERA_SMB_NOTIFY_MAX_RESP) {
            requested = CHIMERA_SMB_NOTIFY_MAX_RESP;
        }
        /* Framing overhead: NetBIOS(4) + SMB2 hdr(64) + reply body(8) + pad(1). */
        total_alloc = 4 + 64 + 8 + 1 + (int) requested;
        if (total_alloc < 4 + 64 + 8 + 1 + 528) {
            /* Floor — one renamed-event worth of room for the overflow-
             * byte-write or single-record success case. */
            total_alloc = 4 + 64 + 8 + 1 + 528;
        }
    }

    evpl_iovec_alloc(nr->thread->evpl, total_alloc, 8, 1, 0, &iov);
    buf = iov.data;
    memset(buf, 0, total_alloc);

    /* Reserve space for NetBIOS + SMB2 header; we fill it in below once
     * we know whether the response is SUCCESS or NOTIFY_ENUM_DIR. */
    p = buf + 4 + sizeof(struct smb2_header);

    /* CHANGE_NOTIFY reply body: StructureSize(2) + OutputBufferOffset(2) + OutputBufferLength(4) */
    uint8_t *reply_body = p;
    p += 8;

    /* Serialize FILE_NOTIFY_INFORMATION records */
    uint8_t *data_start = p;

    /* A deleted watch (or an overflow) returns no records — the client
     * learns the outcome from the status code alone. */
    if (overflowed || deleted) {
        nevents = 0;
    }

    {
        int max_data = total_alloc - (int) (data_start - buf);
        int consumed = 0;

        /* The client's advertised buffer caps how much we may serialize.
         * Treat OutputBufferLength == 0 as "zero bytes allowed" rather
         * than "unlimited"; we must never write more than the client
         * asked for.  Zero forces an overflow → NOTIFY_ENUM_DIR when
         * any events were drained. */
        if ((int) nr->output_buffer_length < max_data) {
            max_data = (int) nr->output_buffer_length;
        }

        notify_data_len = chimera_smb_notify_serialize_events(
            &nr->thread->iconv_ctx, events, nevents, data_start, max_data,
            &consumed);

        /* Per MS-SMB2 §3.3.5.10, when not all queued events fit in the
         * client's OutputBufferLength we must return NOTIFY_ENUM_DIR
         * with no notify data so the client rescans.  Truncating the
         * serialized stream and dropping the un-serialized tail leads
         * to silent data loss. */
        if (consumed < nevents) {
            overflowed      = 1;
            notify_data_len = 0;

            /* The drained events were discarded because they did not fit the
            * client's OutputBufferLength.  A client that asked for a non-zero
            * buffer and still could not be served has lost events, so the
            * next CHANGE_NOTIFY on this handle must also report overflow until
            * the client rescans (smb2.notify.valid-req).  OutputBufferLength
            * == 0 is a deliberate "poll without data" and does not stick. */
            if (nr->output_buffer_length > 0) {
                chimera_vfs_notify_mark_overflow(state->watch);
            }
        }
        p = data_start + notify_data_len;

        /* Per MS-SMB2 §2.2.36 (SMB2 CHANGE_NOTIFY Response): "If
         * OutputBufferLength is zero, the server MUST send a single
         * byte of zero in this field."  The byte is already zero from
         * the memset above; just advance the pointer so the body is 9
         * bytes total, matching StructureSize.  Windows and libsmb2
         * happen to accept the truncated 8-byte body, but emit the
         * spec-mandated padding for symmetry with the synchronous
         * change_notify_reply path and to satisfy strict clients. */
        if (notify_data_len == 0) {
            p += 1;
        }
    }

    /* Per MS-SMB2 §3.3.5.10, when the server cannot fit all queued
     * events in the response buffer it must return NOTIFY_ENUM_DIR
     * with no notify data so the client knows to rescan the directory.
     * Returning STATUS_SUCCESS with zero records would be ambiguous —
     * some clients interpret that as "no events". */
    /* DELETE_PENDING (watched object removed) takes precedence over the
     * overflow/cleanup/success classification. */
    if (deleted) {
        status = SMB2_STATUS_DELETE_PENDING;
    } else if (overflowed) {
        status = SMB2_STATUS_NOTIFY_ENUM_DIR;
    } else if (cleanup) {
        status = SMB2_STATUS_NOTIFY_CLEANUP;
    } else {
        status = SMB2_STATUS_SUCCESS;
    }
    chimera_smb_notify_build_header(buf, nr, status);

    /* Fill in reply body */
    reply_body[0] = 9;  /* StructureSize */
    reply_body[1] = 0;

    if (notify_data_len > 0) {
        uint16_t out_offset = (uint16_t) (data_start - (buf + 4)); /* offset from SMB2 header start */
        memcpy(&reply_body[2], &out_offset, 2);
        memcpy(&reply_body[4], &notify_data_len, 4);
    }

    /* Set NetBIOS length and send */
    int smb2_len = (int) (p - buf - 4);

    chimera_smb_notify_set_netbios_len(buf, smb2_len);

    /* Sign, or wrap in a TRANSFORM header on an encrypting session (every
     * change-notify delivery is a response and must be secured -- MS-SMB2
     * §3.3.4.1.4).  Consumes iov. */
    iov.length = (int) (p - buf);
    chimera_smb_secure_send(nr->conn, &iov, smb2_len, &nr->secure);

    chimera_smb_notify_unlink(nr);
    chimera_smb_notify_request_free(nr);
} /* chimera_smb_notify_do_send_response */

void
chimera_smb_notify_send_response(struct chimera_smb_notify_request *nr)
{
    chimera_smb_notify_do_send_response(nr, nr->state, 0);
} /* chimera_smb_notify_send_response */

/*
 * Complete a parked CHANGE_NOTIFY because its watched open is being torn
 * down (CLOSE / TREE_DISCONNECT / LOGOFF).  Sends STATUS_NOTIFY_CLEANUP
 * with any buffered events and frees the request.  `state` is passed
 * explicitly because the teardown caller may have already detached it from
 * open_file->notify_state (see chimera_smb_tree_free) to preserve the
 * free-list invariant that a recycled open_file has notify_state == NULL.
 */
void
chimera_smb_notify_complete_cleanup(
    struct chimera_smb_notify_request *nr,
    struct chimera_smb_notify_state   *state)
{
    chimera_smb_notify_do_send_response(nr, state, 1);
} /* chimera_smb_notify_complete_cleanup */

/* ----------------------------------------------------------------
 * Reap a notify request from any state it may be in:
 *  - parked in state->pending (no event yet)
 *  - queued on thread->notify_ready (event arrived but not yet processed)
 *  - already past send_response (caller checks state->pending == NULL
 *    and returns NULL — caller must handle)
 *
 * Returns 1 if `nr` was successfully claimed (caller now owns it and
 * may send/free it).  Returns 0 if `nr` had already been claimed (e.g.
 * by send_response) and should NOT be touched further.
 *
 * Must be called on the SMB thread that owns the connection.
 * ---------------------------------------------------------------- */
static int
chimera_smb_notify_claim(struct chimera_smb_notify_request *nr)
{
    struct chimera_smb_notify_state *state = nr->state;
    int                              owned = 0;

    if (nr->admitting) {
        evpl_remove_timer(nr->thread->evpl, &nr->admission_timer);
        nr->admitting = 0;
        return 1;
    }
    if (!state) {
        /* No state — already torn down.  Caller must not free again. */
        return 0;
    }

    evpl_mutex_lock(&state->lock);

    if (chimera_smb_notify_q_unlink(state, nr)) {
        owned = 1;
    }

    if (nr->on_ready_queue) {
        /* nr is sitting in thread->notify_ready waiting to be processed.
         * Pluck it out so the doorbell handler does not double-process. */
        struct chimera_server_smb_thread   *thread = nr->thread;
        struct chimera_smb_notify_request **pp;

        evpl_mutex_lock(&thread->notify_ready_lock);
        pp = &thread->notify_ready;
        while (*pp) {
            if (*pp == nr) {
                *pp            = nr->ready_next;
                nr->ready_next = NULL;
                break;
            }
            pp = &(*pp)->ready_next;
        }
        evpl_mutex_unlock(&thread->notify_ready_lock);

        nr->on_ready_queue = 0;
        owned              = 1;
    }

    evpl_mutex_unlock(&state->lock);
    return owned;
} /* chimera_smb_notify_claim */

/* ----------------------------------------------------------------
 * Cancel a parked CHANGE_NOTIFY request — send STATUS_CANCELLED.
 * Must be called on the SMB thread.
 * ---------------------------------------------------------------- */
static void
smb_notify_fail(
    struct chimera_smb_notify_request *nr,
    uint32_t                           status)
{
    struct evpl_iovec iov;
    uint8_t          *buf, *p;
    int               smb2_len;

    if (!chimera_smb_notify_claim(nr)) {
        /* Already claimed (state was nulled or send_response is in flight).
         * Don't send and don't free — owner will handle it. */
        return;
    }

    chimera_smb_notify_unlink(nr);

    /* Send STATUS_CANCELLED response */
    /* 4 (NetBIOS) + 64 (SMB2 header) + 9 (error body) = 77 */
    evpl_iovec_alloc(nr->thread->evpl, 77, 8, 1, 0, &iov);
    buf = iov.data;
    memset(buf, 0, 77);

    p = chimera_smb_notify_build_header(buf, nr, status);

    p[0] = 9; /* StructureSize */
    p[1] = 0;
    p   += 9;

    smb2_len = (int) (p - buf - 4);
    chimera_smb_notify_set_netbios_len(buf, smb2_len);

    /* Sign, or wrap in a TRANSFORM header on an encrypting session.  Consumes
     * iov. */
    iov.length = (int) (p - buf);
    chimera_smb_secure_send(nr->conn, &iov, smb2_len, &nr->secure);

    chimera_smb_notify_request_free(nr);
} /* smb_notify_fail */

void
chimera_smb_notify_cancel(struct chimera_smb_notify_request *nr)
{
    smb_notify_fail(nr, SMB2_STATUS_CANCELLED);
} /* chimera_smb_notify_cancel */

static void
smb_notify_admission_tick(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_smb_notify_request *nr = container_of(timer,
                                                         struct chimera_smb_notify_request, admission_timer);
    struct chimera_smb_notify_state   *state = NULL;
    /* Connection session_handles belongs to this timer's worker; a live
     * handle retains the session memory while admit checks global flags. */
    struct chimera_smb_session_handle *session_handle;

    HASH_FIND(hh, nr->conn->session_handles, &nr->session_id, sizeof(nr->session_id), session_handle);
    uint32_t                           status = chimera_smb_notify_admit(nr->thread,
                                                                         session_handle && session_handle->is_channel ?
                                                                         session_handle->session : NULL, nr->open_file,
                                                                         chimera_smb_map_completion_filter(nr->
                                                                                                           completion_filter),
                                                                         nr->watch_tree, &state);
    if (status == SMB2_STATUS_PENDING) {
        evpl_add_oneshot_timer(evpl, timer, smb_notify_admission_tick, 1000);
        return;
    }
    if (status != SMB2_STATUS_SUCCESS) {
        smb_notify_fail(nr, status); return;
    }
    evpl_mutex_lock(&state->lock);
    if (state->closing) {
        evpl_mutex_unlock(&state->lock);
        chimera_smb_notify_state_put(state);
        smb_notify_fail(nr, SMB2_STATUS_NOTIFY_CLEANUP);
        return;
    }
    nr->admitting = 0;
    nr->state     = state; /* admission reference transfers to parked request */
    chimera_smb_notify_q_push(state, nr);
    evpl_mutex_unlock(&state->lock);
    /* Events/deletion may precede queue installation. Force one drain on the
     * owning loop; an empty drain simply leaves the request parked. */
    chimera_smb_notify_callback(state->watch, state);
} /* smb_notify_admission_tick */

void
chimera_smb_notify_admission_start(struct chimera_smb_notify_request *nr)
{
    nr->admitting = 1;
    evpl_add_oneshot_timer(nr->thread->evpl, &nr->admission_timer, smb_notify_admission_tick, 1000);
} /* chimera_smb_notify_admission_start */

/* ----------------------------------------------------------------
 * Drop a parked CHANGE_NOTIFY request without sending a reply.
 * Used on connection teardown where conn->bind is going away — sending
 * here would race with bind destruction.  The client has already
 * disconnected, so it can't observe the missing reply anyway.
 * ---------------------------------------------------------------- */
void
chimera_smb_notify_drop(struct chimera_smb_notify_request *nr)
{
    if (!chimera_smb_notify_claim(nr)) {
        /* Already claimed by another path; don't double-free. */
        return;
    }

    chimera_smb_notify_unlink(nr);
    chimera_smb_notify_request_free(nr);
} /* chimera_smb_notify_drop */

/* Transfer the detached attachment reference into asynchronous cleanup.
* state->lock protects closing and queue transitions; its final reference is
* released only after dropping that lock. Last-reference watch destruction
* acquires the VFS registry locks, whose callbacks themselves take state->lock,
* so reversing those phases would deadlock against a concurrent emit. */
SYMBOL_EXPORT void
chimera_smb_notify_close(
    struct chimera_vfs_notify       *vfs_notify,
    struct chimera_smb_notify_state *state)
{
    (void) vfs_notify;
    /* The caller transferred the detached attachment reference. Never send
    * or free an nr owned by another worker. Queued requests retain their own
    * state/watch reference until their owning loop sends or drops them. */
    smb_notify_state_cleanup(state);
    chimera_smb_notify_state_put(state);
} /* chimera_smb_notify_close */

struct chimera_smb_notify_request *
chimera_smb_notify_request_alloc(void)
{
    return calloc(1, sizeof(struct chimera_smb_notify_request));
} /* chimera_smb_notify_request_alloc */

SYMBOL_EXPORT void
chimera_smb_notify_request_free(struct chimera_smb_notify_request *nr)
{
    /* Release the open_file reference held by this parked request */
    if (nr->open_file && nr->tree && nr->thread) {
        chimera_smb_open_file_release_nr(nr->thread, nr->tree, nr->open_file);
        nr->open_file = NULL;
        chimera_smb_tree_memory_unpin(nr->thread->shared, nr->tree);
    }

    chimera_smb_notify_state_put(nr->state);
    if (nr->session) {
        chimera_smb_session_memory_unpin(nr->thread->shared, nr->session);
    }
    free(nr);
} /* chimera_smb_notify_request_free */

/* ----------------------------------------------------------------
 * Doorbell callback — runs on the SMB thread when VFS notify
 * callbacks have queued ready requests.
 * ---------------------------------------------------------------- */
static void
chimera_smb_notify_doorbell_callback(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_server_smb_thread  *thread;
    struct chimera_smb_notify_request *ready, *nr, *next;

    thread = container_of(doorbell, struct chimera_server_smb_thread,
                          notify_doorbell);

    /* Drain the ready queue under the lock */
    evpl_mutex_lock(&thread->notify_ready_lock);
    ready                = thread->notify_ready;
    thread->notify_ready = NULL;
    evpl_mutex_unlock(&thread->notify_ready_lock);

    /* Process each ready request on the SMB thread */
    for (nr = ready; nr; nr = next) {
        next           = nr->ready_next;
        nr->ready_next = NULL;
        /* cleanup/on_ready_queue are synchronized by state->lock inside
         * send_response. Cross-worker CLOSE never frees this nr. */
        chimera_smb_notify_send_response(nr);
    }
} /* chimera_smb_notify_doorbell_callback */

void
chimera_smb_notify_thread_init(struct chimera_server_smb_thread *thread)
{
    thread->notify_ready = NULL;
    evpl_mutex_init(&thread->notify_ready_lock, NULL);
    evpl_add_doorbell(thread->evpl, &thread->notify_doorbell,
                      chimera_smb_notify_doorbell_callback);
} /* chimera_smb_notify_thread_init */

void
chimera_smb_notify_thread_destroy(struct chimera_server_smb_thread *thread)
{
    evpl_remove_doorbell(thread->evpl, &thread->notify_doorbell);
    evpl_mutex_destroy(&thread->notify_ready_lock);
} /* chimera_smb_notify_thread_destroy */
