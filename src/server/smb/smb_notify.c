// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "smb_notify.h"
#include "smb_internal.h"
#include "smb_signing.h"
#include "smb_string.h"
#include "smb2.h"
#include "vfs/vfs_notify.h"

/* Map a Windows-style SMB2 CompletionFilter to a chimera VFS event mask.
 * The mapping is conservative: any VFS event class that *could* be of
 * interest given the requested filter bits is included.  Acceptable
 * over-delivery is preferable to under-delivery (clients filter again
 * on their side from FILE_ACTION codes). */
uint32_t
chimera_smb_map_completion_filter(uint32_t cf)
{
    uint32_t m = 0;

    if (cf & SMB2_NOTIFY_CHANGE_FILE_NAME) {
        m |= CHIMERA_VFS_NOTIFY_FILE_ADDED |
            CHIMERA_VFS_NOTIFY_FILE_REMOVED |
            CHIMERA_VFS_NOTIFY_RENAMED;
    }
    if (cf & SMB2_NOTIFY_CHANGE_DIR_NAME) {
        m |= CHIMERA_VFS_NOTIFY_DIR_ADDED |
            CHIMERA_VFS_NOTIFY_DIR_REMOVED |
            CHIMERA_VFS_NOTIFY_RENAMED;
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
              SMB2_NOTIFY_CHANGE_SECURITY |
              SMB2_NOTIFY_CHANGE_STREAM_NAME |
              SMB2_NOTIFY_CHANGE_STREAM_SIZE |
              SMB2_NOTIFY_CHANGE_STREAM_WRITE)) {
        m |= CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;
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

    hdr->protocol_id[0]          = 0xFE;
    hdr->protocol_id[1]          = 'S';
    hdr->protocol_id[2]          = 'M';
    hdr->protocol_id[3]          = 'B';
    hdr->struct_size             = 64;
    hdr->credit_charge           = nr->credit_charge;
    hdr->status                  = status;
    hdr->command                 = SMB2_CHANGE_NOTIFY;
    hdr->credit_request_response = nr->credit_request ? nr->credit_request : 1;
    hdr->flags                   = SMB2_FLAGS_SERVER_TO_REDIR | SMB2_FLAGS_ASYNC_COMMAND;
    hdr->next_command            = 0;
    hdr->message_id              = nr->message_id;
    hdr->async.async_id          = nr->async_id;
    hdr->session_id              = nr->session_id;

    return buf + sizeof(struct smb2_header);
} /* chimera_smb_notify_build_header */

/* Fill in the 4-byte NetBIOS session header (big-endian length of SMB2 payload) */
static void
chimera_smb_notify_set_netbios_len(
    uint8_t *buf,
    int      smb2_payload_len)
{
    uint32_t nb = __builtin_bswap32((uint32_t) smb2_payload_len);

    memcpy(buf, &nb, 4);
} /* chimera_smb_notify_set_netbios_len */

/* Unlink a parked notify request from the connection's list */
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

        if (ev->action & CHIMERA_VFS_NOTIFY_RENAMED) {
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

            if (ev->action & (CHIMERA_VFS_NOTIFY_FILE_ADDED | CHIMERA_VFS_NOTIFY_DIR_ADDED)) {
                action = FILE_ACTION_ADDED;
            } else if (ev->action & (CHIMERA_VFS_NOTIFY_FILE_REMOVED | CHIMERA_VFS_NOTIFY_DIR_REMOVED)) {
                action = FILE_ACTION_REMOVED;
            } else {
                action = FILE_ACTION_MODIFIED;
            }

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

    if (nr->signed_session) {
        chimera_smb_sign_message(nr->thread->signing_ctx,
                                 nr->conn->dialect,
                                 nr->signing_key,
                                 buf + 4,
                                 smb2_len);
    }

    iov.length = (int) (p - buf);
    evpl_sendv(nr->thread->evpl, nr->conn->bind, &iov, 1, iov.length,
               EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_notify_send_interim */

/* ----------------------------------------------------------------
 * VFS notify callback — called when events arrive on a watched directory.
 * May be called from any thread.  Grabs the pending request and queues
 * it to the SMB thread's doorbell for safe processing.
 * ---------------------------------------------------------------- */
void
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
    pthread_mutex_lock(&state->lock);

    nr = state->pending;
    if (nr && !nr->on_ready_queue) {
        thread             = nr->thread;
        nr->on_ready_queue = 1;

        pthread_mutex_lock(&thread->notify_ready_lock);
        nr->ready_next       = thread->notify_ready;
        thread->notify_ready = nr;
        pthread_mutex_unlock(&thread->notify_ready_lock);

        wake = 1;
    }

    pthread_mutex_unlock(&state->lock);

    if (wake) {
        evpl_ring_doorbell(&thread->notify_doorbell);
    }
} /* chimera_smb_notify_callback */

/* ----------------------------------------------------------------
 * Build and send an async CHANGE_NOTIFY response with events.
 * Must be called on the SMB thread that owns the connection.
 * ---------------------------------------------------------------- */
void
chimera_smb_notify_send_response(struct chimera_smb_notify_request *nr)
{
    struct chimera_smb_open_file    *open_file = nr->open_file;
    struct chimera_smb_notify_state *state     = open_file->notify_state;
    struct chimera_vfs_notify_event  events[16];
    int                              overflowed = 0;
    int                              nevents;
    struct evpl_iovec                iov;
    uint8_t                         *buf, *p;
    int                              total_alloc;
    int                              notify_data_len;
    uint32_t                         status;

    if (!state || !state->watch) {
        /* close already tore down the state behind us; nr was reaped
        * from the ready queue but we got dispatched first.  Drop. */
        chimera_smb_notify_unlink(nr);
        chimera_smb_notify_request_free(nr);
        return;
    }

    pthread_mutex_lock(&state->lock);

    /* Bail if close/cancel claimed nr while it was sitting on the ready
     * queue.  state->pending was either NULL'd or replaced by the time
     * we got here. */
    if (state->pending != nr) {
        nr->on_ready_queue = 0;
        pthread_mutex_unlock(&state->lock);
        chimera_smb_notify_request_free(nr);
        return;
    }

    nevents = chimera_vfs_notify_drain(state->watch, events, 16, &overflowed);

    if (nevents == 0 && !overflowed) {
        /* Spurious wakeup — leave nr parked so the next event re-arms
         * the ready queue via the callback.  Just clear the queued flag. */
        nr->on_ready_queue = 0;
        pthread_mutex_unlock(&state->lock);
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

    if (nevents == 0 && !overflowed) {
        /* All drained events were filtered out — treat as a spurious
         * wakeup.  Leave state->pending == nr so the next matching
         * event (which by then will satisfy emit's enqueue-time filter,
         * since watch_update narrowed the mask) rewakes us. */
        nr->on_ready_queue = 0;
        pthread_mutex_unlock(&state->lock);
        return;
    }

    /* Claim the request before sending the response. */
    state->pending     = NULL;
    nr->on_ready_queue = 0;

    pthread_mutex_unlock(&state->lock);

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

    if (overflowed) {
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
    status = overflowed ? SMB2_STATUS_NOTIFY_ENUM_DIR : SMB2_STATUS_SUCCESS;
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

    if (nr->signed_session) {
        chimera_smb_sign_message(nr->thread->signing_ctx,
                                 nr->conn->dialect,
                                 nr->signing_key,
                                 buf + 4,
                                 smb2_len);
    }

    iov.length = (int) (p - buf);
    evpl_sendv(nr->thread->evpl, nr->conn->bind, &iov, 1, iov.length,
               EVPL_SEND_FLAG_TAKE_REF);

    chimera_smb_notify_unlink(nr);
    chimera_smb_notify_request_free(nr);
} /* chimera_smb_notify_send_response */

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
    struct chimera_smb_open_file    *open_file = nr->open_file;
    struct chimera_smb_notify_state *state     = open_file ? open_file->notify_state : NULL;
    int                              owned     = 0;

    if (!state) {
        /* No state — already torn down.  Caller must not free again. */
        return 0;
    }

    pthread_mutex_lock(&state->lock);

    if (state->pending == nr) {
        state->pending = NULL;
        owned          = 1;
    }

    if (nr->on_ready_queue) {
        /* nr is sitting in thread->notify_ready waiting to be processed.
         * Pluck it out so the doorbell handler does not double-process. */
        struct chimera_server_smb_thread   *thread = nr->thread;
        struct chimera_smb_notify_request **pp;

        pthread_mutex_lock(&thread->notify_ready_lock);
        pp = &thread->notify_ready;
        while (*pp) {
            if (*pp == nr) {
                *pp            = nr->ready_next;
                nr->ready_next = NULL;
                break;
            }
            pp = &(*pp)->ready_next;
        }
        pthread_mutex_unlock(&thread->notify_ready_lock);

        nr->on_ready_queue = 0;
        owned              = 1;
    }

    pthread_mutex_unlock(&state->lock);
    return owned;
} /* chimera_smb_notify_claim */

/* ----------------------------------------------------------------
 * Cancel a parked CHANGE_NOTIFY request — send STATUS_CANCELLED.
 * Must be called on the SMB thread.
 * ---------------------------------------------------------------- */
void
chimera_smb_notify_cancel(struct chimera_smb_notify_request *nr)
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

    p = chimera_smb_notify_build_header(buf, nr, SMB2_STATUS_CANCELLED);

    p[0] = 9; /* StructureSize */
    p[1] = 0;
    p   += 9;

    smb2_len = (int) (p - buf - 4);
    chimera_smb_notify_set_netbios_len(buf, smb2_len);

    if (nr->signed_session) {
        chimera_smb_sign_message(nr->thread->signing_ctx,
                                 nr->conn->dialect,
                                 nr->signing_key,
                                 buf + 4,
                                 smb2_len);
    }

    iov.length = (int) (p - buf);
    evpl_sendv(nr->thread->evpl, nr->conn->bind, &iov, 1, iov.length,
               EVPL_SEND_FLAG_TAKE_REF);

    chimera_smb_notify_request_free(nr);
} /* chimera_smb_notify_cancel */

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

/*
 * LOCK INVARIANT — DO NOT VIOLATE.
 *
 * This function takes state->lock briefly (to peek state->pending),
 * releases it, then calls chimera_vfs_notify_watch_destroy which
 * acquires bucket->lock and mount_entries_lock.  These two phases
 * are strictly sequential: at no point is state->lock held while
 * we reach into the VFS registry.
 *
 * chimera_vfs_notify_emit holds bucket->lock / mount_entries_lock
 * while invoking watch->callback (which then takes state->lock).
 * If a future change to this function (or any other) takes
 * state->lock and then bucket->lock / mount_entries_lock, it will
 * AB-BA deadlock against an in-flight emit callback.  See the
 * lock-graph block comment above chimera_vfs_notify_emit.
 */
void
chimera_smb_notify_close(
    struct chimera_vfs_notify       *vfs_notify,
    struct chimera_smb_notify_state *state)
{
    struct chimera_smb_notify_request *nr;

    if (!state) {
        return;
    }

    /* Take a non-owning peek at the outstanding nr — chimera_smb_notify_cancel
     * does the actual claim under state->lock, which also extracts nr from
     * thread->notify_ready if a callback already queued it.  This is the
     * key fix for the "ready-queued notify lost on close" race: cancel is
     * the only path that can pluck a queued-but-not-yet-dispatched request
     * out of the ready queue. */
    pthread_mutex_lock(&state->lock);
    nr = state->pending;
    pthread_mutex_unlock(&state->lock);

    if (nr) {
        chimera_smb_notify_cancel(nr);
    }

    if (state->watch) {
        /* watch_destroy takes bucket->lock + mount_entries_lock.
         * State->lock MUST NOT be held here — see the invariant block
         * comment above this function. */
        chimera_vfs_notify_watch_destroy(vfs_notify, state->watch);
        state->watch = NULL;
    }

    pthread_mutex_destroy(&state->lock);
    free(state);
} /* chimera_smb_notify_close */

struct chimera_smb_notify_request *
chimera_smb_notify_request_alloc(void)
{
    return calloc(1, sizeof(struct chimera_smb_notify_request));
} /* chimera_smb_notify_request_alloc */

void
chimera_smb_notify_request_free(struct chimera_smb_notify_request *nr)
{
    /* Release the open_file reference held by this parked request */
    if (nr->open_file && nr->tree && nr->thread) {
        chimera_smb_open_file_release_nr(nr->thread, nr->tree, nr->open_file);
        nr->open_file = NULL;
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
    pthread_mutex_lock(&thread->notify_ready_lock);
    ready                = thread->notify_ready;
    thread->notify_ready = NULL;
    pthread_mutex_unlock(&thread->notify_ready_lock);

    /* Process each ready request on the SMB thread */
    for (nr = ready; nr; nr = next) {
        next           = nr->ready_next;
        nr->ready_next = NULL;
        chimera_smb_notify_send_response(nr);
    }
} /* chimera_smb_notify_doorbell_callback */

void
chimera_smb_notify_thread_init(struct chimera_server_smb_thread *thread)
{
    thread->notify_ready = NULL;
    pthread_mutex_init(&thread->notify_ready_lock, NULL);
    evpl_add_doorbell(thread->evpl, &thread->notify_doorbell,
                      chimera_smb_notify_doorbell_callback);
} /* chimera_smb_notify_thread_init */

void
chimera_smb_notify_thread_destroy(struct chimera_server_smb_thread *thread)
{
    evpl_remove_doorbell(thread->evpl, &thread->notify_doorbell);
    pthread_mutex_destroy(&thread->notify_ready_lock);
} /* chimera_smb_notify_thread_destroy */
