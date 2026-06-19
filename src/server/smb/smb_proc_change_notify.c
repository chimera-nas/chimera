// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_notify.h"
#include "smb2.h"
#include "vfs/vfs_notify.h"

int
chimera_smb_parse_change_notify(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t flags;
    uint32_t output_buffer_length;
    uint64_t file_id_pid;
    uint64_t file_id_vid;
    uint32_t completion_filter;

    /* StructureSize (2 bytes) already consumed by the framework */
    int      prc = 0;

    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &flags);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &output_buffer_length);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &file_id_pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &file_id_vid);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &completion_filter);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 CHANGE_NOTIFY request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    request->change_notify.flags                = flags;
    request->change_notify.output_buffer_length = output_buffer_length;
    request->change_notify.file_id.pid          = file_id_pid;
    request->change_notify.file_id.vid          = file_id_vid;
    request->change_notify.completion_filter    = completion_filter;
    request->change_notify.watch_tree           = (flags & SMB2_WATCH_TREE) != 0;

    return 0;
} /* chimera_smb_parse_change_notify */

void
chimera_smb_change_notify(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread  *thread = request->compound->thread;
    struct chimera_smb_open_file      *open_file;
    struct chimera_smb_notify_state   *state;
    struct chimera_smb_notify_request *nr;
    struct chimera_vfs_notify_event    events[16];
    int                                overflowed = 0;
    int                                nevents;
    struct chimera_vfs_notify         *vfs_notify;

    /* When change notify is disabled for the server's shares (the Windows
     * "change notify = no" behaviour), reject the request outright with
     * STATUS_NOT_IMPLEMENTED rather than arming a watch that would never
     * fire — matching MS-SMB2 and smb2.change_notify_disabled. */
    if (thread->shared->config.notify_disabled) {
        chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
        return;
    }

    /* Resolve the open file */
    open_file = chimera_smb_open_file_resolve(request, &request->change_notify.file_id);

    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* CHANGE_NOTIFY is only valid on directories */
    if (!(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* MS-SMB2 3.3.5.19: the open must hold FILE_LIST_DIRECTORY (== READ_DATA)
     * to watch the directory; otherwise reject with STATUS_ACCESS_DENIED
     * (smb2.notify.handle-permissions). */
    if (!(open_file->granted_access & SMB2_FILE_LIST_DIRECTORY)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    /* MS-SMB2 3.3.5.19: if OutputBufferLength exceeds Connection.MaxTransactSize
     * the server MUST fail the request with STATUS_INVALID_PARAMETER rather than
     * arming a watch (smb2.change_notify MaxTransactSize check). */
    if (request->change_notify.output_buffer_length > CHIMERA_SMB_MAX_TRANSACT_SIZE) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    request->change_notify.open_file = open_file;

    vfs_notify = thread->shared->vfs->vfs_notify;

    /* Map this request's CompletionFilter to the VFS event mask. */
    uint32_t vfs_mask = chimera_smb_map_completion_filter(
        request->change_notify.completion_filter);

    /* Create watch on first CHANGE_NOTIFY for this open */
    if (!open_file->notify_state) {
        state = calloc(1, sizeof(*state));
        pthread_mutex_init(&state->lock, NULL);

        state->watch = chimera_vfs_notify_watch_create(
            vfs_notify,
            open_file->handle->fh,
            open_file->handle->fh_len,
            vfs_mask,
            request->change_notify.watch_tree,
            chimera_smb_notify_callback,
            state);

        open_file->notify_state = state;
    } else {
        /* Existing watch — adapt filter mask and watch_tree to this
         * request.  CompletionFilter and WATCH_TREE may differ from
         * the previous request's settings. */
        chimera_vfs_notify_watch_update(vfs_notify,
                                        open_file->notify_state->watch,
                                        vfs_mask,
                                        request->change_notify.watch_tree);
    }

    state = open_file->notify_state;

    /* Drain + park sequence must be atomic against the VFS callback,
     * otherwise an event arriving between them is lost: callback sees
     * pending == NULL and returns, then we install the new pending but
     * no further callback fires until the next event. */
    pthread_mutex_lock(&state->lock);

    /* MS-SMB2 §3.3.5.10: only one CHANGE_NOTIFY may be outstanding per
     * open.  Reject a duplicate so we do not orphan the prior request. */
    if (state->pending) {
        pthread_mutex_unlock(&state->lock);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    nevents = chimera_vfs_notify_drain(state->watch, events, 16, &overflowed);

    /* The watched directory may already have been removed (rmdir / delete-
     * on-close from another handle or connection) before this CHANGE_NOTIFY
     * was armed.  chimera_vfs_notify_emit_delete records that on the watch
     * via the deleted flag and fires the callback, but if that callback ran
     * before we installed state->pending (a cross-connection race —
     * smb2.notify.rmdir3/4 send the CHANGE_NOTIFY and the rmdir without
     * waiting for the interim reply) the wakeup is lost and the request
     * would park forever, never completing.  Check the flag here under
     * state->lock — atomic against the callback, exactly as the async
     * send_response path does — and complete immediately with
     * STATUS_DELETE_PENDING.  DELETE takes precedence over any buffered
     * events, matching MS-SMB2 3.3.4.4 and the send_response classification. */
    if (chimera_vfs_notify_watch_take_deleted(state->watch)) {
        pthread_mutex_unlock(&state->lock);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_DELETE_PENDING);
        return;
    }

    /* Re-filter drained events against this request's filter — the
     * watch's mask may have been broader when these were enqueued
     * (see same logic in the async send_response path).  Done under
     * state->lock so that a "no matching events" outcome still allows
     * us to park atomically vs. the VFS callback. */
    if (nevents > 0) {
        int j = 0;
        for (int i = 0; i < nevents; i++) {
            if (events[i].action & vfs_mask) {
                if (i != j) {
                    events[j] = events[i];
                }
                j++;
            }
        }
        nevents = j;
    }

    nevents = chimera_smb_notify_coalesce_events(events, nevents);

    if (nevents > 0 || overflowed) {
        pthread_mutex_unlock(&state->lock);

        /* If serializing all events at the client's OutputBufferLength
         * would truncate the stream, escalate to NOTIFY_ENUM_DIR rather
         * than silently dropping the un-serialized tail.  Dry-run
         * serialize into a scratch buffer sized to OutputBufferLength
         * and compare consumed against nevents. */
        if (!overflowed && nevents > 0) {
            uint8_t tmp[16384];
            int     consumed = 0;
            int     max_data = (int) sizeof(tmp);

            /* Treat OutputBufferLength == 0 as "zero bytes allowed",
            * not "unlimited".  See same comment in send_response. */
            if ((int) request->change_notify.output_buffer_length < max_data) {
                max_data = (int) request->change_notify.output_buffer_length;
            }

            (void) chimera_smb_notify_serialize_events(
                &thread->iconv_ctx, events, nevents, tmp, max_data,
                &consumed);

            if (consumed < nevents) {
                overflowed = 1;
                nevents    = 0;

                /* Events dropped for not fitting the client's non-zero
                 * OutputBufferLength: make the next CHANGE_NOTIFY on this
                 * handle report overflow too, until the client rescans
                 * (smb2.notify.valid-req).  Buffer == 0 is a poll and does
                 * not stick. */
                if (request->change_notify.output_buffer_length > 0) {
                    chimera_vfs_notify_mark_overflow(state->watch);
                }
            }
        }

        /* Events available — complete immediately.
         * Store events in request for reply building. */
        request->change_notify.nevents    = nevents;
        request->change_notify.overflowed = overflowed;
        memcpy(request->change_notify.events, events,
               nevents * sizeof(struct chimera_vfs_notify_event));

        chimera_smb_open_file_release(request, open_file);
        /* Per MS-SMB2: overflow → STATUS_NOTIFY_ENUM_DIR with no records. */
        chimera_smb_complete_request(request,
                                     overflowed ? SMB2_STATUS_NOTIFY_ENUM_DIR
                                                : SMB2_STATUS_SUCCESS);
        return;
    }

    /* No events — about to park the request.
     *
     * A CHANGE_NOTIFY that would go asynchronous inside a multi-command
     * compound is governed by MS-SMB2 <159>/<162> (the Win7 compound
     * behaviour the smb2.compound.interim* tests pin):
     *
     *   - If it is NOT the last request in the compound, the server fails
     *     it with STATUS_INTERNAL_ERROR (the other, synchronous, requests
     *     in the compound still succeed) -- interim2.
     *
     *   - If it IS the last request, the server is allowed to split it off
     *     and let it go async: the interim STATUS_PENDING is sent as a
     *     standalone async message (just as for a non-compounded notify)
     *     and this slot is dropped from the compound reply.  The parked
     *     request can then be cancelled (interim1) or completed later
     *     (interim3).
     *
     * Synchronous completion (events were already pending) is fine for any
     * position -- that slot's reply lands in the compound normally and we
     * never reach this check. */
    if (request->compound->num_requests > 1 &&
        request != request->compound->requests[request->compound->num_requests - 1]) {
        pthread_mutex_unlock(&state->lock);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* We need to send an interim STATUS_PENDING response and
     * keep the request alive until events arrive.
     *
     * MS-SMB2 3.3.5.2.9 caps the simultaneously-outstanding async operations a
     * connection may hold.  Reuse the per-connection counter and ceiling that
     * 9c5c7c41 wired into the blocking named-pipe READ path: if parking this
     * notify would reach the ceiling, reject it with INSUFFICIENT_RESOURCES
     * instead (the contract smb2.credits.*_notify_max_async_credits asserts). */
    {
        struct chimera_smb_conn          *conn   = request->compound->conn;
        struct chimera_server_smb_shared *shared = thread->shared;

        if (conn->async_outstanding >=
            (uint32_t) shared->config.smb2_max_async_credits - 1) {
            pthread_mutex_unlock(&state->lock);
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request,
                                         SMB2_STATUS_INSUFFICIENT_RESOURCES);
            return;
        }
    }

    nr = chimera_smb_notify_request_alloc();

    if (unlikely(!nr)) {
        /* OOM — bail out cleanly instead of NULL-dereffing.  state->lock
         * was held going into the park path; the open_file resolve ref
         * is still owned by the request. */
        pthread_mutex_unlock(&state->lock);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request,
                                     SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }

    nr->conn                 = request->compound->conn;
    nr->thread               = thread;
    nr->open_file            = open_file;
    nr->tree                 = request->tree;
    nr->message_id           = request->smb2_hdr.message_id;
    nr->async_id             = request->smb2_hdr.message_id; /* Use message_id as async_id */
    nr->session_id           = request->smb2_hdr.session_id;
    nr->tree_id              = request->tree ? request->tree->tree_id : 0;
    nr->credit_charge        = request->smb2_hdr.credit_charge;
    nr->credit_request       = request->smb2_hdr.credit_request_response;
    nr->output_buffer_length = request->change_notify.output_buffer_length;
    nr->completion_filter    = request->change_notify.completion_filter;
    nr->watch_tree           = request->change_notify.watch_tree;

    /* Capture signing/encryption state so the deferred async responses can be
     * secured.  An unsigned async reply on a signed session is rejected by
     * Windows clients with SEC_E_INCOMPLETE_MESSAGE, and on an encrypting
     * session every response must be wrapped in a TRANSFORM header -- either
     * way breaks the watcher if we send it in the clear. */
    chimera_smb_secure_send_snapshot(request, &nr->secure);

    state->pending = nr;
    pthread_mutex_unlock(&state->lock);

    /* Add to connection's parked notify list for CANCEL lookup.
     * conn->parked_notifies is single-threaded under the connection's
     * SMB thread; no lock needed here. */
    nr->next = request->compound->conn->parked_notifies;
    nr->prev = NULL;
    if (nr->next) {
        nr->next->prev = nr;
    }
    request->compound->conn->parked_notifies = nr;

    /* Count this parked notify against the connection's async-operation
     * ceiling.  chimera_smb_notify_unlink (the single un-park chokepoint)
     * decrements it once, guarded by nr->async_counted. */
    nr->async_counted = 1;
    request->compound->conn->async_outstanding++;

    /* Send the interim STATUS_PENDING response directly as a standalone
     * message with proper NetBIOS framing and async header layout.
     * The actual response will be sent later by smb_notify_send_response. */
    chimera_smb_notify_send_interim(nr);

    /* Complete the compound request silently.  The interim response has
     * already been sent, so the compound reply for this slot just needs
     * to be a no-op.  Use STATUS_PENDING which the reply builder will
     * emit as a standard error body. */
    request->async_id = nr->async_id;
    chimera_smb_complete_request(request, SMB2_STATUS_PENDING);
} /* chimera_smb_change_notify */

void
chimera_smb_change_notify_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    int     nevents    = request->change_notify.nevents;
    int     overflowed = request->change_notify.overflowed;
    uint8_t data_buf[16384];
    int     data_len;

    /* StructureSize = 9 */
    evpl_iovec_cursor_append_uint16(reply_cursor, 9);

    if (overflowed || nevents == 0) {
        evpl_iovec_cursor_append_uint16(reply_cursor, 0);
        evpl_iovec_cursor_append_uint32(reply_cursor, 0);
        evpl_iovec_cursor_append_uint8(reply_cursor, 0);
        return;
    }

    /* Serialize events into a flat buffer, capped at client's OutputBufferLength */
    {
        int max_data = (int) sizeof(data_buf);

        /* Treat OutputBufferLength == 0 as "zero bytes allowed", not
         * "unlimited".  In practice the caller in chimera_smb_change_notify
         * already escalated to overflow (nevents=0) in this case, so this
         * is defense-in-depth. */
        if ((int) request->change_notify.output_buffer_length < max_data) {
            max_data = (int) request->change_notify.output_buffer_length;
        }

        /* Events were already pre-fit-checked in chimera_smb_change_notify
         * against OutputBufferLength.  We pass NULL for events_consumed
         * because the caller has externally guaranteed they will fit. */
        data_len = chimera_smb_notify_serialize_events(
            &request->compound->thread->iconv_ctx,
            request->change_notify.events, nevents, data_buf, max_data,
            NULL);
    }

    if (data_len == 0) {
        evpl_iovec_cursor_append_uint16(reply_cursor, 0);
        evpl_iovec_cursor_append_uint32(reply_cursor, 0);
        evpl_iovec_cursor_append_uint8(reply_cursor, 0);
        return;
    }

    /* OutputBufferOffset (relative to SMB2 header start = 64 + 8 bytes into reply body) */
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 8);
    /* OutputBufferLength */
    evpl_iovec_cursor_append_uint32(reply_cursor, data_len);
    /* Copy serialized data into cursor */
    memcpy(evpl_iovec_cursor_data(reply_cursor), data_buf, data_len);
    evpl_iovec_cursor_skip(reply_cursor, data_len);
} /* chimera_smb_change_notify_reply */
