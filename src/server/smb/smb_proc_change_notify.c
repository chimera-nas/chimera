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
    evpl_iovec_cursor_get_uint16(request_cursor, &flags);
    evpl_iovec_cursor_get_uint32(request_cursor, &output_buffer_length);
    evpl_iovec_cursor_get_uint64(request_cursor, &file_id_pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &file_id_vid);
    evpl_iovec_cursor_get_uint32(request_cursor, &completion_filter);

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
     * Reject parking when this CHANGE_NOTIFY is inside a multi-command
     * compound.  Async completion would force the compound reply
     * builder to skip this slot (status STATUS_PENDING is dropped from
     * the compound), and clients that match compound slots positionally
     * may misparse the remaining replies.  Windows itself appears to
     * cope (it matches by message_id), but the safe + portable answer
     * is "don't allow it" — Windows never compounds CHANGE_NOTIFY with
     * anything else in practice anyway.  The client can retry the
     * CHANGE_NOTIFY as a standalone request.
     *
     * Synchronous completion (events were already pending) is fine —
     * that slot's reply lands in the compound normally and we never
     * reach this check. */
    if (request->compound->num_requests > 1) {
        pthread_mutex_unlock(&state->lock);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    /* We need to send an interim STATUS_PENDING response and
     * keep the request alive until events arrive. */

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

    /* Capture signing state so the deferred async responses can be
     * signed.  An unsigned async reply on a signed session is rejected
     * by Windows clients with SEC_E_INCOMPLETE_MESSAGE, breaking the
     * watcher. */
    if ((request->smb2_hdr.flags & SMB2_FLAGS_SIGNED) &&
        request->session_handle) {
        nr->signed_session = 1;
        memcpy(nr->signing_key, request->session_handle->signing_key, 16);
    } else {
        nr->signed_session = 0;
    }

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
