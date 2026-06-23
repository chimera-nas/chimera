// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_notify.h"
#include "smb2.h"

/*
 * SMB2_CANCEL has no request body beyond the header.
 * No response is sent for CANCEL itself — it triggers
 * cancellation of the target async request.
 */

int
chimera_smb_parse_cancel(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    /* No body to parse */
    return 0;
} /* chimera_smb_parse_cancel */

void
chimera_smb_cancel(struct chimera_smb_request *request)
{
    struct chimera_smb_conn           *conn     = request->compound->conn;
    struct chimera_smb_notify_request *nr       = NULL;
    struct chimera_smb_notify_request *match    = NULL;
    struct chimera_smb_notify_request *fallback = NULL;
    uint64_t                           target_id;
    int                                async;

    /* MS-SMB2 3.3.5.2.2 / 2.2.30: a CANCEL StructureSize that is not exactly 4
     * is invalid and the request is failed before command processing.  CANCEL
     * has no response (3.3.5.17), so "fail" here means drop it without
     * processing -- completing it as PENDING suppresses any reply, the same as
     * the normal CANCEL completion below. */
    if (unlikely(request->request_struct_size != SMB2_CANCEL_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CANCEL with invalid struct size (%u expected %u)",
                          request->request_struct_size, SMB2_CANCEL_REQUEST_SIZE);
        chimera_smb_complete_request(request, SMB2_STATUS_PENDING);
        return;
    }

    /* A CANCEL targets an async_id when SMB2_FLAGS_ASYNC_COMMAND is set
     * (the client received our interim STATUS_PENDING and learned the
     * AsyncId), otherwise it targets a MessageId.  See MS-SMB2 3.2.4.24. */
    async = (request->smb2_hdr.flags & SMB2_FLAGS_ASYNC_COMMAND) != 0;

    if (async) {
        target_id = request->smb2_hdr.async.async_id;
    } else {
        target_id = request->smb2_hdr.message_id;
    }

    /* Search parked CHANGE_NOTIFY requests on this connection.  The list is
     * single-threaded under the connection's SMB thread, so no lock. */
    for (nr = conn->parked_notifies; nr; nr = nr->next) {
        if (async) {
            if (nr->async_id == target_id) {
                match = nr;
                break;
            }
        } else if (target_id != 0) {
            if (nr->message_id == target_id) {
                match = nr;
                break;
            }
        } else if (!fallback && nr->session_id == request->smb2_hdr.session_id) {
            /* A Samba client whose negotiated signing algorithm is below
             * AES-128-GMAC (chimera only implements CMAC) sends a *sync*
             * CANCEL carrying MessageId 0 as a sentinel for "the
             * outstanding async request on this session" rather than the
             * real MessageId (libcli smbXcli_base.c sets cancel_mid=0 in
             * that case).  Match the most recently parked notify for the
             * session as a fallback so these cancels are honoured instead
             * of leaking the parked request forever. */
            fallback = nr;
        }
    }

    if (!match && !async && target_id == 0) {
        match = fallback;
    }

    if (match) {
        chimera_smb_notify_cancel(match);
    } else if (async) {
        /* Search requests pending an async-interim.  A blocking byte-range LOCK
         * (MS-SMB2 3.3.5.14) parked on a conflicting range is cancellable: cancel
         * its VFS acquire and complete it with STATUS_CANCELLED (smb2.lock.cancel).
         * A blocking named-pipe READ is likewise cancellable.  A CREATE blocked on
         * a lease break is not (yet) cancellable from here -- absorb that silently;
         * it completes when its break acks. */
        struct chimera_smb_request *parked;

        for (parked = conn->parked_requests; parked;
             parked = parked->async.park_next) {
            if (parked->async_id == target_id) {
                break;
            }
        }

        if (parked && parked->smb2_hdr.command == SMB2_LOCK &&
            parked->lock.parked && parked->lock.open_file) {
            struct chimera_smb_request *abort =
                chimera_smb_lock_abort_parked(request->compound->thread,
                                              parked->lock.open_file);

            /* abort_parked clears open_file->parked_lock_req and returns the same
             * request; complete it with CANCELLED rather than the abort default
             * (RANGE_NOT_LOCKED). */
            if (abort) {
                chimera_smb_lock_park_finish(abort, SMB2_STATUS_CANCELLED);
            }
        } else if (parked && parked->async.pipe_read) {
            /* A blocking named-pipe READ never completes on its own, so a
             * CANCEL resolves it with STATUS_CANCELLED.  complete_request
             * unlinks it from the parked list (via async_interim_cancel) and
             * emits the final READ error response carrying the AsyncId.  Free
             * its async-credit slot. */
            if (conn->async_outstanding) {
                conn->async_outstanding--;
            }
            chimera_smb_complete_request(parked, SMB2_STATUS_CANCELLED);
        }
        /* Other parked requests (e.g. a CREATE blocked on a lease break) are
         * not yet cancellable from SMB2_CANCEL -- absorb silently; they
         * complete when their break acks. */
    }

    /* CANCEL has no response per MS-SMB2.  Use STATUS_PENDING so the
     * compound reply builder skips this slot — no reply body is ever
     * emitted for SMB2_CANCEL.  (There is therefore no
     * chimera_smb_cancel_reply: SMB2_CANCEL never reaches the reply
     * switch in chimera_smb_compound_reply.) */
    chimera_smb_complete_request(request, SMB2_STATUS_PENDING);
} /* chimera_smb_cancel */
