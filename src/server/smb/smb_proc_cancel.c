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
    }

    /* CANCEL has no response per MS-SMB2.  Use STATUS_PENDING so the
     * compound reply builder skips this slot — no reply body is ever
     * emitted for SMB2_CANCEL.  (There is therefore no
     * chimera_smb_cancel_reply: SMB2_CANCEL never reaches the reply
     * switch in chimera_smb_compound_reply.) */
    chimera_smb_complete_request(request, SMB2_STATUS_PENDING);
} /* chimera_smb_cancel */
