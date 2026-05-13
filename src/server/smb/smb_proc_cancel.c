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
    struct chimera_smb_conn           *conn = request->compound->conn;
    struct chimera_smb_notify_request *nr;
    uint64_t                           target_id;

    /* CANCEL targets either an async_id (if ASYNC flag set) or message_id */
    if (request->smb2_hdr.flags & SMB2_FLAGS_ASYNC_COMMAND) {
        target_id = request->smb2_hdr.async.async_id;
    } else {
        target_id = request->smb2_hdr.message_id;
    }

    /* Search parked CHANGE_NOTIFY requests on this connection */
    for (nr = conn->parked_notifies; nr; nr = nr->next) {
        if (nr->async_id == target_id) {
            chimera_smb_notify_cancel(nr);
            break;
        }
    }

    /* CANCEL has no response per MS-SMB2.  Use STATUS_PENDING so the
     * compound reply builder skips this slot — no reply body is ever
     * emitted for SMB2_CANCEL.  (There is therefore no
     * chimera_smb_cancel_reply: SMB2_CANCEL never reaches the reply
     * switch in chimera_smb_compound_reply.) */
    chimera_smb_complete_request(request, SMB2_STATUS_PENDING);
} /* chimera_smb_cancel */
