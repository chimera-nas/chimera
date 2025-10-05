// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"

void
chimera_smb_logoff(struct chimera_smb_request *request)
{
    struct chimera_smb_session_handle *session_handle;
    struct chimera_server_smb_thread  *thread = request->compound->thread;
    struct chimera_smb_conn           *conn   = request->compound->conn;

    HASH_FIND(hh, conn->session_handles, &request->smb2_hdr.session_id, sizeof(uint64_t), session_handle);

    chimera_smb_abort_if(!session_handle,
                         "Received SMB2 LOGOFF request for unknown session, should have been caught by session setup");

    chimera_smb_session_release(thread, thread->shared, session_handle->session);

    HASH_DELETE(hh, conn->session_handles, session_handle);

    conn->last_session_handle = NULL;

    session_handle->session = NULL;

    //chimera_smb_session_handle_free(thread, session_handle);

    //request->session_handle = NULL;

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);

} /* chimera_smb_tree_disconnect */

int
chimera_smb_parse_logoff(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (unlikely(request->request_struct_size != SMB2_LOGOFF_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 LOGOFF request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_LOGOFF_REQUEST_SIZE);
        return -1;
    }

    return 0;
} /* chimera_smb_parse_logoff */

void
chimera_smb_logoff_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_LOGOFF_REPLY_SIZE);
} /* chimera_smb_logoff_reply */

