// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

//#include <gssapi/gssapi.h>

#include "smb_internal.h"
#include "smb_procs.h"

void
chimera_smb_session_setup(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread  *thread = request->compound->thread;
    struct chimera_server_smb_shared  *shared = thread->shared;
    struct chimera_smb_conn           *conn   = request->compound->conn;
    struct chimera_smb_session        *session;
    struct chimera_smb_session_handle *session_handle;

    #if 0

    OM_uint32                          maj, min;
    gss_buffer_desc                    input, output;
    OM_uint32                          flags;
    gss_OID                            mech_type = GSS_C_NT_HOSTBASED_SERVICE;

    dddd

    input.value = (void *) evpl_iovec_cursor_get_blob(&request->request_cursor, request_blob_length);
    input.length = request_blob_length;

    conn->srv_cred = GSS_C_NO_CREDENTIAL;

    // Accept the security context
    maj = gss_accept_sec_context(&min,
                                 &conn->ctx,
                                 conn->srv_cred,
                                 &input,
                                 GSS_C_NO_CHANNEL_BINDINGS,
                                 NULL,       /* src name (ignored) */
                                 &mech_type, /* mech type */
                                 &output,
                                 &flags,
                                 NULL, NULL);

    switch (maj) {
        case GSS_S_COMPLETE:
            request->status = SMB2_STATUS_SUCCESS;
            break;
        case GSS_S_CONTINUE_NEEDED:
            request->status = SMB2_STATUS_MORE_PROCESSING_REQUIRED;
            break;
        default:
            request->status = SMB2_STATUS_LOGON_FAILURE;
            break;
    } /* switch */

    evpl_iovec_cursor_append_uint16(&request->reply_cursor, SMB2_SESSION_SETUP_REPLY_SIZE);

    /* Session Flags */
    evpl_iovec_cursor_append_uint16(&request->reply_cursor, 0);

    /* Security Buffer Offset */
    evpl_iovec_cursor_append_uint16(&request->reply_cursor, sizeof(struct smb2_header) + 8);

    /* Security Buffer Length */
    evpl_iovec_cursor_append_uint16(&request->reply_cursor, output.length);

    /* Security Buffer */
    evpl_iovec_cursor_append_blob(&request->reply_cursor, output.value, output.length);

    chimera_smb_complete(evpl, thread, request, SMB2_STATUS_SUCCESS);

    #endif /* if 0 */

    session = chimera_smb_session_alloc(shared);

    session_handle = chimera_smb_session_handle_alloc(thread);

    session_handle->session_id = session->session_id;
    session_handle->session    = session;

    HASH_ADD(hh, conn->session_handles, session_id, sizeof(uint64_t), session_handle);

    request->compound->conn->last_session = session;

    request->session = session;

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* smb_proc_session_setup */


void
chimera_smb_session_setup_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_SESSION_SETUP_REPLY_SIZE);

    /* Session Flags */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0);

    /* Security Buffer Offset */
    evpl_iovec_cursor_append_uint16(reply_cursor, sizeof(struct smb2_header) + 8);

    /* Security Buffer Length */
    evpl_iovec_cursor_append_uint16(reply_cursor, 0);
} /* chimera_smb_session_setup_reply */

int
chimera_smb_parse_session_setup(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{


    if (request->request_struct_size != SMB2_SESSION_SETUP_REQUEST_SIZE) {
        chimera_smb_error("Received SMB2 SESSION_SETUP request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_SESSION_SETUP_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint8(request_cursor, &request->session_setup.flags);
    evpl_iovec_cursor_get_uint8(request_cursor, &request->session_setup.security_mode);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->session_setup.capabilities);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->session_setup.channel);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->session_setup.blob_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->session_setup.blob_length);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->session_setup.prev_session_id);

    evpl_iovec_cursor_skip(request_cursor,
                           request->session_setup.blob_offset - evpl_iovec_cursor_consumed(request_cursor));

    evpl_iovec_cursor_skip(request_cursor, request->session_setup.blob_length);

    return 0;
} /* chimera_smb_parse_session_setup */
