// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <gssapi/gssapi_ntlmssp.h>

#include "smb_internal.h"
#include "smb_procs.h"

static int
chimera_smb_gss_display_status(
    int       type,
    OM_uint32 err,
    char     *out,
    size_t    outmaxlen)
{
    gss_buffer_desc text;
    OM_uint32       msg_ctx = 0;
    OM_uint32       maj, min;
    size_t          written = 0;

    if (out == NULL || outmaxlen == 0) {
        return -1;
    }

    /* ensure the output buffer starts empty */
    out[0] = '\0';

    do {
        maj = gss_display_status(&min,
                                 err,
                                 type,
                                 GSS_C_NO_OID,
                                 &msg_ctx,
                                 &text);

        if (maj != GSS_S_COMPLETE) {
            return -1;
        }

        /* Prefix with comma if this is not the first message */
        const char *prefix = (written == 0) ? "" : ", ";

        int         needed = snprintf(out + written,
                                      outmaxlen - written,
                                      "%s%.*s",
                                      prefix,
                                      (int) text.length,
                                      (char *) text.value);

        gss_release_buffer(&min, &text);

        if (needed < 0 || (size_t) needed >= outmaxlen - written) {
            /* snprintf would have truncated the buffer */
            return -1;
        }

        written += (size_t) needed;

    } while (msg_ctx != 0);

    return (int) written;
} /* chimera_smb_gss_display_status */

static void
chimera_smb_gss_error(
    const char *func,
    OM_uint32   maj,
    OM_uint32   min)
{
    char err_maj[256];
    char err_min[256];

    if (chimera_smb_gss_display_status(GSS_C_GSS_CODE, maj, err_maj, sizeof(err_maj)) < 0) {
        snprintf(err_maj, sizeof(err_maj), "unknown");
    }

    if (chimera_smb_gss_display_status(GSS_C_MECH_CODE, min, err_min, sizeof(err_min)) < 0) {
        snprintf(err_min, sizeof(err_min), "unknown");
    }

    chimera_smb_error("%s: GSS-API error - Major: %s, Minor: %s", func, err_maj, err_min);
} /* chimera_smb_gss_error */

void
chimera_smb_session_setup(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread  *thread = request->compound->thread;
    struct chimera_server_smb_shared  *shared = thread->shared;
    struct chimera_smb_conn           *conn   = request->compound->conn;
    struct chimera_smb_session        *session;
    struct chimera_smb_session_handle *session_handle;
    struct evpl_iovec_cursor           input_cursor;
    OM_uint32                          flags;
    gss_buffer_desc                    input     = GSS_C_EMPTY_BUFFER;
    gss_OID                            mech_type = GSS_C_NO_OID;

    if (request->session_setup.blob_length > 0) {
        input.length = request->session_setup.blob_length;
        input.value  = alloca(request->session_setup.blob_length);

        evpl_iovec_cursor_init(&input_cursor,
                               request->session_setup.input_iov,
                               request->session_setup.input_niov);

        evpl_iovec_cursor_get_blob(&input_cursor,
                                   input.value,
                                   input.length);
    } else {
        input.length = 0;

    } /* chimera_smb_session_setup */

#if 0
    if (conn->srv_cred == GSS_C_NO_CREDENTIAL) {
        chimera_smb_debug("Acquiring server credentials");
        maj = gss_acquire_cred(&min,
                               GSS_C_NO_NAME,
                               GSS_C_INDEFINITE,
                               GSS_C_NO_OID_SET,
                               GSS_C_ACCEPT,
                               &conn->srv_cred,
                               NULL,
                               NULL);

        if (maj != GSS_S_COMPLETE) {
            chimera_smb_gss_error("gss_acquire_cred", maj, min);
            request->status = SMB2_STATUS_LOGON_FAILURE;
            return;
        }
        chimera_smb_debug("Server credentials acquired successfully");
    }
 #endif /* if 0 */

    // Release any previous GSS output buffer before the next call
    if (conn->gss_output.value != NULL) {
        gss_release_buffer(&conn->gss_minor, &conn->gss_output);
        conn->gss_output.value  = NULL;
        conn->gss_output.length = 0;
    }

    conn->gss_major = gss_accept_sec_context(&conn->gss_minor,
                                             &conn->ctx,
                                             conn->srv_cred,
                                             &input,
                                             GSS_C_NO_CHANNEL_BINDINGS,
                                             NULL,
                                             &mech_type,
                                             &conn->gss_output,
                                             &flags,
                                             NULL, NULL);

                                             #if 0
    {
        char err_buf[256];
        chimera_smb_gss_display_status(GSS_C_GSS_CODE,
                                       conn->gss_major,
                                       err_buf,
                                       sizeof(err_buf));

        chimera_smb_debug("gss_accept_sec_context: %s output length %d",
                          err_buf,
                          conn->gss_output.length);
    }
                          #endif /* if 0 */

    if (conn->gss_major == GSS_S_COMPLETE) {
        session = chimera_smb_session_alloc(shared);

        session_handle = chimera_smb_session_handle_alloc(thread);

        session_handle->session_id = session->session_id;
        session_handle->session    = session;

        HASH_ADD(hh, conn->session_handles, session_id, sizeof(uint64_t), session_handle);

        request->compound->conn->last_session = session;

        request->session = session;
    }

    switch (conn->gss_major) {
        case GSS_S_COMPLETE:
            chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
            break;
        case GSS_S_CONTINUE_NEEDED:
            chimera_smb_complete_request(request, SMB2_STATUS_MORE_PROCESSING_REQUIRED);
            break;
        default:
            chimera_smb_gss_error("gss_accept_sec_context", conn->gss_major, conn->gss_minor);
            chimera_smb_complete_request(request, SMB2_STATUS_LOGON_FAILURE);
            break;
    } /* switch */

} /* smb_proc_session_setup */


void
chimera_smb_session_setup_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    struct chimera_smb_conn *conn                   = request->compound->conn;
    uint16_t                 security_buffer_offset = sizeof(struct smb2_header) + 8;
    uint16_t                 security_buffer_length = conn->gss_output.length;

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_SESSION_SETUP_REPLY_SIZE);

    evpl_iovec_cursor_append_uint16(reply_cursor, 0);

    evpl_iovec_cursor_append_uint16(reply_cursor, security_buffer_offset);

    evpl_iovec_cursor_append_uint16(reply_cursor, security_buffer_length);

    if (security_buffer_length > 0) {
        evpl_iovec_cursor_append_blob(reply_cursor,
                                      conn->gss_output.value,
                                      security_buffer_length);
    }
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


    request->session_setup.input_niov = evpl_iovec_cursor_move(request_cursor,
                                                               request->session_setup.input_iov,
                                                               64,
                                                               request->session_setup.blob_length,
                                                               0);

    return 0;
} /* chimera_smb_parse_session_setup */
