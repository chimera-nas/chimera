// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_signing.h"
#include "smb_auth.h"
#include "smb_wbclient.h"
#include "vfs/vfs.h"

// Process NTLM authentication
static int
process_ntlm_auth(
    struct chimera_smb_request       *request,
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_conn          *conn,
    const uint8_t                    *input,
    size_t                            input_len)
{
    uint8_t *output     = NULL;
    size_t   output_len = 0;
    int      rc;

    rc = smb_ntlm_process(&conn->ntlm_ctx,
                          shared->vfs,
                          &shared->config.auth,
                          input,
                          input_len,
                          &output,
                          &output_len);

    // Store output token for reply
    conn->ntlm_output     = output;
    conn->ntlm_output_len = output_len;

    return rc;
} // process_ntlm_auth

// Process Kerberos authentication
static int
process_kerberos_auth(
    struct chimera_smb_request       *request,
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_conn          *conn,
    const uint8_t                    *input,
    size_t                            input_len)
{
    uint8_t *output     = NULL;
    size_t   output_len = 0;
    int      rc;

    // Initialize GSSAPI context if not already done
    if (!conn->gssapi_ctx.initialized) {
        const char *keytab = shared->config.auth.kerberos_keytab[0] ?
            shared->config.auth.kerberos_keytab : NULL;

        if (smb_gssapi_init(&conn->gssapi_ctx, keytab) < 0) {
            chimera_smb_error("Failed to initialize GSSAPI context");
            return -1;
        }
    }

    rc = smb_gssapi_process(&conn->gssapi_ctx,
                            input,
                            input_len,
                            &output,
                            &output_len);

    // Store output token for reply
    conn->ntlm_output     = output;
    conn->ntlm_output_len = output_len;

    return rc;
} // process_kerberos_auth

void
chimera_smb_session_setup(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread  *thread = request->compound->thread;
    struct chimera_server_smb_shared  *shared = thread->shared;
    struct chimera_smb_conn           *conn   = request->compound->conn;
    struct chimera_smb_session        *session;
    struct chimera_smb_session_handle *session_handle;
    struct evpl_iovec_cursor           input_cursor;
    uint8_t                           *input = NULL;
    size_t                             input_len;
    enum smb_auth_mech                 mech;
    int                                rc;
    uint32_t                           uid = 0, gid = 0, ngids = 0;
    uint32_t                           gids[32];

    // Free any previous output buffer
    if (conn->ntlm_output) {
        free(conn->ntlm_output);
        conn->ntlm_output     = NULL;
        conn->ntlm_output_len = 0;
    }

    if (request->session_setup.blob_length > 0) {
        input_len = request->session_setup.blob_length;
        input     = alloca(input_len);

        evpl_iovec_cursor_init(&input_cursor,
                               request->session_setup.input_iov,
                               request->session_setup.input_niov);

        evpl_iovec_cursor_get_blob(&input_cursor, input, input_len);
    } else {
        input_len = 0;
    }

    // Detect authentication mechanism
    mech = smb_auth_detect_mechanism(input, input_len);
    chimera_smb_debug("Session setup: detected mechanism %s", smb_auth_mech_name(mech));

    // Route to appropriate handler
    switch (mech) {
        case SMB_AUTH_MECH_NTLM:
            rc = process_ntlm_auth(request, shared, conn, input, input_len);
            break;

        case SMB_AUTH_MECH_KERBEROS:
            if (!shared->config.auth.kerberos_enabled) {
                chimera_smb_error("Kerberos authentication not enabled");
                rc = -1;
            } else {
                rc = process_kerberos_auth(request, shared, conn, input, input_len);
            }
            break;

        default:
            chimera_smb_error("Unknown authentication mechanism");
            rc = -1;
            break;
    } /* switch */

    if (rc == 0) {
        // Authentication complete
        if (!request->session_handle) {
            session = chimera_smb_session_alloc(shared);

            session_handle = chimera_smb_session_handle_alloc(thread);

            session_handle->session_id = session->session_id;
            session_handle->session    = session;

            chimera_smb_debug("chimera_smb_session_setup adding session_handle %p\n",
                              session_handle);

            HASH_ADD(hh, conn->session_handles, session_id, sizeof(uint64_t), session_handle);

            session_handle->ctx = GSS_C_NO_CONTEXT;

            request->compound->conn->last_session_handle = session_handle;

            request->session_handle = session_handle;
        }

        session_handle = request->session_handle;
        session        = session_handle->session;

        // Get session key and set credentials based on mechanism
        const char *sid        = NULL;
        const char *username   = NULL;
        int         is_ad_user = 0;
        char        sid_buf[SMB_WBCLIENT_SID_MAX_LEN];

        if (mech == SMB_AUTH_MECH_NTLM) {
            uint8_t session_key[SMB_NTLM_SESSION_KEY_SIZE];

            if (smb_ntlm_get_session_key(&conn->ntlm_ctx, session_key, sizeof(session_key)) == 0) {
                chimera_smb_derive_signing_key(conn->dialect,
                                               session_handle->signing_key,
                                               session_key,
                                               SMB_NTLM_SESSION_KEY_SIZE);
            }

            uid        = smb_ntlm_get_uid(&conn->ntlm_ctx);
            gid        = smb_ntlm_get_gid(&conn->ntlm_ctx);
            ngids      = conn->ntlm_ctx.ngids;
            username   = smb_ntlm_get_username(&conn->ntlm_ctx);
            sid        = smb_ntlm_get_sid(&conn->ntlm_ctx);
            is_ad_user = smb_ntlm_is_winbind_user(&conn->ntlm_ctx);

            if (ngids > 32) {
                ngids = 32;
            }
            memcpy(gids, conn->ntlm_ctx.gids, ngids * sizeof(uint32_t));

            // Synthesize Unix SID for local users
            if (!sid && !is_ad_user) {
                smb_ntlm_synthesize_unix_sid(uid, sid_buf, sizeof(sid_buf));
                sid = sid_buf;
            }

            chimera_smb_info("NTLM auth complete: user=%s uid=%u gid=%u sid=%s",
                             username, uid, gid, sid ? sid : "none");

        } else if (mech == SMB_AUTH_MECH_KERBEROS) {
            uint8_t session_key[SMB_GSSAPI_SESSION_KEY_SIZE];

            if (smb_gssapi_get_session_key(&conn->gssapi_ctx, session_key, sizeof(session_key)) == 0) {
                chimera_smb_derive_signing_key(conn->dialect,
                                               session_handle->signing_key,
                                               session_key,
                                               SMB_GSSAPI_SESSION_KEY_SIZE);
            }

            // Map Kerberos principal to Unix credentials via winbind
            const char *principal = smb_gssapi_get_principal(&conn->gssapi_ctx);
            username = principal;

            if (shared->config.auth.winbind_enabled && smb_wbclient_available()) {
                if (smb_wbclient_map_principal(principal, &uid, &gid, &ngids, gids, sid_buf) == 0) {
                    sid        = sid_buf;
                    is_ad_user = 1;
                } else {
                    chimera_smb_error("Failed to map Kerberos principal to Unix credentials");
                    // Use anonymous credentials as fallback
                    uid   = 65534;
                    gid   = 65534;
                    ngids = 0;
                    smb_ntlm_synthesize_unix_sid(uid, sid_buf, sizeof(sid_buf));
                    sid = sid_buf;
                }
            } else {
                // No winbind - use anonymous credentials
                chimera_smb_debug("Kerberos auth without winbind - using anonymous credentials");
                uid   = 65534;
                gid   = 65534;
                ngids = 0;
                smb_ntlm_synthesize_unix_sid(uid, sid_buf, sizeof(sid_buf));
                sid = sid_buf;
            }

            chimera_smb_info("Kerberos auth complete: principal=%s uid=%u gid=%u sid=%s",
                             principal, uid, gid, sid ? sid : "none");
        }

        // Cache AD users in VFS user cache (non-pinned, will expire)
        if (is_ad_user && username && username[0]) {
            chimera_vfs_add_user(shared->vfs,
                                 username,
                                 NULL,  // No password for AD users
                                 NULL,  // No SMB password hash
                                 sid,
                                 uid, gid, ngids, gids,
                                 0);    // Not pinned - can expire
            chimera_smb_debug("Cached AD user '%s' in VFS user cache", username);
        }

        // Set session credentials
        chimera_vfs_cred_init_unix(&session->cred, uid, gid, ngids, gids);

        if (!(session->flags & CHIMERA_SMB_SESSION_AUTHORIZED)) {
            memcpy(session->signing_key, session_handle->signing_key, sizeof(session_handle->signing_key));
            chimera_smb_session_authorize(shared, session);
        }

        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    } else if (rc == 1) {
        // Continue needed
        chimera_smb_complete_request(request, SMB2_STATUS_MORE_PROCESSING_REQUIRED);
    } else {
        // Authentication failed
        chimera_smb_error("Authentication failed (mechanism: %s)", smb_auth_mech_name(mech));
        chimera_smb_complete_request(request, SMB2_STATUS_LOGON_FAILURE);

        if (request->session_handle &&
            (!(request->session_handle->session->flags & CHIMERA_SMB_SESSION_AUTHORIZED))) {
            chimera_smb_session_release(thread, shared, request->session_handle->session);
            chimera_smb_session_handle_free(thread, request->session_handle);
            request->session_handle   = NULL;
            conn->last_session_handle = NULL;
        }
    }

    evpl_iovecs_release(thread->evpl, request->session_setup.input_iov, request->session_setup.input_niov);

} /* smb_proc_session_setup */


void
chimera_smb_session_setup_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    struct chimera_smb_conn *conn                   = request->compound->conn;
    uint16_t                 security_buffer_offset = sizeof(struct smb2_header) + 8;
    uint16_t                 security_buffer_length = conn->ntlm_output_len;

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_SESSION_SETUP_REPLY_SIZE);

    evpl_iovec_cursor_append_uint16(reply_cursor, 0);

    evpl_iovec_cursor_append_uint16(reply_cursor, security_buffer_offset);

    evpl_iovec_cursor_append_uint16(reply_cursor, security_buffer_length);

    if (security_buffer_length > 0) {
        evpl_iovec_cursor_append_blob(reply_cursor,
                                      conn->ntlm_output,
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
        request->status = SMB2_STATUS_INVALID_PARAMETER;
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
                                                               1);

    return 0;
} /* chimera_smb_parse_session_setup */
