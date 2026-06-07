// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_signing.h"
#include "smb_encrypt.h"
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

    /* SMB3 multichannel session binding (MS-SMB2 3.3.5.5.3).  A SESSION_SETUP
     * whose SMB2_SESSION_FLAG_BINDING bit is set asks to add this connection as
     * an additional channel of an existing session.  The server MUST reject the
     * bind with STATUS_REQUEST_NOT_ACCEPTED when the connection's dialect is not
     * in the SMB 3.x family (or the server is not multichannel-capable — chimera
     * always is).  Caught before authentication so a 2.x client never binds. */
    if ((request->session_setup.flags & SMB2_SESSION_FLAG_BINDING) &&
        conn->dialect < SMB2_DIALECT_3_0) {
        chimera_smb_complete_request(request, SMB2_STATUS_REQUEST_NOT_ACCEPTED);
        evpl_iovecs_release(thread->evpl, request->session_setup.input_iov,
                            request->session_setup.input_niov);
        return;
    }

    /* MS-SMB2 3.3.5.5 step 4: a SESSION_SETUP that binds a new channel to an
     * existing session (SMB2_SESSION_FLAG_BINDING set, non-zero header
     * SessionId, dialect in the 3.x family, and the session found in the
     * dispatcher's lookup) MUST satisfy several constraints before the bind is
     * processed.  A header SessionId of zero is handled earlier as a new
     * authentication (step 3), so binding validation only applies once a
     * session has been resolved.  The checks that are observable here and the
     * status each mandates:
     *
     *   - the bind request MUST be signed (SMB2_FLAGS_SIGNED): an unsigned
     *     bind is failed with STATUS_INVALID_PARAMETER;
     *   - for 3.1.1, Session.SupportsNotifications MUST equal the binding
     *     Connection.SupportsNotifications, else STATUS_INVALID_PARAMETER.
     *
     * (Chimera does not advertise SMB2_GLOBAL_CAP_NOTIFICATIONS, so both
     * values are always FALSE and the notifications check never fires on its
     * own; it is included to keep the validation faithful to the spec for when
     * the capability is implemented.)  WPTS SessionMgmt_SupportsNotifications-
     * Mismatch exercises this path with an unsigned bind and expects
     * STATUS_INVALID_PARAMETER. */
    if ((request->session_setup.flags & SMB2_SESSION_FLAG_BINDING) &&
        conn->dialect >= SMB2_DIALECT_3_0 &&
        request->smb2_hdr.session_id != 0 &&
        request->session_handle) {
        struct chimera_smb_session *bind_session = request->session_handle->session;
        int                         reject       = 0;

        if (!(request->smb2_hdr.flags & SMB2_FLAGS_SIGNED)) {
            reject = 1;
        } else if (conn->dialect == SMB2_DIALECT_3_1_1 &&
                   bind_session->supports_notifications !=
                   conn->supports_notifications) {
            reject = 1;
        }

        if (reject) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            evpl_iovecs_release(thread->evpl, request->session_setup.input_iov,
                                request->session_setup.input_niov);
            return;
        }
    }

    // Detect authentication mechanism
    mech = smb_auth_detect_mechanism(input, input_len);
    chimera_smb_debug("Session setup: detected mechanism %s", smb_auth_mech_name(mech));

    /* A security buffer the server cannot parse as a recognised mechanism token
     * is a malformed request, not an authentication failure: MS-SMB2 answers it
     * with STATUS_INVALID_PARAMETER rather than STATUS_LOGON_FAILURE (the
     * GSS/SSPI layer reports SEC_E_INVALID_TOKEN).  Track it so the failure
     * completion below returns the right status. */
    int bad_token = 0;

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
            rc        = -1;
            bad_token = 1;
            break;
    } /* switch */

    /* Allocate the session (and its SessionId) on the first leg, whether the
     * exchange completes now or needs another round trip. The server MUST
     * return this SessionId in the interim STATUS_MORE_PROCESSING_REQUIRED
     * response so the client echoes it on subsequent legs (MS-SMB2 3.3.5.5).
     * Crucially for SMB 3.1.1, the client keys its session preauth-integrity
     * hash by the response's SessionId: a zero id on the interim response
     * splits the hash across buckets and breaks signing-key derivation. */
    if ((rc == 0 || rc == 1) && !request->session_handle) {
        session = chimera_smb_session_alloc(shared);

        /* Record the connection's negotiated dialect so a later
         * PreviousSessionId reconnect can enforce MS-SMB2 3.3.5.5.1. */
        session->dialect = conn->dialect;

        /* Session.SupportsNotifications is inherited from the connection that
         * first establishes the session (MS-SMB2 3.3.5.5); a later binding
         * connection must match it. */
        session->supports_notifications = conn->supports_notifications;

        /* Bind the lease owner key to the client (ClientGuid captured at
         * NEGOTIATE), not the session id, so same-client sessions share a
         * lease namespace (MS-SMB2 3.3.5.9.8). */
        session->client_key = chimera_smb_lease_client_key(conn->client_guid,
                                                           session->session_id);

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

    if (rc == 0) {
        // Authentication complete
        session_handle = request->session_handle;
        session        = session_handle->session;

        // Get session key and set credentials based on mechanism
        const char *sid        = NULL;
        const char *username   = NULL;
        int         is_ad_user = 0;
        char        sid_buf[SMB_WBCLIENT_SID_MAX_LEN];

        /* Raw session key saved for SMB3 encryption key derivation below; an
         * anonymous/guest (null) session has no usable key and is never
         * encrypted. */
        uint8_t     session_key_saved[32];
        size_t      session_key_saved_len = 0;
        int         is_anonymous          = 0;

        int         is_binding = (request->session_setup.flags &
                                  SMB2_SESSION_FLAG_BINDING) != 0;

        /* SMB3 multichannel session binding (MS-SMB2 §3.3.5.5.3): account for
         * the new channel and enforce the per-session channel limit BEFORE any
         * per-channel key derivation below.  Doing it first keeps the rejection
         * response signed with the established session key the client verifies
         * it against -- the over-limit channel never derives a channel key.  The
         * check and increment share sessions_lock so concurrent binds on
         * different connections cannot race past the limit. */
        if (is_binding &&
            (session->flags & CHIMERA_SMB_SESSION_AUTHORIZED)) {
            int over_limit = 0;

            pthread_mutex_lock(&shared->sessions_lock);
            if (session->num_channels >= SMB2_MAX_CHANNELS) {
                over_limit = 1;
            } else {
                session->num_channels++;
                session_handle->bound_channel = 1;
            }
            pthread_mutex_unlock(&shared->sessions_lock);

            if (over_limit) {
                chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
                evpl_iovecs_release(thread->evpl,
                                    request->session_setup.input_iov,
                                    request->session_setup.input_niov);
                return;
            }
        }

        if (mech == SMB_AUTH_MECH_NTLM) {
            uint8_t session_key[SMB_NTLM_SESSION_KEY_SIZE];

            if (smb_ntlm_get_session_key(&conn->ntlm_ctx, session_key, sizeof(session_key)) == 0) {
                chimera_smb_derive_signing_key(conn->dialect,
                                               session_handle->signing_key,
                                               session_key,
                                               SMB_NTLM_SESSION_KEY_SIZE,
                                               conn->dialect == SMB2_DIALECT_3_1_1 ?
                                               conn->preauth_hash : NULL);
                memcpy(session_key_saved, session_key, SMB_NTLM_SESSION_KEY_SIZE);
                session_key_saved_len = SMB_NTLM_SESSION_KEY_SIZE;
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
                                               SMB_GSSAPI_SESSION_KEY_SIZE,
                                               conn->dialect == SMB2_DIALECT_3_1_1 ?
                                               conn->preauth_hash : NULL);
                memcpy(session_key_saved, session_key, SMB_GSSAPI_SESSION_KEY_SIZE);
                session_key_saved_len = SMB_GSSAPI_SESSION_KEY_SIZE;
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
        chimera_vfs_cred_init_attr(&session->cred, uid, gid, ngids, gids);

        /* SMB3 transport encryption: derive per-session keys from the raw
         * session key.  Skipped for anonymous/guest (null) sessions, which have
         * no usable key and are never encrypted (MS-SMB2 §3.3.5.5.3).
         *
         * Keys are derived whenever encryption is possible at all -- either the
         * global smb_encryption knob OR any per-share encrypt_data share --
         * because a client may tree-connect to a per-share-encrypted share even
         * when global encryption is off.  The global knob alone decides whether
         * the whole session is marked encrypt-all (CHIMERA_SMB_SESSION_ENCRYPT_
         * DATA); per-share encryption leaves the flag clear and encrypts per
         * tree (see the reply path in smb.c). */
        if ((shared->config.encryption || shared->any_share_encrypt) &&
            conn->negotiated.cipher_id != 0 &&
            conn->dialect >= SMB2_DIALECT_3_0 &&
            session_key_saved_len > 0 &&
            !is_anonymous) {
            size_t klen = 0;

            if (chimera_smb_derive_encryption_keys(
                    conn->dialect, conn->negotiated.cipher_id,
                    session_key_saved, session_key_saved_len,
                    conn->dialect == SMB2_DIALECT_3_1_1 ? conn->preauth_hash : NULL,
                    session_handle->enc_key, session_handle->dec_key, &klen) == 0) {
                session_handle->enc_key_len = klen;
                session_handle->cipher_id   = conn->negotiated.cipher_id;
                if (shared->config.encryption) {
                    session->flags |= CHIMERA_SMB_SESSION_ENCRYPT_DATA;
                }
            }
        }

        if (!(session->flags & CHIMERA_SMB_SESSION_AUTHORIZED)) {
            memcpy(session->signing_key, session_handle->signing_key, sizeof(session_handle->signing_key));
            /* The primary channel counts as the session's first channel. */
            session->num_channels = 1;
            if (session_handle->enc_key_len > 0) {
                memcpy(session->enc_key, session_handle->enc_key, sizeof(session->enc_key));
                memcpy(session->dec_key, session_handle->dec_key, sizeof(session->dec_key));
                session->enc_key_len = session_handle->enc_key_len;
                session->cipher_id   = session_handle->cipher_id;
                /* Seed the server's monotonic per-session nonce counter.  It is
                 * never reset for the session's lifetime; reusing a GCM nonce
                 * would be catastrophic. */
                atomic_store(&session->enc_nonce_counter, 1);
            }
            chimera_smb_session_authorize(shared, session);
        }

        /* If the client named a previous session (reconnect on a fresh
         * transport), close it now that this one is established.  MS-SMB2
         * 3.3.5.5.1: when the previous session was negotiated at a different
         * dialect than this connection, the reconnect is rejected -- close the
         * just-created session and fail with STATUS_USER_SESSION_DELETED
         * (smb2.session ReconnectWithDifferentDialect). */
        if (request->session_setup.prev_session_id &&
            chimera_smb_session_invalidate_previous(thread, shared,
                                                    request->session_setup.prev_session_id,
                                                    session, conn->dialect)) {
            chimera_smb_complete_request(request, SMB2_STATUS_USER_SESSION_DELETED);

            HASH_DEL(conn->session_handles, request->session_handle);
            chimera_smb_session_release(thread, shared, session, false);
            chimera_smb_session_handle_free(thread, request->session_handle);
            request->session_handle   = NULL;
            conn->last_session_handle = NULL;
            return;
        }

        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    } else if (rc == 1) {
        // Continue needed
        chimera_smb_complete_request(request, SMB2_STATUS_MORE_PROCESSING_REQUIRED);
    } else {
        // Authentication failed
        chimera_smb_error("Authentication failed (mechanism: %s)", smb_auth_mech_name(mech));
        chimera_smb_complete_request(request,
                                     bad_token ? SMB2_STATUS_INVALID_PARAMETER :
                                     SMB2_STATUS_LOGON_FAILURE);

        /* A failed channel-bind (SMB2_SESSION_FLAG_BINDING) must NOT tear the
         * existing session down — the established channel(s) survive.  Only a
         * failed initial auth (never-authorized session) or a failed non-binding
         * REAUTH invalidates the session. */
        int is_binding = (request->session_setup.flags &
                          SMB2_SESSION_FLAG_BINDING) != 0;

        if (request->session_handle &&
            !((request->session_handle->session->flags &
               CHIMERA_SMB_SESSION_AUTHORIZED) && is_binding)) {
            /* The handle was registered in conn->session_handles when the
             * session was allocated (possibly on an earlier interim leg), so
             * remove it there before freeing — otherwise connection teardown
             * iterates it again and double-releases the session.
             *
             * This tears the session down on auth failure in two cases:
             *   - a brand-new session whose initial authentication failed
             *     (never authorized);
             *   - a failed REAUTH of an already-authorized session, which
             *     MS-SMB2 invalidates.  The LOGON_FAILURE reply above was
             *     already built with the still-valid handle; releasing the
             *     session now frees its trees, which completes any pending
             *     CHANGE_NOTIFY with STATUS_NOTIFY_CLEANUP
             *     (smb2.notify.invalid-reauth) and makes the SessionId
             *     invalid for subsequent requests.  A failed auth holds no
             *     preservable durable opens, so nothing to preserve. */
            HASH_DEL(conn->session_handles, request->session_handle);
            chimera_smb_session_release(thread, shared, request->session_handle->session, false);
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
    uint16_t                 session_flags          = 0;

    /* SessionFlags (MS-SMB2 §2.2.6): advertise per-session encryption so the
     * client encrypts subsequent requests on this session. */
    if (request->session_handle &&
        (request->session_handle->session->flags & CHIMERA_SMB_SESSION_ENCRYPT_DATA)) {
        session_flags |= SMB2_SESSION_FLAG_ENCRYPT_DATA;
    }

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_SESSION_SETUP_REPLY_SIZE);

    evpl_iovec_cursor_append_uint16(reply_cursor, session_flags);

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
