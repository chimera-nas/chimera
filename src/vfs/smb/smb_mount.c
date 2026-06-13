// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#include "smb_internal.h"
#include "smb.h"
#include "common/tcp_flavor.h"
#include "vfs/vfs_attrs.h"
#include "evpl/evpl.h"

/* Fold one raw SMB2 message (header+body, no NetBIOS framing, no trailing pad)
 * into the mount connection's 3.1.1 preauth-integrity hash.  Maintained
 * UNCONDITIONALLY across every NEGOTIATE / SESSION_SETUP message during the
 * handshake: we always advertise 3.1.1 and do not know whether the server will
 * select it until the NEGOTIATE reply, so the running hash must already include
 * the NEGOTIATE request.  The hash is only CONSUMED (for key derivation) when
 * the negotiated dialect is 3.1.1; for any other dialect it is simply unused.
 * Mirrors the server's conn->preauth_hash bookkeeping. */
static void
chimera_smb_client_preauth_fold(
    struct chimera_smb_client_conn *conn,
    const struct smb2_header       *hdr,
    int                             smb2_len)
{
    chimera_smb_client_preauth_extend(conn->preauth_hash, hdr, (uint32_t) smb2_len);
} /* chimera_smb_client_preauth_fold */

/* Extract the contiguous SMB2 message (header + body, no NetBIOS prefix) and
 * its length from a freshly-built request iovec, for preauth folding. */
static struct smb2_header *
chimera_smb_client_msg_from_iov(
    struct evpl_iovec        *iov,
    struct evpl_iovec_cursor *cursor,
    int                      *out_len)
{
    struct smb_client_netbios_header *netbios = evpl_iovec_data(iov);

    *out_len = evpl_iovec_cursor_consumed(cursor);
    return (struct smb2_header *) (netbios + 1);
} /* chimera_smb_client_msg_from_iov */

/* Fold a received reply (header copy + body cursor) into the preauth hash by
 * reconstructing its contiguous SMB2 message.  `body` is positioned at the body
 * start; it is NOT advanced (a private snapshot is used). */
static void
chimera_smb_client_preauth_fold_reply(
    struct chimera_smb_client_conn *conn,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len)
{
    uint8_t *msg = malloc(sizeof(*hdr) + body_len);

    if (!msg) {
        return;
    }
    memcpy(msg, hdr, sizeof(*hdr));
    if (body_len > 0) {
        struct evpl_iovec_cursor c = *body;
        evpl_iovec_cursor_copy(&c, msg + sizeof(*hdr), body_len);
    }
    chimera_smb_client_preauth_fold(conn, (struct smb2_header *) msg,
                                    (int) sizeof(*hdr) + body_len);
    free(msg);
} /* chimera_smb_client_preauth_fold_reply */

/* ---- helpers ----------------------------------------------------------- */

static const char *
chimera_smb_client_get_option(
    const struct chimera_vfs_mount_options *options,
    const char                             *key)
{
    int i;

    for (i = 0; i < options->num_options; i++) {
        if (strcmp(options->options[i].key, key) == 0) {
            return options->options[i].value;
        }
    }
    return NULL;
} /* chimera_smb_client_get_option */

static struct chimera_smb_client_server *
chimera_smb_client_server_alloc(struct chimera_smb_client_shared *shared)
{
    struct chimera_smb_client_server *server = NULL;
    int                               i;

    pthread_mutex_lock(&shared->lock);

    for (i = 0; i < shared->max_servers; i++) {
        if (!shared->servers[i]) {
            server             = calloc(1, sizeof(*server));
            server->index      = i;
            server->in_use     = 1;
            shared->servers[i] = server;
            break;
        }
    }

    pthread_mutex_unlock(&shared->lock);

    return server;
} /* chimera_smb_client_server_alloc */

/* ---- TREE_CONNECT (final mount leg) ------------------------------------ */

static void
chimera_smb_client_tree_connect_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request       *request = conn->mount_request;
    struct chimera_smb_client_server *server  = conn->server;
    uint8_t                           fragment[1];
    XXH128_hash_t                     fsid_hash;
    char                              fsid_input[600];
    int                               fsid_len;
    uint8_t                           fsid[CHIMERA_VFS_FSID_SIZE];

    (void) body;
    (void) body_len;
    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("TREE_CONNECT failed: status 0x%08x", status);
        chimera_smb_client_conn_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    server->tree_id       = hdr->sync.tree_id;
    server->session_ready = 1;

    /* Mount root FH: fsid = hash(host/share), fragment = [server_index][""]. */
    fragment[0] = (uint8_t) server->index;

    fsid_len = snprintf(fsid_input, sizeof(fsid_input), "%s/%s",
                        server->hostname, server->share);
    fsid_hash = XXH3_128bits(fsid_input, fsid_len);
    memcpy(fsid, &fsid_hash, CHIMERA_VFS_FSID_SIZE);

    request->mount.r_attr.va_set_mask = CHIMERA_VFS_ATTR_FH;
    request->mount.r_attr.va_fh_len   = chimera_vfs_encode_fh_mount(
        fsid, fragment, 1, request->mount.r_attr.va_fh);

    request->mount.r_mount_private = server;

    chimera_smbclient_info("SMB mount established: //%s/%s (dialect 0x%04x, session 0x%lx, tree %u)",
                           server->hostname, server->share, server->dialect,
                           server->session_id, server->tree_id);

    conn->mount_request = NULL;
    request->status     = CHIMERA_VFS_OK;
    request->complete(request);

    chimera_smb_client_conn_ready(conn);
} /* chimera_smb_client_tree_connect_reply */

static void
chimera_smb_client_tree_connect_send(struct chimera_smb_client_conn *conn)
{
    struct chimera_smb_client_server *server = conn->server;
    struct evpl_iovec                 iov;
    struct evpl_iovec_cursor          cursor;
    struct smb2_header               *hdr;
    uint8_t                           unc16[1200];
    char                              unc[600];
    size_t                            unc_len, i, n16;

    snprintf(unc, sizeof(unc), "\\\\%s\\%s", server->hostname, server->share);
    unc_len = strlen(unc);

    n16 = 0;
    for (i = 0; i < unc_len; i++) {
        unc16[n16++] = (uint8_t) unc[i];
        unc16[n16++] = 0;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_TREE_CONNECT, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_TREE_CONNECT_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 8);
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) n16);
    evpl_iovec_cursor_append_blob(&cursor, unc16, n16);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, conn->mount_request,
                                  chimera_smb_client_tree_connect_reply, NULL);
} /* chimera_smb_client_tree_connect_send */

/* ---- SESSION_SETUP ----------------------------------------------------- */

static void
chimera_smb_client_session_setup_send(
    struct chimera_smb_client_conn *conn,
    const uint8_t                  *token,
    size_t                          token_len,
    chimera_smb_client_reply_cb     reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    int                      msg_len;

    chimera_smb_client_pdu_begin(conn, SMB2_SESSION_SETUP, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SESSION_SETUP_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, 0);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_SIGNING_ENABLED);
    evpl_iovec_cursor_append_uint32(&cursor, 0);
    evpl_iovec_cursor_append_uint32(&cursor, 0);
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 24);
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) token_len);
    evpl_iovec_cursor_append_uint64(&cursor, 0);
    evpl_iovec_cursor_append_blob(&cursor, (void *) token, token_len);

    /* Fold the raw SESSION_SETUP request into the preauth hash before send.
     * The 3.1.1 signing key is derived over the hash through the final
     * (AUTHENTICATE) request, so both legs must be folded here. */
    (void) chimera_smb_client_msg_from_iov(&iov, &cursor, &msg_len);
    chimera_smb_client_preauth_fold(conn, hdr, msg_len);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, conn->mount_request,
                                  reply_cb, NULL);
} /* chimera_smb_client_session_setup_send */

static void
chimera_smb_client_session_setup_done(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_smb_client_server *server = conn->server;

    (void) hdr;
    (void) body;
    (void) body_len;
    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("SESSION_SETUP (authenticate) failed: status 0x%08x", status);
        chimera_smb_client_conn_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    /* The session is now authenticated.  Derive the per-session signing key and,
     * for a 3.x session with signing on, activate signing on the shared server
     * struct so every (mount + per-thread) connection signs/verifies with it.
     *
     * The key is derived over the same inputs the server used while processing
     * the final AUTHENTICATE request: the NTLM session key, and (for 3.1.1) the
     * preauth-integrity hash accumulated through that request.  We deliberately
     * do NOT fold the SUCCESS reply first — the server derives before signing
     * that reply, so deriving here keeps both sides identical.  2.1 is left
     * unsigned (signing_active stays clear), preserving the working 2.1 path. */
    if (server->dialect >= SMB2_DIALECT_3_0 && conn->ntlm.have_session_key) {
        if (chimera_smb_client_derive_signing_key(
                server->dialect,
                conn->ntlm.session_key, sizeof(conn->ntlm.session_key),
                conn->negotiated_dialect == SMB2_DIALECT_3_1_1 ? conn->preauth_hash : NULL,
                server->signing_key) != 0) {
            chimera_smbclient_error("Failed to derive SMB3 signing key (dialect 0x%04x)",
                                    server->dialect);
            chimera_smb_client_conn_fail(conn, CHIMERA_VFS_EIO);
            return;
        }
        server->signing_alg    = conn->negotiated_signing_alg;
        server->signing_active = 1;
    }

    chimera_smb_client_tree_connect_send(conn);
} /* chimera_smb_client_session_setup_done */

static void
chimera_smb_client_session_setup_challenge(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    uint16_t                 structsize, session_flags, blob_offset, blob_length;
    uint8_t                  challenge[2048];
    uint8_t                  authenticate[2048];
    size_t                   auth_len;
    int                      consumed;
    struct evpl_iovec_cursor fold_cursor;

    (void) arg;

    if (status != SMB2_STATUS_MORE_PROCESSING_REQUIRED &&
        status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("SESSION_SETUP (negotiate) failed: status 0x%08x", status);
        chimera_smb_client_conn_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    conn->server->session_id = hdr->session_id;

    /* Fold the raw challenge reply into the preauth hash before continuing.
    * Snapshot the body cursor first since the parsing below consumes it. */
    fold_cursor = *body;
    chimera_smb_client_preauth_fold_reply(conn, hdr, &fold_cursor, body_len);

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &session_flags);
    evpl_iovec_cursor_get_uint16(body, &blob_offset);
    evpl_iovec_cursor_get_uint16(body, &blob_length);

    (void) structsize;
    (void) session_flags;

    if (blob_length == 0 || blob_length > sizeof(challenge)) {
        chimera_smbclient_error("SESSION_SETUP response has invalid security buffer (%u bytes)", blob_length);
        chimera_smb_client_conn_fail(conn, CHIMERA_VFS_EINVAL);
        return;
    }

    consumed = evpl_iovec_cursor_consumed(body);
    if (blob_offset < consumed ||
        blob_offset - consumed + blob_length > body_len + (int) sizeof(struct smb2_header)) {
        chimera_smbclient_error("SESSION_SETUP security buffer out of range");
        chimera_smb_client_conn_fail(conn, CHIMERA_VFS_EINVAL);
        return;
    }
    evpl_iovec_cursor_skip(body, blob_offset - consumed);
    evpl_iovec_cursor_copy(body, challenge, blob_length);

    if (smb_ntlm_client_parse_challenge(&conn->ntlm, challenge, blob_length) < 0) {
        chimera_smbclient_error("Failed to parse NTLM CHALLENGE");
        chimera_smb_client_conn_fail(conn, CHIMERA_VFS_EACCES);
        return;
    }

    if (smb_ntlm_client_build_authenticate(&conn->ntlm, authenticate,
                                           sizeof(authenticate), &auth_len) < 0) {
        chimera_smbclient_error("Failed to build NTLM AUTHENTICATE");
        chimera_smb_client_conn_fail(conn, CHIMERA_VFS_EACCES);
        return;
    }

    chimera_smb_client_session_setup_send(conn, authenticate, auth_len,
                                          chimera_smb_client_session_setup_done);
} /* chimera_smb_client_session_setup_challenge */

/* ---- NEGOTIATE --------------------------------------------------------- */

/* Parse the 3.1.1 negotiate-context list out of a NEGOTIATE reply to learn the
 * server's selected signing algorithm (and confirm it echoed a preauth-integrity
 * context).  `ctx_off` is the absolute NegotiateContextOffset from the SMB2
 * header; `body` is positioned at the body start (consumed == sizeof header).
 * Returns 0 on success.  Defaults signing_alg to AES-CMAC (the 3.1.1 default)
 * when the server does not echo a SIGNING_CAPABILITIES context. */
static int
chimera_smb_client_parse_neg_contexts(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec_cursor       *body,
    uint32_t                        ctx_off,
    uint16_t                        ctx_count,
    int                             body_len)
{
    uint16_t i;
    int      consumed;
    int      have_preauth = 0;

    /* The whole SMB2 message (header + body) is what body_len covers from the
     * body start; ctx_off is from the header, so the byte we must reach in the
     * body is ctx_off - sizeof(header). */
    consumed = evpl_iovec_cursor_consumed(body); /* == sizeof(smb2_header) */
    if ((int) ctx_off < consumed) {
        return -1;
    }
    evpl_iovec_cursor_skip(body, (int) ctx_off - consumed);

    for (i = 0; i < ctx_count; i++) {
        uint16_t type, len;
        uint8_t  data[256];

        /* Each context is 8-byte aligned from the header; align the cursor (it
         * is consumed-relative to the header). */
        evpl_iovec_cursor_skip(body, (8 - (body->consumed & 7)) & 7);
        evpl_iovec_cursor_get_uint16(body, &type);
        evpl_iovec_cursor_get_uint16(body, &len);
        evpl_iovec_cursor_skip(body, 4); /* Reserved */

        if (len > sizeof(data)) {
            return -1;
        }
        if (len) {
            evpl_iovec_cursor_copy(body, data, len);
        }

        switch (type) {
            case SMB2_PREAUTH_INTEGRITY_CAPABILITIES:
                /* HashAlgorithmCount(2), SaltLength(2), HashAlgorithms[...]. */
                if (len >= 6 && smb_wire_le16(data + 4) == SMB2_PREAUTH_HASH_SHA_512) {
                    have_preauth = 1;
                }
                break;
            case SMB2_SIGNING_CAPABILITIES:
                /* SigningAlgorithmCount(2), SigningAlgorithms[0]. */
                if (len >= 4) {
                    conn->negotiated_signing_alg = smb_wire_le16(data + 2);
                }
                break;
            default:
                break;
        } /* switch */
    }

    (void) body_len;

    if (!have_preauth) {
        chimera_smbclient_error("SMB 3.1.1 NEGOTIATE reply without SHA-512 preauth context");
        return -1;
    }
    return 0;
} /* chimera_smb_client_parse_neg_contexts */

static void
chimera_smb_client_negotiate_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    uint16_t                 structsize, security_mode, dialect, ctx_count;
    uint32_t                 ctx_off;
    uint8_t                  negotiate[64];
    int                      neg_len;
    struct evpl_iovec_cursor ctx_cursor;

    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_error("NEGOTIATE failed: status 0x%08x", status);
        chimera_smb_client_conn_fail(conn, chimera_smb_status_to_errno(status));
        return;
    }

    /* Snapshot the body cursor for context parsing before we consume the fixed
     * NEGOTIATE-reply fields. */
    ctx_cursor = *body;

    /* NEGOTIATE reply fixed fields: StructSize(2), SecurityMode(2), Dialect(2),
     * NegotiateContextCount(2), ServerGuid(16), Capabilities(4),
     * MaxTransact(4), MaxRead(4), MaxWrite(4), SystemTime(8), StartTime(8),
     * SecurityBufferOffset(2), SecurityBufferLength(2),
     * NegotiateContextOffset(4). */
    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &security_mode);
    evpl_iovec_cursor_get_uint16(body, &dialect);
    evpl_iovec_cursor_get_uint16(body, &ctx_count);
    evpl_iovec_cursor_skip(body, 16);            /* ServerGuid */
    evpl_iovec_cursor_skip(body, 4);             /* Capabilities */
    evpl_iovec_cursor_get_uint32(body, &conn->server->max_transact);
    evpl_iovec_cursor_get_uint32(body, &conn->server->max_read);
    evpl_iovec_cursor_get_uint32(body, &conn->server->max_write);
    evpl_iovec_cursor_skip(body, 8 + 8);         /* SystemTime + ServerStartTime */
    evpl_iovec_cursor_skip(body, 2 + 2);         /* SecurityBufferOffset + Length */
    evpl_iovec_cursor_get_uint32(body, &ctx_off);

    (void) structsize;

    conn->server->security_mode = security_mode;
    conn->server->dialect       = dialect;
    conn->negotiated_dialect    = dialect;

    /* Fold the raw NEGOTIATE reply into the preauth hash (header + body, no
     * trailing pad), reconstructing the contiguous message from the body
     * snapshot taken before the fixed fields were consumed. */
    chimera_smb_client_preauth_fold_reply(conn, hdr, &ctx_cursor, body_len);

    if (dialect == SMB2_DIALECT_3_1_1) {
        if (chimera_smb_client_parse_neg_contexts(conn, &ctx_cursor, ctx_off,
                                                  ctx_count, body_len) != 0) {
            chimera_smb_client_conn_fail(conn, CHIMERA_VFS_ENOTSUP);
            return;
        }
    } else if (dialect != SMB2_DIALECT_2_1 && dialect != SMB2_DIALECT_3_0 &&
               dialect != SMB2_DIALECT_3_0_2) {
        chimera_smbclient_error("Server selected unsupported dialect 0x%04x", dialect);
        chimera_smb_client_conn_fail(conn, CHIMERA_VFS_ENOTSUP);
        return;
    }

    /* A secondary connection (the session already exists) is READY once it has
    * negotiated; it reuses the shared session_id/tree_id AND signing state
    * (signing key is per-session, already derived on the mount connection). */
    if (!conn->mount_request) {
        chimera_smb_client_conn_ready(conn);
        return;
    }

    /* The mount connection continues into authentication. */
    neg_len = smb_ntlm_client_build_negotiate(negotiate, sizeof(negotiate));
    if (neg_len < 0) {
        chimera_smb_client_conn_fail(conn, CHIMERA_VFS_EFAULT);
        return;
    }

    chimera_smb_client_session_setup_send(conn, negotiate, neg_len,
                                          chimera_smb_client_session_setup_challenge);
} /* chimera_smb_client_negotiate_reply */

/* Append a single SMB2 NEGOTIATE-request context (ContextType, DataLength,
 * Reserved(4), Data), 8-byte aligned from the start of the SMB2 header.  The
 * cursor is consumed-relative to the SMB2 header (pdu_begin reset it there), so
 * evpl_iovec_cursor_zero to the next 8-byte boundary aligns correctly. */
static void
chimera_smb_client_append_neg_context(
    struct evpl_iovec_cursor *cursor,
    uint16_t                  type,
    const uint8_t            *data,
    uint16_t                  data_len)
{
    evpl_iovec_cursor_zero(cursor, (8 - (cursor->consumed & 7)) & 7);
    evpl_iovec_cursor_append_uint16(cursor, type);
    evpl_iovec_cursor_append_uint16(cursor, data_len);
    evpl_iovec_cursor_append_uint32(cursor, 0); /* Reserved */
    if (data_len) {
        evpl_iovec_cursor_append_blob(cursor, (void *) data, data_len);
    }
} /* chimera_smb_client_append_neg_context */

void
chimera_smb_client_conn_on_connected(struct chimera_smb_client_conn *conn)
{
    static const uint16_t    dialects[CHIMERA_SMB_CLIENT_NUM_DIALECTS] =
        CHIMERA_SMB_CLIENT_DIALECTS;
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  client_guid[SMB2_GUID_SIZE];
    uint8_t                  preauth_ctx[4 + 2 + CHIMERA_SMB_CLIENT_PREAUTH_SALT_LEN];
    uint8_t                  signing_ctx[2 + 2 * 2];
    int                      ctx_offset_pos, ctx_count_pos;
    int                      ctx_offset, msg_len;
    int                      i;

    memset(client_guid, 0, sizeof(client_guid));

    chimera_smb_client_pdu_begin(conn, SMB2_NEGOTIATE, &iov, &cursor, &hdr);

    /* Fresh handshake: reset the preauth-integrity hash for this connection. */
    memset(conn->preauth_hash, 0, sizeof(conn->preauth_hash));
    conn->negotiated_dialect     = 0;
    conn->negotiated_signing_alg = SMB2_SIGNING_AES_CMAC; /* 3.1.1 default */

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_NEGOTIATE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, CHIMERA_SMB_CLIENT_NUM_DIALECTS);
    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SIGNING_ENABLED);
    evpl_iovec_cursor_append_uint16(&cursor, 0); /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0); /* Capabilities */
    evpl_iovec_cursor_append_blob(&cursor, client_guid, SMB2_GUID_SIZE);
    /* NegotiateContextOffset(4) + NegotiateContextCount(2) + Reserved2(2).
     * Backpatched below once the dialects + contexts are laid out. */
    ctx_offset_pos = cursor.consumed;
    evpl_iovec_cursor_append_uint32(&cursor, 0);
    ctx_count_pos = cursor.consumed;
    evpl_iovec_cursor_append_uint16(&cursor, 0);
    evpl_iovec_cursor_append_uint16(&cursor, 0); /* Reserved2 */

    for (i = 0; i < CHIMERA_SMB_CLIENT_NUM_DIALECTS; i++) {
        evpl_iovec_cursor_append_uint16(&cursor, dialects[i]);
    }

    /* 3.1.1 negotiate contexts.  Offset is 8-byte aligned from the SMB2 header;
     * the cursor is consumed-relative to that header. */
    evpl_iovec_cursor_zero(&cursor, (8 - (cursor.consumed & 7)) & 7);
    ctx_offset = cursor.consumed;

    /* SMB2_PREAUTH_INTEGRITY_CAPABILITIES: HashAlgorithmCount(2)=1,
    * SaltLength(2), HashAlgorithms[1]=SHA-512, Salt(SaltLength). */
    smb_wire_set_le16(preauth_ctx + 0, 1);
    smb_wire_set_le16(preauth_ctx + 2, CHIMERA_SMB_CLIENT_PREAUTH_SALT_LEN);
    smb_wire_set_le16(preauth_ctx + 4, SMB2_PREAUTH_HASH_SHA_512);
    memset(preauth_ctx + 6, 0, CHIMERA_SMB_CLIENT_PREAUTH_SALT_LEN); /* salt value is arbitrary */
    chimera_smb_client_append_neg_context(&cursor, SMB2_PREAUTH_INTEGRITY_CAPABILITIES,
                                          preauth_ctx, sizeof(preauth_ctx));

    /* SMB2_SIGNING_CAPABILITIES: SigningAlgorithmCount(2)=2,
     * SigningAlgorithms = { AES-GMAC, AES-CMAC } (client preference order). */
    smb_wire_set_le16(signing_ctx + 0, 2);
    smb_wire_set_le16(signing_ctx + 2, SMB2_SIGNING_AES_GMAC);
    smb_wire_set_le16(signing_ctx + 4, SMB2_SIGNING_AES_CMAC);
    chimera_smb_client_append_neg_context(&cursor, SMB2_SIGNING_CAPABILITIES,
                                          signing_ctx, sizeof(signing_ctx));

    /* Backpatch NegotiateContextOffset (absolute from SMB2 header) and Count. */
    {
        struct smb_client_netbios_header *netbios = evpl_iovec_data(&iov);
        uint8_t                          *smb2    = (uint8_t *) (netbios + 1);
        smb_wire_set_le32(smb2 + ctx_offset_pos, (uint32_t) ctx_offset);
        smb_wire_set_le16(smb2 + ctx_count_pos, 2);
    }

    /* Fold the raw NEGOTIATE request into the preauth hash before send. */
    (void) chimera_smb_client_msg_from_iov(&iov, &cursor, &msg_len);
    chimera_smb_client_preauth_fold(conn, hdr, msg_len);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, conn->mount_request,
                                  chimera_smb_client_negotiate_reply, NULL);
} /* chimera_smb_client_conn_on_connected */

/* ---- MOUNT entry point ------------------------------------------------- */

void
chimera_smb_client_mount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request)
{
    struct chimera_smb_client_shared *shared = thread->shared;
    struct chimera_smb_client_server *server;
    struct chimera_smb_client_conn   *conn;
    const char                       *user, *password, *domain, *port_opt;
    char                              host[256];
    char                              share[256];
    const char                       *colon;
    int                               host_len;
    uint16_t                          port;

    shared->tcp_protocol = chimera_tcp_flavor_to_protocol(request->thread->vfs->tcp_flavor);

    colon = memchr(request->mount.path, ':', request->mount.pathlen);
    if (!colon) {
        chimera_smbclient_error("SMB mount path '%s' is not host:share", request->mount.path);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    host_len = (int) (colon - request->mount.path);
    if (host_len <= 0 || host_len >= (int) sizeof(host)) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }
    memcpy(host, request->mount.path, host_len);
    host[host_len] = '\0';
    snprintf(share, sizeof(share), "%s", colon + 1);

    user     = chimera_smb_client_get_option(&request->mount.options, "user");
    password = chimera_smb_client_get_option(&request->mount.options, "password");
    domain   = chimera_smb_client_get_option(&request->mount.options, "domain");
    port_opt = chimera_smb_client_get_option(&request->mount.options, "port");

    if (!user || !password) {
        chimera_smbclient_error("SMB mount requires user= and password= options");
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    port = port_opt ? (uint16_t) atoi(port_opt) : CHIMERA_SMB_CLIENT_PORT;

    server = chimera_smb_client_server_alloc(shared);
    if (!server) {
        chimera_smbclient_error("SMB client server table full");
        request->status = CHIMERA_VFS_ENOSPC;
        request->complete(request);
        return;
    }

    snprintf(server->hostname, sizeof(server->hostname), "%s", host);
    snprintf(server->share, sizeof(server->share), "%s", share);
    snprintf(server->user, sizeof(server->user), "%s", user);
    snprintf(server->domain, sizeof(server->domain), "%s",
             domain ? domain : CHIMERA_SMB_CLIENT_DEFAULT_DOMAIN);
    snprintf(server->password, sizeof(server->password), "%s", password);
    server->port     = port;
    server->endpoint = evpl_endpoint_create(server->hostname, server->port);

    conn = chimera_smb_client_get_conn(thread, server);

    conn->mount_request = request;
    conn->state         = CHIMERA_SMB_CONN_CONNECTING;

    smb_ntlm_client_init(&conn->ntlm, server->user, server->domain, server->password);

    conn->bind = chimera_smb_client_connect(conn, server->endpoint);
} /* chimera_smb_client_mount */
