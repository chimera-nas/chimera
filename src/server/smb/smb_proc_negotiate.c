// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <openssl/rand.h>

#include "smb_internal.h"
#include "smb_procs.h"

/* Forward declarations for helpers defined later in this file. */
static void chimera_smb_select_negotiated_algorithms(
    struct chimera_smb_conn    *conn,
    struct chimera_smb_request *request);

/*
 * Pre-built SPNEGO negTokenInit advertising available authentication mechanisms.
 * This is the security blob sent in the SMB2 NEGOTIATE response.
 *
 * Structure (ASN.1/DER, RFC 4178 EXPLICIT TAGS):
 *   APPLICATION [0] {
 *     OID 1.3.6.1.5.5.2 (SPNEGO)
 *     [0] {                          -- NegTokenInit (EXPLICIT context tag)
 *       SEQUENCE {                   -- NegTokenInit fields
 *         [0] {                      -- mechTypes (EXPLICIT context tag)
 *           SEQUENCE OF {
 *             OID 1.2.840.48018.1.2.2   (MS KRB5)
 *             OID 1.2.840.113554.1.2.2  (KRB5)
 *             OID 1.3.6.1.4.1.311.2.2.10 (NTLMSSP)
 *           }
 *         }
 *       }
 *     }
 *   }
 */
static const uint8_t spnego_negotiate_token[] = {
    0x60, 0x32, /* APPLICATION [0], len 50 */
    0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02,     /* OID: SPNEGO */
    0xa0, 0x28, /* [0] NegTokenInit, len 40 */
    0x30, 0x26, /* SEQUENCE, len 38 */
    0xa0, 0x24, /* [0] mechTypes, len 36 */
    0x30, 0x22, /* SEQUENCE OF, len 34 */
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12,     /* OID: MS KRB5 */
    0x01, 0x02, 0x02,
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12,     /* OID: KRB5 */
    0x01, 0x02, 0x02,
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82,     /* OID: NTLMSSP */
    0x37, 0x02, 0x02, 0x0a,
};

void
chimera_smb_negotiate(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_conn          *conn   = request->compound->conn;
    struct timespec                   now, up, boot;
    uint16_t                          dialect = 0, candidate;
    int                               i, j;

    /* MS-SMB2 3.3.5.4: a NEGOTIATE received on a connection that already
     * completed one (Connection.NegotiateDialect set to a concrete SMB2
     * dialect) MUST terminate the transport connection with no reply.  WPTS
     * DurableHandleV1_Reconnect_AfterServerDisconnect drives exactly this to
     * force a server-side disconnect before reconnecting.  Mirror the
     * VALIDATE_NEGOTIATE_INFO teardown: drop the compound and close the bind.
     *
     * Exception: 0x02ff is the wildcard "revision" recorded when an SMB1
     * multi-protocol NEGOTIATE offered "SMB 2.???" (MS-SMB2 3.3.5.3.1).  It
     * explicitly means "a real SMB2 NEGOTIATE follows on this same
     * connection", so that mandatory second NEGOTIATE must be processed, not
     * treated as a duplicate (otherwise BVT_Negotiate_Compatible_Wildcard and
     * every real SMB1->SMB2 wildcard client get their connection dropped). */
    if (conn->dialect != 0 && conn->dialect != 0x02ff) {
        struct chimera_smb_compound *compound = request->compound;

        chimera_smb_compound_free(thread, compound);
        evpl_close(thread->evpl, conn->bind);
        return;
    }

    clock_gettime(
        CLOCK_REALTIME,
        &now);

    clock_gettime(
        CLOCK_BOOTTIME,
        &up);

    boot.tv_sec = now.tv_sec  - up.tv_sec;

    if (now.tv_nsec >= up.tv_nsec) {
        boot.tv_nsec = now.tv_nsec - up.tv_nsec;
    } else {
        boot.tv_nsec = up.tv_nsec - now.tv_nsec;
        boot.tv_sec--;
    }

    for (i = 0; i < request->negotiate.dialect_count; i++) {

        candidate = request->negotiate.dialects[i];

        if (candidate == 0x2ff) {
            dialect = 0x2ff;
            break;
        }

        for (j = 0; j < shared->config.num_dialects; j++) {
            if (shared->config.dialects[j] == candidate && candidate > dialect) {
                dialect = candidate;
            }
        }
    }

    if (dialect == 0) {
        /* MS-SMB2 3.3.5.4: DialectCount == 0 (no dialects offered) MUST fail with
         * STATUS_INVALID_PARAMETER; dialects offered but none in common is
         * STATUS_NOT_SUPPORTED. */
        if (request->negotiate.dialect_count == 0) {
            chimera_smb_error("SMB2 NEGOTIATE with DialectCount 0");
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        } else {
            chimera_smb_error("No common SMB2 dialect found");
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_SUPPORTED);
        }
        return;
    }

    conn->capabilities = 0;

    if (dialect >= 0x210 && (shared->config.capabilities & SMB2_GLOBAL_CAP_LARGE_MTU)) {
        conn->capabilities |= SMB2_GLOBAL_CAP_LARGE_MTU;
    }
    if (dialect >= 0x210 && (shared->config.capabilities & SMB2_GLOBAL_CAP_LEASING)) {
        conn->capabilities |= SMB2_GLOBAL_CAP_LEASING;
    }
    /* Only advertise MULTI_CHANNEL when at least one network interface is
     * actually configured (MS-SMB2 §3.3.5.4): the capability is meaningless
     * without interfaces to return from FSCTL_QUERY_NETWORK_INTERFACE_INFO, and
     * advertising it with an empty interface list invites the client to attempt
     * a second channel it can never discover an endpoint for. */
    if (dialect >= 0x300 && (shared->config.capabilities & SMB2_GLOBAL_CAP_MULTI_CHANNEL) &&
        shared->config.num_nic_info > 0) {
        conn->capabilities |= SMB2_GLOBAL_CAP_MULTI_CHANNEL;
    }
    if (dialect >= 0x300 && shared->config.persistent_handles) {
        conn->capabilities |= SMB2_GLOBAL_CAP_PERSISTENT_HANDLES;
    }
    /* Directory leasing is an SMB 3.0+ capability (MS-SMB2 §3.3.5.4); the
     * server only grants R/H leases on directory opens when this is advertised. */
    if (dialect >= 0x300 && shared->config.directory_leases) {
        conn->capabilities |= SMB2_GLOBAL_CAP_DIRECTORY_LEASING;
    }
    /* Encryption is advertised via SMB2_GLOBAL_CAP_ENCRYPTION for 3.0/3.0.2;
     * for 3.1.1 it is negotiated through the encryption-capabilities context
     * instead and the capability bit MUST NOT be set (MS-SMB2 §3.3.5.4).  The
     * server echoes the capability only when the CLIENT also advertised it: per
     * §3.3.5.4 SMB2_GLOBAL_CAP_ENCRYPTION is set in the response only if it was
     * set in the request (SMB2Model Encryption ClientNotSupportsEncryption). */
    if (dialect >= 0x300 && dialect < 0x311 && shared->config.encryption &&
        (request->negotiate.capabilities & SMB2_GLOBAL_CAP_ENCRYPTION)) {
        conn->capabilities |= SMB2_GLOBAL_CAP_ENCRYPTION;
    }

    if (request->negotiate.security_mode & SMB2_SIGNING_REQUIRED) {
        conn->flags |= CHIMERA_SMB_CONN_FLAG_SIGNING_REQUIRED;
    }

    request->negotiate.r_dialect       = dialect;
    request->negotiate.r_security_mode = SMB2_SIGNING_ENABLED;
    if (shared->config.signing_required) {
        /* Server signing = mandatory: advertise REQUIRED so clients sign
         * every request (smb2.session-require-signing / bug15397). */
        request->negotiate.r_security_mode |= SMB2_SIGNING_REQUIRED;
        conn->flags                        |= CHIMERA_SMB_CONN_FLAG_SIGNING_REQUIRED;
    }
    request->negotiate.r_capabilities      = conn->capabilities;
    request->negotiate.r_max_transact_size = CHIMERA_SMB_MAX_TRANSACT_SIZE;
    request->negotiate.r_max_read_size     = 8 * 1024 * 1024;
    request->negotiate.r_max_write_size    = 8 * 1024 * 1024;
    request->negotiate.r_system_time       = chimera_nt_time(&now);
    request->negotiate.r_server_start_time = chimera_nt_time(&boot);

    memcpy(request->negotiate.r_server_guid, shared->guid, SMB2_GUID_SIZE);

    conn->dialect = dialect;

    /* SMB 3.1.1 mandates a PreauthIntegrityCapabilities context offering a
     * hash algorithm we support (SHA-512). Per MS-SMB2 §3.3.5.4:
     *   - if no PreauthIntegrity context is present at all, fail with
     *     STATUS_INVALID_PARAMETER;
     *   - if the context is present but its HashAlgorithms array contains no
     *     algorithm we support, fail with
     *     STATUS_SMB_NO_PREAUTH_INTEGRITY_HASH_OVERLAP (0xC05D0000). */
    if (dialect == SMB2_DIALECT_3_1_1) {
        int have_preauth_ctx =
            (request->negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_PREAUTH) != 0;
        int have_sha512 = 0;

        if (have_preauth_ctx) {
            for (i = 0; i < request->negotiate.preauth_in.hash_alg_count; i++) {
                if (request->negotiate.preauth_in.hash_algs[i] == SMB2_PREAUTH_HASH_SHA_512) {
                    have_sha512 = 1;
                    break;
                }
            }
        }

        if (!have_sha512) {
            chimera_smb_complete_request(
                request,
                have_preauth_ctx ?
                SMB2_STATUS_SMB_NO_PREAUTH_INTEGRITY_OVERLAP :
                SMB2_STATUS_INVALID_PARAMETER);
            return;
        }
    }

    /* Record the client's negotiate parameters so a later
     * FSCTL_VALIDATE_NEGOTIATE_INFO can be checked against them. */
    memcpy(conn->client_guid, request->negotiate.client_guid, SMB2_GUID_SIZE);
    conn->client_security_mode = request->negotiate.security_mode;
    conn->client_capabilities  = request->negotiate.capabilities;

    /* Connection.SupportsNotifications (MS-SMB2 3.3.5.4): only meaningful for
     * 3.1.1, and only when the client advertised SMB2_GLOBAL_CAP_NOTIFICATIONS.
     * Recorded so a binding SESSION_SETUP can be validated against the bound
     * session's value (MS-SMB2 3.3.5.5). */
    conn->supports_notifications =
        (dialect == SMB2_DIALECT_3_1_1 &&
         (request->negotiate.capabilities & SMB2_GLOBAL_CAP_NOTIFICATIONS)) ? 1 : 0;

    /* Pick algorithms from the client's negotiate contexts. Phase 0 records
     * presence; Phases 2/4/5 will actually flip on preauth/encryption/RDMA. */
    chimera_smb_select_negotiated_algorithms(conn, request);

    chimera_smb_complete_request(
        request,
        SMB2_STATUS_SUCCESS);
} /* smb_procs_negotiate */

/* Wire emit for one negotiate-response context.
 *   ContextType(2) DataLength(2) Reserved(4) Data(...)
 * followed by 0..7 padding bytes so the next context starts on an 8-byte
 * boundary. Returns the total bytes consumed (header + data + pad), or 0
 * if the output buffer is too small. */
static uint32_t
emit_negotiate_response_context(
    uint8_t       *buf,
    uint32_t       buf_size,
    uint32_t       pos,
    uint16_t       type,
    const uint8_t *data,
    uint16_t       data_len)
{
    uint32_t total   = 8u + (uint32_t) data_len;
    uint32_t advance = (total + 7u) & ~7u;

    if (pos + advance > buf_size) {
        return 0;
    }

    buf[pos + 0] = type & 0xff;
    buf[pos + 1] = (type >> 8) & 0xff;
    buf[pos + 2] = data_len & 0xff;
    buf[pos + 3] = (data_len >> 8) & 0xff;
    buf[pos + 4] = 0; buf[pos + 5] = 0; buf[pos + 6] = 0; buf[pos + 7] = 0;  /* Reserved */
    if (data_len > 0) {
        memcpy(buf + pos + 8, data, data_len);
    }
    if (advance > total) {
        memset(buf + pos + total, 0, advance - total);
    }
    return advance;
} /* emit_negotiate_response_context */

struct chimera_smb_negotiate_response_emitter {
    uint32_t need_mask_bit;
    uint16_t type;
    int      (*build)(
        struct chimera_smb_conn    *conn,
        struct chimera_smb_request *request,
        uint8_t                    *out,
        uint32_t                    out_size);
};

/* Build the SMB2_PREAUTH_INTEGRITY_CAPABILITIES response context:
 *   HashAlgorithmCount(2)=1, SaltLength(2)=32, HashAlgorithms[1](2)=SHA-512,
 *   Salt(32). */
static int
build_preauth_integrity_response(
    struct chimera_smb_conn    *conn,
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    const uint16_t salt_len = sizeof(conn->negotiated.preauth_salt);

    (void) request;

    if (out_size < (uint32_t) (6 + salt_len)) {
        return -1;
    }

    out[0] = 1;    out[1] = 0;                            /* HashAlgorithmCount */
    out[2] = salt_len & 0xff; out[3] = (salt_len >> 8) & 0xff; /* SaltLength */
    out[4] = SMB2_PREAUTH_HASH_SHA_512 & 0xff;
    out[5] = (SMB2_PREAUTH_HASH_SHA_512 >> 8) & 0xff;     /* HashAlgorithms[0] */
    memcpy(out + 6, conn->negotiated.preauth_salt, salt_len);

    return 6 + salt_len;
} /* build_preauth_integrity_response */

/* Build the SMB2_SIGNING_CAPABILITIES response context:
 *   SigningAlgorithmCount(2)=1, SigningAlgorithms[1]=selected algorithm.
 */
static int
build_signing_capabilities_response(
    struct chimera_smb_conn    *conn,
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    (void) request;

    /* Only emitted when the client sent a SIGNING_CAPABILITIES context (the
     * emitter table gates on that), so echo the selected algorithm — including
     * HMAC-SHA256, whose id is 0. */
    if (out_size < 4) {
        return -1;
    }

    out[0] = 1; out[1] = 0; /* SigningAlgorithmCount */
    out[2] = conn->negotiated.signing_alg & 0xff;
    out[3] = (conn->negotiated.signing_alg >> 8) & 0xff;

    return 4;
} /* build_signing_capabilities_response */

/* Build the SMB2_ENCRYPTION_CAPABILITIES response context:
 *   CipherCount(2)=1, Ciphers[1](2)=selected cipher.
 * The selected cipher is echoed so the client's per-session nonce bookkeeping
 * is well-formed; Chimera does not activate encryption (see cipher selection
 * in chimera_smb_select_negotiated_algorithms). */
static int
build_encryption_capabilities_response(
    struct chimera_smb_conn    *conn,
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    (void) request;

    if (conn->negotiated.cipher_id == 0 || out_size < 4) {
        return -1;
    }

    out[0] = 1; out[1] = 0; /* CipherCount */
    out[2] = conn->negotiated.cipher_id & 0xff;
    out[3] = (conn->negotiated.cipher_id >> 8) & 0xff;

    return 4;
} /* build_encryption_capabilities_response */

/* Build the SMB2_COMPRESSION_CAPABILITIES response context (MS-SMB2 §2.2.3.1.3):
 *   CompressionAlgorithmCount(2), Padding(2)=0, Flags(4),
 *   CompressionAlgorithms[count](2 each).
 * The full mutually-negotiated set is echoed (selection recorded it on the
 * connection).  Returns -1 — and so emits no context — when nothing was
 * negotiated (e.g. compression disabled in config). */
static int
build_compression_capabilities_response(
    struct chimera_smb_conn    *conn,
    struct chimera_smb_request *request,
    uint8_t                    *out,
    uint32_t                    out_size)
{
    uint16_t count = conn->negotiated.compression_alg_count;
    uint32_t need  = 8u + (uint32_t) count * 2u;
    int      i;

    (void) request;

    if (count == 0 || out_size < need) {
        return -1;
    }

    out[0] = count & 0xff; out[1] = (count >> 8) & 0xff;  /* CompressionAlgorithmCount */
    out[2] = 0; out[3] = 0;                               /* Padding */
    out[4] = conn->negotiated.compression_flags & 0xff;   /* Flags */
    out[5] = (conn->negotiated.compression_flags >> 8) & 0xff;
    out[6] = (conn->negotiated.compression_flags >> 16) & 0xff;
    out[7] = (conn->negotiated.compression_flags >> 24) & 0xff;
    for (i = 0; i < count; i++) {
        out[8 + i * 2] = conn->negotiated.compression_algs[i] & 0xff;
        out[9 + i * 2] = (conn->negotiated.compression_algs[i] >> 8) & 0xff;
    }

    return (int) need;
} /* build_compression_capabilities_response */

/* The negotiate-response context emitters. Only consulted when the negotiated
 * dialect is 3.1.1; each entry emits only if the client sent the matching
 * request context (need_mask_bit).
 *
 * PreauthIntegrity is implemented: the server selects SHA-512, returns a salt,
 * maintains Connection.PreauthIntegrityHashValue across NEGOTIATE/SESSION_SETUP
 * (smb.c), and derives the 3.1.1 signing key bound to that hash. AES-CMAC
 * signing negotiation is implemented. The encryption context echoes a selected
 * cipher (required so the client's session nonce bookkeeping is well-formed)
 * but Chimera does not activate encryption; GMAC, compression and
 * RdmaTransform contexts are not yet emitted. */
static const struct chimera_smb_negotiate_response_emitter smb_negotiate_response_emitters[] = {
    { CHIMERA_SMB_NEGOTIATE_CTX_PREAUTH,     SMB2_PREAUTH_INTEGRITY_CAPABILITIES,
      build_preauth_integrity_response },
    { CHIMERA_SMB_NEGOTIATE_CTX_ENCRYPTION,  SMB2_ENCRYPTION_CAPABILITIES,
      build_encryption_capabilities_response },
    { CHIMERA_SMB_NEGOTIATE_CTX_COMPRESSION, SMB2_COMPRESSION_CAPABILITIES,
      build_compression_capabilities_response },
    { CHIMERA_SMB_NEGOTIATE_CTX_SIGNING,     SMB2_SIGNING_CAPABILITIES,
      build_signing_capabilities_response },
    { 0,                                     0,                                      NULL }
};

/* Build the negotiate context list into ctx_buf. Returns total bytes written
 * and writes the number of emitted contexts to *out_count. Contexts are only
 * valid when DialectRevision == 0x0311. */
static uint32_t
chimera_smb_build_negotiate_response_contexts(
    struct chimera_smb_request *request,
    struct chimera_smb_conn    *conn,
    uint8_t                    *ctx_buf,
    uint32_t                    ctx_buf_size,
    uint16_t                   *out_count)
{
    uint32_t                                             pos      = 0;
    uint32_t                                             last_end = 0;
    uint16_t                                             count    = 0;
    const struct chimera_smb_negotiate_response_emitter *e;

    if (request->negotiate.r_dialect != SMB2_DIALECT_3_1_1) {
        *out_count = 0;
        return 0;
    }

    for (e = smb_negotiate_response_emitters; e->build != NULL; e++) {
        uint8_t  data_buf[256];
        int      data_len;
        uint32_t advance;

        if (e->need_mask_bit &&
            (conn->negotiated.ctx_present_mask & e->need_mask_bit) == 0) {
            continue;
        }
        data_len = e->build(conn, request, data_buf, sizeof(data_buf));
        if (data_len < 0) {
            continue;
        }
        advance = emit_negotiate_response_context(ctx_buf, ctx_buf_size, pos,
                                                  e->type, data_buf, (uint16_t) data_len);
        if (advance == 0) {
            break;
        }
        /* The 8-byte alignment padding belongs *between* contexts, so the next
         * context starts aligned. The message itself MUST end at the last
         * context's data (no trailing pad) — Windows/Samba emit it that way,
         * and strict clients (e.g. WPTS) re-serialize to that canonical form
         * when computing the SMB 3.1.1 preauth-integrity hash. Track the end
         * of the current context's data and return that as the list length. */
        last_end = pos + 8u + (uint32_t) data_len;
        pos     += advance;
        count++;
    }

    *out_count = count;
    return last_end;
} /* chimera_smb_build_negotiate_response_contexts */

void
chimera_smb_negotiate_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    struct chimera_smb_conn *conn                   = request->compound->conn;
    uint16_t                 security_buffer_offset = sizeof(struct smb2_header) + 64;
    uint16_t                 security_buffer_length = sizeof(spnego_negotiate_token);
    uint8_t                  ctx_buf[1024];
    uint32_t                 ctx_len;
    uint16_t                 ctx_count;
    uint32_t                 ctx_offset     = 0;
    uint32_t                 after_security = (uint32_t) security_buffer_offset + security_buffer_length;
    uint32_t                 pad_before_ctx = 0;

    /* Reply path runs after chimera_smb_negotiate, which dereferences conn
     * unconditionally — if we got here without it, we have a different bug
     * to find, but make the failure loud. */
    chimera_smb_abort_if(conn == NULL, "chimera_smb_negotiate_reply: NULL conn");

    ctx_len = chimera_smb_build_negotiate_response_contexts(
        request, conn, ctx_buf, sizeof(ctx_buf), &ctx_count);

    if (ctx_count > 0) {
        /* First context must be 8-byte aligned from start of SMB2 header. */
        ctx_offset     = (after_security + 7u) & ~7u;
        pad_before_ctx = ctx_offset - after_security;
    }

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_NEGOTIATE_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, request->negotiate.r_security_mode);
    evpl_iovec_cursor_append_uint16(reply_cursor, request->negotiate.r_dialect);
    /* NegotiateContextCount / Reserved (only meaningful for 0x0311) */
    evpl_iovec_cursor_append_uint16(reply_cursor, ctx_count);
    evpl_iovec_cursor_append_blob(reply_cursor, request->negotiate.r_server_guid, SMB2_GUID_SIZE);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_capabilities);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_max_transact_size);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_max_read_size);
    evpl_iovec_cursor_append_uint32(reply_cursor, request->negotiate.r_max_write_size);
    evpl_iovec_cursor_append_uint64(reply_cursor, request->negotiate.r_system_time);
    evpl_iovec_cursor_append_uint64(reply_cursor, request->negotiate.r_server_start_time);
    evpl_iovec_cursor_append_uint16(reply_cursor, security_buffer_offset);
    evpl_iovec_cursor_append_uint16(reply_cursor, security_buffer_length);
    /* NegotiateContextOffset / Reserved2 — absolute from start of SMB2 header */
    evpl_iovec_cursor_append_uint32(reply_cursor, ctx_offset);

    /* SPNEGO security buffer */
    evpl_iovec_cursor_append_blob(reply_cursor, (void *) spnego_negotiate_token,
                                  security_buffer_length);

    if (pad_before_ctx > 0) {
        static const uint8_t pad_zero[8] = { 0 };
        evpl_iovec_cursor_append_blob(reply_cursor, (void *) pad_zero, pad_before_ctx);
    }

    if (ctx_len > 0) {
        evpl_iovec_cursor_append_blob(reply_cursor, ctx_buf, ctx_len);
    }

} /* chimera_smb_negotiate_reply */

/* Per-type negotiate-context parsers. Each returns 0 on success, -1 on a
 * malformed body. Phase 0 records the parsed fields onto request->negotiate.*_in
 * for later inspection (selection logic, unit tests, future phase consumers). */

static int
parse_neg_ctx_preauth(
    struct chimera_smb_request *request,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    uint16_t hash_count, salt_len, i;

    if (data_len < 4) {
        return -1;
    }
    hash_count = smb_wire_le16(data);
    salt_len   = smb_wire_le16(data + 2);

    if (hash_count == 0 ||
        hash_count > sizeof(request->negotiate.preauth_in.hash_algs) / sizeof(uint16_t)) {
        return -1;
    }
    if ((uint32_t) 4 + (uint32_t) hash_count * 2u + (uint32_t) salt_len > data_len) {
        return -1;
    }
    if (salt_len > sizeof(request->negotiate.preauth_in.salt)) {
        return -1;
    }

    request->negotiate.preauth_in.hash_alg_count = hash_count;
    request->negotiate.preauth_in.salt_length    = salt_len;
    for (i = 0; i < hash_count; i++) {
        request->negotiate.preauth_in.hash_algs[i] = smb_wire_le16(data + 4 + i * 2);
    }
    if (salt_len > 0) {
        memcpy(request->negotiate.preauth_in.salt,
               data + 4 + hash_count * 2,
               salt_len);
    }
    return 0;
} /* parse_neg_ctx_preauth */

static int
parse_neg_ctx_encryption(
    struct chimera_smb_request *request,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    uint16_t count, i;

    if (data_len < 2) {
        return -1;
    }
    count = smb_wire_le16(data);
    if (count == 0 ||
        count > sizeof(request->negotiate.encryption_in.ciphers) / sizeof(uint16_t)) {
        return -1;
    }
    if ((uint32_t) 2 + (uint32_t) count * 2u > data_len) {
        return -1;
    }

    request->negotiate.encryption_in.cipher_count = count;
    for (i = 0; i < count; i++) {
        request->negotiate.encryption_in.ciphers[i] = smb_wire_le16(data + 2 + i * 2);
    }
    return 0;
} /* parse_neg_ctx_encryption */

static int
parse_neg_ctx_compression(
    struct chimera_smb_request *request,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    uint16_t count, i;

    /* MS-SMB2 §3.3.5.4: fail NEGOTIATE if DataLength is below the fixed
     * SMB2_COMPRESSION_CAPABILITIES size (8 bytes). */
    if (data_len < 8) {
        return -1;
    }
    count = smb_wire_le16(data);
    /* data + 2: 2-byte padding (ignored) */
    request->negotiate.compression_in.flags = smb_wire_le32(data + 4);

    if (count == 0) {
        /* A CompressionAlgorithmCount of zero is handled per §3.3.5.4
         * conditionally on whether the server negotiates compression:
         *   - Compression enabled: the server processes the context and MUST
         *     fail with STATUS_INVALID_PARAMETER (WPTS
         *     Negotiate_SMB311_Compression_CompressionAlgorithmEmpty).
         *   - Compression disabled: the context is irrelevant, so an empty
         *     one is tolerated as "no compression offered" — Windows returns
         *     success and WPTS Negotiate_SMB311_WithAllContexts (baseline)
         *     asserts STATUS_SUCCESS. */
        if (request->compound->thread->shared->config.compression) {
            return -1;
        }
        request->negotiate.compression_in.alg_count = 0;
        return 0;
    }

    if (count > sizeof(request->negotiate.compression_in.algs) / sizeof(uint16_t)) {
        return -1;
    }
    if ((uint32_t) 8 + (uint32_t) count * 2u > data_len) {
        return -1;
    }

    request->negotiate.compression_in.alg_count = count;
    for (i = 0; i < count; i++) {
        request->negotiate.compression_in.algs[i] = smb_wire_le16(data + 8 + i * 2);
    }
    return 0;
} /* parse_neg_ctx_compression */

static int
parse_neg_ctx_signing(
    struct chimera_smb_request *request,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    uint16_t count, i;

    if (data_len < 2) {
        return -1;
    }
    count = smb_wire_le16(data);
    if (count == 0 ||
        count > sizeof(request->negotiate.signing_in.algs) / sizeof(uint16_t)) {
        return -1;
    }
    if ((uint32_t) 2 + (uint32_t) count * 2u > data_len) {
        return -1;
    }

    request->negotiate.signing_in.alg_count = count;
    for (i = 0; i < count; i++) {
        request->negotiate.signing_in.algs[i] = smb_wire_le16(data + 2 + i * 2);
    }
    return 0;
} /* parse_neg_ctx_signing */

static int
parse_neg_ctx_netname(
    struct chimera_smb_request *request,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    uint16_t copy_len = data_len;

    if (copy_len > sizeof(request->negotiate.netname_in.utf16le)) {
        copy_len = sizeof(request->negotiate.netname_in.utf16le);
    }
    request->negotiate.netname_in.length_bytes = copy_len;
    if (copy_len > 0) {
        memcpy(request->negotiate.netname_in.utf16le, data, copy_len);
    }
    return 0;
} /* parse_neg_ctx_netname */

static int
parse_neg_ctx_transport(
    struct chimera_smb_request *request,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    if (data_len < 4) {
        return -1;
    }
    request->negotiate.transport_in.flags = smb_wire_le32(data);
    return 0;
} /* parse_neg_ctx_transport */

static int
parse_neg_ctx_rdma_transform(
    struct chimera_smb_request *request,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    uint16_t count, i;

    if (data_len < 8) {
        return -1;
    }
    count = smb_wire_le16(data);
    /* data + 2: 2-byte Reserved1; data + 4: 4-byte Reserved2 */
    if (count > sizeof(request->negotiate.rdma_transform_in.transforms) / sizeof(uint16_t)) {
        return -1;
    }
    if ((uint32_t) 8 + (uint32_t) count * 2u > data_len) {
        return -1;
    }

    request->negotiate.rdma_transform_in.transform_count = count;
    for (i = 0; i < count; i++) {
        request->negotiate.rdma_transform_in.transforms[i] = smb_wire_le16(data + 8 + i * 2);
    }
    return 0;
} /* parse_neg_ctx_rdma_transform */

/* Map a negotiate-context type to its CHIMERA_SMB_NEGOTIATE_CTX_* bit.
 * Returns 0 for unknown types. */
static uint32_t
negotiate_ctx_mask_bit(uint16_t type)
{
    switch (type) {
        case SMB2_PREAUTH_INTEGRITY_CAPABILITIES:
            return CHIMERA_SMB_NEGOTIATE_CTX_PREAUTH;
        case SMB2_ENCRYPTION_CAPABILITIES:
            return CHIMERA_SMB_NEGOTIATE_CTX_ENCRYPTION;
        case SMB2_COMPRESSION_CAPABILITIES:
            return CHIMERA_SMB_NEGOTIATE_CTX_COMPRESSION;
        case SMB2_NETNAME_NEGOTIATE_CONTEXT_ID:
            return CHIMERA_SMB_NEGOTIATE_CTX_NETNAME;
        case SMB2_TRANSPORT_CAPABILITIES:
            return CHIMERA_SMB_NEGOTIATE_CTX_TRANSPORT;
        case SMB2_RDMA_TRANSFORM_CAPABILITIES:
            return CHIMERA_SMB_NEGOTIATE_CTX_RDMA_TRANSFORM;
        case SMB2_SIGNING_CAPABILITIES:
            return CHIMERA_SMB_NEGOTIATE_CTX_SIGNING;
        default:
            return 0;
    } /* switch */
} /* negotiate_ctx_mask_bit */

/*
 * Negotiate-context dispatch policy (Phase 0; Phase 2 will tighten):
 *
 *   - Unknown context types: silently ignored (MS-SMB2 §2.2.3.1 reserved
 *     value 0x0100 and any future unknown ID). This is mandated by spec.
 *
 *   - Known type with a malformed body: parser returns -1 internally, we
 *     log at debug level and return 0 to the caller without setting the
 *     mask bit. The connection is not aborted — selection downstream will
 *     simply not pick that algorithm class. This is slightly more lenient
 *     than the spec strictly requires but matches Samba behavior.
 *
 *   - Duplicate of a known type: rejected with STATUS_INVALID_PARAMETER
 *     (return -1). MS-SMB2 §3.3.5.4 makes duplicates an error for several
 *     context types; we apply the rule uniformly.
 *
 *   - Ordering: contexts may arrive in any order. Selection happens after
 *     all contexts are parsed, so wire order does not influence outcomes.
 *
 *   - Dialect-dependent validity: NOT validated here. A client could send
 *     EncryptionCapabilities while negotiating 2.0.2; we parse and store it
 *     anyway. The selection step (and Phase 2) is responsible for refusing
 *     to act on dialect-illegal contexts. The default dialect ceiling of
 *     3.0 ensures this is moot today.
 *
 *   - Preauth-integrity is mandatory for 3.1.1 per spec. Phase 0 does not
 *     enforce this because Phase 0 does not advertise 3.1.1 by default;
 *     Phase 2 will fail the negotiate when 3.1.1 is selected without a
 *     valid preauth context.
 *
 * Exposed for unit tests in tests/phase0_contexts_test.c.
 * Returns 0 on success or "silently ignored"; -1 if the caller MUST reject
 * the whole NEGOTIATE.
 */
SYMBOL_EXPORT int
chimera_smb_parse_one_negotiate_context(
    struct chimera_smb_request *request,
    uint16_t                    type,
    const uint8_t              *data,
    uint16_t                    data_len)
{
    uint32_t bit = negotiate_ctx_mask_bit(type);
    int      rc  = 0;

    if (bit == 0) {
        /* Unknown context type — per spec, silently ignore on receipt. */
        return 0;
    }

    if (request->negotiate.ctx_present_mask & bit) {
        chimera_smb_error("Duplicate negotiate context type=0x%04x", type);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    switch (type) {
        case SMB2_PREAUTH_INTEGRITY_CAPABILITIES:
            rc = parse_neg_ctx_preauth(request, data, data_len);
            break;
        case SMB2_ENCRYPTION_CAPABILITIES:
            rc = parse_neg_ctx_encryption(request, data, data_len);
            break;
        case SMB2_COMPRESSION_CAPABILITIES:
            rc = parse_neg_ctx_compression(request, data, data_len);
            break;
        case SMB2_NETNAME_NEGOTIATE_CONTEXT_ID:
            rc = parse_neg_ctx_netname(request, data, data_len);
            break;
        case SMB2_TRANSPORT_CAPABILITIES:
            rc = parse_neg_ctx_transport(request, data, data_len);
            break;
        case SMB2_RDMA_TRANSFORM_CAPABILITIES:
            rc = parse_neg_ctx_rdma_transform(request, data, data_len);
            break;
        case SMB2_SIGNING_CAPABILITIES:
            rc = parse_neg_ctx_signing(request, data, data_len);
            break;
        default:
            /* Unreachable: negotiate_ctx_mask_bit returned non-zero. */
            break;
    } /* switch */

    if (rc != 0) {
        /* MS-SMB2 §3.3.5.4 makes a malformed COMPRESSION_CAPABILITIES context
         * (zero algorithms or DataLength below the structure size) a fatal
         * NEGOTIATE error; the caller maps the -1 to STATUS_INVALID_PARAMETER. */
        if (type == SMB2_COMPRESSION_CAPABILITIES) {
            return -1;
        }
        chimera_smb_debug("Failed to parse negotiate context type=0x%04x len=%u (ignored)",
                          type, data_len);
        /* Body was malformed but the type is known; per spec, do not abort the
         * connection — the absent bit in ctx_present_mask will steer selection
         * away from this algorithm class. */
        return 0;
    }

    request->negotiate.ctx_present_mask |= bit;
    return 0;
} /* chimera_smb_parse_one_negotiate_context */

/* Choose algorithms from the client's offer, following client preference order
 * (index 0 = most preferred — matches Samba and Windows Server behavior). For
 * Phase 0 the only honest selection is "nothing" — encryption, GMAC signing,
 * and preauth-bound key derivation are all Phase 2 work. We record presence so
 * the future code paths have everything they need to flip the bits later. */
static void
chimera_smb_select_negotiated_algorithms(
    struct chimera_smb_conn    *conn,
    struct chimera_smb_request *request)
{
    conn->negotiated.ctx_present_mask = request->negotiate.ctx_present_mask;

    conn->negotiated.preauth_hash_alg      = 0;
    conn->negotiated.cipher_id             = 0;
    conn->negotiated.signing_alg           = 0;
    conn->negotiated.compression_flags     = 0;
    conn->negotiated.compression_alg_count = 0;
    conn->negotiated.rdma_transform_count  = 0;
    memset(conn->negotiated.preauth_salt,    0, sizeof(conn->negotiated.preauth_salt));
    memset(conn->negotiated.compression_algs, 0, sizeof(conn->negotiated.compression_algs));
    memset(conn->negotiated.rdma_transforms,  0, sizeof(conn->negotiated.rdma_transforms));

    /* SMB 3.1.1: select SHA-512 preauth integrity and generate a server salt.
     * The salt value need not match anything the client derives independently —
     * both sides fold the negotiate *response* (which carries this salt) into
     * the preauth hash, so any value works as long as it is the one we send.
     * Encryption/compression/RDMA-transform selection remains deferred. */
    if (conn->dialect == SMB2_DIALECT_3_1_1 &&
        (request->negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_PREAUTH)) {
        conn->negotiated.preauth_hash_alg = SMB2_PREAUTH_HASH_SHA_512;
        if (RAND_bytes(conn->negotiated.preauth_salt,
                       sizeof(conn->negotiated.preauth_salt)) != 1) {
            memset(conn->negotiated.preauth_salt, 0, sizeof(conn->negotiated.preauth_salt));
        }
    }

    /* The 3.1.1 default signing algorithm is AES-128-CMAC (MS-SMB2 §3.3.5.4);
     * negotiated.signing_alg always holds the effective algorithm (note that
     * HMAC-SHA256 has id 0, so a separate default avoids any 0-as-unset
     * ambiguity in the signing dispatch). */
    if (conn->dialect == SMB2_DIALECT_3_1_1) {
        conn->negotiated.signing_alg = SMB2_SIGNING_AES_CMAC;

        if (request->negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_SIGNING) {
            /* Server preference order; all three are implemented
             * (smb_signing.c).  Pick the highest-preference algorithm the
             * client offered. */
            static const uint16_t preferred[] = {
                SMB2_SIGNING_AES_GMAC,
                SMB2_SIGNING_AES_CMAC,
                SMB2_SIGNING_HMAC_SHA256,
            };
            int                   i, j, found = 0;

            for (i = 0; i < (int) (sizeof(preferred) / sizeof(preferred[0])) && !found; i++) {
                for (j = 0; j < request->negotiate.signing_in.alg_count; j++) {
                    if (request->negotiate.signing_in.algs[j] == preferred[i]) {
                        conn->negotiated.signing_alg = preferred[i];
                        found                        = 1;
                        break;
                    }
                }
            }

            /* Client offered only algorithms we do not support: do not echo a
             * SIGNING_CAPABILITIES context, and fall back to the CMAC default
             * (which the client also defaults to when it sees no echo). */
            if (!found) {
                conn->negotiated.ctx_present_mask &= ~CHIMERA_SMB_NEGOTIATE_CTX_SIGNING;
            }
        }
    }

    /* SMB 3.1.1: when the client offers an ENCRYPTION_CAPABILITIES context, the
     * server MUST select a cipher and echo it back in the response context (or
     * return AES-128-CCM as a default per MS-SMB2 3.3.5.4).
     *
     * We select the cipher here unconditionally (even when encryption is
     * disabled in config), because a Samba client (smbtorture) records
     * Connection.CipherId from this context and uses it to size the per-session
     * AEAD nonce space.  Leaving CipherId at 0 makes the client compute a zero
     * nonce-space, and a later shallow-copy of the session
     * (smb2.compound.related1, durable-open, replay) then aborts — crashing the
     * client.  Whether traffic is actually encrypted is decided separately at
     * SESSION_SETUP, gated on shared->config.encryption. */
    if (conn->dialect == SMB2_DIALECT_3_1_1 &&
        (request->negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_ENCRYPTION)) {
        static const uint16_t preferred[] = {
            SMB2_ENCRYPTION_AES_128_GCM,
            SMB2_ENCRYPTION_AES_256_GCM,
            SMB2_ENCRYPTION_AES_128_CCM,
            SMB2_ENCRYPTION_AES_256_CCM,
        };
        int                   i, j;

        for (i = 0; i < (int) (sizeof(preferred) / sizeof(preferred[0])) &&
             conn->negotiated.cipher_id == 0; i++) {
            for (j = 0; j < request->negotiate.encryption_in.cipher_count; j++) {
                if (request->negotiate.encryption_in.ciphers[j] == preferred[i]) {
                    conn->negotiated.cipher_id = preferred[i];
                    break;
                }
            }
        }
    } else if (conn->dialect >= SMB2_DIALECT_3_0 &&
               conn->dialect < SMB2_DIALECT_3_1_1 &&
               (conn->thread->shared->config.encryption
                ? (request->negotiate.capabilities & SMB2_GLOBAL_CAP_ENCRYPTION)
                : conn->thread->shared->any_share_encrypt)) {
        /* SMB 3.0/3.0.2 has no encryption-capabilities context; the only cipher
         * is AES-128-CCM (MS-SMB2 §3.1.4.3).  Set it so SESSION_SETUP can derive
         * keys when encryption is enabled globally OR a per-share-encrypted tree
         * may be reached on this connection.  Under GLOBAL (whole-session)
         * encryption the cipher is set only when the CLIENT advertised
         * SMB2_GLOBAL_CAP_ENCRYPTION: a 3.x client that cannot encrypt must not be
         * marked encrypt-all and then have its cleartext traffic rejected (SMB2Model
         * Encryption ClientNotSupportsEncryption).  Under per-share-only encryption
         * the session is not encrypt-all, so the cipher is provisioned whenever an
         * encrypted share may be reached (a non-encrypting client is instead denied
         * by the per-share ENCRYPT_DATA check), preserving the prior behavior. */
        conn->negotiated.cipher_id = SMB2_ENCRYPTION_AES_128_CCM;
    }

    /* SMB 3.1.1 transport compression (MS-SMB2 §3.3.5.4): when the client offers
     * a COMPRESSION_CAPABILITIES context and compression is enabled in config,
     * record the mutually-supported algorithms.  Unlike signing/encryption (one
     * pick), the response context echoes the full intersection — Windows returns
     * every shared algorithm.  CHAINED is agreed only when the client offered it
     * (Pattern_V1 / NONE pass-through are only meaningful inside a chain). */
    if (conn->dialect == SMB2_DIALECT_3_1_1 &&
        conn->thread->shared->config.compression &&
        (request->negotiate.ctx_present_mask & CHIMERA_SMB_NEGOTIATE_CTX_COMPRESSION)) {
        /* Algorithms Chimera implements (smb_compress.c).  LZ77, LZNT1 and
         * LZ77+Huffman are full byte codecs; Pattern_V1 is only meaningful as a
         * chained run-length payload but is supported so peers may send it. */
        static const uint16_t supported[] = {
            SMB2_COMPRESSION_LZ77,
            SMB2_COMPRESSION_LZ77_HUFFMAN,
            SMB2_COMPRESSION_LZNT1,
            SMB2_COMPRESSION_PATTERN_V1,
        };
        int                   i, j, n = 0;

        /* MS-SMB2 §3.3.5.4: set the negotiated CompressionAlgorithms to the
         * CompressionIds from the request that the server supports, preserving
         * the client's order (WPTS asserts an exact, order-sensitive match). */
        for (j = 0; j < request->negotiate.compression_in.alg_count; j++) {
            uint16_t a = request->negotiate.compression_in.algs[j];

            for (i = 0; i < (int) (sizeof(supported) / sizeof(supported[0])); i++) {
                if (a == supported[i]) {
                    conn->negotiated.compression_algs[n++] = a;
                    break;
                }
            }
        }
        conn->negotiated.compression_alg_count = n;

        if (n > 0) {
            if (request->negotiate.compression_in.flags & SMB2_COMPRESSION_FLAG_CHAINED) {
                conn->negotiated.compression_flags = SMB2_COMPRESSION_FLAG_CHAINED;
            }
        } else {
            /* No shared algorithm: do not emit a response context. */
            conn->negotiated.ctx_present_mask &= ~CHIMERA_SMB_NEGOTIATE_CTX_COMPRESSION;
        }
    }
} /* chimera_smb_select_negotiated_algorithms */

int
chimera_smb_parse_negotiate(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    int i;

    if (request->request_struct_size != SMB2_NEGOTIATE_REQUEST_SIZE) {
        chimera_smb_error("Received SMB2 NEGOTIATE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_NEGOTIATE_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }
    int      prc = 0;
    uint16_t security_mode_wire;
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->negotiate.dialect_count);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &security_mode_wire);
    prc |= evpl_iovec_cursor_try_skip(request_cursor, 2); /* Reserved */
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->negotiate.capabilities);
    prc |= evpl_iovec_cursor_try_copy(request_cursor, request->negotiate.client_guid, 16);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->negotiate.negotiate_context_offset);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->negotiate.negotiate_context_count);
    prc |= evpl_iovec_cursor_try_skip(request_cursor, 2); /* Reserved2 */

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 NEGOTIATE request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }
    request->negotiate.security_mode = (uint8_t) security_mode_wire;

    if (request->negotiate.dialect_count > SMB2_MAX_DIALECTS) {
        chimera_smb_error("Received SMB2 NEGOTIATE request with invalid dialect count (%u max %u)",
                          request->negotiate.dialect_count,
                          SMB2_MAX_DIALECTS);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    for (i = 0; i < request->negotiate.dialect_count; i++) {
        prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->negotiate.dialects[i]);
    }

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 NEGOTIATE dialect array past message");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    request->negotiate.ctx_present_mask = 0;

    if (request->negotiate.negotiate_context_count) {
        /* Seek to the context list (validated forward and within the message);
         * smb_cursor_seek_to subsumes the old precedes-consumed underflow guard. */
        if (unlikely(smb_cursor_seek_to(request_cursor,
                                        request->negotiate.negotiate_context_offset) != 0)) {
            chimera_smb_error("Negotiate context offset %u out of range",
                              request->negotiate.negotiate_context_offset);
            return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
        }

        for (i = 0; i < request->negotiate.negotiate_context_count; i++) {
            uint16_t type, length;
            uint8_t  data[512];
            int      crc = 0;

            crc |= evpl_iovec_cursor_try_skip(request_cursor, (8 - (request_cursor->consumed & 7)) & 7);
            crc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &type);
            crc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &length);
            crc |= evpl_iovec_cursor_try_skip(request_cursor, 4);  /* Reserved */

            if (unlikely(crc)) {
                chimera_smb_error("Negotiate context header truncated");
                return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
            }

            if (length > sizeof(data)) {
                chimera_smb_error("Negotiate context length %u exceeds parser cap %zu",
                                  length, sizeof(data));
                request->status = SMB2_STATUS_INVALID_PARAMETER;
                return -1;
            }

            if (length > 0) {
                if (evpl_iovec_cursor_try_copy(request_cursor, data, length) != 0) {
                    chimera_smb_error("Negotiate context body truncated (type=0x%04x len=%u)",
                                      type, length);
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }
            }

            if (chimera_smb_parse_one_negotiate_context(request, type, data, length) < 0) {
                request->status = SMB2_STATUS_INVALID_PARAMETER;
                return -1;
            }
        }
    }

    return 0;
} /* chimera_smb_parse_negotiate */
