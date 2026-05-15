// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

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
        chimera_smb_error("No valid dialect found");
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    conn->capabilities = 0;

    if (dialect >= 0x210 && (shared->config.capabilities & SMB2_GLOBAL_CAP_LARGE_MTU)) {
        conn->capabilities |= SMB2_GLOBAL_CAP_LARGE_MTU;
    }
    if (dialect >= 0x300 && (shared->config.capabilities & SMB2_GLOBAL_CAP_MULTI_CHANNEL)) {
        conn->capabilities |= SMB2_GLOBAL_CAP_MULTI_CHANNEL;
    }

    if (request->negotiate.security_mode & SMB2_SIGNING_REQUIRED) {
        conn->flags |= CHIMERA_SMB_CONN_FLAG_SIGNING_REQUIRED;
    }

    request->negotiate.r_dialect           = dialect;
    request->negotiate.r_security_mode     = SMB2_SIGNING_ENABLED;
    request->negotiate.r_capabilities      = conn->capabilities;
    request->negotiate.r_max_transact_size = 1 * 1024 * 1024;
    request->negotiate.r_max_read_size     = 8 * 1024 * 1024;
    request->negotiate.r_max_write_size    = 8 * 1024 * 1024;
    request->negotiate.r_system_time       = chimera_nt_time(&now);
    request->negotiate.r_server_start_time = chimera_nt_time(&boot);

    memcpy(request->negotiate.r_server_guid, shared->guid, SMB2_GUID_SIZE);

    conn->dialect = dialect;

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

/* Phase 0 ships an empty table on purpose. We have the plumbing to emit
 * negotiate response contexts, but nothing positive to advertise yet:
 *   - PreauthIntegrity, Encryption, Signing → Phase 2
 *   - Compression                            → Phase 8
 *   - RdmaTransform                          → Phase 5
 *   - Transport/Netname are not server-reply contexts in any phase.
 * Phase 2 will register builders here.
 *
 * !! Do NOT add SMB 3.1.1 to server.c's default dialect list until Phase 2
 * lands preauth integrity. With this table empty, a successful 3.1.1
 * negotiation produces a reply that is structurally legal (NegotiateContext-
 * Count=0, Offset=0) but spec-noncompliant: 3.1.1 mandates that the server
 * emit a PreauthIntegrityCapabilities reply. Lenient clients (current
 * Win11, macOS, libsmb2) tolerate the missing context and then derive
 * session-keys using SMB 3.0-style KDF — which diverges from the client's
 * preauth-bound derivation. The first signed message after SESSION_SETUP
 * fails MAC verification on both ends and the connection dies. The dialect
 * ceiling stays at 3.0 in server.c:103-105 specifically to keep this latent
 * mismatch unreachable. */
static const struct chimera_smb_negotiate_response_emitter smb_negotiate_response_emitters[] = {
    { 0, 0, NULL }
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
    uint32_t                                             pos   = 0;
    uint16_t                                             count = 0;
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
        pos += advance;
        count++;
    }

    *out_count = count;
    return pos;
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

    if (data_len < 8) {
        return -1;
    }
    count = smb_wire_le16(data);
    /* data + 2: 2-byte padding (ignored) */
    request->negotiate.compression_in.flags = smb_wire_le32(data + 4);

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

    /* Phase 0 selects nothing new. Phase 2 will replace these zeros with the
     * highest-preference algorithm the client offered that we implement.
     * Leave the salt zeroed for now — Phase 2 generates a real one. */
    conn->negotiated.preauth_hash_alg      = 0;
    conn->negotiated.cipher_id             = 0;
    conn->negotiated.signing_alg           = 0;
    conn->negotiated.compression_flags     = 0;
    conn->negotiated.compression_alg_count = 0;
    conn->negotiated.rdma_transform_count  = 0;
    memset(conn->negotiated.preauth_salt,    0, sizeof(conn->negotiated.preauth_salt));
    memset(conn->negotiated.compression_algs, 0, sizeof(conn->negotiated.compression_algs));
    memset(conn->negotiated.rdma_transforms,  0, sizeof(conn->negotiated.rdma_transforms));
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
    evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.dialect_count);
    {
        uint16_t security_mode_wire;
        evpl_iovec_cursor_get_uint16(request_cursor, &security_mode_wire);
        request->negotiate.security_mode = (uint8_t) security_mode_wire;
    }
    evpl_iovec_cursor_skip(request_cursor, 2); /* Reserved */
    evpl_iovec_cursor_get_uint32(request_cursor, &request->negotiate.capabilities);
    evpl_iovec_cursor_copy(request_cursor, request->negotiate.client_guid, 16);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->negotiate.negotiate_context_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.negotiate_context_count);
    evpl_iovec_cursor_skip(request_cursor, 2); /* Reserved2 */

    if (request->negotiate.dialect_count > SMB2_MAX_DIALECTS) {
        chimera_smb_error("Received SMB2 NEGOTIATE request with invalid dialect count (%u max %u)",
                          request->negotiate.dialect_count,
                          SMB2_MAX_DIALECTS);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    for (i = 0; i < request->negotiate.dialect_count; i++) {
        evpl_iovec_cursor_get_uint16(request_cursor, &request->negotiate.dialects[i]);
    }

    request->negotiate.ctx_present_mask = 0;

    if (request->negotiate.negotiate_context_count) {
        /* Reject a context list that starts before the bytes we've already
         * consumed — without this, the uint32 subtraction below wraps and
         * evpl_iovec_cursor_skip burns through the rest of the cursor. */
        if (request->negotiate.negotiate_context_offset <
            (uint32_t) evpl_iovec_cursor_consumed(request_cursor)) {
            chimera_smb_error("Negotiate context offset %u precedes consumed bytes %d",
                              request->negotiate.negotiate_context_offset,
                              evpl_iovec_cursor_consumed(request_cursor));
            request->status = SMB2_STATUS_INVALID_PARAMETER;
            return -1;
        }

        evpl_iovec_cursor_skip(request_cursor,
                               request->negotiate.negotiate_context_offset -
                               evpl_iovec_cursor_consumed(request_cursor));

        for (i = 0; i < request->negotiate.negotiate_context_count; i++) {
            uint16_t type, length;
            uint8_t  data[512];

            evpl_iovec_cursor_align64(request_cursor);
            evpl_iovec_cursor_get_uint16(request_cursor, &type);
            evpl_iovec_cursor_get_uint16(request_cursor, &length);
            evpl_iovec_cursor_skip(request_cursor, 4);  /* Reserved */

            if (length > sizeof(data)) {
                chimera_smb_error("Negotiate context length %u exceeds parser cap %zu",
                                  length, sizeof(data));
                request->status = SMB2_STATUS_INVALID_PARAMETER;
                return -1;
            }

            if (length > 0) {
                if (evpl_iovec_cursor_get_blob(request_cursor, data, length) != 0) {
                    chimera_smb_error("Negotiate context body truncated (type=0x%04x len=%u)",
                                      type, length);
                    request->status = SMB2_STATUS_INVALID_PARAMETER;
                    return -1;
                }
            }

            if (chimera_smb_parse_one_negotiate_context(request, type, data, length) < 0) {
                return -1;
            }
        }
    }

    return 0;
} /* chimera_smb_parse_negotiate */
