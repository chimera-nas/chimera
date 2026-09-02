// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The server's half of SMB2 signing: applying and verifying a signature over a
 * chimera_smb_request / compound, which are server structures.  The MAC
 * algorithms and key derivation live in src/smb_common/smb_signing.c, shared
 * with the client so both sides compute the same bytes.
 */

#include <string.h>
#include <stdlib.h>
#include <openssl/evp.h>

#include "smb_common/smb_signing.h"
#include "smb_internal.h"
#include "common/evpl_iovec_cursor.h"
#include "smb_common/smb2.h"

static int
chimera_smb_compute_signature(
    struct chimera_smb_signing_ctx *ctx,
    struct chimera_smb_conn        *conn,
    struct smb2_header             *hdr,
    struct evpl_iovec_cursor       *cursor,
    int                             length,
    const uint8_t                  *key,
    uint8_t                        *out_sig16)
{
    return chimera_smb_compute_signature_alg(ctx, conn->dialect,
                                             conn->negotiated.signing_alg,
                                             hdr, cursor, length, key, out_sig16);
} /* chimera_smb_compute_signature */

int
chimera_smb_verify_signature(
    struct chimera_smb_signing_ctx *ctx,
    struct chimera_smb_request     *request,
    const uint8_t                  *signing_key,
    struct evpl_iovec_cursor       *cursor,
    int                             length)
{
    struct chimera_smb_conn *conn = request->compound->conn;
    uint8_t                  signature[16];
    uint8_t                  calculated[16];
    char                     recv_sig[80];
    char                     calc_sig[80];
    int                      rc;

    memcpy(&signature, &request->smb2_hdr.signature, sizeof(signature));
    memset(request->smb2_hdr.signature, 0, sizeof(request->smb2_hdr.signature));

    rc = chimera_smb_compute_signature(ctx, conn, &request->smb2_hdr, cursor,
                                       length, signing_key, calculated);

    if (unlikely(rc != 0)) {
        chimera_smb_error("Failed to calculate signature for dialect %x", conn->dialect);
        return rc;
    }

    /* Constant-time comparison to avoid a timing oracle on the MAC. */
    uint8_t sig_diff = 0;

    for (size_t i = 0; i < sizeof(signature); i++) {
        sig_diff |= signature[i] ^ calculated[i];
    }

    if (unlikely(sig_diff != 0)) {
        format_hex(recv_sig, sizeof(recv_sig), signature, sizeof(signature));
        format_hex(calc_sig, sizeof(calc_sig), calculated, sizeof(calculated));
        chimera_smb_error("Received signature: %s does not match calculated signature: %s", recv_sig, calc_sig);
        return -1;
    }

    return 0;
} /* chimera_smb_verify_signature */

int
chimera_smb_sign_compound(
    struct chimera_smb_signing_ctx *ctx,
    struct chimera_smb_compound    *compound,
    struct evpl_iovec              *iov,
    int                             niov,
    int                             length)
{
    struct chimera_smb_conn           *conn = compound->conn;
    struct chimera_smb_session_handle *session_handle;
    struct chimera_smb_request        *request;
    struct evpl_iovec_cursor           cursor;
    struct smb2_header                *hdr;
    int                                i, rc;
    int                                left = length, payload_length;
    uint8_t                            signature[16];

    evpl_iovec_cursor_init(&cursor, iov, niov);

    if (conn->protocol == EVPL_DATAGRAM_RDMACM_RC) {
        evpl_iovec_cursor_skip(&cursor, sizeof(struct smb_direct_hdr) + 4);
        left -= sizeof(struct smb_direct_hdr) + 4;
    } else {
        evpl_iovec_cursor_skip(&cursor, sizeof(struct netbios_header));
        left -= sizeof(struct netbios_header);
    }

    for (i = 0; i < compound->num_requests && left; i++) {

        request        = compound->requests[i];
        session_handle = request->session_handle;

        /* Skip requests handled asynchronously — no reply header was written
         * for them so there is nothing to sign. */
        if (request->status == SMB2_STATUS_PENDING) {
            continue;
        }

        /* We know hdr is contig since we allocated it that way */
        hdr = evpl_iovec_cursor_data(&cursor);

        evpl_iovec_cursor_skip(&cursor, sizeof(struct smb2_header));
        left -= sizeof(struct smb2_header);

        if (hdr->next_command) {
            payload_length = hdr->next_command - sizeof(struct smb2_header);
        } else {
            payload_length = left;
        }

        if (request->flags & CHIMERA_SMB_REQUEST_FLAG_SIGN) {

            if (unlikely(!session_handle)) {
                chimera_smb_error(
                    "SIGN flag set but session_handle is NULL: "
                    "cmd=0x%x msg_flags=0x%x related=%d status=0x%x req_idx=%d/%d",
                    request->smb2_hdr.command,
                    request->smb2_hdr.flags,
                    !!(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS),
                    request->status, i, compound->num_requests);
                return -1;
            }

            /* A signed response MUST advertise SMB2_FLAGS_SIGNED (MS-SMB2
             * 3.3.4.1.1), and the signature is computed over the header *with*
             * that flag set.  The reply header inherits the request's flags, so
             * a response to a request the client itself signed already carries
             * the bit — but the final SESSION_SETUP response is signed by the
             * server even though the establishing request was unsigned.  Set
             * the flag here, before computing the MAC, so (a) the value we sign
             * matches the bytes the client verifies and (b) a client with
             * signing required does not treat the response as unsigned (which
             * mishandles channel/session setup and can crash it). */
            hdr->flags |= SMB2_FLAGS_SIGNED;

            /* A SESSION_SETUP response other than a final SUCCESS (interim
             * MORE_PROCESSING legs and errors such as a rejected channel bind)
             * is verified by the client against its Session.SigningKey object,
             * which carries the dialect/algorithm of the connection the
             * session was ESTABLISHED on — not this connection's.  Sign those
             * with the session's algorithm (Samba bug 14512; smbtorture
             * smb2.session.bind_negative_smb3to2* receives the bind rejection
             * on a 2.10 connection but verifies it with the 3.x session's
             * AES-CMAC).  A final SUCCESS response is verified with the
             * just-derived per-channel key, whose client-side object uses this
             * connection's algorithm. */
            if (request->smb2_hdr.command == SMB2_SESSION_SETUP &&
                request->status != SMB2_STATUS_SUCCESS &&
                session_handle->session &&
                (session_handle->session->flags & CHIMERA_SMB_SESSION_AUTHORIZED)) {
                rc = chimera_smb_compute_signature_alg(ctx,
                                                       session_handle->session->dialect,
                                                       session_handle->session->sign_alg,
                                                       hdr, &cursor,
                                                       payload_length,
                                                       session_handle->signing_key,
                                                       signature);
            } else {
                rc = chimera_smb_compute_signature(ctx, conn, hdr, &cursor,
                                                   payload_length,
                                                   session_handle->signing_key,
                                                   signature);
            }

            if (unlikely(rc != 0)) {
                chimera_smb_error("Failed to calculate signature for dialect %x", conn->dialect);
                return rc;
            }

            memcpy(hdr->signature, signature, sizeof(signature));
        } else {
            evpl_iovec_cursor_skip(&cursor, payload_length);
        }
        left -= payload_length;
    }

    chimera_smb_abort_if(left, "Left is not 0 after signing compound");

    return 0;
} /* chimera_smb_sign_compound */
