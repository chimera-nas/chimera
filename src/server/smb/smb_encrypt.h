// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

struct evpl;
struct evpl_iovec;
struct evpl_iovec_cursor;
struct chimera_smb_encrypt_ctx;
struct chimera_smb_conn;
struct chimera_smb_request;
struct chimera_smb_session;

/*
 * Snapshot of the signing/encryption state a request needs to finalize a
 * STANDALONE async response (an interim STATUS_PENDING, or a change-notify
 * delivery) on the wire after the originating request/compound is gone.
 *
 * Encryption supersedes signing (MS-SMB2 §3.1.4.3): on a session that encrypts,
 * EVERY response -- including async interims -- must be wrapped in a TRANSFORM
 * header (§3.3.4.1.4), not signed.  When the session only signs, the standalone
 * message is signed in place instead.
 */
struct chimera_smb_secure_send {
    uint8_t                     encrypt;        /* wrap in a TRANSFORM header */
    uint8_t                     sign;           /* sign in place (when !encrypt) */
    uint16_t                    cipher_id;
    size_t                      enc_key_len;
    uint64_t                    session_id;
    uint8_t                     enc_key[32];
    uint8_t                     signing_key[16];
    /* Owns the strictly-monotonic per-session AEAD nonce counter.  The session
     * outlives every parked request on its connection (teardown drains parked
     * requests without sending), so this pointer is valid at send time. */
    struct chimera_smb_session *enc_session;
};

/*
 * Capture `request`'s signing/encryption state into `snap` so a later
 * standalone async response can be secured the same way the synchronous reply
 * path would have secured it.
 */
void
chimera_smb_secure_send_snapshot(
    struct chimera_smb_request     *request,
    struct chimera_smb_secure_send *snap);

/*
 * Finalize and send a standalone SMB2 message built outside the compound reply
 * path.  `iov` holds [4-byte NetBIOS framing][SMB2 message] plaintext and is
 * consumed (sent with TAKE_REF, or released on error).  smb2_len is the SMB2
 * message length (excluding the NetBIOS framing); iov.length must be at least
 * 4 + smb2_len.  Encrypts (per `snap->encrypt`) or signs (per `snap->sign`)
 * before sending; on an encryption failure the connection is closed.
 */
void
chimera_smb_secure_send(
    struct chimera_smb_conn              *conn,
    struct evpl_iovec                    *iov,
    int                                   smb2_len,
    const struct chimera_smb_secure_send *snap);

/*
 * SMB3 transport encryption (MS-SMB2 §3.1.4.3 / §3.3.4.1.4).  Mirrors the
 * signing context: a per-thread object pre-fetches the AEAD ciphers and holds a
 * reusable EVP_CIPHER_CTX (which is NOT thread-safe, hence per-thread).
 */
struct chimera_smb_encrypt_ctx *
chimera_smb_encrypt_ctx_create(
    void);

void
chimera_smb_encrypt_ctx_destroy(
    struct chimera_smb_encrypt_ctx *ctx);

/*
 * Derive the per-session encryption (server->client) and decryption
 * (client->server) keys from the raw NTLM/GSSAPI session key.  cipher_id is the
 * negotiated SMB2_ENCRYPTION_* id and selects the key length (16 or 32 bytes).
 * preauth_hash (64 bytes) is required for SMB 3.1.1, ignored otherwise.
 * Returns 0 on success, -1 on failure.
 */
int
chimera_smb_derive_encryption_keys(
    int            dialect,
    uint16_t       cipher_id,
    const void    *session_key,
    size_t         session_key_len,
    const uint8_t *preauth_hash,
    uint8_t       *enc_key_out,
    uint8_t       *dec_key_out,
    size_t        *key_len_out);

/*
 * Encrypt an assembled plaintext SMB2 reply.  plain_iov/plain_niov describe the
 * full reply buffer whose first transport_hdr_len bytes are the (unencrypted)
 * transport framing; plain_len is the SMB2 message length that follows it.
 * Produces a single contiguous out_iov laid out as
 *   [transport_hdr_len bytes reserved][52-byte TRANSFORM header][ciphertext],
 * with the AEAD tag written into the transform Signature field.  The caller
 * fills the reserved transport header and sends out_iov.  Returns 0 on success.
 */
int
chimera_smb_encrypt_compound(
    struct chimera_smb_encrypt_ctx *ctx,
    struct evpl                    *evpl,
    uint16_t                        cipher_id,
    const uint8_t                  *key,
    size_t                          key_len,
    uint64_t                        nonce_counter,
    uint64_t                        session_id,
    struct evpl_iovec              *plain_iov,
    int                             plain_niov,
    int                             plain_len,
    int                             transport_hdr_len,
    struct evpl_iovec              *out_iov);

/*
 * Decrypt a TRANSFORM message.  cursor is positioned at the transform header
 * (immediately after the 4-byte NetBIOS framing); length is the transform
 * header plus ciphertext byte count.  cipher_id/key/key_len come from the
 * session named by the transform header's SessionId (the caller looks it up).
 * On success allocates plain_out (caller must evpl_iovec_release it) holding the
 * decrypted SMB2 message, sets *plain_len_out, and returns 0.  On a malformed
 * header or AEAD tag-verification failure returns -1 (nothing allocated).
 */
int
chimera_smb_decrypt_message(
    struct chimera_smb_encrypt_ctx *ctx,
    struct evpl                    *evpl,
    uint16_t                        cipher_id,
    const uint8_t                  *key,
    size_t                          key_len,
    struct evpl_iovec_cursor       *cursor,
    int                             length,
    struct evpl_iovec              *plain_out,
    int                            *plain_len_out);
