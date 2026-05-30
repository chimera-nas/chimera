// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct evpl;
struct evpl_iovec;
struct evpl_iovec_cursor;
struct chimera_smb_encrypt_ctx;

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
