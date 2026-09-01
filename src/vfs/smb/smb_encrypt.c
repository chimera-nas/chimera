// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB3 transport encryption for the SMB2 client (MS-SMB2 3.1.4.3 / 3.2.4.1.8).
 *
 * These mirror the server's src/server/smb/smb_encrypt.c exactly so the two
 * sides compute identical keys, nonces and AEAD tags -- the same relationship
 * smb.c's signing primitives have with the server's smb_signing.c.  Mirroring
 * rather than linking is deliberate: the client VFS module must not depend on
 * the server library.
 *
 * The one asymmetry is key direction.  MS-SMB2 3.1.4.2 derives a pair of keys
 * per session; each side ENCRYPTS with the key named for its own direction and
 * DECRYPTS with the peer's:
 *
 *              3.1.1 label            3.0/3.0.2 context
 *   client ->  SMBC2SCipherKey        "ServerIn " (note the trailing space)
 *   server ->  SMBS2CCipherKey        "ServerOut"
 *
 * so the client's enc_key is the server's dec_key and vice versa.  Getting this
 * backwards produces a session that negotiates cleanly and then fails every tag
 * check, which is why the direction is spelled out at each call site.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <openssl/evp.h>
#include <openssl/kdf.h>

#include "smb_internal.h"

/* ---- cipher lookup ------------------------------------------------------ */

/* Resolve an SMB2 cipher id to a fetched EVP_CIPHER plus its key length, nonce
 * length and mode.  Nonce lengths are fixed by MS-SMB2 3.1.4.3: 11 bytes for
 * CCM, 12 for GCM (the transform header always carries a 16-byte nonce field,
 * zero-padded above the significant bytes). */
static EVP_CIPHER *
smb_client_cipher_for_id(
    struct chimera_smb_client_encrypt_ctx *ctx,
    uint16_t                               cipher_id,
    size_t                                *key_len,
    int                                   *nonce_len,
    int                                   *is_ccm)
{
    switch (cipher_id) {
        case SMB2_ENCRYPTION_AES_128_CCM:
            *key_len   = 16;
            *nonce_len = 11;
            *is_ccm    = 1;
            return ctx->aes128ccm;
        case SMB2_ENCRYPTION_AES_128_GCM:
            *key_len   = 16;
            *nonce_len = 12;
            *is_ccm    = 0;
            return ctx->aes128gcm;
        case SMB2_ENCRYPTION_AES_256_CCM:
            *key_len   = 32;
            *nonce_len = 11;
            *is_ccm    = 1;
            return ctx->aes256ccm;
        case SMB2_ENCRYPTION_AES_256_GCM:
            *key_len   = 32;
            *nonce_len = 12;
            *is_ccm    = 0;
            return ctx->aes256gcm;
        default:
            return NULL;
    } /* switch */
} /* smb_client_cipher_for_id */

struct chimera_smb_client_encrypt_ctx *
chimera_smb_client_encrypt_ctx_create(void)
{
    struct chimera_smb_client_encrypt_ctx *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        return NULL;
    }

    ctx->cctx      = EVP_CIPHER_CTX_new();
    ctx->aes128ccm = EVP_CIPHER_fetch(NULL, "AES-128-CCM", NULL);
    ctx->aes128gcm = EVP_CIPHER_fetch(NULL, "AES-128-GCM", NULL);
    ctx->aes256ccm = EVP_CIPHER_fetch(NULL, "AES-256-CCM", NULL);
    ctx->aes256gcm = EVP_CIPHER_fetch(NULL, "AES-256-GCM", NULL);

    if (!ctx->cctx) {
        chimera_smb_client_encrypt_ctx_destroy(ctx);
        return NULL;
    }

    return ctx;
} /* chimera_smb_client_encrypt_ctx_create */

void
chimera_smb_client_encrypt_ctx_destroy(struct chimera_smb_client_encrypt_ctx *ctx)
{
    if (!ctx) {
        return;
    }
    if (ctx->cctx) {
        EVP_CIPHER_CTX_free(ctx->cctx);
    }
    EVP_CIPHER_free(ctx->aes128ccm);
    EVP_CIPHER_free(ctx->aes128gcm);
    EVP_CIPHER_free(ctx->aes256ccm);
    EVP_CIPHER_free(ctx->aes256gcm);
    free(ctx);
} /* chimera_smb_client_encrypt_ctx_destroy */

/* ---- key derivation ----------------------------------------------------- */

int
chimera_smb_client_derive_encryption_keys(
    int            dialect,
    uint16_t       cipher_id,
    const void    *session_key,
    size_t         session_key_len,
    const uint8_t *preauth_hash,
    uint8_t       *enc_key_out,
    uint8_t       *dec_key_out,
    size_t        *key_len_out)
{
    /* 3.0/3.0.2: label is always "SMB2AESCCM" even under GCM; the contexts are
     * direction names, and "ServerIn " carries a trailing space (MS-SMB2
     * 3.1.4.2).  Client-to-server traffic is what the CLIENT encrypts, so the
     * client's enc key uses the ServerIn context. */
    static const char label30[]   = "SMB2AESCCM";    /* incl NUL per spec  */
    static const char ctx30_enc[] = "ServerIn ";     /* client -> server   */
    static const char ctx30_dec[] = "ServerOut";     /* server -> client   */
    /* 3.1.1: labels carry the direction; context is the preauth hash. */
    static const char label311_enc[] = "SMBC2SCipherKey"; /* client -> server */
    static const char label311_dec[] = "SMBS2CCipherKey"; /* server -> client */

    size_t            key_len;
    int               ok_e, ok_d;

    switch (cipher_id) {
        case SMB2_ENCRYPTION_AES_128_CCM:
        case SMB2_ENCRYPTION_AES_128_GCM:
            key_len = 16;
            break;
        case SMB2_ENCRYPTION_AES_256_CCM:
        case SMB2_ENCRYPTION_AES_256_GCM:
            key_len = 32;
            break;
        default:
            chimera_smbclient_error("Unknown SMB3 cipher id 0x%x in key derivation",
                                    cipher_id);
            return -1;
    } /* switch */

    if (dialect == SMB2_DIALECT_3_1_1) {
        if (!preauth_hash) {
            chimera_smbclient_error(
                "SMB 3.1.1 encryption key derivation without preauth hash");
            return -1;
        }
        ok_e = 0 == chimera_smb_client_kbkdf(session_key, session_key_len,
                                             label311_enc, sizeof(label311_enc),
                                             preauth_hash, SMB2_PREAUTH_HASH_SIZE,
                                             enc_key_out, key_len);
        ok_d = 0 == chimera_smb_client_kbkdf(session_key, session_key_len,
                                             label311_dec, sizeof(label311_dec),
                                             preauth_hash, SMB2_PREAUTH_HASH_SIZE,
                                             dec_key_out, key_len);
    } else {
        ok_e = 0 == chimera_smb_client_kbkdf(session_key, session_key_len,
                                             label30, sizeof(label30),
                                             (const uint8_t *) ctx30_enc, sizeof(ctx30_enc),
                                             enc_key_out, key_len);
        ok_d = 0 == chimera_smb_client_kbkdf(session_key, session_key_len,
                                             label30, sizeof(label30),
                                             (const uint8_t *) ctx30_dec, sizeof(ctx30_dec),
                                             dec_key_out, key_len);
    }

    if (!ok_e || !ok_d) {
        chimera_smbclient_error("SMB3 encryption key derivation failed");
        return -1;
    }

    *key_len_out = key_len;
    return 0;
} /* chimera_smb_client_derive_encryption_keys */

/* Lay the 64-bit message counter little-endian into the low bytes of the cipher
 * nonce; the rest stays zero (MS-SMB2 3.1.4.3). */
static void
smb_client_build_nonce(
    uint8_t *nonce16,
    int      nonce_len,
    uint64_t counter)
{
    memset(nonce16, 0, 16);
    for (int i = 0; i < 8 && i < nonce_len; i++) {
        nonce16[i] = (uint8_t) (counter >> (8 * i));
    }
} /* smb_client_build_nonce */

/* ---- encrypt / decrypt -------------------------------------------------- */

int
chimera_smb_client_encrypt_message(
    struct chimera_smb_client_encrypt_ctx *ctx,
    struct evpl                           *evpl,
    uint16_t                               cipher_id,
    const uint8_t                         *key,
    size_t                                 key_len,
    uint64_t                               nonce_counter,
    uint64_t                               session_id,
    struct evpl_iovec                     *plain_iov,
    int                                    plain_len,
    int                                    transport_hdr_len,
    struct evpl_iovec                     *out_iov)
{
    struct smb2_transform_header *th;
    struct evpl_iovec_cursor      cursor;
    EVP_CIPHER                   *cipher;
    EVP_CIPHER_CTX               *c = ctx->cctx;
    uint8_t                      *out, *ct;
    uint8_t                       nonce[16];
    size_t                        ck_len;
    int                           nonce_len, is_ccm, outl, total;
    static const uint8_t          proto[4] = SMB2_TRANSFORM_PROTO_ID;

    cipher = smb_client_cipher_for_id(ctx, cipher_id, &ck_len, &nonce_len,
                                      &is_ccm);

    if (!cipher || ck_len != key_len) {
        chimera_smbclient_error("Invalid cipher/key for SMB3 encryption (id 0x%x)",
                                cipher_id);
        return -1;
    }

    total = transport_hdr_len + (int) sizeof(*th) + plain_len;

    if (evpl_iovec_alloc(evpl, total, 8, 1, 0, out_iov) < 1) {
        chimera_smbclient_error("Failed to allocate SMB3 encryption output buffer");
        return -1;
    }

    out = evpl_iovec_data(out_iov);
    th  = (struct smb2_transform_header *) (out + transport_hdr_len);
    ct  = out + transport_hdr_len + sizeof(*th);

    /* Gather the plaintext SMB2 message (past the transport framing) into the
     * output ciphertext region; the AEAD encrypts it in place below. */
    evpl_iovec_cursor_init(&cursor, plain_iov, 1);
    evpl_iovec_cursor_skip(&cursor, transport_hdr_len);
    evpl_iovec_cursor_copy(&cursor, ct, plain_len);

    smb_client_build_nonce(nonce, nonce_len, nonce_counter);

    /* Signature is the AEAD tag, filled after encryption; bytes [20..52) of the
     * transform header are the associated data. */
    memcpy(th->protocol_id, proto, 4);
    memset(th->signature, 0, sizeof(th->signature));
    memcpy(th->nonce, nonce, sizeof(th->nonce));
    th->original_message_size = plain_len;
    th->reserved              = 0;
    th->flags                 = SMB2_TRANSFORM_FLAGS_ENCRYPTED;
    th->session_id            = session_id;

    if (EVP_EncryptInit_ex(c, cipher, NULL, NULL, NULL) != 1) {
        goto err;
    }

    if (is_ccm) {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_TAG, 16, NULL) != 1 ||
            EVP_EncryptInit_ex(c, NULL, NULL, key, nonce) != 1 ||
            /* CCM wants the total plaintext length, then the AAD length. */
            EVP_EncryptUpdate(c, NULL, &outl, NULL, plain_len) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl,
                              ((uint8_t *) th) + SMB2_TRANSFORM_AAD_OFFSET,
                              SMB2_TRANSFORM_AAD_SIZE) != 1 ||
            EVP_EncryptUpdate(c, ct, &outl, ct, plain_len) != 1 ||
            EVP_EncryptFinal_ex(c, ct + outl, &outl) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_GET_TAG, 16, th->signature) != 1) {
            goto err;
        }
    } else {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_EncryptInit_ex(c, NULL, NULL, key, nonce) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl,
                              ((uint8_t *) th) + SMB2_TRANSFORM_AAD_OFFSET,
                              SMB2_TRANSFORM_AAD_SIZE) != 1 ||
            EVP_EncryptUpdate(c, ct, &outl, ct, plain_len) != 1 ||
            EVP_EncryptFinal_ex(c, ct + outl, &outl) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_GET_TAG, 16, th->signature) != 1) {
            goto err;
        }
    }

    evpl_iovec_set_length(out_iov, total);
    return 0;

 err:
    chimera_smbclient_error("SMB3 encryption failed (cipher id 0x%x)", cipher_id);
    evpl_iovec_release(evpl, out_iov);
    return -1;
} /* chimera_smb_client_encrypt_message */

int
chimera_smb_client_decrypt_message(
    struct chimera_smb_client_encrypt_ctx *ctx,
    struct evpl                           *evpl,
    uint16_t                               cipher_id,
    const uint8_t                         *key,
    size_t                                 key_len,
    struct evpl_iovec_cursor              *cursor,
    int                                    length,
    struct evpl_iovec                     *plain_out,
    int                                   *plain_len_out)
{
    struct smb2_transform_header th;
    static const uint8_t         proto[4] = SMB2_TRANSFORM_PROTO_ID;
    EVP_CIPHER                  *cipher;
    EVP_CIPHER_CTX              *c = ctx->cctx;
    uint8_t                     *pt;
    size_t                       ck_len;
    int                          nonce_len, is_ccm, outl, ct_len;

    cipher = smb_client_cipher_for_id(ctx, cipher_id, &ck_len, &nonce_len,
                                      &is_ccm);

    if (!cipher || ck_len != key_len) {
        chimera_smbclient_error("Invalid cipher/key for SMB3 decryption (id 0x%x)",
                                cipher_id);
        return -1;
    }

    if (length < (int) sizeof(th)) {
        chimera_smbclient_error("Truncated SMB3 transform message (%d bytes)",
                                length);
        return -1;
    }

    evpl_iovec_cursor_copy(cursor, &th, sizeof(th));

    if (memcmp(th.protocol_id, proto, 4) != 0) {
        chimera_smbclient_error("Invalid SMB3 transform protocol id");
        return -1;
    }

    ct_len = (int) th.original_message_size;

    if (ct_len < (int) sizeof(struct smb2_header) ||
        ct_len > length - (int) sizeof(th)) {
        chimera_smbclient_error("Invalid SMB3 transform original_message_size %d",
                                ct_len);
        return -1;
    }

    if (evpl_iovec_alloc(evpl, ct_len, 8, 1, 0, plain_out) < 1) {
        chimera_smbclient_error("Failed to allocate SMB3 decryption buffer");
        return -1;
    }

    pt = evpl_iovec_data(plain_out);

    evpl_iovec_cursor_copy(cursor, pt, ct_len);

    if (EVP_DecryptInit_ex(c, cipher, NULL, NULL, NULL) != 1) {
        goto err;
    }

    if (is_ccm) {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_TAG, 16, th.signature) != 1 ||
            EVP_DecryptInit_ex(c, NULL, NULL, key, th.nonce) != 1 ||
            EVP_DecryptUpdate(c, NULL, &outl, NULL, ct_len) != 1 ||
            EVP_DecryptUpdate(c, NULL, &outl,
                              ((uint8_t *) &th) + SMB2_TRANSFORM_AAD_OFFSET,
                              SMB2_TRANSFORM_AAD_SIZE) != 1) {
            goto err;
        }
        /* Under CCM the ciphertext update itself reports tag failure. */
        if (EVP_DecryptUpdate(c, pt, &outl, pt, ct_len) <= 0) {
            goto err;
        }
    } else {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_DecryptInit_ex(c, NULL, NULL, key, th.nonce) != 1 ||
            EVP_DecryptUpdate(c, NULL, &outl,
                              ((uint8_t *) &th) + SMB2_TRANSFORM_AAD_OFFSET,
                              SMB2_TRANSFORM_AAD_SIZE) != 1 ||
            EVP_DecryptUpdate(c, pt, &outl, pt, ct_len) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_TAG, 16, th.signature) != 1) {
            goto err;
        }
        /* GCM verifies the tag at finalization. */
        if (EVP_DecryptFinal_ex(c, pt + outl, &outl) <= 0) {
            goto err;
        }
    }

    evpl_iovec_set_length(plain_out, ct_len);
    *plain_len_out = ct_len;
    return 0;

 err:
    chimera_smbclient_error(
        "SMB3 decryption / tag verification failed (cipher id 0x%x)", cipher_id);
    evpl_iovec_release(evpl, plain_out);
    return -1;
} /* chimera_smb_client_decrypt_message */
