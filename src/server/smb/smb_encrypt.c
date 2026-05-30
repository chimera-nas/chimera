// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <openssl/evp.h>

#include "smb_encrypt.h"
#include "smb_signing.h"
#include "common/evpl_iovec_cursor.h"
#include "evpl/evpl.h"
#include "smb_internal.h"
#include "smb2.h"

struct chimera_smb_encrypt_ctx {
    EVP_CIPHER     *aes_128_ccm;
    EVP_CIPHER     *aes_128_gcm;
    EVP_CIPHER     *aes_256_ccm;
    EVP_CIPHER     *aes_256_gcm;
    EVP_CIPHER_CTX *cctx;
};

struct chimera_smb_encrypt_ctx *
chimera_smb_encrypt_ctx_create(void)
{
    struct chimera_smb_encrypt_ctx *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        return NULL;
    }

    ctx->aes_128_ccm = EVP_CIPHER_fetch(NULL, "AES-128-CCM", NULL);
    ctx->aes_128_gcm = EVP_CIPHER_fetch(NULL, "AES-128-GCM", NULL);
    ctx->aes_256_ccm = EVP_CIPHER_fetch(NULL, "AES-256-CCM", NULL);
    ctx->aes_256_gcm = EVP_CIPHER_fetch(NULL, "AES-256-GCM", NULL);

    chimera_smb_abort_if(!ctx->aes_128_ccm || !ctx->aes_128_gcm ||
                         !ctx->aes_256_ccm || !ctx->aes_256_gcm,
                         "Failed to fetch SMB3 AEAD ciphers");

    ctx->cctx = EVP_CIPHER_CTX_new();

    chimera_smb_abort_if(!ctx->cctx, "Failed to allocate SMB3 cipher context");

    return ctx;
} /* chimera_smb_encrypt_ctx_create */

void
chimera_smb_encrypt_ctx_destroy(struct chimera_smb_encrypt_ctx *ctx)
{
    if (!ctx) {
        return;
    }
    EVP_CIPHER_CTX_free(ctx->cctx);
    EVP_CIPHER_free(ctx->aes_128_ccm);
    EVP_CIPHER_free(ctx->aes_128_gcm);
    EVP_CIPHER_free(ctx->aes_256_ccm);
    EVP_CIPHER_free(ctx->aes_256_gcm);
    free(ctx);
} /* chimera_smb_encrypt_ctx_destroy */

/* Map a negotiated cipher id to its EVP cipher, key length and nonce length. */
static EVP_CIPHER *
smb_cipher_for_id(
    struct chimera_smb_encrypt_ctx *ctx,
    uint16_t                        cipher_id,
    size_t                         *key_len,
    int                            *nonce_len,
    int                            *is_ccm)
{
    switch (cipher_id) {
        case SMB2_ENCRYPTION_AES_128_CCM:
            *key_len = 16; *nonce_len = 11; *is_ccm = 1;
            return ctx->aes_128_ccm;
        case SMB2_ENCRYPTION_AES_128_GCM:
            *key_len = 16; *nonce_len = 12; *is_ccm = 0;
            return ctx->aes_128_gcm;
        case SMB2_ENCRYPTION_AES_256_CCM:
            *key_len = 32; *nonce_len = 11; *is_ccm = 1;
            return ctx->aes_256_ccm;
        case SMB2_ENCRYPTION_AES_256_GCM:
            *key_len = 32; *nonce_len = 12; *is_ccm = 0;
            return ctx->aes_256_gcm;
        default:
            return NULL;
    } /* switch */
} /* smb_cipher_for_id */

int
chimera_smb_derive_encryption_keys(
    int            dialect,
    uint16_t       cipher_id,
    const void    *session_key,
    size_t         session_key_len,
    const uint8_t *preauth_hash,
    uint8_t       *enc_key_out,
    uint8_t       *dec_key_out,
    size_t        *key_len_out)
{
    /* SMB 3.0/3.0.2 KDF labels/contexts (MS-SMB2 §3.1.4.2, pre-3.1.1 branch).
     * The label is always "SMB2AESCCM" even when GCM is negotiated; the context
     * "ServerIn " carries a *trailing space*. */
    static const char label30[]   = "SMB2AESCCM";       /* incl NUL per spec */
    static const char ctx30_enc[] = "ServerOut";        /* server -> client  */
    static const char ctx30_dec[] = "ServerIn ";        /* client -> server (trailing space) */
    /* SMB 3.1.1 labels; context = PreauthIntegrityHashValue. */
    static const char label311_enc[] = "SMBS2CCipherKey"; /* server -> client */
    static const char label311_dec[] = "SMBC2SCipherKey"; /* client -> server */

    size_t            key_len;
    int               nonce_len, is_ccm, ok_e, ok_d;

    (void) nonce_len;
    (void) is_ccm;

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
            chimera_smb_error("Unknown SMB3 cipher id 0x%x in key derivation", cipher_id);
            return -1;
    } /* switch */

    if (dialect == SMB2_DIALECT_3_1_1) {
        if (!preauth_hash) {
            chimera_smb_error("SMB 3.1.1 encryption key derivation without preauth hash");
            return -1;
        }
        ok_e = kdf_counter_hmac_sha256_ossl3(session_key, session_key_len,
                                             label311_enc, sizeof(label311_enc),
                                             preauth_hash, SMB2_PREAUTH_HASH_SIZE,
                                             enc_key_out, key_len);
        ok_d = kdf_counter_hmac_sha256_ossl3(session_key, session_key_len,
                                             label311_dec, sizeof(label311_dec),
                                             preauth_hash, SMB2_PREAUTH_HASH_SIZE,
                                             dec_key_out, key_len);
    } else {
        /* SMB 3.0 / 3.0.2 */
        ok_e = kdf_counter_hmac_sha256_ossl3(session_key, session_key_len,
                                             label30, sizeof(label30),
                                             (const uint8_t *) ctx30_enc, sizeof(ctx30_enc),
                                             enc_key_out, key_len);
        ok_d = kdf_counter_hmac_sha256_ossl3(session_key, session_key_len,
                                             label30, sizeof(label30),
                                             (const uint8_t *) ctx30_dec, sizeof(ctx30_dec),
                                             dec_key_out, key_len);
    }

    if (!ok_e || !ok_d) {
        chimera_smb_error("SMB3 encryption key derivation failed");
        return -1;
    }

    *key_len_out = key_len;
    return 0;
} /* chimera_smb_derive_encryption_keys */

/* Lay the 64-bit message counter little-endian into the low bytes of the
 * cipher nonce; remaining bytes stay zero (MS-SMB2 §3.1.4.3). */
static void
smb_build_nonce(
    uint8_t *nonce16,
    int      nonce_len,
    uint64_t counter)
{
    memset(nonce16, 0, 16);
    for (int i = 0; i < 8 && i < nonce_len; i++) {
        nonce16[i] = (uint8_t) (counter >> (8 * i));
    }
} /* smb_build_nonce */

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
    struct evpl_iovec              *out_iov)
{
    struct smb2_transform_header *th;
    struct evpl_iovec_cursor      cursor;
    EVP_CIPHER                   *cipher;
    EVP_CIPHER_CTX               *c = ctx->cctx;
    uint8_t                      *out, *ct;
    uint8_t                       nonce[16];
    size_t                        ck_len;
    int                           nonce_len, is_ccm, outl, total;

    cipher = smb_cipher_for_id(ctx, cipher_id, &ck_len, &nonce_len, &is_ccm);

    if (!cipher || ck_len != key_len) {
        chimera_smb_error("Invalid cipher/key for SMB3 encryption (id 0x%x)", cipher_id);
        return -1;
    }

    total = transport_hdr_len + (int) sizeof(*th) + plain_len;

    if (evpl_iovec_alloc(evpl, total, 8, 1, 0, out_iov) < 1) {
        chimera_smb_error("Failed to allocate SMB3 encryption output buffer");
        return -1;
    }

    out = evpl_iovec_data(out_iov);
    th  = (struct smb2_transform_header *) (out + transport_hdr_len);
    ct  = out + transport_hdr_len + sizeof(*th);

    /* Gather the plaintext SMB2 message (skipping the transport framing) into
     * the output ciphertext region; AEAD encrypts it in place below. */
    evpl_iovec_cursor_init(&cursor, plain_iov, plain_niov);
    evpl_iovec_cursor_skip(&cursor, transport_hdr_len);
    evpl_iovec_cursor_copy(&cursor, ct, plain_len);

    smb_build_nonce(nonce, nonce_len, nonce_counter);

    /* Build the transform header (the Signature field is the AEAD tag, filled
     * after encryption; bytes [20..52) are the AEAD associated data). */
    static const uint8_t proto[4] = SMB2_TRANSFORM_PROTO_ID;
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
            /* CCM: declare total plaintext length, then AAD length, up front. */
            EVP_EncryptUpdate(c, NULL, &outl, NULL, plain_len) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl, ((uint8_t *) th) + SMB2_TRANSFORM_AAD_OFFSET,
                              SMB2_TRANSFORM_AAD_SIZE) != 1 ||
            EVP_EncryptUpdate(c, ct, &outl, ct, plain_len) != 1 ||
            EVP_EncryptFinal_ex(c, ct + outl, &outl) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_GET_TAG, 16, th->signature) != 1) {
            goto err;
        }
    } else {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_EncryptInit_ex(c, NULL, NULL, key, nonce) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl, ((uint8_t *) th) + SMB2_TRANSFORM_AAD_OFFSET,
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
    chimera_smb_error("SMB3 encryption failed (cipher id 0x%x)", cipher_id);
    evpl_iovec_release(evpl, out_iov);
    return -1;
} /* chimera_smb_encrypt_compound */

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
    int                            *plain_len_out)
{
    struct smb2_transform_header th;
    static const uint8_t         proto[4] = SMB2_TRANSFORM_PROTO_ID;
    EVP_CIPHER                  *cipher;
    EVP_CIPHER_CTX              *c = ctx->cctx;
    uint8_t                     *pt;
    size_t                       ck_len;
    int                          nonce_len, is_ccm, outl, ct_len;

    cipher = smb_cipher_for_id(ctx, cipher_id, &ck_len, &nonce_len, &is_ccm);

    if (!cipher || ck_len != key_len) {
        chimera_smb_error("Invalid cipher/key for SMB3 decryption (id 0x%x)", cipher_id);
        return -1;
    }

    if (length < (int) sizeof(th)) {
        chimera_smb_error("Truncated SMB3 transform message (%d bytes)", length);
        return -1;
    }

    evpl_iovec_cursor_copy(cursor, &th, sizeof(th));

    if (memcmp(th.protocol_id, proto, 4) != 0) {
        chimera_smb_error("Invalid SMB3 transform protocol id");
        return -1;
    }

    ct_len = (int) th.original_message_size;

    if (ct_len < (int) sizeof(struct smb2_header) ||
        ct_len > length - (int) sizeof(th)) {
        chimera_smb_error("Invalid SMB3 transform original_message_size %d", ct_len);
        return -1;
    }

    if (evpl_iovec_alloc(evpl, ct_len, 8, 1, 0, plain_out) < 1) {
        chimera_smb_error("Failed to allocate SMB3 decryption buffer");
        return -1;
    }

    pt = evpl_iovec_data(plain_out);

    /* Gather ciphertext into the output buffer; AEAD decrypts it in place. */
    evpl_iovec_cursor_copy(cursor, pt, ct_len);

    if (EVP_DecryptInit_ex(c, cipher, NULL, NULL, NULL) != 1) {
        goto err;
    }

    if (is_ccm) {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_TAG, 16, th.signature) != 1 ||
            EVP_DecryptInit_ex(c, NULL, NULL, key, th.nonce) != 1 ||
            EVP_DecryptUpdate(c, NULL, &outl, NULL, ct_len) != 1 ||
            EVP_DecryptUpdate(c, NULL, &outl, ((uint8_t *) &th) + SMB2_TRANSFORM_AAD_OFFSET,
                              SMB2_TRANSFORM_AAD_SIZE) != 1) {
            goto err;
        }
        /* For CCM the ciphertext-processing update returns <=0 on tag failure. */
        if (EVP_DecryptUpdate(c, pt, &outl, pt, ct_len) <= 0) {
            goto err;
        }
    } else {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_DecryptInit_ex(c, NULL, NULL, key, th.nonce) != 1 ||
            EVP_DecryptUpdate(c, NULL, &outl, ((uint8_t *) &th) + SMB2_TRANSFORM_AAD_OFFSET,
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
    chimera_smb_error("SMB3 decryption / tag verification failed (cipher id 0x%x)", cipher_id);
    evpl_iovec_release(evpl, plain_out);
    return -1;
} /* chimera_smb_decrypt_message */
