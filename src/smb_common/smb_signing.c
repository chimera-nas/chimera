// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include "common/crypto.h"

#include "smb_signing.h"
#include "smb_common.h"
#include "common/evpl_iovec_cursor.h"
#include "smb2.h"

struct chimera_smb_signing_ctx {
    int reserved;
};

SYMBOL_EXPORT struct chimera_smb_signing_ctx *
chimera_smb_signing_ctx_create(void)
{
    return calloc(1, sizeof(struct chimera_smb_signing_ctx));
} /* chimera_smb_signing_ctx_new */

SYMBOL_EXPORT void
chimera_smb_signing_ctx_destroy(struct chimera_smb_signing_ctx *ctx)
{
    free(ctx);
} /* chimera_smb_signing_ctx_destroy */


/*
 * SP800-108 KDF (Counter mode) with HMAC-SHA256.
 * out_len is typically 16 for SMB signing keys (128 bits), but any length is allowed.
 *
 * Notes:
 *  - 'label' and 'context' are raw byte strings. If your protocol (e.g., SMB 3.0)
 *    requires the trailing NUL to be included, pass label_len/context_len INCLUDING '\0'.
 *  - For SMB 3.1.1 signing key: label = "SMBSigningKey" (including NUL), context = PreauthHash (SHA-512), 64 bytes.
 *  - For SMB 3.0/3.0.2 signing key: label = "SMB2AESCMAC\0", context = "SmbSign\0".
 */
SYMBOL_EXPORT int
chimera_smb_kbkdf(
    const uint8_t *key,
    size_t         key_len,
    const void    *label,
    size_t         label_len,               /* pass NUL if your spec says so */
    const uint8_t *context,
    size_t         ctx_len,
    uint8_t       *out,
    size_t         out_len)
{
    return chimera_crypto_kdf(key, key_len, label, label_len, context, ctx_len, out, out_len);
} /* kdf_counter_hmac_sha256_ossl3 */

/* Extend an SMB 3.1.1 preauth-integrity hash: hash = SHA512(hash || msg).
 * `hash` is the running 64-byte value, updated in place. */
SYMBOL_EXPORT void
chimera_smb_preauth_extend(
    uint8_t    *hash,
    const void *msg,
    uint32_t    msg_len)
{
    struct chimera_crypto_hash *md = chimera_crypto_hash_new(CHIMERA_CRYPTO_SHA512, NULL, 0, NULL, 0);

    chimera_smb2_abort_if(!md || !chimera_crypto_update(md, hash, SMB2_PREAUTH_HASH_SIZE) ||
                          !chimera_crypto_update(md, msg, msg_len) || !chimera_crypto_final(md, hash,
                                                                                            SMB2_PREAUTH_HASH_SIZE),
                          "SHA-512 preauth hash update failed");
    chimera_crypto_hash_free(md);
} /* chimera_smb_preauth_extend */

SYMBOL_EXPORT int
chimera_smb_derive_signing_key(
    int            dialect,
    void          *output,
    void          *session_key,
    size_t         session_key_len,
    const uint8_t *preauth_hash)
{
    static const char label30[]  = "SMB2AESCMAC";   /* include NUL per spec */
    static const char ctx30[]    = "SmbSign";       /* include NUL per spec */
    static const char label311[] = "SMBSigningKey"; /* include NUL per spec */

    switch (dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            if (session_key_len >= 16) {
                memcpy(output, session_key, 16);
                return 0;
            } else {
                chimera_smb2_error("SMB2 session key length %d is not 16 bytes", session_key_len);
                return -1;
            }
        case SMB2_DIALECT_3_0:
        case SMB2_DIALECT_3_0_2:
            /* 3.0 and 3.0.2 share the SP800-108 signing-key derivation
             * (label "SMB2AESCMAC", context "SmbSign"). */
            return chimera_smb_kbkdf(session_key, session_key_len,
                                     /* both lengths include the '\0' */
                                     label30, sizeof(label30),
                                     (const uint8_t *) ctx30, sizeof(ctx30),
                                     output, 16);
        case SMB2_DIALECT_3_1_1:
            /* 3.1.1 binds the signing key to the preauth-integrity hash:
             * label "SMBSigningKey", context = PreauthIntegrityHashValue. */
            if (!preauth_hash) {
                chimera_smb2_error("SMB 3.1.1 signing key derivation without preauth hash");
                return -1;
            }
            /* Per the WPTS/Windows reference, the label includes its NUL
             * terminator ("SMBSigningKey\0"); the KDF helper then inserts the
             * SP800-108 0x00 separator before the PreauthIntegrityHashValue
             * context. Pass the full sizeof (label + NUL). */
            return chimera_smb_kbkdf(session_key, session_key_len,
                                     label311, sizeof(label311),
                                     preauth_hash, SMB2_PREAUTH_HASH_SIZE,
                                     output, 16);
        default:
            return -1;
    } /* switch */
} /* chimera_smb_derive_smb3_signing_key */


/* Sign the header and scatter/gather body without changing protocol framing. */
static int
chimera_smb_request_mac(
    enum chimera_crypto_algorithm algorithm,
    struct smb2_header           *hdr,
    struct evpl_iovec_cursor     *cursor,
    int                           length,
    const uint8_t                *key,
    size_t                        keylen,
    uint8_t                      *out_sig16)
{
    uint8_t                     iv[12] = { 0 }, mac[32];
    uint32_t                    high_bits = hdr->flags & SMB2_FLAGS_SERVER_TO_REDIR;
    struct chimera_crypto_hash *ctx;
    int                         left = length, ok = -1;

    if (length < 0) {
        return -1;
    }
    if (hdr->command == SMB2_CANCEL) {
        high_bits |= SMB2_FLAGS_ASYNC_COMMAND;
    }
    memcpy(iv, &hdr->message_id, 8);
    for (int i = 0; i < 4; i++) {
        iv[8 + i] = (uint8_t) (high_bits >> (8 * i));
    }
    ctx = chimera_crypto_hash_new(algorithm, key, keylen, iv, sizeof(iv));
    if (!ctx || !chimera_crypto_update(ctx, hdr, sizeof(*hdr))) {
        goto done;
    }
    while (left && cursor->niov) {
        int chunk = cursor->iov->length - cursor->offset;
        if (left < chunk) {
            chunk = left;
        }
        if (!chimera_crypto_update(ctx, (const uint8_t *) cursor->iov->data + cursor->offset, chunk)) {
            goto done;
        }
        left             -= chunk;
        cursor->offset   += chunk;
        cursor->consumed += chunk;
        if (cursor->offset == cursor->iov->length) {
            cursor->iov++;
            cursor->niov--;
            cursor->offset = 0;
        }
    }
    if (left || !chimera_crypto_final(ctx, mac, sizeof(mac))) {
        goto done;
    }
    memcpy(out_sig16, mac, 16);
    ok = 0;
 done:
    chimera_crypto_hash_free(ctx);
    return ok;
} /* chimera_smb_request_mac */

/*
 * Compute the SMB2 signature for a message using the algorithm negotiated for
 * the connection: HMAC-SHA256 for 2.x, AES-128-CMAC for 3.0/3.0.2, and the
 * SMB 3.1.1 negotiated signing algorithm (GMAC / CMAC / HMAC-SHA256) for 3.1.1.
 */
SYMBOL_EXPORT int
chimera_smb_compute_signature_alg(
    struct chimera_smb_signing_ctx *ctx,
    uint16_t                        dialect,
    uint16_t                        signing_alg,
    struct smb2_header             *hdr,
    struct evpl_iovec_cursor       *cursor,
    int                             length,
    const uint8_t                  *key,
    uint8_t                        *out_sig16)
{
    (void) ctx;
    switch (dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            return chimera_smb_request_mac(CHIMERA_CRYPTO_HMAC_SHA256, hdr, cursor, length, key, 16, out_sig16);
        case SMB2_DIALECT_3_0:
        case SMB2_DIALECT_3_0_2:
            return chimera_smb_request_mac(CHIMERA_CRYPTO_AES_CMAC, hdr, cursor, length, key, 16, out_sig16);
        case SMB2_DIALECT_3_1_1:
            switch (signing_alg) {
                case SMB2_SIGNING_AES_GMAC:
                    return chimera_smb_request_mac(CHIMERA_CRYPTO_AES_GMAC, hdr, cursor, length, key, 16, out_sig16);
                case SMB2_SIGNING_HMAC_SHA256:
                    return chimera_smb_request_mac(CHIMERA_CRYPTO_HMAC_SHA256, hdr, cursor, length, key, 16, out_sig16);
                default:
                    return chimera_smb_request_mac(CHIMERA_CRYPTO_AES_CMAC, hdr, cursor, length, key, 16, out_sig16);
            } /* switch */
        default:
            return -1;
    } /* switch */
} /* chimera_smb_compute_signature_alg */

/*
 * Sign a single contiguous SMB2 message in place.
 *
 * smb2_buf must point to the start of the SMB2 header (NOT the NetBIOS
 * header).  smb2_len is the total length of the SMB2 message
 * (header + body).  The function sets SMB2_FLAGS_SIGNED, zeroes the
 * signature field, computes the signature, and writes it back.
 *
 * Used for standalone async messages such as CHANGE_NOTIFY interim
 * (STATUS_PENDING) and final responses, which are not part of the
 * normal compound reply path.
 */
SYMBOL_EXPORT int
chimera_smb_sign_message(
    struct chimera_smb_signing_ctx *ctx,
    int                             dialect,
    int                             signing_alg,
    const uint8_t                  *signing_key,
    uint8_t                        *smb2_buf,
    int                             smb2_len)
{
    (void) ctx;
    struct smb2_header      *hdr = (struct smb2_header *) smb2_buf;
    struct evpl_iovec        body_iov;
    struct evpl_iovec_cursor cursor;
    uint8_t                  signature[16];
    int                      body_len = smb2_len - (int) sizeof(*hdr);
    int                      rc;

    if (body_len < 0) {
        return -1;
    }

    hdr->flags |= SMB2_FLAGS_SIGNED;
    memset(hdr->signature, 0, sizeof(hdr->signature));

    body_iov.data   = smb2_buf + sizeof(*hdr);
    body_iov.length = body_len;
    evpl_iovec_cursor_init(&cursor, &body_iov, 1);

    switch (dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            rc = chimera_smb_request_mac(CHIMERA_CRYPTO_HMAC_SHA256, hdr, &cursor,
                                         body_len,
                                         signing_key, 16, signature);
            break;
        case SMB2_DIALECT_3_0:
        case SMB2_DIALECT_3_0_2:
            rc = chimera_smb_request_mac(CHIMERA_CRYPTO_AES_CMAC, hdr, &cursor,
                                         body_len,
                                         signing_key, 16, signature);
            break;
        case SMB2_DIALECT_3_1_1:
            switch (signing_alg) {
                case SMB2_SIGNING_AES_GMAC:
                    rc = chimera_smb_request_mac(CHIMERA_CRYPTO_AES_GMAC, hdr, &cursor,
                                                 body_len,
                                                 signing_key, 16, signature);
                    break;
                case SMB2_SIGNING_HMAC_SHA256:
                    rc = chimera_smb_request_mac(CHIMERA_CRYPTO_HMAC_SHA256, hdr, &cursor,
                                                 body_len,
                                                 signing_key, 16, signature);
                    break;
                default:
                    rc = chimera_smb_request_mac(CHIMERA_CRYPTO_AES_CMAC, hdr, &cursor,
                                                 body_len,
                                                 signing_key, 16, signature);
                    break;
            } /* switch */
            break;
        default:
            return -1;
    } /* switch */

    if (rc != 0) {
        return rc;
    }

    memcpy(hdr->signature, signature, sizeof(signature));
    return 0;
} /* chimera_smb_sign_message */
