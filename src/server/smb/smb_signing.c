// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <openssl/evp.h>
#include <openssl/core_names.h>
#include <openssl/params.h>
#include <openssl/kdf.h>

#include "smb_signing.h"
#include "common/evpl_iovec_cursor.h"
#include "smb_internal.h"
#include "smb2.h"

struct chimera_smb_signing_ctx {
    EVP_MAC *hmac_mac;
    EVP_MAC *cmac_mac;
};

struct chimera_smb_signing_ctx *
chimera_smb_signing_ctx_create(void)
{
    struct chimera_smb_signing_ctx *ctx = calloc(1, sizeof(struct chimera_smb_signing_ctx));

    if (!ctx) {
        return NULL;
    }

    ctx->hmac_mac = EVP_MAC_fetch(NULL, "HMAC", NULL);

    chimera_smb_abort_if(!ctx->hmac_mac, "Failed to fetch HMAC MAC");

    ctx->cmac_mac = EVP_MAC_fetch(NULL, "CMAC", NULL);

    chimera_smb_abort_if(!ctx->cmac_mac, "Failed to fetch CMAC MAC");

    return ctx;
} /* chimera_smb_signing_ctx_new */

void
chimera_smb_signing_ctx_destroy(struct chimera_smb_signing_ctx *ctx)
{
    EVP_MAC_free(ctx->hmac_mac);
    EVP_MAC_free(ctx->cmac_mac);
    free(ctx);
} /* chimera_smb_signing_ctx_destroy */


/*
 * SP800-108 KDF (Counter mode) with HMAC-SHA256 using OpenSSL 3.x EVP_KDF "KBKDF".
 * out_len is typically 16 for SMB signing keys (128 bits), but any length is allowed.
 *
 * Notes:
 *  - 'label' and 'context' are raw byte strings. If your protocol (e.g., SMB 3.0)
 *    requires the trailing NUL to be included, pass label_len/context_len INCLUDING '\0'.
 *  - For SMB 3.1.1 signing key: label = "SMBSigningKey" (no NUL), context = PreauthHash (SHA-512), 64 bytes.
 *  - For SMB 3.0/3.0.2 signing key: label = "SMB2AESCMAC\0", context = "SmbSign\0".
 */
int
kdf_counter_hmac_sha256_ossl3(
    const uint8_t *key,
    size_t         key_len,
    const void    *label,
    size_t         label_len,               /* pass NUL if your spec says so */
    const uint8_t *context,
    size_t         ctx_len,
    uint8_t       *out,
    size_t         out_len)
{
    int          ok = 0;

    EVP_KDF     *kdf = EVP_KDF_fetch(NULL, "KBKDF", NULL);

    if (!kdf) {
        return 0;
    }

    EVP_KDF_CTX *kctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!kctx) {
        return 0;
    }

/* KBKDF parameters */
    OSSL_PARAM   params[10];
    size_t       n       = 0;
    const char  *mode    = "counter";
    const char  *mac     = "HMAC";
    const char  *digest  = "SHA256";
    int          use_l   = 1; /* include [L]2 */
    int          use_sep = 1; /* include 0x00 separator between label and context */

    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_MODE,   (char *) mode,   0);
    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_MAC,    (char *) mac,    0);
    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, (char *) digest, 0);
    params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY,   (void *) key,    key_len);

    if (label && label_len) {
        params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, (void *) label, label_len);
    }
    if (context && ctx_len) {
        params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, (void *) context, ctx_len);
    }

    params[n++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_KBKDF_USE_L,            &use_l);
    params[n++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_KBKDF_USE_SEPARATOR,    &use_sep);
    params[n++] = OSSL_PARAM_construct_end();

    if (EVP_KDF_derive(kctx, out, out_len, params) == 1) {
        ok = 1;
    }

    EVP_KDF_CTX_free(kctx);
    return ok;
} /* kdf_counter_hmac_sha256_ossl3 */

int
chimera_smb_derive_signing_key(
    int    dialect,
    void  *output,
    void  *session_key,
    size_t session_key_len)
{
    static const char label30[] = "SMB2AESCMAC";  /* include NUL per spec */
    static const char ctx30[]   = "SmbSign";      /* include NUL per spec */

    switch (dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            if (session_key_len >= 16) {
                memcpy(output, session_key, 16);
                return 0;
            } else {
                chimera_smb_error("SMB2 session key length %d is not 16 bytes", session_key_len);
                return -1;
            }
        case SMB2_DIALECT_3_0:
            return kdf_counter_hmac_sha256_ossl3(session_key, session_key_len,
                                                 label30, sizeof(label30), /* includes '\0' */
                                                 (const uint8_t *) ctx30, sizeof(ctx30), /* includes '\0' */
                                                 output, 16);
        default:
            return -1;
    } /* switch */
} /* chimera_smb_derive_smb3_signing_key */


/*
 * Basic: HMAC-SHA256 over cursor input
 * Copies first 16 bytes of the HMAC into out_sig16.
 */
static inline int
chimera_smb_request_hmac_sha256(
    struct chimera_smb_signing_ctx *ctx,
    struct smb2_header             *hdr,
    struct evpl_iovec_cursor       *cursor,
    int                             length,
    const uint8_t                  *key,
    size_t                          keylen,
    uint8_t                        *out_sig16)
{
    int           ok     = -1, chunk, left = length;
    size_t        maclen = 0;
    unsigned char macbuf[32];
    EVP_MAC_CTX  *mctx     = NULL;
    OSSL_PARAM    params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, (char *) "SHA256", 0),
        OSSL_PARAM_construct_end()
    };

    mctx = EVP_MAC_CTX_new(ctx->hmac_mac);
    if (!mctx) {
        goto done;
    }

    if (EVP_MAC_init(mctx, key, keylen, params) != 1) {
        goto done;
    }

    EVP_MAC_update(mctx, (uint8_t *) hdr, sizeof(*hdr));

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        if (EVP_MAC_update(mctx, cursor->iov->data + cursor->offset, chunk) != 1) {
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

    if (left) {
        goto done;
    }

    if (EVP_MAC_final(mctx, macbuf, &maclen, sizeof(macbuf)) != 1) {
        goto done;
    }
    if (maclen < 16) {
        goto done;
    }

    memcpy(out_sig16, macbuf, 16);

    ok = 0;

 done:
    EVP_MAC_CTX_free(mctx);
    return ok;
}  // evpl_iovec_cursor_hmac_sha256


/*
 * Basic: CMAC-AES-128-CBC over cursor input
 * Copies first 16 bytes of the HMAC into out_sig16.
 */
static inline int
chimera_smb_request_cmac_aes_128_cbc(
    struct chimera_smb_signing_ctx *ctx,
    struct smb2_header             *hdr,
    struct evpl_iovec_cursor       *cursor,
    int                             length,
    const uint8_t                  *key,
    size_t                          keylen,
    uint8_t                        *out_sig16)
{
    int          ok     = -1, chunk, left = length;
    size_t       maclen   = 0;
    EVP_MAC_CTX *mctx     = NULL;
    OSSL_PARAM   params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_CIPHER, (char *) "AES-128-CBC", 0),
        OSSL_PARAM_construct_end()
    };

    mctx = EVP_MAC_CTX_new(ctx->cmac_mac);
    if (!mctx) {
        chimera_smb_error("Failed to allocate CMAC-AES-128-CBC context");
        goto done;
    }

    if (EVP_MAC_init(mctx, key, keylen, params) != 1) {
        chimera_smb_error("Failed to initialize CMAC-AES-128-CBC context");
        goto done;
    }

    EVP_MAC_update(mctx, (uint8_t *) hdr, sizeof(*hdr));

    while (left && cursor->niov) {

        chunk = cursor->iov->length - cursor->offset;

        if (left < chunk) {
            chunk = left;
        }

        if (EVP_MAC_update(mctx, cursor->iov->data + cursor->offset, chunk) != 1) {
            chimera_smb_error("Failed to update CMAC-AES-128-CBC context");
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

    if (left) {
        chimera_smb_error("Left is not 0 after updating CMAC-AES-128-CBC context");
        goto done;
    }

    if (EVP_MAC_final(mctx, out_sig16, &maclen, 16) != 1) {
        chimera_smb_error("Failed to finalize CMAC-AES-128-CBC context");
        goto done;
    }

    ok = (maclen != 16);

 done:
    EVP_MAC_CTX_free(mctx);
    return ok;
}   // evpl_iovec_cursor_cmac_aes_128_cbc

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

    switch (conn->dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            rc = chimera_smb_request_hmac_sha256(
                ctx,
                &request->smb2_hdr,
                cursor,
                length,
                signing_key,
                16,
                calculated);

            if (unlikely(rc != 0)) {
                chimera_smb_error("Failed to calculate sha256 signature");
                return rc;
            }
            break;
        case SMB2_DIALECT_3_0:
            rc = chimera_smb_request_cmac_aes_128_cbc(
                ctx,
                &request->smb2_hdr,
                cursor,
                length,
                signing_key,
                16,
                calculated);

            if (unlikely(rc != 0)) {
                chimera_smb_error("Failed to calculate cmac_aes_128_cbc signature");
                return rc;
            }
            break;
        default:
            chimera_smb_error("Signed messages with unsupported dialect %x", conn->dialect);
            return -1;
    } /* switch */

    if (unlikely(memcmp(signature, calculated, sizeof(signature)) != 0)) {
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

            switch (conn->dialect) {
                case SMB2_DIALECT_2_0_2:
                case SMB2_DIALECT_2_1:

                    rc = chimera_smb_request_hmac_sha256(
                        ctx,
                        hdr,
                        &cursor,
                        payload_length,
                        session_handle->signing_key,
                        16,
                        signature);

                    if (unlikely(rc != 0)) {
                        chimera_smb_error("Failed to calculate signature");
                        return rc;
                    }
                    break;
                case SMB2_DIALECT_3_0:
                    rc = chimera_smb_request_cmac_aes_128_cbc(
                        ctx,
                        hdr,
                        &cursor,
                        payload_length,
                        session_handle->signing_key,
                        16,
                        signature);

                    if (unlikely(rc != 0)) {
                        chimera_smb_error("Failed to calculate signature");
                        return rc;
                    }
                    break;
                default:
                    chimera_smb_error("Unsupported dialect for signing %x", conn->dialect);
                    return -1;
            } /* switch */

            memcpy(hdr->signature, signature, sizeof(signature));
        } else {
            evpl_iovec_cursor_skip(&cursor, payload_length);
        }
        left -= payload_length;
    }

    chimera_smb_abort_if(left, "Left is not 0 after signing compound");

    return 0;
} /* chimera_smb_sign_compound */

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
int
chimera_smb_sign_message(
    struct chimera_smb_signing_ctx *ctx,
    int                             dialect,
    const uint8_t                  *signing_key,
    uint8_t                        *smb2_buf,
    int                             smb2_len)
{
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
            rc = chimera_smb_request_hmac_sha256(ctx, hdr, &cursor,
                                                 body_len,
                                                 signing_key, 16, signature);
            break;
        case SMB2_DIALECT_3_0:
        case SMB2_DIALECT_3_0_2:
        case SMB2_DIALECT_3_1_1:
            rc = chimera_smb_request_cmac_aes_128_cbc(ctx, hdr, &cursor,
                                                      body_len,
                                                      signing_key, 16, signature);
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
