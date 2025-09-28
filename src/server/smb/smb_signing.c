// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <openssl/evp.h>
#include <openssl/core_names.h>   // OSSL_MAC_PARAM_* names
#include <openssl/params.h>

#include "common/evpl_iovec_cursor.h"
#include "smb_internal.h"
/*
 * Basic: HMAC-SHA256 over cursor input
 * Copies first 16 bytes of the HMAC into out_sig16.
 */
static inline int
chimera_smb_request_hmac_sha256(
    struct smb2_header       *hdr,
    struct evpl_iovec_cursor *cursor,
    int                       length,
    const uint8_t            *key,
    size_t                    keylen,
    uint8_t                  *out_sig16)
{
    int           ok     = -1, chunk, left = length;
    size_t        maclen = 0;
    unsigned char macbuf[32];

    EVP_MAC      *mac = EVP_MAC_fetch(NULL, "HMAC", NULL);

    if (!mac) {
        return ok;
    }

    EVP_MAC_CTX  *mctx = EVP_MAC_CTX_new(mac);

    if (!mctx) {
        EVP_MAC_free(mac);
        return ok;
    }

    OSSL_PARAM    params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, (char *) "SHA256", 0),
        OSSL_PARAM_construct_end()
    };

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
    EVP_MAC_free(mac);
    return ok;
}  // evpl_iovec_cursor_hmac_sha256

int
chimera_smb_verify_signature(
    struct chimera_smb_request *request,
    struct evpl_iovec_cursor   *cursor,
    int                         length)
{
    struct chimera_smb_conn    *conn    = request->compound->conn;
    struct chimera_smb_session *session = request->session;
    uint8_t                     signature[16];
    uint8_t                     calculated[16];
    char                        recv_sig[80];
    char                        calc_sig[80];
    int                         rc;

    memcpy(&signature, &request->smb2_hdr.signature, sizeof(signature));
    memset(request->smb2_hdr.signature, 0, sizeof(request->smb2_hdr.signature));

    switch (conn->dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            rc = chimera_smb_request_hmac_sha256(
                &request->smb2_hdr,
                cursor,
                length,
                session->session_key,
                session->session_key_len,
                calculated);

            if (unlikely(rc != 0)) {
                chimera_smb_error("Failed to calculate signature");
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
chimera_smb_sign_request(
    struct chimera_smb_request *request,
    struct evpl_iovec_cursor   *cursor,
    int                         length)
{
    int ok = -1;

    return ok;

} /* chimera_smb_sign_request */

int
chimera_smb_sign_compound(
    struct chimera_smb_compound *compound,
    struct evpl_iovec           *iov,
    int                          niov,
    int                          length)
{
    struct chimera_smb_conn    *conn = compound->conn;
    struct chimera_smb_session *session;
    struct chimera_smb_request *request;
    struct evpl_iovec_cursor    cursor;
    struct smb2_header         *hdr;
    int                         i, rc;
    int                         left = length, payload_length;
    uint8_t                     signature[16];

    evpl_iovec_cursor_init(&cursor, iov, niov);
    evpl_iovec_cursor_skip(&cursor, sizeof(struct netbios_header));

    left -= sizeof(struct netbios_header);

    for (i = 0; i < compound->num_requests && left; i++) {

        request = compound->requests[i];
        session = request->session;

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

            switch (conn->dialect) {
                case SMB2_DIALECT_2_0_2:
                case SMB2_DIALECT_2_1:

                    rc = chimera_smb_request_hmac_sha256(
                        hdr,
                        &cursor,
                        payload_length,
                        session->session_key,
                        session->session_key_len,
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
        }

        evpl_iovec_cursor_skip(&cursor, payload_length);
        left -= payload_length;
    }

    chimera_smb_abort_if(left, "Left is not 0 after signing compound");

    return 0;
} /* chimera_smb_sign_compound */