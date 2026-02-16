// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <crypt.h>

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/params.h>
#include <openssl/core_names.h>
#include <jansson.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "vfs/vfs.h"
#include "vfs/vfs_user_cache.h"
#ifdef HAVE_WBCLIENT
#include <wbclient.h>
#endif /* ifdef HAVE_WBCLIENT */
#include "rest_internal.h"
#include "rest_auth.h"

/* ========== base64url helpers (RFC 4648 Section 5, no padding) ========== */

static int
base64url_encode(
    const unsigned char *in,
    int                  in_len,
    char                *out,
    int                  out_size)
{
    static const char table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    int               i, j = 0;

    for (i = 0; i + 2 < in_len; i += 3) {
        if (j + 4 > out_size) {
            return -1;
        }
        out[j++] = table[(in[i] >> 2) & 0x3f];
        out[j++] = table[((in[i] & 0x03) << 4) | ((in[i + 1] >> 4) & 0x0f)];
        out[j++] = table[((in[i + 1] & 0x0f) << 2) |
                         ((in[i + 2] >> 6) & 0x03)];
        out[j++] = table[in[i + 2] & 0x3f];
    }

    if (i < in_len) {
        if (j + 3 > out_size) {
            return -1;
        }
        out[j++] = table[(in[i] >> 2) & 0x3f];
        if (i + 1 < in_len) {
            out[j++] = table[((in[i] & 0x03) << 4) |
                             ((in[i + 1] >> 4) & 0x0f)];
            out[j++] = table[(in[i + 1] & 0x0f) << 2];
        } else {
            out[j++] = table[(in[i] & 0x03) << 4];
        }
    }

    if (j >= out_size) {
        return -1;
    }
    out[j] = '\0';
    return j;
} /* base64url_encode */

static int
base64url_decode_char(char c)
{
    if (c >= 'A' && c <= 'Z') {
        return c - 'A';
    }
    if (c >= 'a' && c <= 'z') {
        return c - 'a' + 26;
    }
    if (c >= '0' && c <= '9') {
        return c - '0' + 52;
    }
    if (c == '-') {
        return 62;
    }
    if (c == '_') {
        return 63;
    }
    return -1;
} /* base64url_decode_char */

static int
base64url_decode(
    const char    *in,
    int            in_len,
    unsigned char *out,
    int            out_size)
{
    int i, j = 0, v;
    int buf = 0, bits = 0;

    for (i = 0; i < in_len; i++) {
        v = base64url_decode_char(in[i]);
        if (v < 0) {
            return -1;
        }
        buf   = (buf << 6) | v;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            if (j >= out_size) {
                return -1;
            }
            out[j++] = (buf >> bits) & 0xff;
        }
    }

    return j;
} /* base64url_decode */

/* ========== HMAC-SHA256 ========== */

static int
hmac_sha256(
    const unsigned char *key,
    int                  key_len,
    const unsigned char *data,
    int                  data_len,
    unsigned char       *out,
    size_t              *out_len)
{
    EVP_MAC     *mac     = NULL;
    EVP_MAC_CTX *mac_ctx = NULL;
    int          rc      = -1;

    OSSL_PARAM   params[] = {
        OSSL_PARAM_construct_utf8_string(
            OSSL_MAC_PARAM_DIGEST,                                                                      (char *)
            "SHA256", 0),
        OSSL_PARAM_construct_end()
    };

    mac = EVP_MAC_fetch(NULL, "HMAC", NULL);
    if (!mac) {
        goto done;
    }

    mac_ctx = EVP_MAC_CTX_new(mac);
    if (!mac_ctx) {
        goto done;
    }

    if (EVP_MAC_init(mac_ctx, key, key_len, params) != 1) {
        goto done;
    }

    if (EVP_MAC_update(mac_ctx, data, data_len) != 1) {
        goto done;
    }

    if (EVP_MAC_final(mac_ctx, out, out_len, 32) != 1) {
        goto done;
    }

    rc = 0;

 done:
    if (mac_ctx) {
        EVP_MAC_CTX_free(mac_ctx);
    }
    if (mac) {
        EVP_MAC_free(mac);
    }
    return rc;
} /* hmac_sha256 */

/* ========== Secret init ========== */

void
chimera_rest_auth_init_secret(struct chimera_rest_server *rest)
{
    RAND_bytes(rest->jwt_secret, CHIMERA_REST_JWT_SECRET_LEN);
    chimera_rest_info("JWT authentication secret initialized");
} /* chimera_rest_auth_init_secret */

/* ========== Credential validation ========== */

int
chimera_rest_auth_validate_credentials(
    struct chimera_rest_server     *rest,
    const char                     *username,
    const char                     *password,
    struct chimera_rest_jwt_claims *claims)
{
    const struct chimera_vfs_user *user;
    time_t                         now;

    /* Try local user first */
    user = chimera_server_get_user(rest->server, username);

    if (user && user->password[0]) {
        struct crypt_data cdata;
        char             *result;

        memset(&cdata, 0, sizeof(cdata));
        result = crypt_r(password, user->password, &cdata);

        if (result && strcmp(result, user->password) == 0) {
            now = time(NULL);
            strncpy(claims->sub, username, sizeof(claims->sub) - 1);
            claims->sub[sizeof(claims->sub) - 1] = '\0';
            claims->iat                          = now;
            claims->exp                          = now + CHIMERA_REST_JWT_EXPIRY;
            return 0;
        }
    }

#ifdef HAVE_WBCLIENT
    /* Winbind fallback */
    if (rest->winbind_enabled) {
        wbcErr wbc_err;

        wbc_err = wbcAuthenticateUser(username, password);

        if (wbc_err == WBC_ERR_SUCCESS) {
            now = time(NULL);
            strncpy(claims->sub, username, sizeof(claims->sub) - 1);
            claims->sub[sizeof(claims->sub) - 1] = '\0';
            claims->iat                          = now;
            claims->exp                          = now + CHIMERA_REST_JWT_EXPIRY;
            return 0;
        }
    }
#endif /* ifdef HAVE_WBCLIENT */

    return -1;
} /* chimera_rest_auth_validate_credentials */

/* ========== JWT create ========== */

int
chimera_rest_jwt_create(
    struct chimera_rest_server           *rest,
    const struct chimera_rest_jwt_claims *claims,
    char                                 *token_out,
    int                                   token_out_size)
{
    /* Header: {"alg":"HS256","typ":"JWT"} */
    static const char header_json[] =
        "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

    char              payload_json[512];
    char              header_b64[128];
    char              payload_b64[512];
    char              signing_input[1024];
    unsigned char     sig[32];
    size_t            sig_len = 32;
    char              sig_b64[64];
    int               h_len, p_len, s_len, si_len;

    snprintf(payload_json, sizeof(payload_json),
             "{\"sub\":\"%s\",\"iat\":%ld,\"exp\":%ld}",
             claims->sub, (long) claims->iat, (long) claims->exp);

    h_len = base64url_encode((const unsigned char *) header_json,
                             strlen(header_json),
                             header_b64, sizeof(header_b64));
    if (h_len < 0) {
        return -1;
    }

    p_len = base64url_encode((const unsigned char *) payload_json,
                             strlen(payload_json),
                             payload_b64, sizeof(payload_b64));
    if (p_len < 0) {
        return -1;
    }

    si_len = snprintf(signing_input, sizeof(signing_input),
                      "%s.%s", header_b64, payload_b64);

    if (hmac_sha256(rest->jwt_secret, CHIMERA_REST_JWT_SECRET_LEN,
                    (const unsigned char *) signing_input, si_len,
                    sig, &sig_len) != 0) {
        return -1;
    }

    s_len = base64url_encode(sig, sig_len, sig_b64, sizeof(sig_b64));
    if (s_len < 0) {
        return -1;
    }

    if (snprintf(token_out, token_out_size, "%s.%s",
                 signing_input, sig_b64) >= token_out_size) {
        return -1;
    }

    return 0;
} /* chimera_rest_jwt_create */

/* ========== JWT verify ========== */

int
chimera_rest_jwt_verify(
    struct chimera_rest_server     *rest,
    const char                     *token,
    struct chimera_rest_jwt_claims *claims)
{
    const char   *first_dot, *second_dot;
    int           si_len;
    unsigned char expected_sig[32];
    size_t        expected_sig_len = 32;
    unsigned char actual_sig[32];
    int           actual_sig_len;
    unsigned char payload_raw[512];
    int           payload_raw_len;
    json_t       *root;
    json_error_t  error;
    const char   *sub;
    time_t        now;

    /* Find the two dots */
    first_dot = strchr(token, '.');
    if (!first_dot) {
        return -1;
    }

    second_dot = strchr(first_dot + 1, '.');
    if (!second_dot) {
        return -1;
    }

    /* Verify no additional dots */
    if (strchr(second_dot + 1, '.')) {
        return -1;
    }

    /* signing_input = header.payload (everything before second dot) */
    si_len = second_dot - token;

    if (hmac_sha256(rest->jwt_secret, CHIMERA_REST_JWT_SECRET_LEN,
                    (const unsigned char *) token, si_len,
                    expected_sig, &expected_sig_len) != 0) {
        return -1;
    }

    /* Decode actual signature */
    actual_sig_len = base64url_decode(second_dot + 1, strlen(second_dot + 1),
                                      actual_sig, sizeof(actual_sig));
    if (actual_sig_len != (int) expected_sig_len) {
        return -1;
    }

    /* Constant-time comparison */
    if (CRYPTO_memcmp(expected_sig, actual_sig, expected_sig_len) != 0) {
        return -1;
    }

    /* Decode payload */
    payload_raw_len = base64url_decode(first_dot + 1,
                                       second_dot - first_dot - 1,
                                       payload_raw, sizeof(payload_raw) - 1);
    if (payload_raw_len < 0) {
        return -1;
    }
    payload_raw[payload_raw_len] = '\0';

    /* Parse payload JSON */
    root = json_loadb((const char *) payload_raw, payload_raw_len, 0, &error);
    if (!root) {
        return -1;
    }

    sub = json_string_value(json_object_get(root, "sub"));
    if (!sub) {
        json_decref(root);
        return -1;
    }

    strncpy(claims->sub, sub, sizeof(claims->sub) - 1);
    claims->sub[sizeof(claims->sub) - 1] = '\0';
    claims->iat                          = json_integer_value(json_object_get(root, "iat"));
    claims->exp                          = json_integer_value(json_object_get(root, "exp"));

    json_decref(root);

    /* Check expiration */
    now = time(NULL);
    if (now >= claims->exp) {
        return -1;
    }

    return 0;
} /* chimera_rest_jwt_verify */

/* ========== Bearer token check ========== */

int
chimera_rest_auth_check_bearer(
    struct chimera_rest_server     *rest,
    struct evpl_http_request       *request,
    struct chimera_rest_jwt_claims *claims)
{
    const char *auth_header;

    auth_header = evpl_http_request_header(request, "Authorization");
    if (!auth_header) {
        return -1;
    }

    if (strncmp(auth_header, "Bearer ", 7) != 0) {
        return -1;
    }

    return chimera_rest_jwt_verify(rest, auth_header + 7, claims);
} /* chimera_rest_auth_check_bearer */

/* ========== Login handler ========== */

void
chimera_rest_handle_auth_login(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t                        *root;
    json_error_t                   error;
    const char                    *username;
    const char                    *password;
    struct chimera_rest_jwt_claims claims;
    char                           token[2048];
    char                           response[2560];

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        chimera_rest_send_json_response(evpl, request, 400,
                                        "{\"error\":\"Bad Request\","
                                        "\"message\":\"Invalid JSON\"}");
        return;
    }

    username = json_string_value(json_object_get(root, "username"));
    password = json_string_value(json_object_get(root, "password"));

    if (!username || !password) {
        json_decref(root);
        chimera_rest_send_json_response(evpl, request, 400,
                                        "{\"error\":\"Bad Request\","
                                        "\"message\":\"Missing username or "
                                        "password\"}");
        return;
    }

    if (chimera_rest_auth_validate_credentials(
            thread->shared, username, password, &claims) != 0) {
        json_decref(root);
        chimera_rest_send_json_response(evpl, request, 401,
                                        "{\"error\":\"Unauthorized\","
                                        "\"message\":\"Invalid credentials\"}");
        return;
    }

    json_decref(root);

    if (chimera_rest_jwt_create(thread->shared, &claims,
                                token, sizeof(token)) != 0) {
        chimera_rest_send_json_response(evpl, request, 500,
                                        "{\"error\":\"Internal Server Error\","
                                        "\"message\":\"Failed to create "
                                        "token\"}");
        return;
    }

    snprintf(response, sizeof(response),
             "{\"token\":\"%s\",\"expires_in\":%d}",
             token, CHIMERA_REST_JWT_EXPIRY);

    chimera_rest_send_json_response(evpl, request, 200, response);
} /* chimera_rest_handle_auth_login */
