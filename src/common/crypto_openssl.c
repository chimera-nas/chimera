// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include <limits.h>
#include <stdlib.h>
#include <openssl/evp.h>
#include <openssl/core_names.h>
#include <openssl/kdf.h>
#include <openssl/provider.h>
#include <openssl/rand.h>
#include "crypto.h"

/* Retain the legacy provider for NTLM's MD4 and RC4 for the process lifetime.
* Loading a provider explicitly also requires loading the default provider. */
static CRYPTO_ONCE    legacy_once = CRYPTO_ONCE_STATIC_INIT;
static OSSL_PROVIDER *legacy_provider, *default_provider;
static void
load_legacy(void)
{
    default_provider = OSSL_PROVIDER_load(NULL, "default");
    legacy_provider  = OSSL_PROVIDER_load(NULL, "legacy");
} /* load_legacy */

/* Algorithm descriptions are immutable. Cache them once so packet signing
 * allocates only the per-message MAC state, as the SMB backend did before. */
static CRYPTO_ONCE mac_once = CRYPTO_ONCE_STATIC_INIT;
static EVP_MAC    *hmac_algorithm, *cmac_algorithm;
static void
load_macs(void)
{
    hmac_algorithm = EVP_MAC_fetch(NULL, "HMAC", NULL);
    cmac_algorithm = EVP_MAC_fetch(NULL, "CMAC", NULL);
} /* load_macs */

struct chimera_crypto_hash {
    EVP_MD_CTX     *digest;
    EVP_MAC_CTX    *mac;
    EVP_CIPHER_CTX *gmac;
    size_t          size;
    int             finished;
};
struct chimera_crypto_aead {
    EVP_CIPHER_CTX *ctx;
    EVP_CIPHER     *cipher[4];
};

struct chimera_crypto_hash *
chimera_crypto_hash_new(
    enum chimera_crypto_algorithm algorithm,
    const void                   *key,
    size_t                        key_len,
    const void                   *nonce,
    size_t                        nonce_len)
{
    struct chimera_crypto_hash *ctx    = calloc(1, sizeof(*ctx));
    const EVP_MD               *md     = NULL;
    const char                 *digest = NULL;
    EVP_MAC                    *mac;
    OSSL_PARAM                  params[2];

    if (!ctx || (!key && key_len) || !(ctx->size = chimera_crypto_size(algorithm))) {
        free(ctx);
        return NULL;
    }
    switch (algorithm) {
        case CHIMERA_CRYPTO_MD4:
            if (!CRYPTO_THREAD_run_once(&legacy_once, load_legacy) || !legacy_provider || !default_provider) {
                goto fail;
            }
            md                                  = EVP_md4(); break;
        case CHIMERA_CRYPTO_MD5: md             = EVP_md5(); break;
        case CHIMERA_CRYPTO_SHA1: md            = EVP_sha1(); break;
        case CHIMERA_CRYPTO_SHA256: md          = EVP_sha256(); break;
        case CHIMERA_CRYPTO_SHA512: md          = EVP_sha512(); break;
        case CHIMERA_CRYPTO_HMAC_MD5: digest    = "MD5"; break;
        case CHIMERA_CRYPTO_HMAC_SHA1: digest   = "SHA1"; break;
        case CHIMERA_CRYPTO_HMAC_SHA256: digest = "SHA256"; break;
        case CHIMERA_CRYPTO_AES_CMAC:
            if (key_len != 16) {
                goto fail;
            }
            break;
        case CHIMERA_CRYPTO_AES_GMAC:
            if (key_len != 16 || nonce_len != 12 || !nonce) {
                goto fail;
            }
            ctx->gmac = EVP_CIPHER_CTX_new();
            if (!ctx->gmac || EVP_EncryptInit_ex(ctx->gmac, EVP_aes_128_gcm(), NULL, key, nonce) != 1) {
                goto fail;
            }
            return ctx;
        default: goto fail;
    } /* switch */
    if (md) {
        ctx->digest = EVP_MD_CTX_new();
        if (!ctx->digest || EVP_DigestInit_ex(ctx->digest, md, NULL) != 1) {
            goto fail;
        }
    } else {
        if (!CRYPTO_THREAD_run_once(&mac_once, load_macs)) {
            goto fail;
        }
        mac = digest ? hmac_algorithm : cmac_algorithm;
        if (!mac) {
            goto fail;
        }
        ctx->mac  = EVP_MAC_CTX_new(mac);
        params[0] = OSSL_PARAM_construct_utf8_string(digest ? OSSL_MAC_PARAM_DIGEST : OSSL_MAC_PARAM_CIPHER,
                                                     (char *) (digest ? digest : "AES-128-CBC"), 0);
        params[1] = OSSL_PARAM_construct_end();
        if (!ctx->mac || EVP_MAC_init(ctx->mac, key ? key : "", key_len, params) != 1) {
            goto fail;
        }
    }
    return ctx;
 fail:
    chimera_crypto_hash_free(ctx);
    return NULL;
} /* chimera_crypto_hash_new */

int
chimera_crypto_update(
    struct chimera_crypto_hash *ctx,
    const void                 *data,
    size_t                      len)
{
    int outl;

    if (!ctx || ctx->finished || (!data && len)) {
        return 0;
    }
    if (!len) {
        return 1;
    }
    if (ctx->digest) {
        return EVP_DigestUpdate(ctx->digest, data, len) == 1;
    }
    if (ctx->mac) {
        return EVP_MAC_update(ctx->mac, data, len) == 1;
    }
    return len <= INT_MAX && EVP_EncryptUpdate(ctx->gmac, NULL, &outl, data, (int) len) == 1;
} /* chimera_crypto_update */

int
chimera_crypto_final(
    struct chimera_crypto_hash *ctx,
    void                       *out,
    size_t                      capacity)
{
    unsigned int size;
    size_t       mac_size;
    int          outl;

    if (!ctx || ctx->finished || !out || capacity < ctx->size) {
        return 0;
    }
    ctx->finished = 1;
    if (ctx->digest) {
        return EVP_DigestFinal_ex(ctx->digest, out, &size) == 1 && size == ctx->size;
    }
    if (ctx->mac) {
        return EVP_MAC_final(ctx->mac, out, &mac_size, capacity) == 1 && mac_size == ctx->size;
    }
    return EVP_EncryptFinal_ex(ctx->gmac, NULL, &outl) == 1 && EVP_CIPHER_CTX_ctrl(ctx->gmac, EVP_CTRL_GCM_GET_TAG, 16,
                                                                                   out) == 1;
} /* chimera_crypto_final */

void
chimera_crypto_hash_free(struct chimera_crypto_hash *ctx)
{
    if (!ctx) {
        return;
    }
    EVP_MD_CTX_free(ctx->digest);
    EVP_MAC_CTX_free(ctx->mac);
    EVP_CIPHER_CTX_free(ctx->gmac);
    free(ctx);
} /* chimera_crypto_hash_free */

int
chimera_crypto_kdf(
    const void *key,
    size_t      key_len,
    const void *label,
    size_t      label_len,
    const void *context,
    size_t      context_len,
    void       *out,
    size_t      out_len)
{
    EVP_KDF     *kdf = EVP_KDF_fetch(NULL, "KBKDF", NULL);
    EVP_KDF_CTX *ctx;
    int          one = 1, ok;
    OSSL_PARAM   params[] = {
        OSSL_PARAM_utf8_string(OSSL_KDF_PARAM_MODE,        "counter",              0),
        OSSL_PARAM_utf8_string(OSSL_KDF_PARAM_MAC,         "HMAC",                 0),
        OSSL_PARAM_utf8_string(OSSL_KDF_PARAM_DIGEST,      "SHA256",               0),
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_KEY,        (void *) key,           key_len),
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_SALT,       (void *) label,         label_len),
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_INFO,       (void *) context,       context_len),
        OSSL_PARAM_int(OSSL_KDF_PARAM_KBKDF_USE_L,         &one),
        OSSL_PARAM_int(OSSL_KDF_PARAM_KBKDF_USE_SEPARATOR, &one),
        OSSL_PARAM_END
    };

    if (!kdf) {
        return 0;
    }
    ctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!ctx) {
        return 0;
    }
    ok = EVP_KDF_derive(ctx, out, out_len, params) == 1;
    EVP_KDF_CTX_free(ctx);
    return ok;
} /* chimera_crypto_kdf */

struct chimera_crypto_aead *
chimera_crypto_aead_new(void)
{
    struct chimera_crypto_aead *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        return NULL;
    }
    ctx->ctx       = EVP_CIPHER_CTX_new();
    ctx->cipher[0] = EVP_CIPHER_fetch(NULL, "AES-128-GCM", NULL);
    ctx->cipher[1] = EVP_CIPHER_fetch(NULL, "AES-128-CCM", NULL);
    ctx->cipher[2] = EVP_CIPHER_fetch(NULL, "AES-256-GCM", NULL);
    ctx->cipher[3] = EVP_CIPHER_fetch(NULL, "AES-256-CCM", NULL);
    if (!ctx->ctx || !ctx->cipher[0] || !ctx->cipher[1] || !ctx->cipher[2] || !ctx->cipher[3]) {
        chimera_crypto_aead_free(ctx);
        return NULL;
    }
    return ctx;
} /* chimera_crypto_aead_new */

void
chimera_crypto_aead_free(struct chimera_crypto_aead *ctx)
{
    if (!ctx) {
        return;
    }
    EVP_CIPHER_CTX_free(ctx->ctx);
    for (int i = 0; i < 4; i++) {
        EVP_CIPHER_free(ctx->cipher[i]);
    }
    free(ctx);
} /* chimera_crypto_aead_free */

int
chimera_crypto_aead_crypt(
    struct chimera_crypto_aead *ctx,
    int                         encrypt,
    int                         ccm,
    const void                 *key,
    size_t                      key_len,
    const void                 *nonce,
    size_t                      nonce_len,
    const void                 *aad,
    size_t                      aad_len,
    void                       *data,
    size_t                      data_len,
    void                       *tag)
{
    EVP_CIPHER_CTX *c;
    int             len, final_len, ok = 0;
    unsigned char   empty[16];
    unsigned char  *bytes = data ? data : empty;

    if (!ctx || !key || !nonce || !tag || (!data && data_len) || (!aad && aad_len) ||
        (key_len != 16 && key_len != 32) || data_len > INT_MAX || aad_len > INT_MAX ||
        nonce_len != (ccm ? 11u : 12u)) {
        goto done;
    }
    c = ctx->ctx;
    if (EVP_CipherInit_ex(c, ctx->cipher[(key_len == 32 ? 2 : 0) + !!ccm], NULL, NULL, NULL, !!encrypt) != 1 ||
        EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_SET_IVLEN, (int) nonce_len, NULL) != 1) {
        goto done;
    }
    if ((ccm || !encrypt) && EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_SET_TAG, 16, encrypt ? NULL : tag) != 1) {
        goto done;
    }
    if (EVP_CipherInit_ex(c, NULL, NULL, key, nonce, !!encrypt) != 1) {
        goto done;
    }
    if (ccm && EVP_CipherUpdate(c, NULL, &len, NULL, (int) data_len) != 1) {
        goto done;
    }
    if (aad_len && EVP_CipherUpdate(c, NULL, &len, aad, (int) aad_len) != 1) {
        goto done;
    }
    if (EVP_CipherUpdate(c, bytes, &len, bytes, (int) data_len) != 1) {
        goto done;
    }
    if ((!ccm || encrypt) && EVP_CipherFinal_ex(c, bytes + len, &final_len) != 1) {
        goto done;
    }
    if (encrypt && EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_GET_TAG, 16, tag) != 1) {
        goto done;
    }
    ok = 1;
 done:
    if (!ok && !encrypt && data) {
        chimera_crypto_clear(data, data_len);
    }
    return ok;
} /* chimera_crypto_aead_crypt */

int
chimera_crypto_ntlm_key_exchange(
    const uint8_t key[16],
    const uint8_t input[16],
    uint8_t       output[16])
{
    EVP_CIPHER_CTX *ctx;
    int             outl, ok;

    if (!key || !input || !output) {
        return 0;
    }
    if (!CRYPTO_THREAD_run_once(&legacy_once, load_legacy) || !legacy_provider || !default_provider) {
        return 0;
    }
    ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return 0;
    }
    /* MS-NLMP 3.2.5.1.2 specifies RC4 for NTLM's negotiated key exchange. */
    ok = EVP_EncryptInit_ex(ctx, EVP_rc4(), NULL, key, NULL) == 1 &&
        EVP_EncryptUpdate(ctx, output, &outl, input, 16) == 1 && outl == 16;
    EVP_CIPHER_CTX_free(ctx);
    return ok;
} /* chimera_crypto_ntlm_key_exchange */

int
chimera_crypto_random(
    void  *out,
    size_t len)
{
    return len <= INT_MAX && RAND_bytes(out, (int) len) == 1;
} /* chimera_crypto_random */

int
chimera_crypto_base64(
    const void *data,
    size_t      len,
    char       *out,
    size_t      capacity)
{
    if (!out || (!data && len) || len > INT_MAX / 4 * 3 || capacity <= 4 * ((len + 2) / 3)) {
        return -1;
    }
    return EVP_EncodeBlock((unsigned char *) out, data, (int) len);
} /* chimera_crypto_base64 */
