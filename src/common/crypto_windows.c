// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include <windows.h>
#include <bcrypt.h>
#include <wincrypt.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include "crypto.h"

/* Algorithm providers are immutable after initialization and shared across
 * threads. CNG keys and hash state are private to each operation/context. */
struct crypto_provider {
    INIT_ONCE         once;
    LPCWSTR           algorithm;
    LPCWSTR           mode;
    ULONG             flags;
    BCRYPT_ALG_HANDLE handle;
};
static struct crypto_provider providers[] = {
    { INIT_ONCE_STATIC_INIT, BCRYPT_MD4_ALGORITHM,      NULL,      0,                                NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_MD5_ALGORITHM,      NULL,      0,                                NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_SHA1_ALGORITHM,     NULL,      0,                                NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_SHA256_ALGORITHM,   NULL,      0,                                NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_SHA512_ALGORITHM,   NULL,      0,                                NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_MD5_ALGORITHM,      NULL,      BCRYPT_ALG_HANDLE_HMAC_FLAG,      NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_SHA1_ALGORITHM,     NULL,      BCRYPT_ALG_HANDLE_HMAC_FLAG,      NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_SHA256_ALGORITHM,   NULL,      BCRYPT_ALG_HANDLE_HMAC_FLAG,      NULL
    },
    { INIT_ONCE_STATIC_INIT, BCRYPT_AES_CMAC_ALGORITHM, NULL,      0,                                NULL
    }
};
static struct crypto_provider aes_gcm = { INIT_ONCE_STATIC_INIT, BCRYPT_AES_ALGORITHM, BCRYPT_CHAIN_MODE_GCM, 0,
                                          NULL };
static struct crypto_provider aes_ccm = { INIT_ONCE_STATIC_INIT, BCRYPT_AES_ALGORITHM, BCRYPT_CHAIN_MODE_CCM, 0,
                                          NULL };
static struct crypto_provider rc4_provider = { INIT_ONCE_STATIC_INIT, BCRYPT_RC4_ALGORITHM, NULL, 0, NULL };
static struct crypto_provider kdf_provider = { INIT_ONCE_STATIC_INIT, BCRYPT_SP800108_CTR_HMAC_ALGORITHM, NULL, 0, NULL
};

static BOOL CALLBACK
provider_init(
    PINIT_ONCE once,
    PVOID      parameter,
    PVOID     *context)
{
    struct crypto_provider *p = parameter;

    (void) once;
    (void) context;
    if (BCryptOpenAlgorithmProvider(&p->handle, p->algorithm, NULL, p->flags) < 0) {
        return FALSE;
    }
    if (p->mode && BCryptSetProperty(p->handle, BCRYPT_CHAINING_MODE,
                                     (PUCHAR) p->mode, (ULONG) ((wcslen(p->mode) + 1) * sizeof(WCHAR)), 0) < 0) {
        BCryptCloseAlgorithmProvider(p->handle, 0);
        p->handle = NULL;
        return FALSE;
    }
    return TRUE;
} /* provider_init */

static BCRYPT_ALG_HANDLE
provider_get(struct crypto_provider *p)
{
    return InitOnceExecuteOnce(&p->once, provider_init, p, NULL) ? p->handle : NULL;
} /* provider_get */

struct chimera_crypto_hash {
    BCRYPT_HASH_HANDLE hash;
    size_t             size;
    int                finished;
    /* GMAC is GCM with the message as AAD and an empty plaintext. Buffer only
     * this algorithm because the CNG one-shot AEAD API takes contiguous AAD. */
    unsigned char      key[16], nonce[12];
    unsigned char     *aad;
    size_t             aad_len, aad_capacity;
};
struct chimera_crypto_aead {
    BCRYPT_ALG_HANDLE gcm, ccm;
};

struct chimera_crypto_hash *
chimera_crypto_hash_new(
    enum chimera_crypto_algorithm algorithm,
    const void                   *key,
    size_t                        key_len,
    const void                   *nonce,
    size_t                        nonce_len)
{
    struct chimera_crypto_hash *ctx;
    BCRYPT_ALG_HANDLE           provider;

    if ((!key && key_len) || algorithm < CHIMERA_CRYPTO_MD4 || algorithm > CHIMERA_CRYPTO_AES_GMAC || key_len >
        ULONG_MAX) {
        return NULL;
    }
    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return NULL;
    }
    ctx->size = chimera_crypto_size(algorithm);
    if (algorithm == CHIMERA_CRYPTO_AES_GMAC) {
        if (key_len != 16 || nonce_len != 12 || !key || !nonce) {
            goto fail;
        }
        memcpy(ctx->key, key, 16);
        memcpy(ctx->nonce, nonce, 12);
    } else {
        if (algorithm == CHIMERA_CRYPTO_AES_CMAC && key_len != 16) {
            goto fail;
        }
        provider = provider_get(&providers[algorithm]);
        if (!provider || BCryptCreateHash(provider, &ctx->hash, NULL, 0,
                                          (PUCHAR) key, (ULONG) key_len, 0) < 0) {
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
    size_t         capacity;
    unsigned char *aad;

    if (!ctx || ctx->finished || len > ULONG_MAX || (!data && len)) {
        return 0;
    }
    if (!len) {
        return 1;
    }
    if (ctx->hash) {
        return BCryptHashData(ctx->hash, (PUCHAR) data, (ULONG) len, 0) >= 0;
    }
    if (len > ULONG_MAX - ctx->aad_len) {
        return 0;
    }
    if (ctx->aad_len + len > ctx->aad_capacity) {
        capacity = ctx->aad_len + len;
        if (capacity < ULONG_MAX / 2) {
            capacity *= 2;
        }
        aad = realloc(ctx->aad, capacity);
        if (!aad) {
            return 0;
        }
        ctx->aad          = aad;
        ctx->aad_capacity = capacity;
    }
    memcpy(ctx->aad + ctx->aad_len, data, len);
    ctx->aad_len += len;
    return 1;
} /* chimera_crypto_update */

int
chimera_crypto_final(
    struct chimera_crypto_hash *ctx,
    void                       *out,
    size_t                      capacity)
{
    struct chimera_crypto_aead *aead;
    int                         ok;

    if (!ctx || ctx->finished || !out || capacity < ctx->size) {
        return 0;
    }
    ctx->finished = 1;
    if (ctx->hash) {
        return BCryptFinishHash(ctx->hash, out, (ULONG) ctx->size, 0) >= 0;
    }
    aead = chimera_crypto_aead_new();
    if (!aead) {
        return 0;
    }
    ok = chimera_crypto_aead_crypt(aead, 1, 0, ctx->key, 16, ctx->nonce, 12, ctx->aad, ctx->aad_len, NULL, 0, out);
    chimera_crypto_aead_free(aead);
    return ok;
} /* chimera_crypto_final */

void
chimera_crypto_hash_free(struct chimera_crypto_hash *ctx)
{
    if (!ctx) {
        return;
    }
    if (ctx->hash) {
        BCryptDestroyHash(ctx->hash);
    }
    free(ctx->aad);
    chimera_crypto_clear(ctx, sizeof(*ctx));
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
    BCRYPT_ALG_HANDLE provider;
    BCRYPT_KEY_HANDLE handle  = NULL;
    ULONG             written = 0;
    int               ok;
    BCryptBuffer      params[3];
    BCryptBufferDesc  desc;

    if (key_len > ULONG_MAX || label_len > ULONG_MAX || context_len > ULONG_MAX || out_len > ULONG_MAX) {
        return 0;
    }
    provider = provider_get(&kdf_provider);
    if (!provider || BCryptGenerateSymmetricKey(provider, &handle, NULL, 0, (PUCHAR) key, (ULONG) key_len, 0) < 0) {
        return 0;
    }
    params[0].BufferType = KDF_LABEL;
    params[0].pvBuffer   = (void *) label;
    params[0].cbBuffer   = (ULONG) label_len;
    params[1].BufferType = KDF_CONTEXT;
    params[1].pvBuffer   = (void *) context;
    params[1].cbBuffer   = (ULONG) context_len;
    params[2].BufferType = KDF_HASH_ALGORITHM;
    params[2].pvBuffer   = (void *) BCRYPT_SHA256_ALGORITHM;
    params[2].cbBuffer   = sizeof(BCRYPT_SHA256_ALGORITHM);
    desc.ulVersion       = BCRYPTBUFFER_VERSION;
    desc.cBuffers        = 3;
    desc.pBuffers        = params;
    ok                   = BCryptKeyDerivation(handle, &desc, out, (ULONG) out_len, &written, 0) >= 0 && written ==
        out_len;
    BCryptDestroyKey(handle);
    return ok;
} /* chimera_crypto_kdf */

struct chimera_crypto_aead *
chimera_crypto_aead_new(void)
{
    struct chimera_crypto_aead *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        return NULL;
    }
    ctx->gcm = provider_get(&aes_gcm);
    ctx->ccm = provider_get(&aes_ccm);
    if (!ctx->gcm || !ctx->ccm) {
        free(ctx);
        return NULL;
    }
    return ctx;
} /* chimera_crypto_aead_new */

void
chimera_crypto_aead_free(struct chimera_crypto_aead *ctx)
{
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
    BCRYPT_KEY_HANDLE                     handle = NULL;
    BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO info;
    NTSTATUS                              status;
    ULONG                                 written;
    int                                   ok = 0;
    /* Non-NULL output distinguishes empty-message encryption from a size query. */
    unsigned char                         empty  = 0;
    void                                 *buffer = data ? data : &empty;

    if (!ctx || !key || !nonce || !tag || (!data && data_len) || (!aad && aad_len) ||
        (key_len != 16 && key_len != 32) || nonce_len != (ccm ? 11u : 12u) ||
        aad_len > ULONG_MAX || data_len > ULONG_MAX) {
        goto done;
    }
    if (BCryptGenerateSymmetricKey(ccm ? ctx->ccm : ctx->gcm, &handle, NULL, 0,
                                   (PUCHAR) key, (ULONG) key_len, 0) < 0) {
        goto done;
    }
    BCRYPT_INIT_AUTH_MODE_INFO(info);
    info.pbNonce    = (PUCHAR) nonce;
    info.cbNonce    = (ULONG) nonce_len;
    info.pbAuthData = (PUCHAR) aad;
    info.cbAuthData = (ULONG) aad_len;
    info.pbTag      = tag;
    info.cbTag      = 16;
    if (encrypt) {
        status = BCryptEncrypt(handle, buffer, (ULONG) data_len, &info, NULL, 0,
                               buffer, (ULONG) data_len, &written, 0);
    } else {
        status = BCryptDecrypt(handle, buffer, (ULONG) data_len, &info, NULL, 0,
                               buffer, (ULONG) data_len, &written, 0);
    }
    ok = status >= 0 && written == data_len;
 done:
    if (handle) {
        BCryptDestroyKey(handle);
    }
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
    BCRYPT_ALG_HANDLE provider;
    BCRYPT_KEY_HANDLE handle = NULL;
    ULONG             written;
    int               ok;

    if (!key || !input || !output) {
        return 0;
    }
    provider = provider_get(&rc4_provider);
    if (!provider || BCryptGenerateSymmetricKey(provider, &handle, NULL, 0, (PUCHAR) key, 16, 0) < 0) {
        return 0;
    }
    ok = BCryptEncrypt(handle, (PUCHAR) input, 16, NULL, NULL, 0,
                       output, 16, &written, 0) >= 0 && written == 16;
    BCryptDestroyKey(handle);
    return ok;
} /* chimera_crypto_ntlm_key_exchange */

int
chimera_crypto_random(
    void  *out,
    size_t len)
{
    return len <= ULONG_MAX && BCryptGenRandom(NULL, out, (ULONG) len, BCRYPT_USE_SYSTEM_PREFERRED_RNG) >= 0;
} /* chimera_crypto_random */

int
chimera_crypto_base64(
    const void *data,
    size_t      len,
    char       *out,
    size_t      capacity)
{
    DWORD size;

    if (!out || (!data && len) || len > INT_MAX / 4 * 3 || capacity <= 4 * ((len + 2) / 3) || capacity > ULONG_MAX) {
        return -1;
    }
    size = (DWORD) capacity;
    if (!CryptBinaryToStringA(data, (DWORD) len, CRYPT_STRING_BASE64 | CRYPT_STRING_NOCRLF, out, &size)) {
        return -1;
    }
    return (int) (4 * ((len + 2) / 3));
} /* chimera_crypto_base64 */
