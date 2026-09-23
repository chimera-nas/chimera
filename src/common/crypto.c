// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "crypto.h"

size_t
chimera_crypto_size(enum chimera_crypto_algorithm algorithm)
{
    switch (algorithm) {
        case CHIMERA_CRYPTO_MD4:
        case CHIMERA_CRYPTO_MD5:
        case CHIMERA_CRYPTO_HMAC_MD5:
        case CHIMERA_CRYPTO_AES_CMAC:
        case CHIMERA_CRYPTO_AES_GMAC: return 16;
        case CHIMERA_CRYPTO_SHA1:
        case CHIMERA_CRYPTO_HMAC_SHA1: return 20;
        case CHIMERA_CRYPTO_SHA256:
        case CHIMERA_CRYPTO_HMAC_SHA256: return 32;
        case CHIMERA_CRYPTO_SHA512: return 64;
        default: return 0;
    } /* switch */
} /* chimera_crypto_size */

int
chimera_crypto_hmac(
    enum chimera_crypto_algorithm algorithm,
    const void                   *key,
    size_t                        key_len,
    const void                   *data,
    size_t                        len,
    void                         *out,
    size_t                        capacity)
{
    struct chimera_crypto_hash *ctx = chimera_crypto_hash_new(algorithm, key, key_len, NULL, 0);
    int                         ok  = ctx && chimera_crypto_update(ctx, data, len) && chimera_crypto_final(ctx, out,
                                                                                                           capacity);

    chimera_crypto_hash_free(ctx);
    return ok;
} /* chimera_crypto_hmac */

int
chimera_crypto_digest(
    enum chimera_crypto_algorithm algorithm,
    const void                   *data,
    size_t                        len,
    void                         *out,
    size_t                        capacity)
{
    return chimera_crypto_hmac(algorithm, NULL, 0, data, len, out, capacity);
} /* chimera_crypto_digest */

int
chimera_crypto_compare(
    const void *a,
    const void *b,
    size_t      len)
{
    const unsigned char   *ap = a, *bp = b;
    volatile unsigned char difference = 0;

    while (len--) {
        difference |= *ap++ ^ *bp++;
    }
    return difference;
} /* chimera_crypto_compare */

void
chimera_crypto_clear(
    void  *data,
    size_t len)
{
    volatile unsigned char *p = data;

    while (len--) {
        *p++ = 0;
    }
} /* chimera_crypto_clear */
