// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include <stddef.h>
#include <stdint.h>
#include "macros.h"

/* Internal cryptographic primitives. Operations return 1 on success, 0 on
 * failure, except compare (zero means equal) and base64 (encoded length or -1).
 * Contexts belong to one thread. MAC output is never implicitly truncated. */
enum chimera_crypto_algorithm {
    CHIMERA_CRYPTO_MD4, CHIMERA_CRYPTO_MD5, CHIMERA_CRYPTO_SHA1,
    CHIMERA_CRYPTO_SHA256, CHIMERA_CRYPTO_SHA512,
    CHIMERA_CRYPTO_HMAC_MD5, CHIMERA_CRYPTO_HMAC_SHA1,
    CHIMERA_CRYPTO_HMAC_SHA256, CHIMERA_CRYPTO_AES_CMAC,
    CHIMERA_CRYPTO_AES_GMAC
};
struct chimera_crypto_hash;
struct chimera_crypto_aead;
SYMBOL_EXPORT size_t chimera_crypto_size(
    enum chimera_crypto_algorithm algorithm);
SYMBOL_EXPORT struct chimera_crypto_hash * chimera_crypto_hash_new(
    enum chimera_crypto_algorithm algorithm,
    const void                   *key,
    size_t                        key_len,
    const void                   *nonce,
    size_t                        nonce_len);
SYMBOL_EXPORT int chimera_crypto_update(
    struct chimera_crypto_hash *ctx,
    const void                 *data,
    size_t                      len);
SYMBOL_EXPORT int chimera_crypto_final(
    struct chimera_crypto_hash *ctx,
    void                       *out,
    size_t                      capacity);
SYMBOL_EXPORT void chimera_crypto_hash_free(
    struct chimera_crypto_hash *ctx);
SYMBOL_EXPORT int chimera_crypto_digest(
    enum chimera_crypto_algorithm algorithm,
    const void                   *data,
    size_t                        len,
    void                         *out,
    size_t                        capacity);
SYMBOL_EXPORT int chimera_crypto_hmac(
    enum chimera_crypto_algorithm algorithm,
    const void                   *key,
    size_t                        key_len,
    const void                   *data,
    size_t                        len,
    void                         *out,
    size_t                        capacity);
SYMBOL_EXPORT int chimera_crypto_kdf(
    const void *key,
    size_t      key_len,
    const void *label,
    size_t      label_len,
    const void *context,
    size_t      context_len,
    void       *out,
    size_t      out_len);
SYMBOL_EXPORT struct chimera_crypto_aead * chimera_crypto_aead_new(
    void);
SYMBOL_EXPORT void chimera_crypto_aead_free(
    struct chimera_crypto_aead *ctx);
/* In-place AES-128/256 GCM or CCM with a 16-byte tag. On failed decryption,
 * the output is cleared. The caller must discard failed encryption output. */
SYMBOL_EXPORT int chimera_crypto_aead_crypt(
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
    void                       *tag);
SYMBOL_EXPORT int chimera_crypto_rc4(
    const void *key,
    size_t      key_len,
    const void *input,
    void       *output,
    size_t      len);
SYMBOL_EXPORT int chimera_crypto_random(
    void  *out,
    size_t len);
SYMBOL_EXPORT int chimera_crypto_compare(
    const void *a,
    const void *b,
    size_t      len);
SYMBOL_EXPORT void chimera_crypto_clear(
    void  *data,
    size_t len);
SYMBOL_EXPORT int chimera_crypto_base64(
    const void *data,
    size_t      len,
    char       *out,
    size_t      capacity);
