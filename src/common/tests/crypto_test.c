// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "common/crypto.h"

static void
check(
    const void *data,
    size_t      len,
    const char *hex)
{
    const unsigned char *bytes = data;

    assert(strlen(hex) == len * 2);
    for (size_t i = 0; i < len; i++) {
        unsigned value;
        assert(sscanf(hex + i * 2, "%2x", &value) == 1);
        if (bytes[i] != value) {
            fprintf(stderr, "Mismatch at byte %zu: %02x != %02x\n", i, bytes[i], value);
            assert(0);
        }
    }
} /* check */

int
main(void)
{
    unsigned char               out[128], key[32], nonce[12], data[37], aad[19], tag[16];
    unsigned char               expected_data[37], original_tag[16];
    struct chimera_crypto_hash *hash;
    struct chimera_crypto_aead *aead;
    char                        base64[32];

    for (size_t i = 0; i < sizeof(key); i++) {
        key[i] = (unsigned char) i;
    }
    for (size_t i = 0; i < sizeof(nonce); i++) {
        nonce[i] = (unsigned char) (i + 16);
    }
    for (size_t i = 0; i < sizeof(data); i++) {
        expected_data[i] = (unsigned char) (i + 32);
    }
    for (size_t i = 0; i < sizeof(aad); i++) {
        aad[i] = (unsigned char) (i + 64);
    }
    assert(chimera_crypto_digest(CHIMERA_CRYPTO_MD4, "abc", 3, out, sizeof(out)));
    check(out, 16, "a448017aaf21d8525fc10ae87aa6729d");
    assert(chimera_crypto_digest(CHIMERA_CRYPTO_MD5, "abc", 3, out, sizeof(out)));
    check(out, 16, "900150983cd24fb0d6963f7d28e17f72");
    assert(chimera_crypto_digest(CHIMERA_CRYPTO_SHA1, "abc", 3, out, sizeof(out)));
    check(out, 20, "a9993e364706816aba3e25717850c26c9cd0d89d");
    assert(chimera_crypto_digest(CHIMERA_CRYPTO_SHA256, "abc", 3, out, sizeof(out)));
    check(out, 32, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    assert(chimera_crypto_digest(CHIMERA_CRYPTO_SHA512, "abc", 3, out, sizeof(out)));
    check(out, 64,
          "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f");
    assert(chimera_crypto_hmac(CHIMERA_CRYPTO_HMAC_MD5, key, 32, "abc", 3, out, sizeof(out)));
    check(out, 16, "402b833eacaf1bff45d89bba5d52c9da");
    hash = chimera_crypto_hash_new(CHIMERA_CRYPTO_HMAC_MD5, key, 32, NULL, 0);
    assert(hash && chimera_crypto_update(hash, "a", 1) && chimera_crypto_update(hash, NULL, 0) && chimera_crypto_update(
               hash, "bc", 2));
    assert(!chimera_crypto_final(hash, out, 15));
    assert(chimera_crypto_final(hash, out, sizeof(out)));
    check(out, 16, "402b833eacaf1bff45d89bba5d52c9da");
    assert(!chimera_crypto_final(hash, out, sizeof(out)));
    chimera_crypto_hash_free(hash);
    assert(chimera_crypto_hmac(CHIMERA_CRYPTO_HMAC_SHA1, key, 32, "abc", 3, out, sizeof(out)));
    check(out, 20, "fde25bea45b90744715078c176caef9942f77498");
    hash = chimera_crypto_hash_new(CHIMERA_CRYPTO_HMAC_SHA1, key, 32, NULL, 0);
    assert(hash && chimera_crypto_update(hash, "a", 1) && chimera_crypto_update(hash, NULL, 0) && chimera_crypto_update(
               hash, "bc", 2));
    assert(!chimera_crypto_final(hash, out, 19));
    assert(chimera_crypto_final(hash, out, sizeof(out)));
    check(out, 20, "fde25bea45b90744715078c176caef9942f77498");
    assert(!chimera_crypto_final(hash, out, sizeof(out)));
    chimera_crypto_hash_free(hash);
    assert(chimera_crypto_hmac(CHIMERA_CRYPTO_HMAC_SHA256, key, 32, "abc", 3, out, sizeof(out)));
    check(out, 32, "f0133729c4163dede81e21cd47839256da58171238c8a0d874397c73b14e1e47");
    hash = chimera_crypto_hash_new(CHIMERA_CRYPTO_HMAC_SHA256, key, 32, NULL, 0);
    assert(hash && chimera_crypto_update(hash, "a", 1) && chimera_crypto_update(hash, NULL, 0) && chimera_crypto_update(
               hash, "bc", 2));
    assert(!chimera_crypto_final(hash, out, 31));
    assert(chimera_crypto_final(hash, out, sizeof(out)));
    check(out, 32, "f0133729c4163dede81e21cd47839256da58171238c8a0d874397c73b14e1e47");
    assert(!chimera_crypto_final(hash, out, sizeof(out)));
    chimera_crypto_hash_free(hash);
    hash = chimera_crypto_hash_new(CHIMERA_CRYPTO_AES_CMAC, key, 16, NULL, 0);
    assert(hash && chimera_crypto_update(hash, "a", 1) && chimera_crypto_update(hash, "bc", 2) && chimera_crypto_final(
               hash, out, sizeof(out)));
    check(out, 16, "0a54a6a425d48438c3f8bbe09bf944cc");
    chimera_crypto_hash_free(hash);
    hash = chimera_crypto_hash_new(CHIMERA_CRYPTO_AES_GMAC, key, 16, nonce, 12);
    assert(hash && chimera_crypto_update(hash, "a", 1) && chimera_crypto_update(hash, "bc", 2) && chimera_crypto_final(
               hash, out, sizeof(out)));
    check(out, 16, "0c58487d2dc7cf440e0f86ba93255027");
    chimera_crypto_hash_free(hash);
    assert(chimera_crypto_kdf(key, 32, "label", 6, "context", 8, out, 16));
    check(out, 16, "04bd1aeb726a084c8097c8604e712b04");
    assert(chimera_crypto_kdf(key, 32, "label", 6, "context", 8, out, 32));
    check(out, 32, "c407168e4a148e4a76982f95e210207704aadbe1a89b4a8dd0a0770e0e9b21a9");
    assert(chimera_crypto_kdf(key, 32, "label", 6, "context", 8, out, 48));
    check(out, 48, "e8e3bee8c14ccddd654e451ef5eaa02049921b921e3dd5fb9caa94497cc7d75597e917bdd7ed7cbdf3e6aed91e6759ff");
    aead = chimera_crypto_aead_new();
    assert(aead);
    /* Fixed AES-128-GCM fixture, non-block-aligned AAD and plaintext. */
    memcpy(data, expected_data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 1, 0, key, 16, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    check(data, sizeof(data), "e40f218c2b6a90c83ff477deeb0ac5110a8d46b402c15d88bdf2132a5b382f929298637cc5");
    check(tag, sizeof(tag), "a2e866bffdb7743eff771a4acc88f449");
    memcpy(original_tag, tag, sizeof(tag));
    memcpy(out, data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 0, 0, key, 16, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    assert(!memcmp(data, expected_data, sizeof(data)));
    memcpy(data, out, sizeof(data));
    tag[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 0, key, 16, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    for (size_t i = 0; i < sizeof(data); i++) {
        assert(data[i] == 0);
    }
    memcpy(data, out, sizeof(data));
    memcpy(tag, original_tag, sizeof(tag));
    aad[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 0, key, 16, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    aad[0] ^= 1;
    /* Fixed AES-256-GCM fixture, non-block-aligned AAD and plaintext. */
    memcpy(data, expected_data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 1, 0, key, 32, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    check(data, sizeof(data), "5ddfba356dec1c94e25c22362354477ce7617c3d2ff76186dfc0d85c62696ae410a81540b1");
    check(tag, sizeof(tag), "c0e3d9f9ceb45b031aac4414a0c1790e");
    memcpy(original_tag, tag, sizeof(tag));
    memcpy(out, data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 0, 0, key, 32, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    assert(!memcmp(data, expected_data, sizeof(data)));
    memcpy(data, out, sizeof(data));
    tag[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 0, key, 32, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    for (size_t i = 0; i < sizeof(data); i++) {
        assert(data[i] == 0);
    }
    memcpy(data, out, sizeof(data));
    memcpy(tag, original_tag, sizeof(tag));
    aad[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 0, key, 32, nonce, 12, aad, sizeof(aad), data, sizeof(data), tag));
    aad[0] ^= 1;
    /* Fixed AES-128-CCM fixture, non-block-aligned AAD and plaintext. */
    memcpy(data, expected_data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 1, 1, key, 16, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    check(data, sizeof(data), "6c4e5ff8e498778ca625c3480e4eb0811159ea6a7be84cd8d279bc38a1c5ecd5498b36f907");
    check(tag, sizeof(tag), "61e48ebef723e610acbbca25f27cef43");
    memcpy(original_tag, tag, sizeof(tag));
    memcpy(out, data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 0, 1, key, 16, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    assert(!memcmp(data, expected_data, sizeof(data)));
    memcpy(data, out, sizeof(data));
    tag[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 1, key, 16, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    for (size_t i = 0; i < sizeof(data); i++) {
        assert(data[i] == 0);
    }
    memcpy(data, out, sizeof(data));
    memcpy(tag, original_tag, sizeof(tag));
    aad[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 1, key, 16, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    aad[0] ^= 1;
    /* Fixed AES-256-CCM fixture, non-block-aligned AAD and plaintext. */
    memcpy(data, expected_data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 1, 1, key, 32, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    check(data, sizeof(data), "077fcdbb6be33b2d4750588ece53fbb313e2fd4248a29b14996bb62a67ffad8be756df6bce");
    check(tag, sizeof(tag), "74a1d1a04ced8b69eeeb5ecac6984529");
    memcpy(original_tag, tag, sizeof(tag));
    memcpy(out, data, sizeof(data));
    assert(chimera_crypto_aead_crypt(aead, 0, 1, key, 32, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    assert(!memcmp(data, expected_data, sizeof(data)));
    memcpy(data, out, sizeof(data));
    tag[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 1, key, 32, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    for (size_t i = 0; i < sizeof(data); i++) {
        assert(data[i] == 0);
    }
    memcpy(data, out, sizeof(data));
    memcpy(tag, original_tag, sizeof(tag));
    aad[0] ^= 1;
    assert(!chimera_crypto_aead_crypt(aead, 0, 1, key, 32, nonce, 11, aad, sizeof(aad), data, sizeof(data), tag));
    aad[0] ^= 1;
    chimera_crypto_aead_free(aead);
    /* RFC 6229, 128-bit key 01..10, first sixteen bytes of RC4 output. */
    for (int i = 0; i < 16; i++) {
        key[i] = (unsigned char) (i + 1);
    }
    memset(data, 0, 16);
    assert(chimera_crypto_rc4(key, 16, data, out, 16));
    check(out, 16, "9ac7cc9a609d1ef7b2932899cde41b97");
    assert(chimera_crypto_random(out, 32));
    assert(chimera_crypto_compare("abc", "abc", 3) == 0);
    assert(chimera_crypto_compare("abc", "abd", 3) != 0);
    assert(chimera_crypto_base64("abcde", 5, base64, sizeof(base64)) == 8);
    assert(!strcmp(base64, "YWJjZGU="));
    assert(chimera_crypto_base64("abcde", 5, base64, 8) == -1);
    puts("All crypto vectors passed");
    return 0;
} /* main */
