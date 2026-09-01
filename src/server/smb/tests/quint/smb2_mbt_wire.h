/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Wire primitives for the in-process SMB2 test client: little-endian field
 * access, and the cryptography a protected connection needs -- NTLMv2
 * authentication, SMB3 key derivation, message signing and SMB3 transport
 * encryption.
 *
 * WHY THIS EXISTS.  The MBT harness drove every trace over one connection
 * profile: SMB 2.1/3.0, an anonymous NTLM logon, no signing, no encryption.
 * That leaves the entire protection layer of the server dark -- signing, the
 * transform header, the 3.1.1 negotiate contexts and their algorithm
 * selection, and real NTLMv2 credential validation -- even though every trace
 * in the corpus already crosses it.  Adding profiles here lets the SAME corpus
 * replay over a signed or encrypted connection, so the protection layer is
 * exercised on every packet of every trace rather than needing a model of its
 * own.
 *
 * These are CLIENT implementations, written from MS-SMB2 / MS-NLMP against the
 * server's own reading of them (src/server/smb/smb_signing.c, smb_encrypt.c,
 * smb_ntlm.c).  Deliberately independent code -- shared helpers would let a
 * server-side error cancel out and prove nothing -- but pinned to the same
 * OpenSSL primitives, since agreeing on AES is not what is under test.
 */

#pragma once

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/kdf.h>
#include <openssl/params.h>
#include <openssl/core_names.h>
#include <openssl/provider.h>

/* ---- little-endian field put/get ---------------------------------------- */

static inline void
p16(
    uint8_t *b,
    int      off,
    uint16_t v)
{
    b[off]     = (uint8_t) v;
    b[off + 1] = (uint8_t) (v >> 8);
} /* p16 */

static inline void
p32(
    uint8_t *b,
    int      off,
    uint32_t v)
{
    b[off]     = (uint8_t) v;
    b[off + 1] = (uint8_t) (v >> 8);
    b[off + 2] = (uint8_t) (v >> 16);
    b[off + 3] = (uint8_t) (v >> 24);
} /* p32 */

static inline void
p64(
    uint8_t *b,
    int      off,
    uint64_t v)
{
    for (int i = 0; i < 8; i++) {
        b[off + i] = (uint8_t) (v >> (8 * i));
    }
} /* p64 */

static inline uint16_t
g16(
    const uint8_t *b,
    int            off)
{
    return (uint16_t) (b[off] | (b[off + 1] << 8));
} /* g16 */

static inline uint32_t
g32(
    const uint8_t *b,
    int            off)
{
    return (uint32_t) b[off] | ((uint32_t) b[off + 1] << 8) |
           ((uint32_t) b[off + 2] << 16) | ((uint32_t) b[off + 3] << 24);
} /* g32 */

static inline uint64_t
g64(
    const uint8_t *b,
    int            off)
{
    uint64_t v = 0;

    for (int i = 0; i < 8; i++) {
        v |= (uint64_t) b[off + i] << (8 * i);
    }
    return v;
} /* g64 */

/* ASCII -> UTF-16LE; returns byte length written. */
static inline int
utf16le(
    const char *s,
    uint8_t    *out)
{
    int n = 0;

    for (; *s; s++) {
        out[n++] = (uint8_t) *s;
        out[n++] = 0;
    }
    return n;
} /* utf16le */

/* ---- wire-protection constants ------------------------------------------ */

/* Negotiated signing algorithms (MS-SMB2 2.2.3.1.7). */
#define SMB2W_SIGN_HMAC_SHA256           0x0000
#define SMB2W_SIGN_AES_CMAC              0x0001
#define SMB2W_SIGN_AES_GMAC              0x0002

/* Negotiated ciphers (MS-SMB2 2.2.3.1.2). */
#define SMB2W_CIPHER_AES128_CCM          0x0001
#define SMB2W_CIPHER_AES128_GCM          0x0002
#define SMB2W_CIPHER_AES256_CCM          0x0003
#define SMB2W_CIPHER_AES256_GCM          0x0004

/* Negotiate context types (MS-SMB2 2.2.3.1). */
#define SMB2W_CTX_PREAUTH                0x0001
#define SMB2W_CTX_ENCRYPTION             0x0002
#define SMB2W_CTX_COMPRESSION            0x0003
#define SMB2W_CTX_SIGNING                0x0008

/* Transport compression (MS-SMB2 2.2.3.1.3 CompressionAlgorithm IDs, and the
 * 2.2.42 transform Flags).  Restated here rather than included from the
 * server's smb2.h, like every other constant in this harness: the client is
 * meant to be an independent reading of the protocol, so that a wrong constant
 * on one side disagrees with the other instead of matching it by construction. */
#define SMB2_COMPRESSION_NONE            0x0000
#define SMB2_COMPRESSION_LZNT1           0x0001
#define SMB2_COMPRESSION_LZ77            0x0002
#define SMB2_COMPRESSION_LZ77_HUFFMAN    0x0003
#define SMB2_COMPRESSION_PATTERN_V1      0x0004

#define SMB2_COMPRESSION_FLAG_NONE       0x00000000u
#define SMB2_COMPRESSION_FLAG_CHAINED    0x00000001u

/* READ request Flags (MS-SMB2 2.2.19): ask the server to compress this reply. */
#define SMB2_READFLAG_REQUEST_COMPRESSED 0x02

#define SMB2W_PREAUTH_SHA_512            0x0001
#define SMB2W_PREAUTH_HASH_SIZE          64

/* TRANSFORM_HEADER (MS-SMB2 2.2.41): 52 bytes, of which [20,52) is the AEAD
 * associated data. */
#define SMB2W_XFORM_SIZE                 52
#define SMB2W_XFORM_AAD_OFF              20
#define SMB2W_XFORM_AAD_LEN              32
#define SMB2W_XFORM_FLAG_ENC             0x0001

/* NTLMSSP negotiate flags (MS-NLMP 2.2.2.5). */
#define SMB2W_NTLMSSP_UNICODE            0x00000001u
#define SMB2W_NTLMSSP_REQ_TARGET         0x00000004u
#define SMB2W_NTLMSSP_SIGN               0x00000010u
#define SMB2W_NTLMSSP_NTLM               0x00000200u
#define SMB2W_NTLMSSP_ANONYMOUS          0x00000800u
#define SMB2W_NTLMSSP_ALWAYS_SIGN        0x00008000u
#define SMB2W_NTLMSSP_EXT_SESSEC         0x00080000u
#define SMB2W_NTLMSSP_KEY_EXCH           0x40000000u
#define SMB2W_NTLMSSP_128                0x20000000u

static inline void
smb2w_die(const char *what)
{
    fprintf(stderr, "smb2 wire: %s failed\n", what);
    exit(6);
} /* smb2w_die */

/* MD4 (the NT hash) lives in OpenSSL 3's legacy provider; the server loads it
 * the same way (smb_ntlm.c ensure_legacy_provider).  Both providers must be
 * held: fetching "legacy" alone displaces the default one that supplies every
 * other digest and cipher here. */
static inline void
smb2w_need_legacy(void)
{
    static int done = 0;

    if (done) {
        return;
    }
    done = 1;
    OSSL_PROVIDER_load(NULL, "legacy");
    OSSL_PROVIDER_load(NULL, "default");
} /* smb2w_need_legacy */

/* ---- SP800-108 counter-mode KDF (MS-SMB2 3.1.4.2) ----------------------- */

/* HMAC-SHA256 counter KDF with the [L]2 length suffix and the 0x00 separator
 * between label and context, which is the shape SMB3 key derivation wants.
 * Label and context lengths are passed by the caller INCLUDING any trailing
 * NUL the spec folds in. */
static inline void
smb2w_kdf(
    const uint8_t *key,
    size_t         key_len,
    const void    *label,
    size_t         label_len,
    const uint8_t *context,
    size_t         ctx_len,
    uint8_t       *out,
    size_t         out_len)
{
    EVP_KDF     *kdf = EVP_KDF_fetch(NULL, "KBKDF", NULL);
    EVP_KDF_CTX *kctx;
    OSSL_PARAM   params[10];
    size_t       n       = 0;
    int          use_l   = 1;
    int          use_sep = 1;

    if (!kdf) {
        smb2w_die("EVP_KDF_fetch(KBKDF)");
    }
    kctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!kctx) {
        smb2w_die("EVP_KDF_CTX_new");
    }

    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_MODE, (char *) "counter", 0);
    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_MAC, (char *) "HMAC", 0);
    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, (char *) "SHA256", 0);
    params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY, (void *) key, key_len);
    params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, (void *) label, label_len);
    params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, (void *) context, ctx_len);
    params[n++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_KBKDF_USE_L, &use_l);
    params[n++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_KBKDF_USE_SEPARATOR, &use_sep);
    params[n++] = OSSL_PARAM_construct_end();

    if (EVP_KDF_derive(kctx, out, out_len, params) != 1) {
        smb2w_die("EVP_KDF_derive");
    }
    EVP_KDF_CTX_free(kctx);
} /* smb2w_kdf */

/* preauth = SHA512(preauth || msg), updated in place (MS-SMB2 3.1.4.4). */
static inline void
smb2w_preauth_extend(
    uint8_t    *hash,
    const void *msg,
    uint32_t    msg_len)
{
    EVP_MD_CTX  *md = EVP_MD_CTX_new();
    unsigned int out_len;

    if (!md) {
        smb2w_die("EVP_MD_CTX_new");
    }
    if (EVP_DigestInit_ex(md, EVP_sha512(), NULL) != 1 ||
        EVP_DigestUpdate(md, hash, SMB2W_PREAUTH_HASH_SIZE) != 1 ||
        EVP_DigestUpdate(md, msg, msg_len) != 1 ||
        EVP_DigestFinal_ex(md, hash, &out_len) != 1) {
        smb2w_die("SHA-512 preauth extend");
    }
    EVP_MD_CTX_free(md);
} /* smb2w_preauth_extend */

/* Signing key: 2.x uses the session key verbatim, 3.0/3.0.2 derive it from a
 * fixed label/context pair, 3.1.1 binds it to the preauth hash. */
static inline void
smb2w_derive_signing_key(
    uint16_t       dialect,
    const uint8_t *session_key,
    const uint8_t *preauth,
    uint8_t       *out16)
{
    static const char label30[]  = "SMB2AESCMAC";
    static const char ctx30[]    = "SmbSign";
    static const char label311[] = "SMBSigningKey";

    if (dialect < 0x0300) {
        memcpy(out16, session_key, 16);
    } else if (dialect == 0x0311) {
        smb2w_kdf(session_key, 16, label311, sizeof(label311),
                  preauth, SMB2W_PREAUTH_HASH_SIZE, out16, 16);
    } else {
        smb2w_kdf(session_key, 16, label30, sizeof(label30),
                  (const uint8_t *) ctx30, sizeof(ctx30), out16, 16);
    }
} /* smb2w_derive_signing_key */

/* Encryption keys.  Named from the CLIENT's point of view, so `send_key`
 * protects client-to-server traffic and `recv_key` unwraps the server's
 * replies -- the mirror of the server's enc/dec pair. */
static inline void
smb2w_derive_encryption_keys(
    uint16_t       dialect,
    uint16_t       cipher,
    const uint8_t *session_key,
    const uint8_t *preauth,
    uint8_t       *send_key,
    uint8_t       *recv_key,
    size_t        *key_len)
{
    static const char label30[]      = "SMB2AESCCM";
    static const char ctx30_s2c[]    = "ServerOut";
    static const char ctx30_c2s[]    = "ServerIn ";
    static const char label311_s2c[] = "SMBS2CCipherKey";
    static const char label311_c2s[] = "SMBC2SCipherKey";

    *key_len = (cipher == SMB2W_CIPHER_AES256_CCM ||
                cipher == SMB2W_CIPHER_AES256_GCM) ? 32 : 16;

    if (dialect == 0x0311) {
        smb2w_kdf(session_key, 16, label311_c2s, sizeof(label311_c2s),
                  preauth, SMB2W_PREAUTH_HASH_SIZE, send_key, *key_len);
        smb2w_kdf(session_key, 16, label311_s2c, sizeof(label311_s2c),
                  preauth, SMB2W_PREAUTH_HASH_SIZE, recv_key, *key_len);
    } else {
        smb2w_kdf(session_key, 16, label30, sizeof(label30),
                  (const uint8_t *) ctx30_c2s, sizeof(ctx30_c2s), send_key, *key_len);
        smb2w_kdf(session_key, 16, label30, sizeof(label30),
                  (const uint8_t *) ctx30_s2c, sizeof(ctx30_s2c), recv_key, *key_len);
    }
} /* smb2w_derive_encryption_keys */

/* ---- message signing ---------------------------------------------------- */

/* Compute the 16-byte signature over one SMB2 message (64-byte header +
 * body).  `msg` points at the SMB2 header, NOT at the NetBIOS framing, and
 * its Signature field must already be zeroed. */
static inline void
smb2w_sign(
    uint16_t       dialect,
    uint16_t       signing_alg,
    const uint8_t *key,
    const uint8_t *msg,
    int            msg_len,
    uint8_t       *sig16)
{
    int use_gmac = (dialect == 0x0311 && signing_alg == SMB2W_SIGN_AES_GMAC);
    int use_cmac = (dialect >= 0x0300 && !use_gmac &&
                    !(dialect == 0x0311 && signing_alg == SMB2W_SIGN_HMAC_SHA256));

    if (use_gmac) {
        /* AES-GMAC signs with the whole message as associated data and an IV
         * built from the MessageId plus two header flag bits (MS-SMB2
         * 3.1.4.1); the resulting GCM tag is the signature. */
        EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
        uint8_t         iv[12];
        uint32_t        high_bits;
        int             outl;

        if (!c) {
            smb2w_die("EVP_CIPHER_CTX_new");
        }

        high_bits = g32(msg, 16) & 0x00000001u;      /* SERVER_TO_REDIR */
        if (g16(msg, 12) == 0x000C) {                /* SMB2_CANCEL */
            high_bits |= 0x00000002u;                /* ASYNC_COMMAND */
        }

        memset(iv, 0, sizeof(iv));
        memcpy(iv, msg + 24, 8);                     /* MessageId */
        p32(iv, 8, high_bits);

        if (EVP_EncryptInit_ex(c, EVP_aes_128_gcm(), NULL, NULL, NULL) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, sizeof(iv), NULL) != 1 ||
            EVP_EncryptInit_ex(c, NULL, NULL, key, iv) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl, msg, msg_len) != 1 ||
            EVP_EncryptFinal_ex(c, NULL, &outl) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_GET_TAG, 16, sig16) != 1) {
            smb2w_die("AES-GMAC signature");
        }
        EVP_CIPHER_CTX_free(c);
    } else if (use_cmac) {
        EVP_MAC     *mac = EVP_MAC_fetch(NULL, "CMAC", NULL);
        EVP_MAC_CTX *mctx;
        OSSL_PARAM   params[2];
        size_t       maclen = 0;
        uint8_t      out[16];

        if (!mac) {
            smb2w_die("EVP_MAC_fetch(CMAC)");
        }
        mctx = EVP_MAC_CTX_new(mac);
        EVP_MAC_free(mac);
        params[0] = OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_CIPHER,
                                                     (char *) "AES-128-CBC", 0);
        params[1] = OSSL_PARAM_construct_end();

        if (!mctx ||
            EVP_MAC_init(mctx, key, 16, params) != 1 ||
            EVP_MAC_update(mctx, msg, msg_len) != 1 ||
            EVP_MAC_final(mctx, out, &maclen, sizeof(out)) != 1) {
            smb2w_die("AES-CMAC signature");
        }
        EVP_MAC_CTX_free(mctx);
        memcpy(sig16, out, 16);
    } else {
        uint8_t      out[32];
        unsigned int outlen = 0;

        if (!HMAC(EVP_sha256(), key, 16, msg, msg_len, out, &outlen) ||
            outlen < 16) {
            smb2w_die("HMAC-SHA256 signature");
        }
        memcpy(sig16, out, 16);
    }
} /* smb2w_sign */

/* ---- SMB3 transport encryption ------------------------------------------ */

static inline const EVP_CIPHER *
smb2w_cipher(
    uint16_t cipher,
    int     *nonce_len,
    int     *is_ccm)
{
    switch (cipher) {
        case SMB2W_CIPHER_AES128_CCM:
            *nonce_len = 11; *is_ccm = 1; return EVP_aes_128_ccm();
        case SMB2W_CIPHER_AES128_GCM:
            *nonce_len = 12; *is_ccm = 0; return EVP_aes_128_gcm();
        case SMB2W_CIPHER_AES256_CCM:
            *nonce_len = 11; *is_ccm = 1; return EVP_aes_256_ccm();
        case SMB2W_CIPHER_AES256_GCM:
            *nonce_len = 12; *is_ccm = 0; return EVP_aes_256_gcm();
        default:
            return NULL;
    } /* switch */
} /* smb2w_cipher */

/* Wrap one plaintext SMB2 message in a TRANSFORM_HEADER.  `out` receives the
 * 52-byte header followed by the ciphertext and must have room for
 * SMB2W_XFORM_SIZE + plain_len; returns the total byte count. */
static inline int
smb2w_encrypt(
    uint16_t       cipher_id,
    const uint8_t *key,
    size_t         key_len,
    uint64_t       nonce_counter,
    uint64_t       session_id,
    const uint8_t *plain,
    int            plain_len,
    uint8_t       *out)
{
    const EVP_CIPHER *cipher;
    EVP_CIPHER_CTX   *c;
    uint8_t          *ct = out + SMB2W_XFORM_SIZE;
    uint8_t           nonce[16];
    int               nonce_len, is_ccm, outl;

    cipher = smb2w_cipher(cipher_id, &nonce_len, &is_ccm);
    if (!cipher) {
        smb2w_die("unknown cipher");
    }

    memset(nonce, 0, sizeof(nonce));
    for (int i = 0; i < 8 && i < nonce_len; i++) {
        nonce[i] = (uint8_t) (nonce_counter >> (8 * i));
    }

    memset(out, 0, SMB2W_XFORM_SIZE);
    out[0] = 0xFD; out[1] = 'S'; out[2] = 'M'; out[3] = 'B';
    memcpy(out + 20, nonce, 16);
    p32(out, 36, (uint32_t) plain_len);
    p16(out, 42, SMB2W_XFORM_FLAG_ENC);
    p64(out, 44, session_id);

    memcpy(ct, plain, plain_len);

    c = EVP_CIPHER_CTX_new();
    if (!c) {
        smb2w_die("EVP_CIPHER_CTX_new");
    }
    if (EVP_EncryptInit_ex(c, cipher, NULL, NULL, NULL) != 1) {
        smb2w_die("EncryptInit");
    }
    if (is_ccm) {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_TAG, 16, NULL) != 1 ||
            EVP_EncryptInit_ex(c, NULL, NULL, key, nonce) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl, NULL, plain_len) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl, out + SMB2W_XFORM_AAD_OFF,
                              SMB2W_XFORM_AAD_LEN) != 1 ||
            EVP_EncryptUpdate(c, ct, &outl, ct, plain_len) != 1 ||
            EVP_EncryptFinal_ex(c, ct + outl, &outl) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_GET_TAG, 16, out + 4) != 1) {
            smb2w_die("AES-CCM encrypt");
        }
    } else {
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, nonce_len, NULL) != 1 ||
            EVP_EncryptInit_ex(c, NULL, NULL, key, nonce) != 1 ||
            EVP_EncryptUpdate(c, NULL, &outl, out + SMB2W_XFORM_AAD_OFF,
                              SMB2W_XFORM_AAD_LEN) != 1 ||
            EVP_EncryptUpdate(c, ct, &outl, ct, plain_len) != 1 ||
            EVP_EncryptFinal_ex(c, ct + outl, &outl) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_GET_TAG, 16, out + 4) != 1) {
            smb2w_die("AES-GCM encrypt");
        }
    }
    EVP_CIPHER_CTX_free(c);

    (void) key_len;
    return SMB2W_XFORM_SIZE + plain_len;
} /* smb2w_encrypt */

/* Unwrap a TRANSFORM_HEADER-framed message into `out`.  Returns the plaintext
 * length, or -1 if the tag does not verify (which is a server bug or a key
 * disagreement, never something the corpus should produce). */
static inline int
smb2w_decrypt(
    uint16_t       cipher_id,
    const uint8_t *key,
    size_t         key_len,
    const uint8_t *xform,
    int            xform_len,
    uint8_t       *out)
{
    const EVP_CIPHER *cipher;
    EVP_CIPHER_CTX   *c;
    const uint8_t    *ct = xform + SMB2W_XFORM_SIZE;
    uint8_t           nonce[16], tag[16];
    int               nonce_len, is_ccm, outl, ct_len, ok;
    uint32_t          orig_len;

    if (xform_len < SMB2W_XFORM_SIZE) {
        return -1;
    }
    cipher = smb2w_cipher(cipher_id, &nonce_len, &is_ccm);
    if (!cipher) {
        return -1;
    }

    orig_len = g32(xform, 36);
    ct_len   = xform_len - SMB2W_XFORM_SIZE;
    if ((uint32_t) ct_len != orig_len) {
        return -1;
    }

    memset(nonce, 0, sizeof(nonce));
    memcpy(nonce, xform + 20, (size_t) nonce_len);
    memcpy(tag, xform + 4, 16);

    c = EVP_CIPHER_CTX_new();
    if (!c) {
        return -1;
    }
    ok = EVP_DecryptInit_ex(c, cipher, NULL, NULL, NULL) == 1;
    if (is_ccm) {
        ok = ok &&
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_IVLEN, nonce_len, NULL) == 1 &&
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_CCM_SET_TAG, 16, tag) == 1 &&
            EVP_DecryptInit_ex(c, NULL, NULL, key, nonce) == 1 &&
            EVP_DecryptUpdate(c, NULL, &outl, NULL, ct_len) == 1 &&
            EVP_DecryptUpdate(c, NULL, &outl, xform + SMB2W_XFORM_AAD_OFF,
                              SMB2W_XFORM_AAD_LEN) == 1 &&
            /* CCM reports authentication through DecryptUpdate's return; there
             * is no Final step. */
            EVP_DecryptUpdate(c, out, &outl, ct, ct_len) == 1;
    } else {
        ok = ok &&
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, nonce_len, NULL) == 1 &&
            EVP_DecryptInit_ex(c, NULL, NULL, key, nonce) == 1 &&
            EVP_DecryptUpdate(c, NULL, &outl, xform + SMB2W_XFORM_AAD_OFF,
                              SMB2W_XFORM_AAD_LEN) == 1 &&
            EVP_DecryptUpdate(c, out, &outl, ct, ct_len) == 1 &&
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_TAG, 16, tag) == 1 &&
            EVP_DecryptFinal_ex(c, out + outl, &outl) == 1;
    }
    EVP_CIPHER_CTX_free(c);

    (void) key_len;
    return ok ? ct_len : -1;
} /* smb2w_decrypt */

/* ---- NTLMv2 ------------------------------------------------------------- */

/* NTOWFv2 = HMAC-MD5(MD4(UTF16LE(password)), UTF16LE(UPPER(user) || domain)) */
static inline void
smb2w_ntowfv2(
    const char *user,
    const char *password,
    const char *domain,
    uint8_t    *out16)
{
    uint8_t      nt_hash[16];
    uint8_t      pw16[512], id16[512];
    char         upper[128];
    int          pwlen, idlen = 0;
    unsigned int len;
    EVP_MD_CTX  *md;
    size_t       i;

    smb2w_need_legacy();

    pwlen = utf16le(password, pw16);

    md = EVP_MD_CTX_new();
    if (!md ||
        EVP_DigestInit_ex(md, EVP_md4(), NULL) != 1 ||
        EVP_DigestUpdate(md, pw16, pwlen) != 1 ||
        EVP_DigestFinal_ex(md, nt_hash, &len) != 1) {
        smb2w_die("MD4 NT hash (is the OpenSSL legacy provider available?)");
    }
    EVP_MD_CTX_free(md);

    for (i = 0; i < sizeof(upper) - 1 && user[i]; i++) {
        upper[i] = (char) toupper((unsigned char) user[i]);
    }
    upper[i] = '\0';

    idlen  = utf16le(upper, id16);
    idlen += utf16le(domain ? domain : "", id16 + idlen);

    if (!HMAC(EVP_md5(), nt_hash, 16, id16, idlen, out16, &len)) {
        smb2w_die("HMAC-MD5 NTOWFv2");
    }
} /* smb2w_ntowfv2 */

/* Build the NTLMSSP NEGOTIATE (type 1) message. */
static inline int
smb2w_ntlm_negotiate(
    uint8_t *o,
    int      anonymous)
{
    uint32_t flags = SMB2W_NTLMSSP_UNICODE | SMB2W_NTLMSSP_NTLM |
        SMB2W_NTLMSSP_REQ_TARGET;

    if (anonymous) {
        flags |= SMB2W_NTLMSSP_ANONYMOUS;
    } else {
        flags |= SMB2W_NTLMSSP_SIGN | SMB2W_NTLMSSP_ALWAYS_SIGN |
            SMB2W_NTLMSSP_EXT_SESSEC | SMB2W_NTLMSSP_KEY_EXCH |
            SMB2W_NTLMSSP_128;
    }

    memset(o, 0, 32);
    memcpy(o, "NTLMSSP\0", 8);
    p32(o, 8, 1);
    p32(o, 12, flags);
    return 32;
} /* smb2w_ntlm_negotiate */

/* Build the anonymous NTLMSSP AUTHENTICATE (type 3): no credentials at all,
 * which is how a null session is requested. */
static inline int
smb2w_ntlm_auth_anon(uint8_t *o)
{
    memset(o, 0, 88);
    memcpy(o, "NTLMSSP\0", 8);
    p32(o, 8, 3);
    p32(o, 16, 88);                   /* LmChallengeResponse offset */
    p32(o, 24, 88);                   /* NtChallengeResponse offset */
    p32(o, 32, 88);                   /* DomainName offset */
    p32(o, 40, 88);                   /* UserName offset */
    p32(o, 48, 88);                   /* Workstation offset */
    p32(o, 56, 88);                   /* EncryptedRandomSessionKey offset */
    p32(o, 60, SMB2W_NTLMSSP_ANONYMOUS | SMB2W_NTLMSSP_UNICODE);
    return 88;
} /* smb2w_ntlm_auth_anon */

/* Build an NTLMv2 AUTHENTICATE (type 3) in reply to the server's CHALLENGE,
 * and hand back the session key the SMB3 keys derive from.
 *
 * The NTv2 response is NTProofStr || blob, where the blob carries a timestamp,
 * a client challenge and the server's own TargetInfo echoed back verbatim; the
 * server recomputes the proof over ServerChallenge || blob, so the blob's
 * contents matter only in that both sides must see the same bytes.  With
 * KEY_EXCH negotiated the real session key is a random value wrapped under the
 * key-exchange key, which is the path Windows clients take and therefore the
 * one worth exercising (MS-NLMP 3.1.5.1.2). */
static inline int
smb2w_ntlm_auth_ntlmv2(
    const uint8_t *challenge,     /* the type 2 message */
    int            challenge_len,
    const char    *user,
    const char    *password,
    const char    *domain,
    uint8_t       *o,
    uint8_t       *session_key16)
{
    uint8_t        ntowf[16], proof[16], kxkey[16], blob[1024];
    uint8_t        nt_resp[1040], client_chal[8], hmac_in[2048];
    const uint8_t *server_chal, *target_info;
    uint16_t       ti_len;
    uint32_t       ti_off;
    int            blob_len = 0, nt_len, off, dlen, ulen, elen;
    unsigned int   len;

    if (challenge_len < 48) {
        smb2w_die("NTLM CHALLENGE too short");
    }
    server_chal = challenge + 24;
    ti_len      = g16(challenge, 40);
    ti_off      = g32(challenge, 44);
    if ((size_t) ti_off + ti_len > (size_t) challenge_len ||
        ti_len > sizeof(blob) - 32) {
        ti_len = 0;
        ti_off = 0;
    }
    target_info = challenge + ti_off;

    smb2w_ntowfv2(user, password, domain, ntowf);

    if (RAND_bytes(client_chal, sizeof(client_chal)) != 1) {
        smb2w_die("RAND_bytes");
    }

    /* NTLMv2_CLIENT_CHALLENGE (MS-NLMP 2.2.2.7). */
    memset(blob, 0, 32);
    blob[0] = 1;                                   /* RespType */
    blob[1] = 1;                                   /* HiRespType */
    memcpy(blob + 8, "\0\0\0\0\0\0\0\0", 8);       /* TimeStamp */
    memcpy(blob + 16, client_chal, 8);
    blob_len = 28;
    if (ti_len) {
        memcpy(blob + blob_len, target_info, ti_len);
        blob_len += ti_len;
    }
    memset(blob + blob_len, 0, 4);                 /* MsvAvEOL / padding */
    blob_len += 4;

    /* NTProofStr = HMAC-MD5(NTOWFv2, ServerChallenge || blob) */
    memcpy(hmac_in, server_chal, 8);
    memcpy(hmac_in + 8, blob, blob_len);
    if (!HMAC(EVP_md5(), ntowf, 16, hmac_in, 8 + blob_len, proof, &len)) {
        smb2w_die("HMAC-MD5 NTProofStr");
    }

    memcpy(nt_resp, proof, 16);
    memcpy(nt_resp + 16, blob, blob_len);
    nt_len = 16 + blob_len;

    /* Key-exchange key = HMAC-MD5(NTOWFv2, NTProofStr); the exported session
     * key rides RC4-wrapped under it. */
    if (!HMAC(EVP_md5(), ntowf, 16, proof, 16, kxkey, &len)) {
        smb2w_die("HMAC-MD5 key-exchange key");
    }
    if (RAND_bytes(session_key16, 16) != 1) {
        smb2w_die("RAND_bytes");
    }

    memset(o, 0, 88);
    memcpy(o, "NTLMSSP\0", 8);
    p32(o, 8, 3);

    off = 88;

    /* LmChallengeResponse: 24 zero bytes (unused with NTLMv2). */
    p16(o, 12, 24); p16(o, 14, 24); p32(o, 16, off);
    memset(o + off, 0, 24);
    off += 24;

    /* NtChallengeResponse */
    p16(o, 20, (uint16_t) nt_len); p16(o, 22, (uint16_t) nt_len); p32(o, 24, off);
    memcpy(o + off, nt_resp, nt_len);
    off += nt_len;

    /* DomainName */
    dlen = utf16le(domain ? domain : "", o + off);
    p16(o, 28, (uint16_t) dlen); p16(o, 30, (uint16_t) dlen); p32(o, 32, off);
    off += dlen;

    /* UserName */
    ulen = utf16le(user, o + off);
    p16(o, 36, (uint16_t) ulen); p16(o, 38, (uint16_t) ulen); p32(o, 40, off);
    off += ulen;

    /* Workstation */
    p16(o, 44, 0); p16(o, 46, 0); p32(o, 48, off);

    /* EncryptedRandomSessionKey = RC4(kxkey, session key) */
    {
        EVP_CIPHER_CTX *rc4  = EVP_CIPHER_CTX_new();
        int             outl = 0;

        if (!rc4 ||
            EVP_EncryptInit_ex(rc4, EVP_rc4(), NULL, kxkey, NULL) != 1 ||
            EVP_EncryptUpdate(rc4, o + off, &outl, session_key16, 16) != 1 ||
            outl != 16) {
            smb2w_die("RC4 session-key wrap");
        }
        EVP_CIPHER_CTX_free(rc4);
    }
    elen = 16;
    p16(o, 52, (uint16_t) elen); p16(o, 54, (uint16_t) elen); p32(o, 56, off);
    off += elen;

    p32(o, 60, SMB2W_NTLMSSP_UNICODE | SMB2W_NTLMSSP_NTLM |
        SMB2W_NTLMSSP_REQ_TARGET | SMB2W_NTLMSSP_SIGN |
        SMB2W_NTLMSSP_ALWAYS_SIGN | SMB2W_NTLMSSP_EXT_SESSEC |
        SMB2W_NTLMSSP_KEY_EXCH | SMB2W_NTLMSSP_128);

    return off;
} /* smb2w_ntlm_auth_ntlmv2 */

/* ---- SMB3 transport compression (MS-SMB2 2.2.42) ------------------------ */

/* The codecs themselves are the server's.  Reimplementing LZ77, LZNT1 and
 * LZ77+Huffman for the client would be a second bug surface rather than a
 * second opinion, and the asymmetric check that a shared codec cannot give is
 * supplied instead by the MS-XCA reference vectors in smb2_compress_probe.c --
 * bytes produced by Windows, not by chimera.  What IS independent here is the
 * framing above: field offsets, the chained payload walk, and the bounds. */
#include "server/smb/smb_compress.h"

/* Dispatch one raw codec segment. */
static inline int
smb2w_decompress_codec(
    uint16_t       alg,
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_cap)
{
    switch (alg) {
        case SMB2_COMPRESSION_LZNT1:
            return chimera_smb_lznt1_decompress(in, in_len, out, out_cap);
        case SMB2_COMPRESSION_LZ77:
            return chimera_smb_lz77_decompress(in, in_len, out, out_cap);
        case SMB2_COMPRESSION_LZ77_HUFFMAN:
            return chimera_smb_lz77huffman_decompress(in, in_len, out, out_cap);
        default:
            return -1;
    } /* switch */
} /* smb2w_decompress_codec */

/* One chained payload (2.2.42.2.1/2.2.42.2.2).  Pattern_V1 is a run-length
 * payload -- Pattern(1), Reserved1(1), Reserved2(2), Repetitions(4) -- and
 * NONE is a verbatim copy; neither is a codec call. */
static inline int
smb2w_decompress_payload(
    uint16_t       alg,
    const uint8_t *payload,
    int            payload_len,
    uint8_t       *out,
    int            out_avail)
{
    switch (alg) {
        case SMB2_COMPRESSION_NONE:
            if (payload_len > out_avail) {
                return -1;
            }
            memcpy(out, payload, (size_t) payload_len);
            return payload_len;

        case SMB2_COMPRESSION_PATTERN_V1: {
            uint32_t reps;

            if (payload_len != 8) {
                return -1;
            }
            reps = g32(payload, 4);
            if (reps > (uint32_t) out_avail) {
                return -1;
            }
            memset(out, payload[0], (size_t) reps);
            return (int) reps;
        }

        default: {
            /* A real codec inside a chain carries its decompressed size ahead
             * of the data (MS-SMB2 2.2.42.2.1 OriginalPayloadSize), and the
             * header's Length counts those 4 bytes.  Without it the chain is
             * undecodable: a codec payload is not the last one, so the
             * remaining output space is not its size. */
            uint32_t orig;

            if (payload_len < 4) {
                return -1;
            }
            orig = g32(payload, 0);
            if (orig > (uint32_t) out_avail) {
                return -1;
            }
            if (smb2w_decompress_codec(alg, payload + 4, payload_len - 4,
                                       out, (int) orig) != (int) orig) {
                return -1;
            }
            return (int) orig;
        }
    } /* switch */
} /* smb2w_decompress_payload */



/* COMPRESSION_TRANSFORM_HEADER, unchained (2.2.42.1): 16 bytes.  The chained
 * form (2.2.42.2) shares the first 8 and is followed by payload headers. */
#define SMB2W_CXFORM_SIZE         16
#define SMB2W_CXFORM_CHAINED_SIZE 8
#define SMB2W_CXFORM_PAYLOAD_SIZE 8

/* Dispatch one raw codec segment for compression. */
static inline int
smb2w_compress_codec(
    uint16_t       alg,
    const uint8_t *in,
    int            in_len,
    uint8_t       *out,
    int            out_cap)
{
    switch (alg) {
        case SMB2_COMPRESSION_LZNT1:
            return chimera_smb_lznt1_compress(in, in_len, out, out_cap);
        case SMB2_COMPRESSION_LZ77:
            return chimera_smb_lz77_compress(in, in_len, out, out_cap);
        case SMB2_COMPRESSION_LZ77_HUFFMAN:
            return chimera_smb_lz77huffman_compress(in, in_len, out, out_cap);
        default:
            return -1;
    } /* switch */
} /* smb2w_compress_codec */

/* Wrap one plaintext SMB2 REQUEST in an unchained COMPRESSION_TRANSFORM
 * (MS-SMB2 2.2.42.1), leaving the first `prefix` bytes uncompressed as the
 * transform's uncompressed segment.  `out` receives the 16-byte header
 * followed by that prefix and the compressed remainder; returns the total byte
 * count, or -1 when the result would not shrink (the caller then sends
 * plaintext, exactly as the server does).
 *
 * A client that only ever DECOMPRESSES leaves the server's inbound path -- the
 * one that parses a peer's compressed bytes -- completely untested, which is
 * the direction that matters most: it is the side handling input it did not
 * produce.  Sending compressed requests is what reaches it. */
static inline int
smb2w_compress(
    uint16_t       alg,
    const uint8_t *plain,
    int            plain_len,
    int            prefix,
    uint8_t       *out,
    int            out_cap)
{
    int seg = plain_len - prefix;
    int clen;

    if (seg <= 0 || SMB2W_CXFORM_SIZE + prefix > out_cap) {
        return -1;
    }
    memcpy(out + SMB2W_CXFORM_SIZE, plain, (size_t) prefix);
    clen = smb2w_compress_codec(alg, plain + prefix, seg,
                                out + SMB2W_CXFORM_SIZE + prefix,
                                out_cap - SMB2W_CXFORM_SIZE - prefix);
    if (clen < 0 || SMB2W_CXFORM_SIZE + prefix + clen >= plain_len) {
        return -1;
    }

    out[0] = 0xFC; out[1] = 'S'; out[2] = 'M'; out[3] = 'B';
    p32(out, 4, (uint32_t) seg);      /* OriginalCompressedSegmentSize */
    p16(out, 8, alg);
    p16(out, 10, (uint16_t) SMB2_COMPRESSION_FLAG_NONE);
    p32(out, 12, (uint32_t) prefix);  /* Offset of the compressed segment */
    return SMB2W_CXFORM_SIZE + prefix + clen;
} /* smb2w_compress */

/* Wrap a request in the CHAINED form (MS-SMB2 2.2.42.2): the uncompressed
 * prefix becomes a leading NONE pass-through payload, and the rest a codec
 * payload carrying its OriginalPayloadSize.  Two payloads is the smallest
 * chain that is still a chain, which is all this needs to be -- its purpose is
 * to make the server's chained decoder run on bytes from a peer.  Returns the
 * transform length, or -1 if it would not shrink. */
static inline int
smb2w_compress_chained(
    uint16_t       alg,
    const uint8_t *plain,
    int            plain_len,
    int            prefix,
    uint8_t       *out,
    int            out_cap)
{
    int seg = plain_len - prefix;
    int pos = SMB2W_CXFORM_CHAINED_SIZE;
    int clen;

    if (seg <= 0 || prefix <= 0 ||
        pos + 8 + prefix + 8 + 4 > out_cap) {
        return -1;
    }

    out[0] = 0xFC; out[1] = 'S'; out[2] = 'M'; out[3] = 'B';
    /* OriginalCompressedSegmentSize is the WHOLE message for the chained form
     * (every payload's decompressed size summed), not just the codec part. */
    p32(out, 4, (uint32_t) plain_len);

    /* Leading NONE payload: the SMB2 header, verbatim. */
    p16(out, pos, (uint16_t) SMB2_COMPRESSION_NONE);
    p16(out, pos + 2, (uint16_t) SMB2_COMPRESSION_FLAG_CHAINED);
    p32(out, pos + 4, (uint32_t) prefix);
    pos += 8;
    memcpy(out + pos, plain, (size_t) prefix);
    pos += prefix;

    /* Codec payload: Length counts the 4-byte OriginalPayloadSize as well. */
    clen = smb2w_compress_codec(alg, plain + prefix, seg,
                                out + pos + 8 + 4, out_cap - pos - 8 - 4);
    if (clen < 0 || pos + 8 + 4 + clen >= plain_len) {
        return -1;
    }
    p16(out, pos, alg);
    p16(out, pos + 2, (uint16_t) SMB2_COMPRESSION_FLAG_NONE);
    p32(out, pos + 4, (uint32_t) (clen + 4));
    pos += 8;
    p32(out, pos, (uint32_t) seg);      /* OriginalPayloadSize */
    pos += 4 + clen;
    return pos;
} /* smb2w_compress_chained */

/* Unwrap a COMPRESSION_TRANSFORM reply into plaintext.  `in` points at the
 * transform header (past the 4-byte NetBIOS framing), `len` is its byte count;
 * returns the plaintext length written to `out`, or -1 on a malformed frame.
 *
 * The FRAMING here is deliberately a second, independent implementation rather
 * than a call into the server's chimera_smb_decompress_message: a loopback in
 * which both sides run the same code cannot fail on a wrong field offset,
 * because both would be wrong identically.  The CODECS below are the server's
 * -- reimplementing LZ77/LZNT1/Huffman would be a second bug surface, not a
 * second opinion -- so the asymmetric check on those lives in the reference
 * vectors in smb2_compress_probe.c, which come from MS-XCA and not from
 * chimera.  Between the two, a framing bug fails here and a codec bug fails
 * against the vectors. */
static inline int
smb2w_decompress(
    const uint8_t *in,
    int            len,
    uint8_t       *out,
    int            out_cap)
{
    uint32_t seg, prefix;
    int      chained;

    if (len < SMB2W_CXFORM_CHAINED_SIZE ||
        in[0] != 0xFC || in[1] != 'S' || in[2] != 'M' || in[3] != 'B') {
        return -1;
    }

    seg = g32(in, 4);   /* OriginalCompressedSegmentSize */

    /* Flags at offset 10 discriminates the two forms, and only the unchained
     * form has it -- so it may only be read once the length covers it. */
    chained = !(len >= SMB2W_CXFORM_SIZE && g16(in, 10) == SMB2_COMPRESSION_FLAG_NONE);
    prefix  = chained ? 0 : g32(in, 12);

    if ((uint64_t) prefix + seg > (uint64_t) out_cap) {
        return -1;
    }

    if (!chained) {
        uint16_t alg = g16(in, 8);
        int      n;

        if ((uint64_t) SMB2W_CXFORM_SIZE + prefix > (uint64_t) len) {
            return -1;
        }
        memcpy(out, in + SMB2W_CXFORM_SIZE, prefix);
        n = smb2w_decompress_codec(alg, in + SMB2W_CXFORM_SIZE + prefix,
                                   len - SMB2W_CXFORM_SIZE - (int) prefix,
                                   out + prefix, (int) seg);
        if (n != (int) seg) {
            return -1;
        }
        return (int) (prefix + seg);
    }

    {
        int pos = SMB2W_CXFORM_CHAINED_SIZE, outpos = 0;

        while (pos < len) {
            uint16_t palg;
            uint32_t plen;
            int      produced;

            if (pos + SMB2W_CXFORM_PAYLOAD_SIZE > len) {
                return -1;
            }
            palg = g16(in, pos);
            plen = g32(in, pos + 4);
            pos += SMB2W_CXFORM_PAYLOAD_SIZE;

            if (plen > (uint32_t) (len - pos)) {
                return -1;
            }
            produced = smb2w_decompress_payload(palg, in + pos, (int) plen,
                                                out + outpos, out_cap - outpos);
            if (produced < 0) {
                return -1;
            }
            outpos += produced;
            pos    += (int) plen;
        }
        return outpos == (int) seg ? outpos : -1;
    }
} /* smb2w_decompress */
