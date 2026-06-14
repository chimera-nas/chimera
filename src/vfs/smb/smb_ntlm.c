// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/provider.h>

#include "smb_ntlm.h"
#include "smb_internal.h"

/* NTLMSSP message types. */
#define NTLM_NEGOTIATE_MESSAGE                     0x00000001
#define NTLM_CHALLENGE_MESSAGE                     0x00000002
#define NTLM_AUTHENTICATE_MESSAGE                  0x00000003

/* NTLMSSP negotiate flags (subset). */
#define NTLMSSP_NEGOTIATE_128                      0x20000000
#define NTLMSSP_NEGOTIATE_TARGET_INFO              0x00800000
#define NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY 0x00080000
#define NTLMSSP_NEGOTIATE_NTLM                     0x00000200
#define NTLMSSP_REQUEST_TARGET                     0x00000004
#define NTLMSSP_NEGOTIATE_UNICODE                  0x00000001

/* OpenSSL 3.0 requires the legacy provider for MD4 (the NT hash). */
static OSSL_PROVIDER *smb_ntlm_legacy_provider;
static OSSL_PROVIDER *smb_ntlm_default_provider;
static int            smb_ntlm_providers_loaded;

static void
smb_ntlm_ensure_legacy_provider(void)
{
    if (smb_ntlm_providers_loaded) {
        return;
    }
    smb_ntlm_legacy_provider  = OSSL_PROVIDER_load(NULL, "legacy");
    smb_ntlm_default_provider = OSSL_PROVIDER_load(NULL, "default");
    smb_ntlm_providers_loaded = 1;
} /* smb_ntlm_ensure_legacy_provider */

/* Convert an ASCII/UTF-8 string to UTF-16LE.  Writes 2*len bytes to out (which
 * must be large enough) and returns the byte count. */
static size_t
smb_ntlm_utf16le(
    const char *in,
    uint8_t    *out)
{
    size_t len = in ? strlen(in) : 0;
    size_t i;

    for (i = 0; i < len; i++) {
        out[i * 2]     = (uint8_t) in[i];
        out[i * 2 + 1] = 0;
    }
    return len * 2;
} /* smb_ntlm_utf16le */

/* NT hash = MD4(UTF16LE(password)). */
static int
smb_ntlm_nt_hash(
    const char *password,
    uint8_t     nt_hash[16])
{
    uint8_t       utf16[512];
    size_t        utf16_len;
    EVP_MD_CTX   *ctx;
    const EVP_MD *md4;
    unsigned int  hash_len;
    int           rc = -1;

    smb_ntlm_ensure_legacy_provider();

    utf16_len = smb_ntlm_utf16le(password, utf16);

    ctx = EVP_MD_CTX_new();
    if (!ctx) {
        return -1;
    }

    md4 = EVP_md4();
    if (md4 &&
        EVP_DigestInit_ex(ctx, md4, NULL) == 1 &&
        EVP_DigestUpdate(ctx, utf16, utf16_len) == 1 &&
        EVP_DigestFinal_ex(ctx, nt_hash, &hash_len) == 1) {
        rc = 0;
    }

    EVP_MD_CTX_free(ctx);
    return rc;
} /* smb_ntlm_nt_hash */

/* ntlmv2_hash = HMAC-MD5(NT_hash, UTF16LE(UPPER(user)) || UTF16LE(domain)). */
static int
smb_ntlm_v2_hash(
    const char *user,
    const char *password,
    const char *domain,
    uint8_t     ntlmv2_hash[16])
{
    uint8_t      nt_hash[16];
    char         user_upper[256];
    uint8_t      concat[1024];
    size_t       user_len, concat_len;
    unsigned int hmac_len;
    size_t       i;

    if (smb_ntlm_nt_hash(password, nt_hash) < 0) {
        return -1;
    }

    user_len = strlen(user);
    if (user_len >= sizeof(user_upper)) {
        return -1;
    }
    for (i = 0; i < user_len; i++) {
        user_upper[i] = (char) toupper((unsigned char) user[i]);
    }
    user_upper[user_len] = '\0';

    concat_len  = smb_ntlm_utf16le(user_upper, concat);
    concat_len += smb_ntlm_utf16le(domain, concat + concat_len);

    if (!HMAC(EVP_md5(), nt_hash, 16, concat, concat_len, ntlmv2_hash, &hmac_len)) {
        return -1;
    }
    return 0;
} /* smb_ntlm_v2_hash */

void
smb_ntlm_client_init(
    struct smb_ntlm_client *c,
    const char             *user,
    const char             *domain,
    const char             *password)
{
    memset(c, 0, sizeof(*c));
    snprintf(c->user, sizeof(c->user), "%s", user ? user : "");
    snprintf(c->domain, sizeof(c->domain), "%s", domain ? domain : "");
    snprintf(c->password, sizeof(c->password), "%s", password ? password : "");
} /* smb_ntlm_client_init */

int
smb_ntlm_client_build_negotiate(
    uint8_t *out,
    size_t   out_max)
{
    uint32_t flags;

    /* Fixed 32-byte NTLMSSP_NEGOTIATE: signature(8) + type(4) + flags(4) +
     * DomainNameFields(8, empty) + WorkstationFields(8, empty).  The chimera
     * server ignores these fields on the first leg (it generates its own
     * CHALLENGE), so a minimal message is sufficient. */
    if (out_max < 32) {
        return -1;
    }

    memset(out, 0, 32);
    memcpy(out, "NTLMSSP\0", 8);
    smb_wire_set_le32(out + 8, NTLM_NEGOTIATE_MESSAGE);

    flags = NTLMSSP_NEGOTIATE_UNICODE | NTLMSSP_REQUEST_TARGET |
        NTLMSSP_NEGOTIATE_NTLM | NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY |
        NTLMSSP_NEGOTIATE_TARGET_INFO | NTLMSSP_NEGOTIATE_128;
    smb_wire_set_le32(out + 12, flags);

    return 32;
} /* smb_ntlm_client_build_negotiate */

int
smb_ntlm_client_parse_challenge(
    struct smb_ntlm_client *c,
    const uint8_t          *buf,
    size_t                  len)
{
    uint32_t msg_type;
    uint16_t ti_len;
    uint32_t ti_offset;

    /* Fixed CHALLENGE header is 48 bytes; target-info fields live at 40/44. */
    if (len < 48 || memcmp(buf, "NTLMSSP\0", 8) != 0) {
        return -1;
    }

    msg_type = smb_wire_le32(buf + 8);
    if (msg_type != NTLM_CHALLENGE_MESSAGE) {
        return -1;
    }

    memcpy(c->server_challenge, buf + 24, SMB_NTLM_CLIENT_CHALLENGE_SIZE);

    ti_len    = smb_wire_le16(buf + 40);
    ti_offset = smb_wire_le32(buf + 44);

    c->target_info_len = 0;
    if (ti_len > 0) {
        if (ti_len > SMB_NTLM_CLIENT_TARGET_INFO_MAX ||
            (size_t) ti_offset + ti_len > len) {
            return -1;
        }
        memcpy(c->target_info, buf + ti_offset, ti_len);
        c->target_info_len = ti_len;
    }

    return 0;
} /* smb_ntlm_client_parse_challenge */

int
smb_ntlm_client_build_authenticate(
    struct smb_ntlm_client *c,
    uint8_t                *out,
    size_t                  out_max,
    size_t                 *out_len)
{
    uint8_t      ntlmv2_hash[16];
    uint8_t      client_blob[64 + SMB_NTLM_CLIENT_TARGET_INFO_MAX];
    size_t       blob_len;
    uint8_t      nt_proof[16];
    uint8_t     *hmac_input;
    size_t       hmac_input_len;
    unsigned int hmac_len;
    uint64_t     filetime;
    uint32_t     flags;

    uint8_t      dom16[512], user16[512];
    size_t       dom16_len, user16_len;

    size_t       domain_off, user_off, ws_off, lm_off, nt_off;
    size_t       nt_response_len;
    size_t       pos;

    if (smb_ntlm_v2_hash(c->user, c->password, c->domain, ntlmv2_hash) < 0) {
        return -1;
    }

    /* Build the NTLMv2 client blob: RespType(1)=1, HiRespType(1)=1,
     * Reserved(6), Timestamp(8, FILETIME), ClientChallenge(8), Reserved(4),
     * TargetInfo(var), Reserved(4).  The exact contents are irrelevant to the
     * server beyond being echoed into the HMAC -- it recomputes NTProofStr over
     * the same bytes. */
    filetime = ((uint64_t) time(NULL) + 11644473600ULL) * 10000000ULL;

    blob_len                = 0;
    client_blob[blob_len++] = 0x01;            /* RespType   */
    client_blob[blob_len++] = 0x01;            /* HiRespType */
    memset(client_blob + blob_len, 0, 6);      /* Reserved   */
    blob_len += 6;
    smb_wire_set_le64(client_blob + blob_len, filetime);
    blob_len += 8;
    if (RAND_bytes(client_blob + blob_len, 8) != 1) {
        return -1;
    }
    blob_len += 8;
    memset(client_blob + blob_len, 0, 4);      /* Reserved */
    blob_len += 4;
    memcpy(client_blob + blob_len, c->target_info, c->target_info_len);
    blob_len += c->target_info_len;
    memset(client_blob + blob_len, 0, 4);      /* Reserved (trailing) */
    blob_len += 4;

    /* NTProofStr = HMAC-MD5(ntlmv2_hash, server_challenge || client_blob). */
    hmac_input_len = SMB_NTLM_CLIENT_CHALLENGE_SIZE + blob_len;
    hmac_input     = malloc(hmac_input_len);
    if (!hmac_input) {
        return -1;
    }
    memcpy(hmac_input, c->server_challenge, SMB_NTLM_CLIENT_CHALLENGE_SIZE);
    memcpy(hmac_input + SMB_NTLM_CLIENT_CHALLENGE_SIZE, client_blob, blob_len);

    if (!HMAC(EVP_md5(), ntlmv2_hash, 16, hmac_input, hmac_input_len, nt_proof, &hmac_len)) {
        free(hmac_input);
        return -1;
    }
    free(hmac_input);

    /* Session base key (== SMB2 session key; no key exchange negotiated) =
     * HMAC-MD5(ntlmv2_hash, NTProofStr). */
    if (!HMAC(EVP_md5(), ntlmv2_hash, 16, nt_proof, 16, c->session_key, &hmac_len)) {
        return -1;
    }
    c->have_session_key = 1;

    /* NtChallengeResponse = NTProofStr(16) || client_blob. */
    nt_response_len = 16 + blob_len;

    dom16_len  = smb_ntlm_utf16le(c->domain, dom16);
    user16_len = smb_ntlm_utf16le(c->user, user16);

    /* Lay out the AUTHENTICATE message.  Fixed header is 64 bytes (offsets the
     * server reads in validate_authenticate), followed by the payload. */
    domain_off = 64;
    user_off   = domain_off + dom16_len;
    ws_off     = user_off + user16_len;
    lm_off     = ws_off;                       /* zero-length workstation */
    nt_off     = lm_off + 24;                  /* 24-byte LM response slot */

    if (out_max < nt_off + nt_response_len) {
        return -1;
    }

    memset(out, 0, nt_off + nt_response_len);
    memcpy(out, "NTLMSSP\0", 8);
    smb_wire_set_le32(out + 8, NTLM_AUTHENTICATE_MESSAGE);

    /* LmChallengeResponse fields (offset 12): 24 zero bytes. */
    smb_wire_set_le16(out + 12, 24);
    smb_wire_set_le16(out + 14, 24);
    smb_wire_set_le32(out + 16, (uint32_t) lm_off);

    /* NtChallengeResponse fields (offset 20). */
    smb_wire_set_le16(out + 20, (uint16_t) nt_response_len);
    smb_wire_set_le16(out + 22, (uint16_t) nt_response_len);
    smb_wire_set_le32(out + 24, (uint32_t) nt_off);

    /* DomainName fields (offset 28). */
    smb_wire_set_le16(out + 28, (uint16_t) dom16_len);
    smb_wire_set_le16(out + 30, (uint16_t) dom16_len);
    smb_wire_set_le32(out + 32, (uint32_t) domain_off);

    /* UserName fields (offset 36). */
    smb_wire_set_le16(out + 36, (uint16_t) user16_len);
    smb_wire_set_le16(out + 38, (uint16_t) user16_len);
    smb_wire_set_le32(out + 40, (uint32_t) user_off);

    /* Workstation fields (offset 44): empty. */
    smb_wire_set_le16(out + 44, 0);
    smb_wire_set_le16(out + 46, 0);
    smb_wire_set_le32(out + 48, (uint32_t) ws_off);

    /* EncryptedRandomSessionKey fields (offset 52): absent. */
    smb_wire_set_le16(out + 52, 0);
    smb_wire_set_le16(out + 54, 0);
    smb_wire_set_le32(out + 56, (uint32_t) nt_off + nt_response_len);

    /* NegotiateFlags (offset 60).  KEY_EXCH deliberately omitted. */
    flags = NTLMSSP_NEGOTIATE_UNICODE | NTLMSSP_REQUEST_TARGET |
        NTLMSSP_NEGOTIATE_NTLM | NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY |
        NTLMSSP_NEGOTIATE_TARGET_INFO | NTLMSSP_NEGOTIATE_128;
    smb_wire_set_le32(out + 60, flags);

    /* Payload. */
    pos = domain_off;
    memcpy(out + pos, dom16, dom16_len);
    pos = user_off;
    memcpy(out + pos, user16, user16_len);
    /* workstation: empty */
    memset(out + lm_off, 0, 24);               /* LM response */
    memcpy(out + nt_off, nt_proof, 16);        /* NTProofStr */
    memcpy(out + nt_off + 16, client_blob, blob_len);

    *out_len = nt_off + nt_response_len;
    return 0;
} /* smb_ntlm_client_build_authenticate */
