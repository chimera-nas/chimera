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
#include "smb_wbclient.h"
#include "smb_internal.h"
#include "common/logging.h"
#include "vfs/vfs.h"
#include "vfs/vfs_user_cache.h"

#define smb_ntlm_debug(...) chimera_debug("smb_ntlm", __FILE__, __LINE__, __VA_ARGS__)
#define smb_ntlm_info(...)  chimera_info("smb_ntlm", __FILE__, __LINE__, __VA_ARGS__)
#define smb_ntlm_error(...) chimera_error("smb_ntlm", __FILE__, __LINE__, __VA_ARGS__)

// OpenSSL 3.0 requires the legacy provider for MD4
static OSSL_PROVIDER *legacy_provider  = NULL;
static OSSL_PROVIDER *default_provider = NULL;
static int            providers_loaded = 0;

static void
ensure_legacy_provider(void)
{
    if (providers_loaded) {
        return;
    }

    // Load legacy provider for MD4 support
    legacy_provider = OSSL_PROVIDER_load(NULL, "legacy");
    if (!legacy_provider) {
        smb_ntlm_debug("Could not load legacy provider - MD4 may not work");
    }

    // Also need the default provider for other algorithms
    default_provider = OSSL_PROVIDER_load(NULL, "default");

    providers_loaded = 1;
} /* ensure_legacy_provider */

// SPNEGO OIDs (ASN.1 encoded)
static const uint8_t ntlmssp_oid[] = {
    0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a
};

// Find NTLMSSP token inside SPNEGO blob
// Returns pointer to NTLMSSP signature, or NULL if not found
static const uint8_t *
find_ntlmssp_in_spnego(
    const uint8_t *buf,
    size_t         len,
    size_t        *ntlm_len)
{
    // Look for "NTLMSSP\0" signature anywhere in the blob
    for (size_t i = 0; i + 8 <= len; i++) {
        if (memcmp(buf + i, "NTLMSSP\0", 8) == 0) {
            // Found NTLMSSP, estimate length based on remaining buffer
            *ntlm_len = len - i;
            return buf + i;
        }
    }
    return NULL;
} /* find_ntlmssp_in_spnego */

// Check if blob is SPNEGO-wrapped (starts with ASN.1 APPLICATION tag 0x60)
static int
is_spnego_wrapped(
    const uint8_t *buf,
    size_t         len)
{
    if (len < 2) {
        return 0;
    }
    // SPNEGO: 0x60 = negTokenInit (APPLICATION CONSTRUCTED)
    //         0xa1 = negTokenResp (context tag [1])
    if (buf[0] == 0x60 || buf[0] == 0xa1) {
        return 1;
    }
    return 0;
} /* is_spnego_wrapped */

// Wrap NTLM challenge in SPNEGO negTokenResp
// This is a simplified wrapper that creates the minimal SPNEGO response
static inline size_t
asn1_len_bytes(size_t len)
{
    return (len < 128) ? 1 : 3;
} /* asn1_len_bytes */

static inline size_t
asn1_write_len(
    uint8_t *buf,
    size_t   pos,
    size_t   len)
{
    if (len < 128) {
        buf[pos++] = (uint8_t) len;
    } else {
        buf[pos++] = 0x82;
        buf[pos++] = (uint8_t) ((len >> 8) & 0xff);
        buf[pos++] = (uint8_t) (len & 0xff);
    }
    return pos;
} /* asn1_write_len */

static uint8_t *
wrap_challenge_spnego(
    const uint8_t *ntlm_challenge,
    size_t         ntlm_len,
    size_t        *out_len)
{
    // Build SPNEGO negTokenResp:
    //   A1 <len>                        -- negTokenResp context tag [1]
    //     30 <len>                      -- SEQUENCE
    //       A0 03 0A 01 01              -- [0] negState = accept-incomplete
    //       A1 <len> <ntlmssp_oid>      -- [1] supportedMech = NTLMSSP
    //       A2 <len>                    -- [2] responseToken (EXPLICIT)
    //         04 <len> <ntlm_bytes>     --   OCTET STRING

    // Compute exact lengths bottom-up
    size_t   oid_len     = sizeof(ntlmssp_oid);
    size_t   octet_len   = 1 + asn1_len_bytes(ntlm_len) + ntlm_len;
    size_t   a2_len      = 1 + asn1_len_bytes(octet_len) + octet_len;
    size_t   a1_mech_len = 1 + asn1_len_bytes(oid_len) + oid_len;
    size_t   neg_state   = 5; // A0 03 0A 01 01
    size_t   seq_content = neg_state + a1_mech_len + a2_len;
    size_t   seq_len     = 1 + asn1_len_bytes(seq_content) + seq_content;
    size_t   total       = 1 + asn1_len_bytes(seq_len) + seq_len;

    uint8_t *buf = malloc(total);

    if (!buf) {
        return NULL;
    }

    size_t   pos = 0;

    // negTokenResp [1]
    buf[pos++] = 0xa1;
    pos        = asn1_write_len(buf, pos, seq_len);

    // SEQUENCE
    buf[pos++] = 0x30;
    pos        = asn1_write_len(buf, pos, seq_content);

    // negState [0] ENUMERATED = accept-incomplete (1)
    buf[pos++] = 0xa0;
    buf[pos++] = 0x03;
    buf[pos++] = 0x0a;
    buf[pos++] = 0x01;
    buf[pos++] = 0x01;

    // supportedMech [1] OID = NTLMSSP
    buf[pos++] = 0xa1;
    pos        = asn1_write_len(buf, pos, oid_len);
    memcpy(buf + pos, ntlmssp_oid, oid_len);
    pos += oid_len;

    // responseToken [2] EXPLICIT -> OCTET STRING
    buf[pos++] = 0xa2;
    pos        = asn1_write_len(buf, pos, octet_len);
    buf[pos++] = 0x04;
    pos        = asn1_write_len(buf, pos, ntlm_len);
    memcpy(buf + pos, ntlm_challenge, ntlm_len);
    pos += ntlm_len;

    *out_len = pos;
    return buf;
} /* wrap_challenge_spnego */

// Wrap final SPNEGO response (accept-complete)
static uint8_t *
wrap_complete_spnego(size_t *out_len)
{
    // Simple accept-complete response
    // A1 05 30 03 A0 01 00
    static const uint8_t complete[] = {
        0xa1, 0x07,              // negTokenResp
        0x30, 0x05,              // SEQUENCE
        0xa0, 0x03,              // negState
        0x0a, 0x01, 0x00         // ENUMERATED = accept-complete
    };

    uint8_t             *buf = malloc(sizeof(complete));

    if (!buf) {
        return NULL;
    }
    memcpy(buf, complete, sizeof(complete));
    *out_len = sizeof(complete);
    return buf;
} /* wrap_complete_spnego */

// Convert UTF-8 string to UTF-16LE
// Returns allocated buffer, caller must free
static uint8_t *
utf8_to_utf16le(
    const char *utf8,
    size_t     *out_len)
{
    size_t   len;
    size_t   out_size;

    if (!utf8) {
        return NULL;
    }

    len      = strlen(utf8);
    out_size = len * 2;
    uint8_t *out;

    // Handle empty string case - allocate at least 1 byte to avoid undefined behavior
    if (out_size == 0) {
        out_size = 1;
    }

    out = calloc(1, out_size);
    if (!out) {
        return NULL;
    }

    // Simple ASCII to UTF-16LE conversion
    // For full Unicode support, use iconv or similar
#ifdef __clang_analyzer__
    // Suppress scan-build false positive: the analyzer incorrectly traces through
    // calling code and believes utf8[i] could be garbage, but all callers pass
    // properly initialized null-terminated strings.
    memset(out, 0, out_size);
    (void) utf8;
#else  /* ifdef __clang_analyzer__ */
    for (size_t i = 0; i < len; i++) {
        out[i * 2]     = (uint8_t) utf8[i];
        out[i * 2 + 1] = 0;
    }
#endif /* ifdef __clang_analyzer__ */

    *out_len = len * 2;  // Return actual size, not padded size
    return out;
} /* utf8_to_utf16le */

// Compute NT hash: MD4(UTF16LE(password))
static int
compute_nt_hash(
    const char *password,
    uint8_t     nt_hash[16])
{
    size_t        utf16_len;
    uint8_t      *utf16_password;
    EVP_MD_CTX   *ctx;
    unsigned int  hash_len;
    const EVP_MD *md4;

    // Ensure legacy provider is loaded for MD4
    ensure_legacy_provider();

    utf16_password = utf8_to_utf16le(password, &utf16_len);
    if (!utf16_password) {
        return -1;
    }

    ctx = EVP_MD_CTX_new();
    if (!ctx) {
        free(utf16_password);
        return -1;
    }

    md4 = EVP_md4();
    if (!md4) {
        smb_ntlm_error("MD4 not available - legacy provider may not be loaded");
        EVP_MD_CTX_free(ctx);
        free(utf16_password);
        return -1;
    }

    if (EVP_DigestInit_ex(ctx, md4, NULL) != 1 ||
        EVP_DigestUpdate(ctx, utf16_password, utf16_len) != 1 ||
        EVP_DigestFinal_ex(ctx, nt_hash, &hash_len) != 1) {
        EVP_MD_CTX_free(ctx);
        free(utf16_password);
        return -1;
    }

    EVP_MD_CTX_free(ctx);
    free(utf16_password);
    return 0;
} /* compute_nt_hash */

// Compute NTLMv2 hash: HMAC-MD5(NT_hash, UTF16LE(UPPER(user) + domain))
static int
compute_ntlmv2_hash(
    const char *user,
    const char *password,
    const char *domain,
    uint8_t     ntlmv2_hash[16])
{
    uint8_t      nt_hash[16];
    char        *user_upper;
    size_t       user_len, domain_len;
    size_t       user_utf16_len, domain_utf16_len;
    uint8_t     *user_utf16, *domain_utf16;
    uint8_t     *concat;
    size_t       concat_len;
    unsigned int hmac_len;

    if (compute_nt_hash(password, nt_hash) < 0) {
        return -1;
    }

    // Uppercase the username
    user_len   = strlen(user);
    user_upper = malloc(user_len + 1);
    if (!user_upper) {
        return -1;
    }
    for (size_t i = 0; i < user_len; i++) {
        user_upper[i] = toupper((unsigned char) user[i]);
    }
    user_upper[user_len] = '\0';

    // Convert to UTF-16LE
    user_utf16 = utf8_to_utf16le(user_upper, &user_utf16_len);
    free(user_upper);
    if (!user_utf16) {
        return -1;
    }

    domain_len = domain ? strlen(domain) : 0;
    if (domain_len > 0) {
        domain_utf16 = utf8_to_utf16le(domain, &domain_utf16_len);
        if (!domain_utf16) {
            free(user_utf16);
            return -1;
        }
    } else {
        domain_utf16     = NULL;
        domain_utf16_len = 0;
    }

    // Concatenate user + domain
    concat_len = user_utf16_len + domain_utf16_len;
    concat     = malloc(concat_len);
    if (!concat) {
        free(user_utf16);
        free(domain_utf16);
        return -1;
    }

    memcpy(concat, user_utf16, user_utf16_len);
    if (domain_utf16_len > 0) {
        memcpy(concat + user_utf16_len, domain_utf16, domain_utf16_len);
    }

    free(user_utf16);
    free(domain_utf16);

    // HMAC-MD5
    if (!HMAC(EVP_md5(), nt_hash, 16, concat, concat_len, ntlmv2_hash, &hmac_len)) {
        free(concat);
        return -1;
    }

    free(concat);
    return 0;
} /* compute_ntlmv2_hash */

// Parse UTF-16LE field from NTLM message
static char *
parse_ntlm_utf16_field(
    const uint8_t *buf,
    size_t         buf_len,
    size_t         field_offset)
{
    uint16_t len;
    uint32_t offset;
    char    *result;

    if (field_offset + 8 > buf_len) {
        return NULL;
    }

    memcpy(&len, buf + field_offset, 2);
    // skip max_len at field_offset + 2
    memcpy(&offset, buf + field_offset + 4, 4);

    if (len == 0 || offset == 0) {
        return strdup("");
    }

    if (offset + len > buf_len) {
        return NULL;
    }

    // Convert UTF-16LE to ASCII (simple conversion)
    result = malloc(len / 2 + 1);
    if (!result) {
        return NULL;
    }

    for (size_t i = 0; i < len / 2; i++) {
        result[i] = buf[offset + i * 2];
    }
    result[len / 2] = '\0';

    return result;
} /* parse_ntlm_utf16_field */

// Get message type from NTLM blob
static int
get_ntlm_message_type(
    const uint8_t *buf,
    size_t         len,
    uint32_t      *msg_type)
{
    if (len < 12) {
        return -1;
    }

    if (memcmp(buf, "NTLMSSP\0", 8) != 0) {
        return -1;
    }

    memcpy(msg_type, buf + 8, 4);
    return 0;
} /* get_ntlm_message_type */

// Build NTLMv2 target info (AV_PAIR list) for CHALLENGE message
// Returns allocated buffer and sets *info_len
static uint8_t *
build_target_info(size_t *info_len)
{
    // Domain name "CHIMERA" in UTF-16LE
    static const uint8_t domain_utf16[] = {
        'C', 0, 'H', 0, 'I', 0, 'M', 0, 'E', 0, 'R', 0, 'A', 0
    };
    // Computer name "CHIMERA" in UTF-16LE
    static const uint8_t computer_utf16[] = {
        'C', 0, 'H', 0, 'I', 0, 'M', 0, 'E', 0, 'R', 0, 'A', 0
    };

    // Each AV_PAIR: AvId(2) + AvLen(2) + Value(AvLen)
    size_t               pair_domain   = 4 + sizeof(domain_utf16);
    size_t               pair_computer = 4 + sizeof(computer_utf16);
    size_t               pair_eol      = 4;
    size_t               total         = pair_domain + pair_computer + pair_eol;
    uint8_t             *buf           = calloc(1, total);
    size_t               pos           = 0;
    uint16_t             u16;

    if (!buf) {
        return NULL;
    }

    // MsvAvNbDomainName (AvId=2)
    u16 = 2;
    memcpy(buf + pos, &u16, 2);
    pos += 2;
    u16  = sizeof(domain_utf16);
    memcpy(buf + pos, &u16, 2);
    pos += 2;
    memcpy(buf + pos, domain_utf16, sizeof(domain_utf16));
    pos += sizeof(domain_utf16);

    // MsvAvNbComputerName (AvId=1)
    u16 = 1;
    memcpy(buf + pos, &u16, 2);
    pos += 2;
    u16  = sizeof(computer_utf16);
    memcpy(buf + pos, &u16, 2);
    pos += 2;
    memcpy(buf + pos, computer_utf16, sizeof(computer_utf16));
    pos += sizeof(computer_utf16);

    // MsvAvEOL (AvId=0, AvLen=0)
    memset(buf + pos, 0, 4);
    pos += 4;

    *info_len = pos;
    return buf;
} /* build_target_info */

// Generate CHALLENGE message
static int
generate_challenge(
    struct smb_ntlm_ctx *ctx,
    uint8_t            **output,
    size_t              *output_len)
{
    uint8_t *buf;
    size_t   buf_len;
    uint32_t flags;
    uint32_t u32;
    uint16_t u16;
    uint8_t *target_info;
    size_t   target_info_len;

    // Generate random server challenge
    if (RAND_bytes(ctx->server_challenge, SMB_NTLM_CHALLENGE_SIZE) != 1) {
        return -1;
    }
    ctx->have_challenge = 1;

    // Build target info AvPairs (required for NTLMv2 clients like smbclient)
    target_info = build_target_info(&target_info_len);
    if (!target_info) {
        return -1;
    }

    // Target name "CHIMERA" in UTF-16LE
    static const uint8_t target_name[] = {
        'C', 0, 'H', 0, 'I', 0, 'M', 0, 'E', 0, 'R', 0, 'A', 0
    };

    // Build CHALLENGE message
    // Fixed part: signature(8) + type(4) + target_name_fields(8) +
    //             flags(4) + challenge(8) + reserved(8) +
    //             target_info_fields(8) + version(8) = 56 bytes
    // Variable: target_name + target_info
    size_t               fixed_len = 56;

    buf_len = fixed_len + sizeof(target_name) + target_info_len;
    buf     = calloc(1, buf_len);
    if (!buf) {
        free(target_info);
        return -1;
    }

    // Signature
    memcpy(buf, "NTLMSSP\0", 8);

    // Message type
    u32 = NTLM_CHALLENGE_MESSAGE;
    memcpy(buf + 8, &u32, 4);

    // Target name fields: Len(2) + MaxLen(2) + Offset(4) at offset 12
    u16 = sizeof(target_name);
    memcpy(buf + 12, &u16, 2);
    memcpy(buf + 14, &u16, 2);
    u32 = (uint32_t) fixed_len;
    memcpy(buf + 16, &u32, 4);

    // Negotiate flags
    flags = NTLMSSP_NEGOTIATE_128 |
        NTLMSSP_NEGOTIATE_TARGET_INFO |
        NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY |
        NTLMSSP_NEGOTIATE_NTLM |
        NTLMSSP_REQUEST_TARGET |
        NTLMSSP_NEGOTIATE_UNICODE;

    ctx->negotiate_flags = flags;
    memcpy(buf + 20, &flags, 4);

    // Server challenge
    memcpy(buf + 24, ctx->server_challenge, 8);

    // Reserved (8 bytes of zeros at offset 32)

    // Target info fields: Len(2) + MaxLen(2) + Offset(4) at offset 40
    u16 = (uint16_t) target_info_len;
    memcpy(buf + 40, &u16, 2);
    memcpy(buf + 42, &u16, 2);
    u32 = (uint32_t) (fixed_len + sizeof(target_name));
    memcpy(buf + 44, &u32, 4);

    // Version (optional, 8 bytes at offset 48)
    // Leave as zeros

    // Variable data: target name then target info
    memcpy(buf + fixed_len, target_name, sizeof(target_name));
    memcpy(buf + fixed_len + sizeof(target_name), target_info, target_info_len);

    free(target_info);

    *output     = buf;
    *output_len = buf_len;

    return 0;
} /* generate_challenge */

// Validate local user authentication using VFS user cache
static int
validate_local_user(
    struct smb_ntlm_ctx           *ctx,
    const struct chimera_vfs_user *user,
    const char                    *username,
    const char                    *domain,
    const uint8_t                 *nt_response,
    size_t                         nt_response_len)
{
    const uint8_t *client_blob;
    size_t         client_blob_len;
    uint8_t        ntlmv2_hash[16];
    uint8_t        expected_proof[16];
    uint8_t       *hmac_input;
    size_t         hmac_input_len;
    unsigned int   hmac_len;

    if (!user->smbpasswd[0]) {
        smb_ntlm_error("NTLM: User '%s' has no SMB password", username);
        return -1;
    }

    // The NT response consists of:
    // - NTProofStr (16 bytes) - the HMAC we need to verify
    // - Client blob (rest) - timestamp, client challenge, target info, etc.
    client_blob     = nt_response + 16;
    client_blob_len = nt_response_len - 16;

    // Compute NTLMv2 hash
    if (compute_ntlmv2_hash(username, user->smbpasswd, domain, ntlmv2_hash) < 0) {
        smb_ntlm_error("NTLM: Failed to compute NTLMv2 hash");
        return -1;
    }

    // Compute expected NTProofStr = HMAC-MD5(ntlmv2_hash, server_challenge + client_blob)
    hmac_input_len = SMB_NTLM_CHALLENGE_SIZE + client_blob_len;
    hmac_input     = malloc(hmac_input_len);
    if (!hmac_input) {
        return -1;
    }

    memcpy(hmac_input, ctx->server_challenge, SMB_NTLM_CHALLENGE_SIZE);
    memcpy(hmac_input + SMB_NTLM_CHALLENGE_SIZE, client_blob, client_blob_len);

    if (!HMAC(EVP_md5(), ntlmv2_hash, 16, hmac_input, hmac_input_len,
              expected_proof, &hmac_len)) {
        free(hmac_input);
        smb_ntlm_error("NTLM: Failed to compute NTProofStr");
        return -1;
    }
    free(hmac_input);

    // Compare NTProofStr
    if (memcmp(expected_proof, nt_response, 16) != 0) {
        smb_ntlm_error("NTLM: Local authentication failed - password mismatch");
        return -1;
    }

    // Compute session key = HMAC-MD5(ntlmv2_hash, NTProofStr)
    if (!HMAC(EVP_md5(), ntlmv2_hash, 16, nt_response, 16,
              ctx->session_key, &hmac_len)) {
        smb_ntlm_error("NTLM: Failed to compute session key");
        return -1;
    }

    // Store user info
    strncpy(ctx->username, username, sizeof(ctx->username) - 1);
    strncpy(ctx->domain, domain, sizeof(ctx->domain) - 1);
    ctx->uid   = user->uid;
    ctx->gid   = user->gid;
    ctx->ngids = user->ngids;
    if (ctx->ngids > 32) {
        ctx->ngids = 32;
    }
    memcpy(ctx->gids, user->gids, ctx->ngids * sizeof(uint32_t));

    ctx->authenticated = 1;

    smb_ntlm_info("NTLM: Local user '%s' authenticated successfully (uid=%u, gid=%u)",
                  username, ctx->uid, ctx->gid);

    return 0;
} /* validate_local_user */

// Validate AUTHENTICATE message
static int
validate_authenticate(
    struct smb_ntlm_ctx                  *ctx,
    struct chimera_vfs                   *vfs,
    const struct chimera_smb_auth_config *auth_config,
    const uint8_t                        *buf,
    size_t                                buf_len)
{
    char                          *username    = NULL;
    char                          *domain      = NULL;
    char                          *workstation = NULL;
    const struct chimera_vfs_user *user;
    uint16_t                       nt_response_len;
    uint32_t                       nt_response_offset;
    uint16_t                       lm_response_len;
    uint32_t                       lm_response_offset;
    const uint8_t                 *nt_response;
    const uint8_t                 *lm_response;
    int                            result = -1;

    if (buf_len < 88) {
        smb_ntlm_error("NTLM AUTHENTICATE message too short");
        goto cleanup;
    }

    // Parse username (offset 36)
    username = parse_ntlm_utf16_field(buf, buf_len, 36);
    if (!username) {
        smb_ntlm_error("Failed to parse NTLM username");
        goto cleanup;
    }

    // Parse domain (offset 28)
    domain = parse_ntlm_utf16_field(buf, buf_len, 28);
    if (!domain) {
        smb_ntlm_error("Failed to parse NTLM domain");
        goto cleanup;
    }

    // Parse workstation (offset 44)
    workstation = parse_ntlm_utf16_field(buf, buf_len, 44);

    smb_ntlm_debug("NTLM auth: user='%s' domain='%s' workstation='%s'",
                   username, domain, workstation ? workstation : "");

    // Get LM response field (offset 12)
    memcpy(&lm_response_len, buf + 12, 2);
    memcpy(&lm_response_offset, buf + 16, 4);

    if (lm_response_offset + lm_response_len > buf_len) {
        smb_ntlm_error("NTLM: Invalid LM response field");
        goto cleanup;
    }
    lm_response = buf + lm_response_offset;

    // Get NT response field (offset 20)
    memcpy(&nt_response_len, buf + 20, 2);
    memcpy(&nt_response_offset, buf + 24, 4);

    if (nt_response_len < 24 || nt_response_offset + nt_response_len > buf_len) {
        smb_ntlm_error("NTLM: Invalid NT response field");
        goto cleanup;
    }
    nt_response = buf + nt_response_offset;

    // First, try to look up user in local VFS cache
    user = chimera_vfs_lookup_user_by_name(vfs, username);
    if (user && user->smbpasswd[0]) {
        // Found a local user with SMB password - validate locally
        result = validate_local_user(ctx, user, username, domain,
                                     nt_response, nt_response_len);
        goto cleanup;
    }

    // User not found locally (or cached AD user without password) - try winbind
    if (auth_config && auth_config->winbind_enabled) {
        smb_ntlm_debug("NTLM: User '%s' not found locally, trying winbind", username);

        if (smb_wbclient_available()) {
            result = smb_wbclient_auth_ntlm(
                username,
                domain,
                workstation,
                ctx->server_challenge,
                lm_response,
                lm_response_len,
                nt_response,
                nt_response_len,
                &ctx->uid,
                &ctx->gid,
                &ctx->ngids,
                ctx->gids,
                ctx->sid,
                ctx->session_key);

            if (result == 0) {
                // Winbind auth succeeded
                strncpy(ctx->username, username, sizeof(ctx->username) - 1);
                strncpy(ctx->domain, domain, sizeof(ctx->domain) - 1);
                ctx->authenticated   = 1;
                ctx->is_winbind_user = 1;

                smb_ntlm_info("NTLM: Winbind user '%s\\%s' authenticated (uid=%u, gid=%u, sid=%s)",
                              domain, username, ctx->uid, ctx->gid,
                              ctx->sid[0] ? ctx->sid : "none");
            }
            goto cleanup;
        } else {
            smb_ntlm_debug("NTLM: Winbind not available");
        }
    }

    // User not found in any authentication backend - reject
    smb_ntlm_error("NTLM: User '%s' not found in any authentication backend", username);
    result = -1;

 cleanup:
    free(username);
    free(domain);
    free(workstation);
    return result;
} /* validate_authenticate */

void
smb_ntlm_ctx_init(struct smb_ntlm_ctx *ctx)
{
    memset(ctx, 0, sizeof(*ctx));
} /* smb_ntlm_ctx_init */

int
smb_ntlm_process(
    struct smb_ntlm_ctx                  *ctx,
    struct chimera_vfs                   *vfs,
    const struct chimera_smb_auth_config *auth_config,
    const uint8_t                        *input,
    size_t                                input_len,
    uint8_t                             **output,
    size_t                               *output_len)
{
    uint32_t       msg_type;
    const uint8_t *ntlm_input;
    size_t         ntlm_input_len;
    int            spnego_wrapped  = 0;
    uint8_t       *ntlm_output     = NULL;
    size_t         ntlm_output_len = 0;
    int            rc;

    *output     = NULL;
    *output_len = 0;

    // Check if input is SPNEGO-wrapped
    if (is_spnego_wrapped(input, input_len)) {
        spnego_wrapped = 1;
        ntlm_input     = find_ntlmssp_in_spnego(input, input_len, &ntlm_input_len);
        if (!ntlm_input) {
            smb_ntlm_error("NTLM: Could not find NTLMSSP in SPNEGO blob");
            return -1;
        }
        smb_ntlm_debug("NTLM: Unwrapped SPNEGO, NTLM token at offset %zu, len %zu",
                       (size_t) (ntlm_input - input), ntlm_input_len);
    } else {
        ntlm_input     = input;
        ntlm_input_len = input_len;
    }

    if (get_ntlm_message_type(ntlm_input, ntlm_input_len, &msg_type) < 0) {
        smb_ntlm_error("NTLM: Invalid message format");
        return -1;
    }

    switch (msg_type) {
        case NTLM_NEGOTIATE_MESSAGE:
            smb_ntlm_debug("NTLM: Processing NEGOTIATE message");
            if (generate_challenge(ctx, &ntlm_output, &ntlm_output_len) < 0) {
                return -1;
            }

            // Wrap output in SPNEGO if input was wrapped
            if (spnego_wrapped) {
                *output = wrap_challenge_spnego(ntlm_output, ntlm_output_len, output_len);
                free(ntlm_output);
                if (!*output) {
                    return -1;
                }
            } else {
                *output     = ntlm_output;
                *output_len = ntlm_output_len;
            }
            return 1; // Continue needed

        case NTLM_AUTHENTICATE_MESSAGE:
            smb_ntlm_debug("NTLM: Processing AUTHENTICATE message");
            if (!ctx->have_challenge) {
                smb_ntlm_error("NTLM: AUTHENTICATE without prior CHALLENGE");
                return -1;
            }
            rc = validate_authenticate(ctx, vfs, auth_config, ntlm_input, ntlm_input_len);
            if (rc < 0) {
                return -1;
            }

            // Generate SPNEGO accept-complete response if input was wrapped
            if (spnego_wrapped) {
                *output = wrap_complete_spnego(output_len);
                if (!*output) {
                    return -1;
                }
            }
            return 0; // Success

        default:
            smb_ntlm_error("NTLM: Unknown message type %u", msg_type);
            return -1;
    } /* switch */
} /* smb_ntlm_process */

int
smb_ntlm_get_session_key(
    struct smb_ntlm_ctx *ctx,
    uint8_t             *key,
    size_t               key_len)
{
    if (!ctx->authenticated) {
        return -1;
    }

    size_t copy_len = key_len < SMB_NTLM_SESSION_KEY_SIZE ?
        key_len : SMB_NTLM_SESSION_KEY_SIZE;

    memcpy(key, ctx->session_key, copy_len);
    return 0;
} /* smb_ntlm_get_session_key */

int
smb_ntlm_is_authenticated(struct smb_ntlm_ctx *ctx)
{
    return ctx->authenticated;
} /* smb_ntlm_is_authenticated */

const char *
smb_ntlm_get_username(struct smb_ntlm_ctx *ctx)
{
    return ctx->username;
} /* smb_ntlm_get_username */

uint32_t
smb_ntlm_get_uid(struct smb_ntlm_ctx *ctx)
{
    return ctx->uid;
} /* smb_ntlm_get_uid */

uint32_t
smb_ntlm_get_gid(struct smb_ntlm_ctx *ctx)
{
    return ctx->gid;
} /* smb_ntlm_get_gid */

const char *
smb_ntlm_get_sid(struct smb_ntlm_ctx *ctx)
{
    return ctx->sid[0] ? ctx->sid : NULL;
} /* smb_ntlm_get_sid */

int
smb_ntlm_is_winbind_user(struct smb_ntlm_ctx *ctx)
{
    return ctx->is_winbind_user;
} /* smb_ntlm_is_winbind_user */

void
smb_ntlm_synthesize_unix_sid(
    uint32_t uid,
    char    *sid_buf,
    size_t   sid_buf_len)
{
    // Synthesize a Unix user SID using the well-known S-1-22-1-<uid> format
    // S-1-22-1 is the "Unix User" authority used by Samba/winbind
    snprintf(sid_buf, sid_buf_len, "S-1-22-1-%u", uid);
} /* smb_ntlm_synthesize_unix_sid */
