// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

#define SMB_NTLM_CHALLENGE_SIZE                    8
#define SMB_NTLM_SESSION_KEY_SIZE                  16
#define SMB_NTLM_HASH_SIZE                         16
#define SMB_NTLM_SID_MAX_LEN                       80

/* smb_ntlm_process() result for a structurally malformed authentication token
 * (e.g. an NTLMv2_RESPONSE whose AvPairs cannot be parsed): the SMB layer
 * answers STATUS_INVALID_PARAMETER instead of STATUS_LOGON_FAILURE. */
#define SMB_NTLM_STATUS_BAD_TOKEN                  (-2)

// NTLM message types
#define NTLM_NEGOTIATE_MESSAGE                     0x00000001
#define NTLM_CHALLENGE_MESSAGE                     0x00000002
#define NTLM_AUTHENTICATE_MESSAGE                  0x00000003

// NTLM negotiate flags
#define NTLMSSP_NEGOTIATE_56                       0x80000000
#define NTLMSSP_NEGOTIATE_KEY_EXCH                 0x40000000
#define NTLMSSP_NEGOTIATE_128                      0x20000000
#define NTLMSSP_NEGOTIATE_VERSION                  0x02000000
#define NTLMSSP_NEGOTIATE_TARGET_INFO              0x00800000
#define NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY 0x00080000
#define NTLMSSP_TARGET_TYPE_SERVER                 0x00020000
#define NTLMSSP_NEGOTIATE_ALWAYS_SIGN              0x00008000
#define NTLMSSP_NEGOTIATE_ANONYMOUS                0x00000800
#define NTLMSSP_NEGOTIATE_NTLM                     0x00000200
#define NTLMSSP_NEGOTIATE_SEAL                     0x00000020
#define NTLMSSP_NEGOTIATE_SIGN                     0x00000010
#define NTLMSSP_REQUEST_TARGET                     0x00000004
#define NTLMSSP_NEGOTIATE_OEM                      0x00000002
#define NTLMSSP_NEGOTIATE_UNICODE                  0x00000001

struct chimera_vfs;
struct chimera_smb_auth_config;

/*
 * Decode one NTLM *Fields descriptor (Len/MaxLen/BufferOffset) at field_offset
 * in an AUTHENTICATE message and return its UTF-16LE payload as a NUL-terminated
 * ASCII string the caller must free, or NULL if the descriptor does not lie
 * wholly within buf_len.  Exposed for the hardening tests, which drive it with
 * adversarial offsets; production callers reach it through smb_ntlm_process().
 */
char *
smb_ntlm_parse_utf16_field(
    const uint8_t *buf,
    size_t         buf_len,
    size_t         field_offset);

/* Names the server advertises in the CHALLENGE target info.  Domain
 * controllers validate the target info echoed back inside the client's NTLMv2
 * response against the machine account on the netlogon channel (NTLM relay
 * protection), so a pass-through (winbind) logon fails with LOGON_FAILURE
 * unless these match what the host is actually joined as. */
struct smb_ntlm_server_identity {
    char netbios_name[16];
    char netbios_domain[256];
    char dns_name[256];
};

// Resolve the identity advertised in CHALLENGE messages, preferring the
// winbind join identity when auth_config enables winbind.  The winbind lookup
// is a blocking winbindd round trip, so call this once at server startup and
// cache the result; it must not run on the per-connection request path.
void
smb_ntlm_resolve_server_identity(
    const struct chimera_smb_auth_config *auth_config,
    struct smb_ntlm_server_identity      *id);

// NTLM authentication context (per-connection)
struct smb_ntlm_ctx {
    uint8_t  server_challenge[SMB_NTLM_CHALLENGE_SIZE];
    uint8_t  session_key[SMB_NTLM_SESSION_KEY_SIZE];
    uint32_t negotiate_flags;
    int      have_challenge;
    int      authenticated;
    int      is_anonymous;       // AUTHENTICATE was an anonymous (null-session) logon
    int      is_winbind_user;    // True if authenticated via winbind (AD user)
    char     username[256];
    char     domain[256];
    char     sid[SMB_NTLM_SID_MAX_LEN];
    uint32_t uid;
    uint32_t gid;
    uint32_t ngids;
    uint32_t gids[32];
};

// Result of processing an NTLM message
struct smb_ntlm_result {
    int      status;           // 0 = success, -1 = error, 1 = continue needed
    uint8_t *output_token;     // Response token (caller must free)
    size_t   output_len;
};

// Initialize NTLM context
void
smb_ntlm_ctx_init(
    struct smb_ntlm_ctx *ctx);

// Process incoming NTLM token, returns response token
// vfs is used to look up local user credentials
// auth_config controls winbind fallback behavior
int
smb_ntlm_process(
    struct smb_ntlm_ctx                  *ctx,
    struct chimera_vfs                   *vfs,
    const struct chimera_smb_auth_config *auth_config,
    const uint8_t                        *input,
    size_t                                input_len,
    uint8_t                             **output,
    size_t                               *output_len);

// Get the session key after successful authentication
int
smb_ntlm_get_session_key(
    struct smb_ntlm_ctx *ctx,
    uint8_t             *key,
    size_t               key_len);

// Check if authentication completed successfully
int
smb_ntlm_is_authenticated(
    struct smb_ntlm_ctx *ctx);

// Get authenticated user info
const char *
smb_ntlm_get_username(
    struct smb_ntlm_ctx *ctx);

uint32_t
smb_ntlm_get_uid(
    struct smb_ntlm_ctx *ctx);

uint32_t
smb_ntlm_get_gid(
    struct smb_ntlm_ctx *ctx);

const char *
smb_ntlm_get_sid(
    struct smb_ntlm_ctx *ctx);

int
smb_ntlm_is_winbind_user(
    struct smb_ntlm_ctx *ctx);

// Synthesize a Unix user SID (S-1-22-1-<uid>) for local users
void
smb_ntlm_synthesize_unix_sid(
    uint32_t uid,
    char    *sid_buf,
    size_t   sid_buf_len);
