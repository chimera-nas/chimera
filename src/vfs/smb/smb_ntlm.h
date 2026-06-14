// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

/*
 * Client-side NTLMSSP (NTLMv2) for the SMB2 client.  This is the mirror image
 * of the server's src/server/smb/smb_ntlm.c: the client BUILDS the NEGOTIATE
 * and AUTHENTICATE messages and PARSES the CHALLENGE.  Only raw NTLMSSP tokens
 * are produced/consumed (no SPNEGO wrapper) -- the chimera SMB server accepts
 * bare NTLMSSP and replies in kind.
 *
 * Crypto (OpenSSL): NT hash = MD4(UTF16LE(password)); ntlmv2_hash =
 * HMAC-MD5(NT, UTF16LE(UPPER(user)) || UTF16LE(domain)); NTProofStr =
 * HMAC-MD5(ntlmv2_hash, server_challenge || client_blob); the session base key
 * (== SMB2 session key, no key exchange) = HMAC-MD5(ntlmv2_hash, NTProofStr).
 */

#define SMB_NTLM_CLIENT_CHALLENGE_SIZE   8
#define SMB_NTLM_CLIENT_SESSION_KEY_SIZE 16
#define SMB_NTLM_CLIENT_TARGET_INFO_MAX  1024

struct smb_ntlm_client {
    char    user[256];
    char    domain[256];
    char    password[256];

    /* Captured from the server's CHALLENGE message. */
    uint8_t server_challenge[SMB_NTLM_CLIENT_CHALLENGE_SIZE];
    uint8_t target_info[SMB_NTLM_CLIENT_TARGET_INFO_MAX];
    size_t  target_info_len;

    /* Derived during build_authenticate; the SMB2 session key. */
    uint8_t session_key[SMB_NTLM_CLIENT_SESSION_KEY_SIZE];
    int     have_session_key;
};

void smb_ntlm_client_init(
    struct smb_ntlm_client *c,
    const char             *user,
    const char             *domain,
    const char             *password);

/* Build a minimal NTLMSSP_NEGOTIATE token into out (capacity out_max).
 * Returns the token length, or -1 on overflow. */
int smb_ntlm_client_build_negotiate(
    uint8_t *out,
    size_t   out_max);

/* Parse an NTLMSSP_CHALLENGE token (the server's interim SESSION_SETUP blob),
 * capturing the server challenge and target-info AV_PAIR list.  Returns 0 on
 * success, -1 on a malformed token. */
int smb_ntlm_client_parse_challenge(
    struct smb_ntlm_client *c,
    const uint8_t          *buf,
    size_t                  len);

/* Build an NTLMSSP_AUTHENTICATE token into out (capacity out_max) and derive
 * c->session_key.  Must be called after parse_challenge.  Returns 0 on success
 * (with *out_len set), -1 on error. */
int smb_ntlm_client_build_authenticate(
    struct smb_ntlm_client *c,
    uint8_t                *out,
    size_t                  out_max,
    size_t                 *out_len);
