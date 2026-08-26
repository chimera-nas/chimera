// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

#include "vfs/vfs_identity.h"

struct chimera_vfs_user;

#define SMB_WBCLIENT_MAX_GROUPS  32
#define SMB_WBCLIENT_SID_MAX_LEN 80

/* Resolve the LmChallengeResponse to hand a winbind network logon.
 *
 * MS-NLMP 3.2.5.1.2: a client that authenticates with NTLMv2 alone may send a
 * zero-length LmChallengeResponse, which the server reads as the implied Z(24).
 * Samba's own clients and Windows send those 24 zero bytes explicitly; the Linux
 * kernel cifs client (mount.cifs) sends length 0, and winbind's auth_crap
 * interface refuses a zero-length LM response even when the NTLMv2 response
 * verifies.  Materialize the implied value so the NTLMv2 proof alone decides the
 * logon.  Safe: `lanman auth = no` is the Samba default, so the LM branch is
 * never tried, and 24 zero bytes cannot validate against a real LM hash anyway.
 *
 * This normalization belongs here and NOT at parse time: validate_authenticate()
 * uses `lm_response_len <= 1` to detect an anonymous logon (kernel
 * `mount -o sec=none`) and must keep seeing the length the client really sent.
 */
static inline void
smb_wbclient_lm_response(
    const uint8_t  *lm_response,
    size_t          lm_response_len,
    const uint8_t **out_data,
    uint32_t       *out_len)
{
    static const uint8_t lm_implied_zeros[24] = { 0 };

    if (lm_response_len == 0) {
        *out_data = lm_implied_zeros;
        *out_len  = sizeof(lm_implied_zeros);
    } else {
        *out_data = lm_response;
        *out_len  = (uint32_t) lm_response_len;
    }
} // smb_wbclient_lm_response

// Authenticate a user via winbind using NTLM challenge/response
// Returns: 0 on success, -1 on failure
// sid_out should be at least SMB_WBCLIENT_SID_MAX_LEN bytes (can be NULL)
// session_key should be at least 16 bytes (can be NULL)
int smb_wbclient_auth_ntlm(
    const char    *username,
    const char    *domain,
    const char    *workstation,
    const uint8_t *challenge,
    const uint8_t *lm_response,
    size_t         lm_response_len,
    const uint8_t *nt_response,
    size_t         nt_response_len,
    uint32_t      *uid,
    uint32_t      *gid,
    uint32_t      *ngids,
    uint32_t      *gids,
    char          *sid_out,
    uint8_t       *session_key);

// Map a Kerberos principal name to Unix credentials via winbind
// principal format: "user@REALM" or "DOMAIN\user"
// Returns: 0 on success, -1 on failure
// sid_out should be at least SMB_WBCLIENT_SID_MAX_LEN bytes (can be NULL)
int smb_wbclient_map_principal(
    const char *principal,
    uint32_t   *uid,
    uint32_t   *gid,
    uint32_t   *ngids,
    uint32_t   *gids,
    char       *sid_out);

// Check if winbind is available
// Returns: 1 if available, 0 if not
int smb_wbclient_available(
    void);

// Fetch the NetBIOS identity winbind is joined with (name of the machine
// account, short domain and DNS domain).  Domain controllers validate the
// NTLMv2 target info a pass-through logon carries against the machine account
// on the netlogon channel, so the CHALLENGE must advertise these names.
// Any output may be NULL when not wanted; empty string when unknown.
// Returns: 0 on success, -1 on failure (output buffers left unmodified)
int smb_wbclient_netbios_identity(
    char  *netbios_name,
    size_t netbios_name_len,
    char  *netbios_domain,
    size_t netbios_domain_len,
    char  *dns_domain,
    size_t dns_domain_len);

// Identity-resolver miss handler backed by winbind.  Resolves BY_UID / BY_NAME
// to a full user record (uid/gid/groups/name/real SID), BY_GID to a group
// record (gid/name/real SID), and BY_SID to whichever of the two the SID names.
// Registered with the VFS identity authority at SMB server init when winbind is
// enabled.  Matches the chimera_vfs_identity_handler signature.
int smb_wbclient_identity_handler(
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out,
    void                               *private_data);

// Authenticate a user via winbind using plaintext password
// Returns: 0 on success, -1 on failure
// sid_out should be at least SMB_WBCLIENT_SID_MAX_LEN bytes (can be NULL)
int smb_wbclient_auth_password(
    const char *username,
    const char *domain,
    const char *password,
    uint32_t   *uid,
    uint32_t   *gid,
    uint32_t   *ngids,
    uint32_t   *gids,
    char       *sid_out);
