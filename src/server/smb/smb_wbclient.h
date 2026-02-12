// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

#define SMB_WBCLIENT_MAX_GROUPS  32
#define SMB_WBCLIENT_SID_MAX_LEN 80

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
