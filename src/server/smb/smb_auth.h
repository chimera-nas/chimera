// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>

// Authentication mechanism types detected from SPNEGO tokens
enum smb_auth_mech {
    SMB_AUTH_MECH_UNKNOWN,
    SMB_AUTH_MECH_NTLM,
    SMB_AUTH_MECH_KERBEROS,
};

// Detect the authentication mechanism from a SPNEGO/GSSAPI token
// Returns the detected mechanism type
enum smb_auth_mech
smb_auth_detect_mechanism(
    const uint8_t *token,
    size_t         token_len);

// Get a string name for a mechanism type (for logging)
const char *
smb_auth_mech_name(
    enum smb_auth_mech mech);
