// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/export.h"
#include <stdint.h>
#include <stddef.h>

// Authentication mechanism types detected from SPNEGO tokens
enum smb_auth_mech {
    SMB_AUTH_MECH_UNKNOWN,
    SMB_AUTH_MECH_NTLM,
    SMB_AUTH_MECH_KERBEROS,
    SMB_AUTH_MECH_NEGOEX,
};

// Detect the authentication mechanism from a SPNEGO/GSSAPI token.  A raw
// NTLMSSP token is NTLM; a negTokenInit is classified by its first (optimistic)
// mechType, the mechanism whose token it carries; a negTokenResp by the token
// it carries; a bare Kerberos InitialContextToken (no SPNEGO) is Kerberos.
// Anything malformed or unrecognised is UNKNOWN.
SYMBOL_EXPORT enum smb_auth_mech
smb_auth_detect_mechanism(
    const uint8_t *token,
    size_t         token_len);

// 1 when token is a SPNEGO negTokenInit whose mechTypes list offers NTLMSSP
// (anywhere in the list), 0 otherwise.
SYMBOL_EXPORT int
smb_auth_spnego_offers_ntlmssp(
    const uint8_t *token,
    size_t         token_len);

// RFC 4178 negTokenResp { accept-incomplete, supportedMech NTLMSSP } with no
// responseToken: sent under STATUS_MORE_PROCESSING_REQUIRED to a client whose
// optimistic mechanism the server does not accept, so it restarts with
// NTLMSSP.  Returns a malloc'd buffer the caller frees, NULL if out of memory.
SYMBOL_EXPORT uint8_t *
smb_auth_spnego_ntlmssp_hint(
    size_t *out_len);

// Get a string name for a mechanism type (for logging)
SYMBOL_EXPORT const char *
smb_auth_mech_name(
    enum smb_auth_mech mech);
