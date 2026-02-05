// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "smb_auth.h"

// SPNEGO OIDs (ASN.1 encoded without tag/length)
// NTLMSSP: 1.3.6.1.4.1.311.2.2.10
static const uint8_t ntlmssp_oid_bytes[] = {
    0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a
};

// Kerberos: 1.2.840.113554.1.2.2
static const uint8_t kerberos_oid_bytes[] = {
    0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02
};

// MS Kerberos: 1.2.840.48018.1.2.2
static const uint8_t ms_kerberos_oid_bytes[] = {
    0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02
};

// Helper function to search for a byte sequence in a buffer
static int
find_bytes(
    const uint8_t *haystack,
    size_t         haystack_len,
    const uint8_t *needle,
    size_t         needle_len)
{
    if (needle_len > haystack_len) {
        return 0;
    }

    for (size_t i = 0; i + needle_len <= haystack_len; i++) {
        if (memcmp(haystack + i, needle, needle_len) == 0) {
            return 1;
        }
    }

    return 0;
} // find_bytes

enum smb_auth_mech
smb_auth_detect_mechanism(
    const uint8_t *token,
    size_t         token_len)
{
    if (!token || token_len < 8) {
        return SMB_AUTH_MECH_UNKNOWN;
    }

    // Check for raw NTLMSSP token (starts with "NTLMSSP\0")
    if (token_len >= 8 && memcmp(token, "NTLMSSP\0", 8) == 0) {
        return SMB_AUTH_MECH_NTLM;
    }

    // For SPNEGO-wrapped tokens, look for mechanism OIDs
    // SPNEGO tokens start with 0x60 (APPLICATION CONSTRUCTED)
    // or negTokenResp starts with 0xa1
    if (token[0] == 0x60 || token[0] == 0xa1) {
        // Search for NTLMSSP OID
        if (find_bytes(token, token_len, ntlmssp_oid_bytes, sizeof(ntlmssp_oid_bytes))) {
            return SMB_AUTH_MECH_NTLM;
        }

        // Search for Kerberos OIDs
        if (find_bytes(token, token_len, kerberos_oid_bytes, sizeof(kerberos_oid_bytes)) ||
            find_bytes(token, token_len, ms_kerberos_oid_bytes, sizeof(ms_kerberos_oid_bytes))) {
            return SMB_AUTH_MECH_KERBEROS;
        }

        // Also check for NTLMSSP signature embedded in SPNEGO
        if (find_bytes(token, token_len, (const uint8_t *) "NTLMSSP", 7)) {
            return SMB_AUTH_MECH_NTLM;
        }
    }

    // Unknown mechanism
    return SMB_AUTH_MECH_UNKNOWN;
} // smb_auth_detect_mechanism

const char *
smb_auth_mech_name(enum smb_auth_mech mech)
{
    switch (mech) {
        case SMB_AUTH_MECH_NTLM:
            return "NTLM";
        case SMB_AUTH_MECH_KERBEROS:
            return "Kerberos";
        case SMB_AUTH_MECH_UNKNOWN:
        default:
            return "Unknown";
    } /* switch */
} // smb_auth_mech_name
