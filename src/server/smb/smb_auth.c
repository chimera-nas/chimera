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

// NEGOEX: 1.3.6.1.4.1.311.2.2.30
static const uint8_t negoex_oid_bytes[] = {
    0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x1e
};

// Kerberos: 1.2.840.113554.1.2.2
static const uint8_t kerberos_oid_bytes[] = {
    0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02
};

// MS Kerberos: 1.2.840.48018.1.2.2
static const uint8_t ms_kerberos_oid_bytes[] = {
    0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02
};

// SPNEGO: 1.3.6.1.5.5.2
static const uint8_t spnego_oid_bytes[] = {
    0x2b, 0x06, 0x01, 0x05, 0x05, 0x02
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

/* Read one DER TLV at buf[0..len).  On success stores the tag, the offset of
 * the value and the value length (which fits inside len) and returns 1.
 * Only definite short and 1..2-byte long forms occur in SPNEGO tokens. */
static int
asn1_tlv(
    const uint8_t *buf,
    size_t         len,
    uint8_t       *tag,
    size_t        *value_off,
    size_t        *value_len)
{
    size_t pos = 0, vlen;

    if (len < 2) {
        return 0;
    }
    *tag = buf[pos++];

    if (buf[pos] < 0x80) {
        vlen = buf[pos++];
    } else if (buf[pos] == 0x81 && len >= 3) {
        vlen = buf[2];
        pos  = 3;
    } else if (buf[pos] == 0x82 && len >= 4) {
        vlen = ((size_t) buf[2] << 8) | buf[3];
        pos  = 4;
    } else {
        return 0;
    }

    if (vlen > len - pos) {
        return 0;
    }
    *value_off = pos;
    *value_len = vlen;
    return 1;
} // asn1_tlv

/* Read the framing of a GSS-API InitialContextToken (RFC 2743 3.1):
 *   60 L  06 L <thisMech>  innerContextToken
 * Sets oid and oid_len to thisMech and inner and inner_len to the token that
 * follows it, and returns 1, or returns 0 if the framing is malformed. */
static int
gss_this_mech(
    const uint8_t  *token,
    size_t          token_len,
    const uint8_t **oid,
    size_t         *oid_len,
    const uint8_t **inner,
    size_t         *inner_len)
{
    uint8_t tag;
    size_t  off, len;

    if (!asn1_tlv(token, token_len, &tag, &off, &len) || tag != 0x60) {
        return 0;
    }
    token     = token + off;
    token_len = len;

    if (!asn1_tlv(token, token_len, &tag, &off, &len) || tag != 0x06) {
        return 0;
    }
    *oid       = token + off;
    *oid_len   = len;
    *inner     = token + off + len;
    *inner_len = token_len - off - len;
    return 1;
} // gss_this_mech

/* Locate the mechTypes OID list of a negTokenInit:
 *   60 L  06 06 <spnego oid>  a0 L  30 L  a0 L  30 L  { 06 L oid }...
 * Sets list and list_len to the bytes inside the inner SEQUENCE OF and returns
 * 1, or returns 0 for anything that is not a well-formed negTokenInit. */
static int
spnego_mech_types(
    const uint8_t  *token,
    size_t          token_len,
    const uint8_t **list,
    size_t         *list_len)
{
    const uint8_t *oid;
    uint8_t        tag;
    size_t         oid_len, off, len;

    if (!gss_this_mech(token, token_len, &oid, &oid_len, &token, &token_len) ||
        oid_len != sizeof(spnego_oid_bytes) ||
        memcmp(oid, spnego_oid_bytes, oid_len) != 0) {
        return 0;
    }

    if (!asn1_tlv(token, token_len, &tag, &off, &len) || tag != 0xa0) {
        return 0;
    }
    token     = token + off;
    token_len = len;

    if (!asn1_tlv(token, token_len, &tag, &off, &len) || tag != 0x30) {
        return 0;
    }
    token     = token + off;
    token_len = len;

    if (!asn1_tlv(token, token_len, &tag, &off, &len) || tag != 0xa0) {
        return 0;
    }
    token     = token + off;
    token_len = len;

    if (!asn1_tlv(token, token_len, &tag, &off, &len) || tag != 0x30) {
        return 0;
    }
    *list     = token + off;
    *list_len = len;
    return 1;
} // spnego_mech_types

static enum smb_auth_mech
mech_from_oid(
    const uint8_t *oid,
    size_t         oid_len)
{
    if (oid_len == sizeof(ntlmssp_oid_bytes) &&
        memcmp(oid, ntlmssp_oid_bytes, oid_len) == 0) {
        return SMB_AUTH_MECH_NTLM;
    }
    if ((oid_len == sizeof(kerberos_oid_bytes) &&
         memcmp(oid, kerberos_oid_bytes, oid_len) == 0) ||
        (oid_len == sizeof(ms_kerberos_oid_bytes) &&
         memcmp(oid, ms_kerberos_oid_bytes, oid_len) == 0)) {
        return SMB_AUTH_MECH_KERBEROS;
    }
    if (oid_len == sizeof(negoex_oid_bytes) &&
        memcmp(oid, negoex_oid_bytes, oid_len) == 0) {
        return SMB_AUTH_MECH_NEGOEX;
    }
    return SMB_AUTH_MECH_UNKNOWN;
} // mech_from_oid

int
smb_auth_spnego_offers_ntlmssp(
    const uint8_t *token,
    size_t         token_len)
{
    const uint8_t *list;
    size_t         list_len, off, len;
    uint8_t        tag;

    if (!token || !spnego_mech_types(token, token_len, &list, &list_len)) {
        return 0;
    }

    while (list_len > 0) {
        if (!asn1_tlv(list, list_len, &tag, &off, &len) || tag != 0x06) {
            return 0;
        }
        if (mech_from_oid(list + off, len) == SMB_AUTH_MECH_NTLM) {
            return 1;
        }
        list     += off + len;
        list_len -= off + len;
    }
    return 0;
} // smb_auth_spnego_offers_ntlmssp

enum smb_auth_mech
smb_auth_detect_mechanism(
    const uint8_t *token,
    size_t         token_len)
{
    const uint8_t *list, *oid, *inner;
    size_t list_len, oid_len, inner_len, off, len;
    uint8_t tag;

    if (!token || token_len < 8) {
        return SMB_AUTH_MECH_UNKNOWN;
    }

    // Raw NTLMSSP token (starts with "NTLMSSP\0")
    if (memcmp(token, "NTLMSSP\0", 8) == 0) {
        return SMB_AUTH_MECH_NTLM;
    }

    /* A negTokenInit names its optimistic mechanism first in mechTypes and
     * carries that mechanism's token.  The list routinely also contains
     * NTLMSSP as a fallback, so the presence of the NTLMSSP OID says nothing
     * about what the mechToken is: a Kerberos-first client that was classed
     * as NTLM here had its AP-REQ handed to the NTLM parser and was failed
     * with LOGON_FAILURE instead of being steered to NTLMSSP. */
    if (token[0] == 0x60) {
        if (spnego_mech_types(token, token_len, &list, &list_len)) {
            if (!asn1_tlv(list, list_len, &tag, &off, &len) || tag != 0x06) {
                return SMB_AUTH_MECH_UNKNOWN;
            }
            return mech_from_oid(list + off, len);
        }

        /* Not SPNEGO: a bare InitialContextToken names its mechanism in
         * thisMech.  Kerberos is the one GSSAPI accepts unwrapped, so a
         * client that sends the krb5 AP-REQ without SPNEGO still reaches
         * it. */
        if (gss_this_mech(token, token_len, &oid, &oid_len, &inner, &inner_len) &&
            mech_from_oid(oid, oid_len) == SMB_AUTH_MECH_KERBEROS) {
            return SMB_AUTH_MECH_KERBEROS;
        }
        return SMB_AUTH_MECH_UNKNOWN;
    }

    /* A negTokenResp carries no mechanism list on later legs; the token
     * itself tells: an NTLMSSP signature means NTLM, a Kerberos OID (the
     * supportedMech a client echoes on its first response leg) Kerberos. */
    if (token[0] == 0xa1) {
        if (find_bytes(token, token_len, (const uint8_t *) "NTLMSSP\0", 8)) {
            return SMB_AUTH_MECH_NTLM;
        }
        if (find_bytes(token, token_len, kerberos_oid_bytes, sizeof(kerberos_oid_bytes)) ||
            find_bytes(token, token_len, ms_kerberos_oid_bytes, sizeof(ms_kerberos_oid_bytes))) {
            return SMB_AUTH_MECH_KERBEROS;
        }
    }

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
        case SMB_AUTH_MECH_NEGOEX:
            return "NEGOEX";
        case SMB_AUTH_MECH_UNKNOWN:
        default:
            return "Unknown";
    } /* switch */
} // smb_auth_mech_name
