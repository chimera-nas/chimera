// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "smb_wbclient.h"

/* The identity an authenticated Kerberos principal is served as when the
 * deployment has explicitly opted into serving unmapped principals as nobody. */
#define SMB_KERBEROS_NOBODY_ID 65534

/* Unix identity resolved for an authenticated Kerberos principal. */
struct smb_kerberos_identity {
    uint32_t uid;
    uint32_t gid;
    uint32_t ngids;
    uint32_t gids[SMB_WBCLIENT_MAX_GROUPS];
    char     sid[SMB_WBCLIENT_SID_MAX_LEN];
    /* The primary group's SID, empty when the mapper named none for gid. */
    char     group_sid[SMB_WBCLIENT_SID_MAX_LEN];
    /* 1 when an identity module mapped the principal: sid is the SID it named
     * and the caller caches the user in the VFS user cache.  0 for the
     * synthesized nobody identity. */
    int      is_ad_user;
    /* 1 once a policy path filled this identity; the zero value is never a
     * valid identity. */
    int      resolved;
};

struct chimera_vfs;

/* Decide the Unix identity an accepted Kerberos context stands for.
 *
 * A GSSAPI accept only proves who the client is; it is not a logon until the
 * principal maps to a Unix identity.  The mapping comes from the identity
 * engine: the modules that declare CAP_PRINCIPAL (winbind) are asked, in
 * order.  When one is configured -- or mapper_required is set, which
 * smb_auth.winbind_enabled implies -- the mapping MUST come from one: a
 * backend that is down, or a principal none of them can map, refuses the
 * logon, the same answer the NTLM pass-through path gives a logon winbind
 * cannot validate.  A deployment that configured a domain asked for real
 * identities, and an anonymous session is not a degraded form of that.
 *
 * With no principal mapper there is no identity source, and the logon is
 * refused unless anonymous_fallback is set, in which case the principal is
 * served as SMB_KERBEROS_NOBODY_ID with a synthesized Unix SID.  The knob is
 * ignored when a mapper is present or required: it exists for a KDC with no
 * domain behind it (a plain MIT realm), never as a degraded mode for a mapper
 * failure.
 *
 * vfs may be NULL, which means no mapper.  The lookup blocks (a winbindd round
 * trip), as session setup always has.
 *
 * Returns 0 with *out filled, or -1 when the logon must be refused (the caller
 * answers STATUS_LOGON_FAILURE).  Logs the reason for every refusal.
 */
int
smb_kerberos_resolve_identity(
    struct chimera_vfs           *vfs,
    int                           mapper_required,
    int                           anonymous_fallback,
    const char                   *principal,
    struct smb_kerberos_identity *out);
