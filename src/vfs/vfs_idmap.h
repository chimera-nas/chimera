// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Identity mapping for ACL principals.
 *
 * Converts the canonical chimera_principal (special-who / numeric uid / numeric
 * gid) to and from the two protocol identity encodings:
 *
 *   - NFSv4 owner / who strings  ("OWNER@", "1000", "alice@domain")
 *   - Windows SID strings        ("S-1-1-0", "S-1-22-1-1000", ...)
 *
 * The numeric, special-who and algorithmic-SID conversions are pure and
 * deterministic.  When an NFSv4 domain is configured, named resolution of
 * numeric ids to "name@domain" is attempted via nsswitch (getpwuid/getgrgid);
 * resolution of real Active Directory SIDs is layered on top via the VFS user
 * cache by callers that have one.  This is the single place every protocol
 * marshaller maps identities, so a principal stored by one protocol is named
 * consistently by another.
 */

#include <stdint.h>
#include "vfs_acl.h"

/* Max length of an NFSv4 who string or a SID string (with NUL). */
#define CHIMERA_IDMAP_WHO_MAX 256
#define CHIMERA_IDMAP_SID_MAX 96

/* Build a numeric-id principal. */
static inline struct chimera_principal
chimera_idmap_uid_principal(uint32_t uid)
{
    struct chimera_principal p = { .type = CHIMERA_PRINCIPAL_USER, .id = uid };

    return p;
} /* chimera_idmap_uid_principal */

static inline struct chimera_principal
chimera_idmap_gid_principal(uint32_t gid)
{
    struct chimera_principal p = { .type = CHIMERA_PRINCIPAL_GROUP, .id = gid };

    return p;
} /* chimera_idmap_gid_principal */

static inline struct chimera_principal
chimera_idmap_special_principal(uint8_t special)
{
    struct chimera_principal p = { .type    = CHIMERA_PRINCIPAL_SPECIAL,
                                   .special = special };

    return p;
} /* chimera_idmap_special_principal */

/*
 * Encode `p` as an NFSv4 owner/who string into `buf` (capacity `buflen`).
 * `domain` may be NULL (then numeric ids are emitted verbatim).  Returns the
 * string length, or -1 if the buffer is too small.
 */
int chimera_idmap_principal_to_who(
    const struct chimera_principal *p,
    const char                     *domain,
    char                           *buf,
    int                             buflen);

/*
 * Decode an NFSv4 owner/who string (`who`, length `len`) into `p`.
 * `is_group` selects group vs user when the string names a principal.
 * `domain` may be NULL.  Returns 0 on success, -1 if it could not be resolved.
 */
int chimera_idmap_who_to_principal(
    const char               *who,
    int                       len,
    int                       is_group,
    const char               *domain,
    struct chimera_principal *p);

/*
 * Encode `p` as a Windows SID string ("S-1-...") into `buf`.  Special-whos map
 * to well-known SIDs (EVERYONE@ -> S-1-1-0, OWNER@ -> S-1-3-0, GROUP@ ->
 * S-1-3-1); numeric ids map algorithmically (uid -> S-1-22-1-<uid>, gid ->
 * S-1-22-2-<gid>).  Returns the string length, or -1 if the buffer is too
 * small / the principal is not representable.
 */
int chimera_idmap_principal_to_sid(
    const struct chimera_principal *p,
    char                           *buf,
    int                             buflen);

/*
 * Decode a Windows SID string into `p`.  Recognises the well-known special
 * SIDs, the Samba unix-id SIDs (S-1-22-1/2), and the modefromsid SIDs
 * (S-1-5-88-1/2).  Returns 0 on success, -1 if unrecognised.
 */
int chimera_idmap_sid_to_principal(
    const char               *sid,
    struct chimera_principal *p);
