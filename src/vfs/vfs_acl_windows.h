// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/export.h"
#include "sdk/vfs_acl.h"

/*
 * Strip to SID-only: every USER/GROUP principal carrying a native SID becomes
 * CHIMERA_PRINCIPAL_SID (id dropped); every USER/GROUP without one gets the
 * algorithmic modefromsid SID (S-1-5-88-1-<uid> / S-1-5-88-2-<gid>) encoded
 * into a CHIMERA_PRINCIPAL_SID; SPECIAL and PRINCIPAL_SID pass through.
 * Returns the number of principals that were numeric-only (callers that want
 * native SIDs resolve those BY_UID/BY_GID first and call this after).
 */
SYMBOL_EXPORT int chimera_vfs_acl_windows_strip(
    struct chimera_acl *acl);

/*
 * Synchronous enrichment: decode every PRINCIPAL_SID whose SID is an
 * algorithmic modefromsid form into USER/GROUP with the numeric id and no
 * stored native SID.  Returns the number of PRINCIPAL_SID entries remaining
 * (native SIDs needing the resolver).
 */
SYMBOL_EXPORT int chimera_vfs_acl_windows_enrich_sync(
    struct chimera_acl *acl);
