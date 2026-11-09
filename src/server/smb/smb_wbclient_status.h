// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#ifdef HAVE_WBCLIENT

/* wbclient.h uses fixed-width types, bool and time_t without including them. */
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <wbclient.h>
#include "vfs/vfs_identity.h"

/*
 * Classify a wbc error for the identity resolver.  Winbind answering that an
 * identity does not exist, or a SID string too malformed to name anyone, is
 * authoritative (NOT_FOUND).  Anything suggesting the lookup did not really
 * run -- the daemon missing, out of memory, a garbled response, a bad
 * request, or a code this table does not know -- is UNAVAILABLE, so it is
 * never cached as an answer.
 *
 * One limit is libwbclient's.  It reports every failure winbindd returns as
 * WBC_ERR_DOMAIN_NOT_FOUND (wb_common.c turns any result other than
 * WINBINDD_OK into NSS_STATUS_NOTFOUND, which wbclient.c maps to that code),
 * and its single-SID calls use the same code for "not mapped"; winbindd
 * itself reports an idmap backend that failed as "not mapped"
 * (wb_sids2xids.c).  So while winbindd is up, a domain controller or idmap
 * backend it cannot reach looks here exactly like an identity that does not
 * exist, and is classified NOT_FOUND.  On a windows-identity mount that means:
 *
 *  - reads: an unresolved SID stays an opaque principal, negatively cached
 *    for up to CHIMERA_VFS_IDENTITY_NEGATIVE_TTL;
 *  - writes: a uid/gid whose domain SID cannot be looked up is stored as its
 *    algorithmic S-1-5-88 SID, on disk and for good; and if NSS knows the
 *    id, its SID-less answer is cached as positive, for the user cache's
 *    full TTL, rather than as a short-lived miss.
 *
 * Only winbindd itself being down is reliably UNAVAILABLE.  This is the
 * canonical statement of the limit; docs/vfs-module-sdk.md points here.
 */
static inline enum chimera_vfs_identity_status
smb_wbclient_err_to_status(wbcErr wbc_err)
{
    switch (wbc_err) {
        case WBC_ERR_SUCCESS:
            return CHIMERA_VFS_IDENTITY_OK;
        case WBC_ERR_DOMAIN_NOT_FOUND:
        case WBC_ERR_UNKNOWN_USER:
        case WBC_ERR_UNKNOWN_GROUP:
        case WBC_ERR_NOT_MAPPED:
        case WBC_ERR_INVALID_SID:
            return CHIMERA_VFS_IDENTITY_NOT_FOUND;
        default:
            return CHIMERA_VFS_IDENTITY_UNAVAILABLE;
    } /* switch */
} /* smb_wbclient_err_to_status */

#endif /* HAVE_WBCLIENT */
