// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Central access-control gate.
 *
 * A single implementation of "may this credential perform these operations on
 * this object" that every protocol server shares, so NFSv3, NFSv4 and SMB2 all
 * reach identical decisions regardless of which protocol stored the ACL.  The
 * caller supplies attributes it has already fetched (mode, uid, gid, and -- if
 * CHIMERA_VFS_ATTR_ACL is set -- the canonical ACL via va_acl); this routine
 * evaluates them with the shared engine in vfs_acl.c.
 */

#include <stdint.h>

struct chimera_vfs_attrs;
struct chimera_vfs_cred;

/*
 * Return the subset of `requested` (canonical CHIMERA_ACE_* mask bits) that is
 * granted to `cred` for the object described by `attr`.  If `attr` carries a
 * native ACL it is evaluated; otherwise the decision falls back to the POSIX
 * mode bits.  Callers test `(granted & requested) == requested` for a hard
 * allow, or inspect individual bits (e.g. for an NFS ACCESS reply).
 */
uint32_t chimera_vfs_access_check(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        requested);

/* Convenience: non-zero if every bit in `requested` is granted. */
static inline int
chimera_vfs_access_allowed(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        requested)
{
    return chimera_vfs_access_check(attr, cred, requested) == requested;
} /* chimera_vfs_access_allowed */
