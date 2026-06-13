// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * POSIX.1e draft ACL <-> canonical (NFSv4/NT) ACL translation.
 *
 * This is the bridge for the unofficial NFSv3 ACL sideband protocol (NFSACL,
 * RPC program 100227), which carries Solaris/Linux POSIX.1e ACLs.  Chimera's
 * single canonical model is the NFSv4/NT ACL (see vfs_acl.h); POSIX.1e is a
 * deliberately LOSSY projection of it, never a peer:
 *
 *   - canonical -> POSIX (chimera_acl_to_posix): the OWNER@/GROUP@/EVERYONE@
 *     classes are projected with the same deny-aware logic chimera_acl_to_mode
 *     uses (so a mode-derived ACL round-trips exactly); named USER/GROUP ALLOW
 *     ACEs become POSIX named entries; a synthetic ACL_MASK is computed as the
 *     union of the group-class permissions.  Named DENY ACEs cannot be
 *     represented in POSIX and are DROPPED.
 *
 *   - POSIX -> canonical (chimera_acl_from_posix): the three base classes are
 *     rebuilt via chimera_acl_from_mode (bit-identical to the mode path, so a
 *     SETACL of a previously GETACL'd ACL is idempotent); the ACL_MASK is folded
 *     into the group-class effective permissions (POSIX effective-perm
 *     semantics) since the canonical engine has no mask concept; named entries
 *     become ALLOW ACEs; default entries become INHERIT_ONLY templates.
 *
 * Strict POSIX owner->user->group->mask->other precedence (which would require
 * synthesising per-named DENY ACEs) is intentionally out of scope; named
 * entries are emitted as ALLOW only.  See the comments in vfs_acl_posix.c.
 */

#include <stdint.h>

struct chimera_acl;

/* ---- POSIX.1e ACL entry tags (uapi/linux/posix_acl.h) ------------------- */
#define CHIMERA_POSIX_ACL_USER_OBJ  0x0001
#define CHIMERA_POSIX_ACL_USER      0x0002
#define CHIMERA_POSIX_ACL_GROUP_OBJ 0x0004
#define CHIMERA_POSIX_ACL_GROUP     0x0008
#define CHIMERA_POSIX_ACL_MASK      0x0010
#define CHIMERA_POSIX_ACL_OTHER     0x0020

/* ---- POSIX.1e permission bits (== the S_IRWXO rwx bit values) ----------- */
#define CHIMERA_POSIX_ACL_READ      0x04
#define CHIMERA_POSIX_ACL_WRITE     0x02
#define CHIMERA_POSIX_ACL_EXECUTE   0x01

/* ---- NFSACL (prog 100227) protocol constants ---------------------------- */
/* getacl/setacl mode flags (which ACL components the request/reply carries). */
#define CHIMERA_NFS_ACL             0x0001 /* access ACL entries           */
#define CHIMERA_NFS_ACLCNT          0x0002 /* access ACL entry count       */
#define CHIMERA_NFS_DFACL           0x0004 /* default ACL entries          */
#define CHIMERA_NFS_DFACLCNT        0x0008 /* default ACL entry count      */

/* OR'd into the wire e_tag of every default-ACL entry (stripped on decode). */
#define CHIMERA_NFS_ACL_DEFAULT     0x1000

/* Protocol cap on entries in one ACL (matches CHIMERA_ACL_MAX_ACES). */
#define CHIMERA_NFSACL_MAXENTRIES   1024

/* One POSIX.1e ACL entry as it travels on the NFSACL wire: (tag, id, perm). */
struct chimera_posix_acl_entry {
    uint16_t e_tag;  /* CHIMERA_POSIX_ACL_* (without the DEFAULT wire flag)  */
    uint16_t e_perm; /* CHIMERA_POSIX_ACL_READ|WRITE|EXECUTE                 */
    uint32_t e_id;   /* uid for USER, gid for GROUP, owner for *_OBJ, else 0 */
};

/*
 * Project a canonical ACL down to POSIX.1e access (and, for directories,
 * default) entries.  `owner_uid`/`owner_gid` fill the e_id of the USER_OBJ /
 * GROUP_OBJ entries (informational, as Linux/Solaris encode them).  Writes up
 * to `access_max` / `deflt_max` entries and sets `*aclcnt` / `*dfaclcnt`.
 * `deflt`/`dfaclcnt` are only populated when `is_dir`.  Returns 0 on success,
 * -1 if an output array is too small.
 */
int chimera_acl_to_posix(
    const struct chimera_acl       *acl,
    uint32_t                        owner_uid,
    uint32_t                        owner_gid,
    int                             is_dir,
    struct chimera_posix_acl_entry *access,
    unsigned                        access_max,
    uint32_t                       *aclcnt,
    struct chimera_posix_acl_entry *deflt,
    unsigned                        deflt_max,
    uint32_t                       *dfaclcnt);

/*
 * Build a canonical ACL from POSIX.1e access + default entries.  The ACL_MASK
 * is folded into the group-class effective permissions; default entries become
 * INHERIT_ONLY|FILE_INHERIT|DIR_INHERIT templates.  Writes into `out` (caller
 * provides storage for `max_aces` ACEs); returns the ACE count, or -1 on
 * overflow / malformed input.
 */
int chimera_acl_from_posix(
    const struct chimera_posix_acl_entry *access,
    uint32_t                              aclcnt,
    const struct chimera_posix_acl_entry *deflt,
    uint32_t                              dfaclcnt,
    struct chimera_acl                   *out,
    unsigned                              max_aces);
