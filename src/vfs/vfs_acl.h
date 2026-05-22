// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Canonical access-control model for Chimera.
 *
 * Chimera carries a single ACL model in the VFS: the NFSv4 / Windows-NTFS ACL.
 * These two protocol models are isomorphic by design -- identical ACE types,
 * the exact same access-mask bit layout, and the same low inheritance flags --
 * so one representation holds both losslessly.  POSIX 9-bit mode is the only
 * other thing we project to, and it is a deliberately lossy, one-way
 * approximation (see chimera_acl_to_mode).  POSIX.1e draft ACLs are not
 * represented at all.
 *
 * Access-mask bit values below are chosen to match both NFSv4 ACE4_* and the
 * Windows FILE_ and STANDARD_ rights constants, which share the same layout.
 */

#include <stdint.h>
#include <stddef.h>

struct chimera_vfs_cred;

/* ---- ACE types (match nfsace4 type and the NT ACE type family) ---------- */
#define CHIMERA_ACE_ALLOWED                0
#define CHIMERA_ACE_DENIED                 1
#define CHIMERA_ACE_AUDIT                  2
#define CHIMERA_ACE_ALARM                  3

/* ---- Access-mask bits --------------------------------------------------- */
#define CHIMERA_ACE_READ_DATA              0x00000001 /* LIST_DIRECTORY    */
#define CHIMERA_ACE_WRITE_DATA             0x00000002 /* ADD_FILE          */
#define CHIMERA_ACE_APPEND_DATA            0x00000004 /* ADD_SUBDIRECTORY  */
#define CHIMERA_ACE_READ_NAMED_ATTRS       0x00000008
#define CHIMERA_ACE_WRITE_NAMED_ATTRS      0x00000010
#define CHIMERA_ACE_EXECUTE                0x00000020
#define CHIMERA_ACE_DELETE_CHILD           0x00000040
#define CHIMERA_ACE_READ_ATTRIBUTES        0x00000080
#define CHIMERA_ACE_WRITE_ATTRIBUTES       0x00000100
#define CHIMERA_ACE_DELETE                 0x00010000
#define CHIMERA_ACE_READ_ACL               0x00020000
#define CHIMERA_ACE_WRITE_ACL              0x00040000
#define CHIMERA_ACE_WRITE_OWNER            0x00080000
#define CHIMERA_ACE_SYNCHRONIZE            0x00100000

#define CHIMERA_ACE_MASK_ALL               0x001f01ff

/* ---- ACE flags (canonical = NFSv4 layout; SMB marshaller translates) ---- */
#define CHIMERA_ACE_FLAG_FILE_INHERIT      0x01
#define CHIMERA_ACE_FLAG_DIR_INHERIT       0x02
#define CHIMERA_ACE_FLAG_NO_PROPAGATE      0x04
#define CHIMERA_ACE_FLAG_INHERIT_ONLY      0x08
#define CHIMERA_ACE_FLAG_SUCCESSFUL_ACCESS 0x10
#define CHIMERA_ACE_FLAG_FAILED_ACCESS     0x20
#define CHIMERA_ACE_FLAG_IDENTIFIER_GROUP  0x40
#define CHIMERA_ACE_FLAG_INHERITED         0x80

#define CHIMERA_ACE_FLAG_INHERIT_MASK      ( \
            CHIMERA_ACE_FLAG_FILE_INHERIT | \
            CHIMERA_ACE_FLAG_DIR_INHERIT | \
            CHIMERA_ACE_FLAG_NO_PROPAGATE | \
            CHIMERA_ACE_FLAG_INHERIT_ONLY)

/* ---- ACL control flags -------------------------------------------------- */
#define CHIMERA_ACL_CTRL_PROTECTED         0x01 /* do not inherit from parent  */
#define CHIMERA_ACL_CTRL_AUTO_INHERITED    0x02
#define CHIMERA_ACL_CTRL_DEFAULTED         0x04

/* ---- Principal ---------------------------------------------------------- */
enum chimera_principal_type {
    CHIMERA_PRINCIPAL_USER    = 0, /* numeric uid                            */
    CHIMERA_PRINCIPAL_GROUP   = 1, /* numeric gid                            */
    CHIMERA_PRINCIPAL_SPECIAL = 2, /* special-who below                      */
};

enum chimera_special_who {
    CHIMERA_WHO_OWNER         = 0, /* OWNER@         */
    CHIMERA_WHO_GROUP         = 1, /* GROUP@         */
    CHIMERA_WHO_EVERYONE      = 2, /* EVERYONE@      */
    CHIMERA_WHO_INTERACTIVE   = 3,
    CHIMERA_WHO_NETWORK       = 4,
    CHIMERA_WHO_AUTHENTICATED = 5,
    CHIMERA_WHO_ANONYMOUS     = 6,
    /* Inheritance-template placeholders (Windows CREATOR OWNER / CREATOR GROUP,
     * SIDs S-1-3-0 / S-1-3-1).  These match no caller on the object they sit on;
     * during inheritance they are substituted with the new object's OWNER@ /
     * GROUP@ for the effective inherited ACE. */
    CHIMERA_WHO_CREATOR_OWNER = 7,
    CHIMERA_WHO_CREATOR_GROUP = 8,
    /* NT AUTHORITY\SYSTEM (S-1-5-18).  Emitted in the Windows-style default DACL
     * so SMB clients see the owner+SYSTEM full-control default they expect; it
     * matches no Unix caller during access evaluation. */
    CHIMERA_WHO_SYSTEM        = 9,
    /* OWNER RIGHTS (S-1-3-4): matches the object's current owner, but its mere
     * presence in a DACL suppresses the otherwise-implicit owner rights so the
     * owner's access is defined entirely by the ACEs. */
    CHIMERA_WHO_OWNER_RIGHTS  = 10,
};

struct chimera_principal {
    uint8_t  type;    /* enum chimera_principal_type   */
    uint8_t  special; /* enum chimera_special_who      */
    uint32_t id;      /* uid or gid when type != SPECIAL */
};

struct chimera_ace {
    uint16_t                 type;
    uint16_t                 flags;
    uint32_t                 access_mask;
    struct chimera_principal who;
};

struct chimera_acl {
    uint16_t           num_aces;
    uint16_t           ctrl_flags;
    struct chimera_ace aces[];
};

/* Cap on ACE count for a single object (defends parsing/inheritance loops). */
#define CHIMERA_ACL_MAX_ACES 1024

static inline size_t
chimera_acl_size(unsigned num_aces)
{
    return sizeof(struct chimera_acl) + (size_t) num_aces * sizeof(struct chimera_ace);
} /* chimera_acl_size */

/* Convenience perm-class masks used by mode<->ACL projection. */
#define CHIMERA_ACE_PERM_R (CHIMERA_ACE_READ_DATA | CHIMERA_ACE_READ_NAMED_ATTRS)
#define CHIMERA_ACE_PERM_W (CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_APPEND_DATA | \
                            CHIMERA_ACE_WRITE_NAMED_ATTRS)
#define CHIMERA_ACE_PERM_X (CHIMERA_ACE_EXECUTE)

/*
 * Evaluate the canonical ACL for `cred` and return the subset of `requested`
 * (canonical access-mask bits) that is granted.  Implements the RFC 7530
 * ordered ALLOW/DENY walk.  If `acl` is NULL, falls back to a POSIX mode-bit
 * check synthesised from `mode`.  uid 0 is granted everything requested.
 *
 * `is_dir` selects directory-meaningful semantics where they differ.
 */
uint32_t chimera_acl_access_check(
    const struct chimera_acl      *acl,
    uint32_t                       mode,
    uint64_t                       owner_uid,
    uint64_t                       owner_gid,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested,
    int                            is_dir);

/*
 * Strict ACL evaluation: the ordered ALLOW/DENY walk over the explicit ACEs
 * only, with none of the baseline / owner-implicit grants chimera_acl_access_
 * check applies (and a NULL ACL grants nothing).  Used for access-based
 * enumeration, where an entry is hidden unless its DACL itself grants the
 * caller the requested read rights.
 */
uint32_t chimera_acl_access_raw(
    const struct chimera_acl      *acl,
    uint64_t                       owner_uid,
    uint64_t                       owner_gid,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested);

/*
 * Synthesise a canonical ACL from POSIX mode (lower 9 bits). Exact direction.
 * Writes up to 5 ACEs into `out`; returns the ACE count, or -1 if max_aces is
 * too small.
 */
int chimera_acl_from_mode(
    uint32_t            mode,
    struct chimera_acl *out,
    unsigned            max_aces);

/*
 * Build a Windows-style default DACL from POSIX mode: the OWNER@ gets full
 * control; GROUP@/EVERYONE@ track the mode bits.  Used to seed objects created
 * over SMB with no explicit SD and no inheritable parent ACE, so a Windows
 * client sees owner-full-control (plain mode would deny e.g. FILE_EXECUTE on a
 * 0644 file).  Writes up to 4 ACEs into `out`; returns the ACE count, or -1.
 */
int chimera_acl_default_acl(
    uint32_t            mode,
    struct chimera_acl *out,
    unsigned            max_aces);

/*
 * Project a canonical ACL down to POSIX 9-bit permission bits for NFSv3/POSIX
 * getattr.  Lossy, restrictive rounding (a class bit is set only if granted by
 * the ACEs that bear on that class).  Returns the 9 permission bits (no type).
 */
uint32_t chimera_acl_to_mode(
    const struct chimera_acl *acl);

/*
 * Apply a POSIX chmod to an existing rich ACL: regenerate the special-who
 * (OWNER@/GROUP@/EVERYONE@) ACEs from new_mode while preserving explicit
 * named-user/named-group ACEs.  Writes into `out`; returns ACE count or -1.
 */
int chimera_acl_chmod(
    const struct chimera_acl *in,
    uint32_t                  new_mode,
    struct chimera_acl       *out,
    unsigned                  max_aces);

/*
 * Compute the ACL a freshly-created child inherits from `parent`, honouring the
 * FILE_INHERIT/DIR_INHERIT/INHERIT_ONLY/NO_PROPAGATE flags.  `is_dir` selects
 * which inheritable ACEs apply and how propagation flags carry forward.  If the
 * parent contributes no inheritable ACE, falls back to from_mode(create_mode).
 * Writes into `out`; returns ACE count or -1.
 */
int chimera_acl_inherit(
    const struct chimera_acl *parent,
    int                       is_dir,
    uint32_t                  create_mode,
    struct chimera_acl       *out,
    unsigned                  max_aces);
