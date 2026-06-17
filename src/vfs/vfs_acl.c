// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "vfs_acl.h"
#include "vfs_cred.h"
#include "common/macros.h"

/* Bits everyone is granted regardless of rwx (attribute and ACL reads).
 * SYNCHRONIZE is deliberately excluded: Windows grants it only as part of a
 * specific/generic right, not unconditionally, so it must come from an ACE. */
#define ACE_BASELINE       (CHIMERA_ACE_READ_ATTRIBUTES | \
                            CHIMERA_ACE_READ_ACL)

/* Extra bits the owner always holds (it owns the object). */
#define ACE_OWNER_EXTRA    (CHIMERA_ACE_WRITE_ATTRIBUTES | \
                            CHIMERA_ACE_WRITE_ACL | \
                            CHIMERA_ACE_WRITE_OWNER | \
                            CHIMERA_ACE_READ_ACL)

/* Owner-implied rights under an explicit ACL (Windows "owner rights"): the
 * object owner always holds READ_CONTROL, WRITE_DAC and DELETE, but NOT, for
 * example, WRITE_ATTRIBUTES or WRITE_OWNER -- those must be granted by an ACE. */
#define ACE_OWNER_IMPLICIT (CHIMERA_ACE_READ_ACL | CHIMERA_ACE_WRITE_ACL | \
                            CHIMERA_ACE_DELETE)

/* Translate the rwx bits of one POSIX permission class into a canonical mask. */
static uint32_t
perm_class_to_mask(
    int r,
    int w,
    int x)
{
    uint32_t mask = 0;

    if (r) {
        mask |= CHIMERA_ACE_PERM_R;
    }
    if (w) {
        mask |= CHIMERA_ACE_PERM_W;
    }
    if (x) {
        mask |= CHIMERA_ACE_PERM_X;
    }
    return mask;
} /* perm_class_to_mask */

/*
 * Is `cred` a member of group `gid` (primary or supplementary)?
 */
static int
cred_in_group(
    const struct chimera_vfs_cred *cred,
    uint64_t                       gid)
{
    if ((uint64_t) cred->gid == gid) {
        return 1;
    }
    for (uint32_t i = 0; i < cred->ngids; i++) {
        if ((uint64_t) cred->gids[i] == gid) {
            return 1;
        }
    }
    return 0;
} /* cred_in_group */

/*
 * Does the principal in `ace` apply to the calling credential, given the file
 * owner/owning-group?
 */
static int
ace_applies(
    const struct chimera_ace      *ace,
    uint64_t                       owner_uid,
    uint64_t                       owner_gid,
    const struct chimera_vfs_cred *cred)
{
    const struct chimera_principal *who = &ace->who;

    switch (who->type) {
        case CHIMERA_PRINCIPAL_SPECIAL:
            switch (who->special) {
                case CHIMERA_WHO_OWNER:
                    return (uint64_t) cred->uid == owner_uid;
                case CHIMERA_WHO_GROUP:
                    return cred_in_group(cred, owner_gid);
                case CHIMERA_WHO_EVERYONE:
                    return 1;
                case CHIMERA_WHO_AUTHENTICATED:
                    /* AUTHENTICATED@ (S-1-5-11, "Authenticated Users") matches
                     * any caller with an established, non-anonymous identity.
                     * AUTH_NONE is short-circuited to a full grant before ACL
                     * evaluation, so a cred reaching here is authenticated. */
                    return cred->flavor != CHIMERA_VFS_AUTH_NONE;
                case CHIMERA_WHO_OWNER_RIGHTS:
                    /* OWNER RIGHTS matches the caller when it is the owner. */
                    return (uint64_t) cred->uid == owner_uid;
                default:
                    /* INTERACTIVE@/NETWORK@/... -- no session-class signal yet.
                     * CREATOR_OWNER@/CREATOR_GROUP@ are inheritance-template
                     * placeholders and match no caller on the object itself. */
                    return 0;
            } /* switch */
        case CHIMERA_PRINCIPAL_USER:
            return (uint64_t) cred->uid == who->id;
        case CHIMERA_PRINCIPAL_GROUP:
            return cred_in_group(cred, who->id);
        default:
            return 0;
    } /* switch */
} /* ace_applies */

/*
 * Mode-bit access fallback used when no ACL is present.  Mirrors traditional
 * UNIX rwx evaluation but expressed in canonical mask bits.
 */
static uint32_t
mode_access_check(
    uint32_t                       mode,
    uint64_t                       owner_uid,
    uint64_t                       owner_gid,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested)
{
    int      r, w, x;
    uint32_t allowed;

    if ((uint64_t) cred->uid == owner_uid) {
        r = !!(mode & S_IRUSR);
        w = !!(mode & S_IWUSR);
        x = !!(mode & S_IXUSR);
    } else if (cred_in_group(cred, owner_gid)) {
        r = !!(mode & S_IRGRP);
        w = !!(mode & S_IWGRP);
        x = !!(mode & S_IXGRP);
    } else {
        r = !!(mode & S_IROTH);
        w = !!(mode & S_IWOTH);
        x = !!(mode & S_IXOTH);
    }

    allowed = ACE_BASELINE | perm_class_to_mask(r, w, x);

    /* The owner always holds the same implicit rights as under an explicit ACL
     * (READ_CONTROL/WRITE_DAC/DELETE) -- not WRITE_ATTRIBUTES or WRITE_OWNER,
     * which Windows grants the owner only via an explicit ACE.  (DELETE_CHILD is
     * derived from write+execute by the caller, for both this and the ACL
     * path.) */
    if ((uint64_t) cred->uid == owner_uid) {
        allowed |= ACE_OWNER_IMPLICIT;
    }

    return requested & allowed;
} /* mode_access_check */

SYMBOL_EXPORT uint32_t
chimera_acl_access_check(
    const struct chimera_acl      *acl,
    uint32_t                       mode,
    uint64_t                       owner_uid,
    uint64_t                       owner_gid,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested,
    int                            is_dir)
{
    uint32_t granted = 0;
    uint32_t eval, remaining;

    /* Root bypasses ACL evaluation (no_root_squash semantics; squashing is
     * handled when the credential is mapped). */
    if (cred->uid == 0) {
        return requested;
    }

    /* POSIX governs deleting a directory entry by write+execute on the
     * directory.  Map that to DELETE_CHILD here -- in BOTH the mode and ACL
     * paths -- so it is granted consistently whether the object is mode-only or
     * carries a (possibly mode-synthesised) ACL; otherwise DELETE_CHILD, which
     * lives in no perm class and is not emitted by chimera_acl_from_mode, would
     * never be granted once an ACL is present and every rename/delete by a
     * non-owner in a writable dir would be refused.  Evaluate write+execute even
     * when only DELETE_CHILD was requested. */
    eval = requested;
    if (is_dir && (requested & CHIMERA_ACE_DELETE_CHILD)) {
        eval |= CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE;
    }
    remaining = eval;

    if (!acl || acl->num_aces == 0) {
        granted = mode_access_check(mode, owner_uid, owner_gid, cred, eval);

        if (is_dir && (granted & CHIMERA_ACE_WRITE_DATA) &&
            (granted & CHIMERA_ACE_EXECUTE)) {
            granted |= CHIMERA_ACE_DELETE_CHILD;
        }
        return granted & requested;
    }

    uint32_t denied               = 0;
    int      owner_rights_present = 0;

    /* An OWNER_RIGHTS ACE anywhere in the DACL disables the implicit owner
     * rights -- the owner's access is then defined solely by the ACEs. */
    for (unsigned i = 0; i < acl->num_aces; i++) {
        if (acl->aces[i].who.type == CHIMERA_PRINCIPAL_SPECIAL &&
            acl->aces[i].who.special == CHIMERA_WHO_OWNER_RIGHTS) {
            owner_rights_present = 1;
            break;
        }
    }

    for (unsigned i = 0; i < acl->num_aces && remaining; i++) {
        const struct chimera_ace *ace = &acl->aces[i];

        /* Audit/alarm ACEs do not affect the access decision. */
        if (ace->type != CHIMERA_ACE_ALLOWED &&
            ace->type != CHIMERA_ACE_DENIED) {
            continue;
        }

        /* INHERIT_ONLY ACEs never apply to the object they sit on. */
        if (ace->flags & CHIMERA_ACE_FLAG_INHERIT_ONLY) {
            continue;
        }

        if (!ace_applies(ace, owner_uid, owner_gid, cred)) {
            continue;
        }

        if (ace->type == CHIMERA_ACE_ALLOWED) {
            granted   |= (ace->access_mask & remaining);
            remaining &= ~ace->access_mask;
        } else { /* DENIED */
            /* Denied bits drop out of consideration and are never granted. */
            denied    |= ace->access_mask;
            remaining &= ~ace->access_mask;
        }
    }

    /* Implicit rights (Windows): metadata-read/sync are available to anyone,
     * and the object's owner additionally always holds the attribute/ACL/owner
     * write rights -- unless an explicit DENY ACE removed them.  These fill in
     * bits a minimal explicit ALLOW ACE would otherwise leave ungranted. */
    {
        uint32_t implicit = ACE_BASELINE;

        if ((uint64_t) cred->uid == owner_uid && !owner_rights_present) {
            implicit |= ACE_OWNER_IMPLICIT;
        }

        granted |= implicit & eval & ~denied;
    }

    /* Derive DELETE_CHILD from the directory's effective write+execute (see the
     * note above); applies to mode-synthesised and explicit ACLs alike.  An
     * explicit DENY of DELETE_CHILD must win, though: a directory may grant
     * WRITE_DATA (add entries) while still vetoing removal via a
     * DELETE_CHILD::deny ACE, so do not re-synthesise a right that was
     * explicitly denied. */
    if (is_dir && (granted & CHIMERA_ACE_WRITE_DATA) &&
        (granted & CHIMERA_ACE_EXECUTE) &&
        !(denied & CHIMERA_ACE_DELETE_CHILD)) {
        granted |= CHIMERA_ACE_DELETE_CHILD;
    }

    return granted & requested;
} /* chimera_acl_access_check */

SYMBOL_EXPORT uint32_t
chimera_acl_access_raw(
    const struct chimera_acl      *acl,
    uint64_t                       owner_uid,
    uint64_t                       owner_gid,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested)
{
    uint32_t granted   = 0;
    uint32_t remaining = requested;

    /* Strict evaluation: only the explicit ALLOW/DENY ACEs are considered --
     * none of the baseline/owner-implicit grants chimera_acl_access_check adds.
     * Used for access-based enumeration, where Windows hides an entry unless the
     * DACL itself grants the caller the read rights.  No ACL grants nothing. */
    if (!acl) {
        return 0;
    }

    for (unsigned i = 0; i < acl->num_aces && remaining; i++) {
        const struct chimera_ace *ace = &acl->aces[i];

        if (ace->type != CHIMERA_ACE_ALLOWED &&
            ace->type != CHIMERA_ACE_DENIED) {
            continue;
        }
        if (ace->flags & CHIMERA_ACE_FLAG_INHERIT_ONLY) {
            continue;
        }
        if (!ace_applies(ace, owner_uid, owner_gid, cred)) {
            continue;
        }
        if (ace->type == CHIMERA_ACE_ALLOWED) {
            granted   |= (ace->access_mask & remaining);
            remaining &= ~ace->access_mask;
        } else {
            remaining &= ~ace->access_mask;
        }
    }

    return granted;
} /* chimera_acl_access_raw */

SYMBOL_EXPORT int
chimera_acl_from_mode(
    uint32_t            mode,
    struct chimera_acl *out,
    unsigned            max_aces)
{
    uint32_t owner_p = perm_class_to_mask(!!(mode & S_IRUSR),
                                          !!(mode & S_IWUSR),
                                          !!(mode & S_IXUSR));
    uint32_t group_p = perm_class_to_mask(!!(mode & S_IRGRP),
                                          !!(mode & S_IWGRP),
                                          !!(mode & S_IXGRP));
    uint32_t other_p = perm_class_to_mask(!!(mode & S_IROTH),
                                          !!(mode & S_IWOTH),
                                          !!(mode & S_IXOTH));
    uint32_t owner_deny = (group_p | other_p) & ~owner_p;
    uint32_t group_deny = other_p & ~group_p;
    unsigned n          = 0;

#define EMIT(t, m, sp) \
        do { \
            if (n >= max_aces) { return -1; } \
            out->aces[n].type        = (t); \
            out->aces[n].flags       = 0; \
            out->aces[n].access_mask = (m); \
            out->aces[n].who.type    = CHIMERA_PRINCIPAL_SPECIAL; \
            out->aces[n].who.special = (sp); \
            out->aces[n].who.id      = 0; \
            n++; \
        } while (0)

    /* Order matters: deny the owner/group their "extra" cumulative bits before
     * the broader EVERYONE@/GROUP@ ALLOW entries can grant them. */
    EMIT(CHIMERA_ACE_ALLOWED, owner_p | ACE_BASELINE | ACE_OWNER_EXTRA,
         CHIMERA_WHO_OWNER);
    if (owner_deny) {
        EMIT(CHIMERA_ACE_DENIED, owner_deny, CHIMERA_WHO_OWNER);
    }
    EMIT(CHIMERA_ACE_ALLOWED, group_p | ACE_BASELINE, CHIMERA_WHO_GROUP);
    if (group_deny) {
        EMIT(CHIMERA_ACE_DENIED, group_deny, CHIMERA_WHO_GROUP);
    }
    EMIT(CHIMERA_ACE_ALLOWED, other_p | ACE_BASELINE, CHIMERA_WHO_EVERYONE);
#undef EMIT

    out->num_aces   = n;
    out->ctrl_flags = 0;
    return n;
} /* chimera_acl_from_mode */

SYMBOL_EXPORT int
chimera_acl_default_acl(
    uint32_t            mode,
    struct chimera_acl *out,
    unsigned            max_aces)
{
    unsigned n = 0;

    (void) mode;

#define EMIT(t, m, sp) \
        do { \
            if (n >= max_aces) { return -1; } \
            out->aces[n].type        = (t); \
            out->aces[n].flags       = 0; \
            out->aces[n].access_mask = (m); \
            out->aces[n].who.type    = CHIMERA_PRINCIPAL_SPECIAL; \
            out->aces[n].who.special = (sp); \
            out->aces[n].who.id      = 0; \
            n++; \
        } while (0)

    /* Windows-style default DACL for an object created with neither an explicit
     * security descriptor nor an inheritable parent ACE: the owner and SYSTEM
     * each get full control, matching the token default DACL a Windows client
     * (and the smbtorture acls suite) expects.  The POSIX mode is left intact
     * for NFS; group/other access on this object is governed by this DACL. */
    EMIT(CHIMERA_ACE_ALLOWED, CHIMERA_ACE_MASK_ALL, CHIMERA_WHO_OWNER);
    EMIT(CHIMERA_ACE_ALLOWED, CHIMERA_ACE_MASK_ALL, CHIMERA_WHO_SYSTEM);
#undef EMIT

    out->num_aces   = n;
    out->ctrl_flags = 0;
    return n;
} /* chimera_acl_default_acl */

/*
 * Accumulate the effective allowed mask for one POSIX class, walking the ACL in
 * order and honouring the deny-removes-bit semantics.  `match_owner`/
 * `match_group`/`match_everyone` select which special-who ACEs bear on the
 * class being projected.
 */
static uint32_t
project_class(
    const struct chimera_acl *acl,
    int                       match_owner,
    int                       match_group,
    int                       match_everyone)
{
    uint32_t granted   = 0;
    uint32_t remaining = CHIMERA_ACE_MASK_ALL;

    for (unsigned i = 0; i < acl->num_aces && remaining; i++) {
        const struct chimera_ace *ace = &acl->aces[i];
        int                       applies;

        if (ace->type != CHIMERA_ACE_ALLOWED &&
            ace->type != CHIMERA_ACE_DENIED) {
            continue;
        }
        if (ace->flags & CHIMERA_ACE_FLAG_INHERIT_ONLY) {
            continue;
        }
        if (ace->who.type != CHIMERA_PRINCIPAL_SPECIAL) {
            /* Named users/groups are not part of the POSIX owner/group/other
             * model and are not represented in mode. */
            continue;
        }

        applies = (ace->who.special == CHIMERA_WHO_OWNER && match_owner) ||
            (ace->who.special == CHIMERA_WHO_GROUP && match_group) ||
            (ace->who.special == CHIMERA_WHO_EVERYONE && match_everyone);

        if (!applies) {
            continue;
        }

        if (ace->type == CHIMERA_ACE_ALLOWED) {
            granted   |= (ace->access_mask & remaining);
            remaining &= ~ace->access_mask;
        } else {
            remaining &= ~ace->access_mask;
        }
    }

    return granted;
} /* project_class */

static uint32_t
mask_to_perm_bits(
    uint32_t mask,
    int      shift)
{
    uint32_t bits = 0;

    if (mask & CHIMERA_ACE_READ_DATA) {
        bits |= S_IROTH;
    }
    if (mask & CHIMERA_ACE_WRITE_DATA) {
        bits |= S_IWOTH;
    }
    if (mask & CHIMERA_ACE_EXECUTE) {
        bits |= S_IXOTH;
    }
    return bits << shift;
} /* mask_to_perm_bits */

SYMBOL_EXPORT uint32_t
chimera_acl_to_mode(const struct chimera_acl *acl)
{
    uint32_t mode = 0;

    if (!acl || acl->num_aces == 0) {
        return 0;
    }

    /* owner class: OWNER@ and EVERYONE@ apply; group: GROUP@ and EVERYONE@;
     * other: only EVERYONE@. */
    mode |= mask_to_perm_bits(project_class(acl, 1, 0, 1), 6); /* user  */
    mode |= mask_to_perm_bits(project_class(acl, 0, 1, 1), 3); /* group */
    mode |= mask_to_perm_bits(project_class(acl, 0, 0, 1), 0); /* other */

    return mode;
} /* chimera_acl_to_mode */

SYMBOL_EXPORT int
chimera_acl_chmod(
    const struct chimera_acl *in,
    uint32_t                  new_mode,
    struct chimera_acl       *out,
    unsigned                  max_aces)
{
    unsigned n = 0;

    /* Preserve explicit named-user/named-group ACEs in their original order. */
    if (in) {
        for (unsigned i = 0; i < in->num_aces; i++) {
            const struct chimera_ace *ace = &in->aces[i];

            if (ace->who.type == CHIMERA_PRINCIPAL_SPECIAL) {
                continue;
            }
            if (n >= max_aces) {
                return -1;
            }
            out->aces[n++] = *ace;
        }
    }

    /* Append freshly-derived special-who ACEs from the new mode.  Build them in
     * a small stack buffer first to avoid aliasing `out`'s header. */
    {
        uint8_t             buf[sizeof(struct chimera_acl) +
                                5 * sizeof(struct chimera_ace)];
        struct chimera_acl *mb = (struct chimera_acl *) buf;
        int                 mn = chimera_acl_from_mode(new_mode, mb, 5);

        if (mn < 0) {
            return -1;
        }
        for (int i = 0; i < mn; i++) {
            if (n >= max_aces) {
                return -1;
            }
            out->aces[n++] = mb->aces[i];
        }
    }

    out->num_aces   = n;
    out->ctrl_flags = in ? in->ctrl_flags : 0;
    return n;
} /* chimera_acl_chmod */

/* Substitute the CREATOR_OWNER/CREATOR_GROUP placeholder for the concrete
 * OWNER@/GROUP@ when an inheritable ACE is materialised as an effective entry
 * on a child; other principals pass through unchanged. */
static struct chimera_principal
inherit_subst_creator(const struct chimera_principal *who)
{
    struct chimera_principal p = *who;

    if (p.type == CHIMERA_PRINCIPAL_SPECIAL) {
        if (p.special == CHIMERA_WHO_CREATOR_OWNER) {
            p.special = CHIMERA_WHO_OWNER;
        } else if (p.special == CHIMERA_WHO_CREATOR_GROUP) {
            p.special = CHIMERA_WHO_GROUP;
        }
    }
    return p;
} /* inherit_subst_creator */

SYMBOL_EXPORT int
chimera_acl_inherit(
    const struct chimera_acl *parent,
    int                       is_dir,
    uint32_t                  create_mode,
    struct chimera_acl       *out,
    unsigned                  max_aces)
{
    unsigned n = 0;
    /* When the parent DACL is in auto-inherit mode, the inherited ACEs it
     * produces are marked SEC_ACE_FLAG_INHERITED; under a legacy (manually-set)
     * parent DACL they are not.  This is the bit smbtorture's INHERITFLAGS
     * compares. */
    uint16_t inh = (parent &&
                    (parent->ctrl_flags & CHIMERA_ACL_CTRL_AUTO_INHERITED)) ?
        CHIMERA_ACE_FLAG_INHERITED : 0;

    if (parent) {
        for (unsigned i = 0; i < parent->num_aces; i++) {
            const struct chimera_ace *src = &parent->aces[i];
            uint16_t                  f   = src->flags;
            int                       oi  = !!(f & CHIMERA_ACE_FLAG_FILE_INHERIT);
            int                       ci  = !!(f & CHIMERA_ACE_FLAG_DIR_INHERIT);
            int                       np  = !!(f & CHIMERA_ACE_FLAG_NO_PROPAGATE);

            /* Only ACEs flagged inheritable propagate at all. */
            if (!oi && !ci) {
                continue;
            }

            if (!is_dir) {
                /* A file inherits an OBJECT_INHERIT ACE as a single effective
                 * entry: CREATOR_* resolved to OWNER@/GROUP@. */
                if (!oi) {
                    continue;
                }
                if (n >= max_aces) {
                    return -1;
                }
                out->aces[n]       = *src;
                out->aces[n].who   = inherit_subst_creator(&src->who);
                out->aces[n].flags = inh;
                n++;
            } else {
                /* A directory the ACE applies to (CONTAINER_INHERIT) gets an
                 * effective entry: CREATOR_* resolved, no inheritance flags. */
                if (ci) {
                    if (n >= max_aces) {
                        return -1;
                    }
                    out->aces[n]       = *src;
                    out->aces[n].who   = inherit_subst_creator(&src->who);
                    out->aces[n].flags = inh;
                    n++;
                }

                /* Unless NO_PROPAGATE, the ACE continues to propagate from this
                 * directory as an INHERIT_ONLY copy carrying the original
                 * trustee (CREATOR_* stays a placeholder so grandchildren
                 * resolve their own owner) and the original inherit flags. */
                if (!np) {
                    if (n >= max_aces) {
                        return -1;
                    }
                    out->aces[n]       = *src;
                    out->aces[n].flags = (f & (CHIMERA_ACE_FLAG_FILE_INHERIT |
                                               CHIMERA_ACE_FLAG_DIR_INHERIT)) |
                        CHIMERA_ACE_FLAG_INHERIT_ONLY | inh;
                    n++;
                }
            }
        }
    }

    (void) create_mode;

    /* Return the number of inherited ACEs (0 = nothing inheritable applied; the
     * caller seeds its own default in that case).  AUTO_INHERITED propagates
     * from the parent: a child is in auto-inherit mode only if the parent was. */
    out->num_aces   = n;
    out->ctrl_flags = (n && parent) ?
        (parent->ctrl_flags & CHIMERA_ACL_CTRL_AUTO_INHERITED) : 0;
    return n;
} /* chimera_acl_inherit */
