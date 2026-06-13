// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs_acl_posix.h"
#include "vfs_acl.h"
#include "common/macros.h"

/* Baseline / owner-extra bit sets, kept bit-identical to chimera_acl_from_mode
 * (vfs_acl.c) so a mode-derived ACL round-trips and a named/default ALLOW ACE
 * synthesised here grants the same metadata rights the special-who ACEs do. */
#define ACE_BASELINE      (CHIMERA_ACE_READ_ATTRIBUTES | CHIMERA_ACE_READ_ACL)
#define ACE_OWNER_EXTRA   (CHIMERA_ACE_WRITE_ATTRIBUTES | CHIMERA_ACE_WRITE_ACL | \
                           CHIMERA_ACE_WRITE_OWNER | CHIMERA_ACE_READ_ACL)

/* Inheritance flags carried by every default-ACL template ACE. */
#define ACE_DEFAULT_FLAGS (CHIMERA_ACE_FLAG_INHERIT_ONLY | \
                           CHIMERA_ACE_FLAG_FILE_INHERIT | \
                           CHIMERA_ACE_FLAG_DIR_INHERIT)

/* Expand POSIX rwx (3 bits) into a canonical access mask. */
static inline uint32_t
posix_perm_to_mask(uint16_t perm)
{
    uint32_t mask = 0;

    if (perm & CHIMERA_POSIX_ACL_READ) {
        mask |= CHIMERA_ACE_PERM_R;
    }
    if (perm & CHIMERA_POSIX_ACL_WRITE) {
        mask |= CHIMERA_ACE_PERM_W;
    }
    if (perm & CHIMERA_POSIX_ACL_EXECUTE) {
        mask |= CHIMERA_ACE_PERM_X;
    }
    return mask;
} /* posix_perm_to_mask */

/* Collapse a canonical access mask down to POSIX rwx (3 bits).  Mirrors the
 * data-bit test used by chimera_acl_to_mode; metadata bits do not leak in. */
static inline uint16_t
mask_to_posix_perm(uint32_t mask)
{
    uint16_t perm = 0;

    if (mask & CHIMERA_ACE_READ_DATA) {
        perm |= CHIMERA_POSIX_ACL_READ;
    }
    if (mask & CHIMERA_ACE_WRITE_DATA) {
        perm |= CHIMERA_POSIX_ACL_WRITE;
    }
    if (mask & CHIMERA_ACE_EXECUTE) {
        perm |= CHIMERA_POSIX_ACL_EXECUTE;
    }
    return perm;
} /* mask_to_posix_perm */

/* True for an ACE that bears on the object itself (effective, not a pure
 * inheritance template) and contributes to access. */
static inline int
ace_is_effective_allow(const struct chimera_ace *ace)
{
    return ace->type == CHIMERA_ACE_ALLOWED &&
           !(ace->flags & CHIMERA_ACE_FLAG_INHERIT_ONLY);
} /* ace_is_effective_allow */

/* True for an inheritable ALLOW ACE (a default-ACL template entry). */
static inline int
ace_is_inheritable_allow(const struct chimera_ace *ace)
{
    return ace->type == CHIMERA_ACE_ALLOWED &&
           (ace->flags & (CHIMERA_ACE_FLAG_FILE_INHERIT |
                          CHIMERA_ACE_FLAG_DIR_INHERIT));
} /* ace_is_inheritable_allow */

SYMBOL_EXPORT int
chimera_acl_to_posix(
    const struct chimera_acl       *acl,
    uint32_t                        owner_uid,
    uint32_t                        owner_gid,
    int                             is_dir,
    struct chimera_posix_acl_entry *access,
    unsigned                        access_max,
    uint32_t                       *aclcnt,
    struct chimera_posix_acl_entry *deflt,
    unsigned                        deflt_max,
    uint32_t                       *dfaclcnt)
{
    unsigned n     = 0;
    unsigned named = 0;
    uint32_t mode;
    uint16_t group_class;

#define EMIT(arr, max, idx, tag, perm, id) \
        do { \
            if ((idx) >= (max)) { return -1; } \
            (arr)[idx].e_tag  = (tag); \
            (arr)[idx].e_perm = (perm); \
            (arr)[idx].e_id   = (id); \
            (idx)++; \
        } while (0)

    /* ---- Access ACL ----------------------------------------------------- */
    /* OWNER@/GROUP@/EVERYONE@ classes via the same deny-aware projection the
     * mode path uses, so a mode-only ACL renders the canonical three entries. */
    mode        = chimera_acl_to_mode(acl);
    group_class = (mode >> 3) & 7; /* seed the mask with the group-obj perms */

    EMIT(access, access_max, n, CHIMERA_POSIX_ACL_USER_OBJ,
         (mode >> 6) & 7, owner_uid);

    /* Named users: each USER-principal effective ALLOW ACE. */
    if (acl) {
        for (unsigned i = 0; i < acl->num_aces; i++) {
            const struct chimera_ace *ace = &acl->aces[i];

            if (!ace_is_effective_allow(ace) ||
                ace->who.type != CHIMERA_PRINCIPAL_USER) {
                continue;
            }
            EMIT(access, access_max, n, CHIMERA_POSIX_ACL_USER,
                 mask_to_posix_perm(ace->access_mask), ace->who.id);
            named++;
        }
    }

    EMIT(access, access_max, n, CHIMERA_POSIX_ACL_GROUP_OBJ,
         (mode >> 3) & 7, owner_gid);

    /* Named groups. */
    if (acl) {
        for (unsigned i = 0; i < acl->num_aces; i++) {
            const struct chimera_ace *ace = &acl->aces[i];
            uint16_t                  perm;

            if (!ace_is_effective_allow(ace) ||
                ace->who.type != CHIMERA_PRINCIPAL_GROUP) {
                continue;
            }
            perm         = mask_to_posix_perm(ace->access_mask);
            group_class |= perm;
            EMIT(access, access_max, n, CHIMERA_POSIX_ACL_GROUP, perm,
                 ace->who.id);
            named++;
        }
        /* Fold named-user perms into the mask (group class = everything but
         * USER_OBJ and OTHER). */
        for (unsigned i = 0; i < acl->num_aces; i++) {
            const struct chimera_ace *ace = &acl->aces[i];

            if (ace_is_effective_allow(ace) &&
                ace->who.type == CHIMERA_PRINCIPAL_USER) {
                group_class |= mask_to_posix_perm(ace->access_mask);
            }
        }
    }

    /* POSIX has (and the Linux client accepts) a mask entry only when named
     * USER/GROUP entries exist; a minimal three-entry ACL must omit it (a lone
     * mask makes the decoded POSIX ACL invalid on the client). */
    if (named) {
        EMIT(access, access_max, n, CHIMERA_POSIX_ACL_MASK, group_class, 0);
    }
    EMIT(access, access_max, n, CHIMERA_POSIX_ACL_OTHER, mode & 7, 0);

    *aclcnt = n;

    /* ---- Default ACL (directories only) --------------------------------- */
    *dfaclcnt = 0;
    if (is_dir && acl && deflt && deflt_max) {
        unsigned dn       = 0;
        unsigned dnamed   = 0;
        int      have_def = 0;
        uint16_t duo = 0, dgo = 0, dot = 0, dgroup_class = 0;

        for (unsigned i = 0; i < acl->num_aces; i++) {
            const struct chimera_ace *ace = &acl->aces[i];
            uint16_t                  perm;

            if (!ace_is_inheritable_allow(ace)) {
                continue;
            }
            have_def = 1;
            perm     = mask_to_posix_perm(ace->access_mask);

            if (ace->who.type == CHIMERA_PRINCIPAL_SPECIAL) {
                switch (ace->who.special) {
                    case CHIMERA_WHO_CREATOR_OWNER:
                    case CHIMERA_WHO_OWNER:
                        duo = perm;
                        break;
                    case CHIMERA_WHO_CREATOR_GROUP:
                    case CHIMERA_WHO_GROUP:
                        dgo           = perm;
                        dgroup_class |= perm;
                        break;
                    case CHIMERA_WHO_EVERYONE:
                        dot = perm;
                        break;
                    default:
                        break;
                } /* switch */
            } else {
                dgroup_class |= perm;
                dnamed++;
            }
        }

        if (have_def) {
            EMIT(deflt, deflt_max, dn, CHIMERA_POSIX_ACL_USER_OBJ, duo,
                 owner_uid);

            for (unsigned i = 0; i < acl->num_aces; i++) {
                const struct chimera_ace *ace = &acl->aces[i];

                if (ace_is_inheritable_allow(ace) &&
                    ace->who.type == CHIMERA_PRINCIPAL_USER) {
                    EMIT(deflt, deflt_max, dn, CHIMERA_POSIX_ACL_USER,
                         mask_to_posix_perm(ace->access_mask), ace->who.id);
                }
            }

            EMIT(deflt, deflt_max, dn, CHIMERA_POSIX_ACL_GROUP_OBJ, dgo,
                 owner_gid);

            for (unsigned i = 0; i < acl->num_aces; i++) {
                const struct chimera_ace *ace = &acl->aces[i];

                if (ace_is_inheritable_allow(ace) &&
                    ace->who.type == CHIMERA_PRINCIPAL_GROUP) {
                    EMIT(deflt, deflt_max, dn, CHIMERA_POSIX_ACL_GROUP,
                         mask_to_posix_perm(ace->access_mask), ace->who.id);
                }
            }

            if (dnamed) {
                EMIT(deflt, deflt_max, dn, CHIMERA_POSIX_ACL_MASK,
                     dgroup_class, 0);
            }
            EMIT(deflt, deflt_max, dn, CHIMERA_POSIX_ACL_OTHER, dot, 0);

            *dfaclcnt = dn;
        }
    }
#undef EMIT

    return 0;
} /* chimera_acl_to_posix */

/* Parse the three base classes + mask out of a POSIX entry list. */
static void
posix_parse_base(
    const struct chimera_posix_acl_entry *entries,
    uint32_t                              count,
    uint16_t                             *user_obj,
    uint16_t                             *group_obj,
    uint16_t                             *other,
    uint16_t                             *mask,
    int                                  *has_mask)
{
    *user_obj = *group_obj = *other = 0;
    *mask     = 0x7;
    *has_mask = 0;

    for (uint32_t i = 0; i < count; i++) {
        uint16_t tag  = entries[i].e_tag & ~CHIMERA_NFS_ACL_DEFAULT;
        uint16_t perm = entries[i].e_perm & 0x7;

        switch (tag) {
            case CHIMERA_POSIX_ACL_USER_OBJ:
                *user_obj = perm;
                break;
            case CHIMERA_POSIX_ACL_GROUP_OBJ:
                *group_obj = perm;
                break;
            case CHIMERA_POSIX_ACL_OTHER:
                *other = perm;
                break;
            case CHIMERA_POSIX_ACL_MASK:
                *mask     = perm;
                *has_mask = 1;
                break;
            default:
                break;
        } /* switch */
    }
} /* posix_parse_base */

SYMBOL_EXPORT int
chimera_acl_from_posix(
    const struct chimera_posix_acl_entry *access,
    uint32_t                              aclcnt,
    const struct chimera_posix_acl_entry *deflt,
    uint32_t                              dfaclcnt,
    struct chimera_acl                   *out,
    unsigned                              max_aces)
{
    uint16_t uo, go, ot, mask;
    int      has_mask;
    uint16_t eff_go;
    uint32_t mode;
    int      n;

    posix_parse_base(access, aclcnt, &uo, &go, &ot, &mask, &has_mask);

    /* The group-obj effective permission is the stored group-obj intersected
     * with the mask (POSIX effective-perm semantics). */
    eff_go = has_mask ? (go & mask) : go;
    mode   = ((uint32_t) uo << 6) | ((uint32_t) eff_go << 3) | ot;

    /* Special-who block, bit-identical to the mode path (idempotent round
     * trip with chimera_acl_from_mode). */
    n = chimera_acl_from_mode(mode, out, max_aces);
    if (n < 0) {
        return -1;
    }

#define APPEND(t, m, fl, ptype, pspecial, pid) \
        do { \
            if ((unsigned) n >= max_aces) { return -1; } \
            out->aces[n].type        = (t); \
            out->aces[n].flags       = (fl); \
            out->aces[n].access_mask = (m); \
            out->aces[n].who.type    = (ptype); \
            out->aces[n].who.special = (pspecial); \
            out->aces[n].who.id      = (pid); \
            n++; \
        } while (0)

    /* Named access entries become ALLOW ACEs (perms masked).  DENY is not
     * representable in POSIX, so there is nothing to drop here. */
    for (uint32_t i = 0; i < aclcnt; i++) {
        uint16_t tag  = access[i].e_tag & ~CHIMERA_NFS_ACL_DEFAULT;
        uint16_t perm = access[i].e_perm & 0x7;

        if (has_mask) {
            perm &= mask;
        }
        if (tag == CHIMERA_POSIX_ACL_USER) {
            APPEND(CHIMERA_ACE_ALLOWED,
                   posix_perm_to_mask(perm) | ACE_BASELINE, 0,
                   CHIMERA_PRINCIPAL_USER, 0, access[i].e_id);
        } else if (tag == CHIMERA_POSIX_ACL_GROUP) {
            APPEND(CHIMERA_ACE_ALLOWED,
                   posix_perm_to_mask(perm) | ACE_BASELINE,
                   CHIMERA_ACE_FLAG_IDENTIFIER_GROUP,
                   CHIMERA_PRINCIPAL_GROUP, 0, access[i].e_id);
        }
    }

    /* Default entries become INHERIT_ONLY templates carrying the placeholder
    * CREATOR_OWNER/CREATOR_GROUP so a child resolves its own owner/group. */
    if (dfaclcnt) {
        uint16_t duo, dgo, dot, dmask;
        int      has_dmask;
        uint16_t deff_go;

        posix_parse_base(deflt, dfaclcnt, &duo, &dgo, &dot, &dmask, &has_dmask);
        deff_go = has_dmask ? (dgo & dmask) : dgo;

        APPEND(CHIMERA_ACE_ALLOWED,
               posix_perm_to_mask(duo) | ACE_BASELINE | ACE_OWNER_EXTRA,
               ACE_DEFAULT_FLAGS, CHIMERA_PRINCIPAL_SPECIAL,
               CHIMERA_WHO_CREATOR_OWNER, 0);
        APPEND(CHIMERA_ACE_ALLOWED,
               posix_perm_to_mask(deff_go) | ACE_BASELINE,
               ACE_DEFAULT_FLAGS, CHIMERA_PRINCIPAL_SPECIAL,
               CHIMERA_WHO_CREATOR_GROUP, 0);
        APPEND(CHIMERA_ACE_ALLOWED,
               posix_perm_to_mask(dot) | ACE_BASELINE,
               ACE_DEFAULT_FLAGS, CHIMERA_PRINCIPAL_SPECIAL,
               CHIMERA_WHO_EVERYONE, 0);

        for (uint32_t i = 0; i < dfaclcnt; i++) {
            uint16_t tag  = deflt[i].e_tag & ~CHIMERA_NFS_ACL_DEFAULT;
            uint16_t perm = deflt[i].e_perm & 0x7;

            if (has_dmask) {
                perm &= dmask;
            }
            if (tag == CHIMERA_POSIX_ACL_USER) {
                APPEND(CHIMERA_ACE_ALLOWED,
                       posix_perm_to_mask(perm) | ACE_BASELINE,
                       ACE_DEFAULT_FLAGS, CHIMERA_PRINCIPAL_USER, 0,
                       deflt[i].e_id);
            } else if (tag == CHIMERA_POSIX_ACL_GROUP) {
                APPEND(CHIMERA_ACE_ALLOWED,
                       posix_perm_to_mask(perm) | ACE_BASELINE,
                       ACE_DEFAULT_FLAGS | CHIMERA_ACE_FLAG_IDENTIFIER_GROUP,
                       CHIMERA_PRINCIPAL_GROUP, 0, deflt[i].e_id);
            }
        }
    }
#undef APPEND

    out->num_aces   = n;
    out->ctrl_flags = 0;
    return n;
} /* chimera_acl_from_posix */
