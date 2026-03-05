// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include "vfs_attrs.h"

/* setgroups is gated behind _GNU_SOURCE in <grp.h>; declare it directly */
extern int setgroups(
    size_t       size,
    const gid_t *list);

/*
 * Maximum number of supplementary groups in VFS credentials.
 * This matches the NFS AUTH_SYS limit per RFC 1831.
 */
#define CHIMERA_VFS_CRED_MAX_GIDS 16

/*
 * Default anonymous UID/GID values.
 * These match the Linux kernel NFS server defaults (nfsnobody).
 */
#define CHIMERA_VFS_ANON_UID      65534
#define CHIMERA_VFS_ANON_GID      65534

/*
 * VFS credential flavor enumeration.
 * Currently only UNIX credentials are supported, but this allows
 * for future extension to other authentication mechanisms.
 */
enum chimera_vfs_cred_flavor {
    CHIMERA_VFS_AUTH_NONE = 0,
    CHIMERA_VFS_AUTH_UNIX = 1,
    CHIMERA_VFS_AUTH_ATTR = 2
};

/*
 * VFS credential structure.
 *
 * This is the generic credential representation used throughout
 * the VFS layer, independent of the protocol that provided it.
 * It contains the essential identity information needed for
 * access control decisions.
 */
struct chimera_vfs_cred {
    enum chimera_vfs_cred_flavor flavor;
    uint32_t                     uid;
    uint32_t                     gid;
    uint32_t                     ngids;
    uint32_t                     gids[CHIMERA_VFS_CRED_MAX_GIDS];
};

/*
 * Return a pointer to the cached server process credentials.
 *
 * Captures the UID and GID of the running server process on first call so
 * they can be restored after impersonating a client credential. Safe to call
 * multiple times; the static is initialized exactly once per translation unit.
 */
static inline struct chimera_vfs_cred *
chimera_vfs_get_server_cred(void)
{
    static struct chimera_vfs_cred cred = { 0 };

    if (cred.flavor == CHIMERA_VFS_AUTH_NONE) {
        cred.flavor = CHIMERA_VFS_AUTH_UNIX;
        cred.uid    = getuid();
        cred.gid    = getgid();
        cred.ngids  = 0;
    }
    return &cred;
} // chimera_vfs_get_server_cred

/*
 * Initialize a VFS credential as anonymous.
 *
 * @param cred     The credential to initialize
 * @param anonuid  Anonymous UID value
 * @param anongid  Anonymous GID value
 */
static inline void
chimera_vfs_cred_init_anonymous(
    struct chimera_vfs_cred *cred,
    uint32_t                 anonuid,
    uint32_t                 anongid)
{
    cred->flavor = CHIMERA_VFS_AUTH_UNIX;
    cred->uid    = anonuid;
    cred->gid    = anongid;
    cred->ngids  = 0;
} // chimera_vfs_cred_init_anonymous

/*
 * Initialize a VFS credential with UNIX identity.
 *
 * @param cred   The credential to initialize
 * @param uid    User ID
 * @param gid    Primary group ID
 * @param ngids  Number of supplementary group IDs
 * @param gids   Array of supplementary group IDs (may be NULL if ngids == 0)
 */
static inline void
chimera_vfs_cred_init_unix(
    struct chimera_vfs_cred *cred,
    uint32_t                 uid,
    uint32_t                 gid,
    uint32_t                 ngids,
    const uint32_t          *gids)
{
    cred->flavor = CHIMERA_VFS_AUTH_UNIX;
    cred->uid    = uid;
    cred->gid    = gid;

    if (ngids > CHIMERA_VFS_CRED_MAX_GIDS) {
        ngids = CHIMERA_VFS_CRED_MAX_GIDS;
    }

    cred->ngids = ngids;
    if (ngids > 0 && gids) {
        memcpy(cred->gids, gids, ngids * sizeof(uint32_t));
    }
} // chimera_vfs_cred_init_unix

/*
 * Initialize a VFS credential for attribute-based access control.
 *
 * With ATTR flavor, the UID and GID are applied to newly created file
 * attributes rather than changing the process identity.
 *
 * @param cred   The credential to initialize
 * @param uid    User ID to assign to created files
 * @param gid    Group ID to assign to created files
 * @param ngids  Unused; reserved for future use
 * @param gids   Unused; reserved for future use
 */
static inline void
chimera_vfs_cred_init_attr(
    struct chimera_vfs_cred *cred,
    uint32_t                 uid,
    uint32_t                 gid,
    uint32_t                 ngids,
    const uint32_t          *gids)
{
    cred->flavor = CHIMERA_VFS_AUTH_ATTR;
    cred->uid    = uid;
    cred->gid    = gid;
    cred->ngids  = 0;
} // chimera_vfs_cred_init_unix


/*
 * Apply a client credential to the current thread.
 *
 * For UNIX credentials, impersonates the client by calling seteuid/setegid.
 * If the client identity matches the server identity, the call is a no-op.
 * For ATTR credentials, injects UID/GID into set_attrs if not already set.
 *
 * @param cred       The client credential to apply
 * @param set_attrs  Attribute set to inject UID/GID into (ATTR flavor only,
 *                   may be NULL)
 * @return 0 on success, errno value on failure
 */
static inline int
chimera_setup_credential(
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_attrs      *set_attrs)
{
    const struct chimera_vfs_cred *sc = chimera_vfs_get_server_cred();

    switch (cred->flavor) {
        case CHIMERA_VFS_AUTH_NONE:
            /* No credential provided, do nothing */
            break;
        case CHIMERA_VFS_AUTH_UNIX:
            if ((cred->uid == sc->uid) &&
                (cred->gid == sc->gid)) {
                return 0;
            }
            if (cred->ngids > 0) {
                if (setgroups(cred->ngids, cred->gids) < 0) {
                    return errno;
                }
            }
            if (setegid(cred->gid) < 0) {
                return errno;
            }
            if (seteuid(cred->uid) < 0) {
                return errno;
            }
            break;
        case CHIMERA_VFS_AUTH_ATTR:
            if (set_attrs != NULL) {
                if (!(set_attrs->va_set_mask & CHIMERA_VFS_ATTR_UID)) {
                    set_attrs->va_uid       = cred->uid;
                    set_attrs->va_set_mask |= CHIMERA_VFS_ATTR_UID;
                }
                if (!(set_attrs->va_set_mask & CHIMERA_VFS_ATTR_GID)) {
                    set_attrs->va_gid       = cred->gid;
                    set_attrs->va_set_mask |= CHIMERA_VFS_ATTR_GID;
                }
            }
            break;
        default:
            /* Unknown credential flavor */
            return EINVAL;
    } // switch
    return 0;
} /* chimera_setup_credential */

/*
 * Restore the server process credentials after client impersonation.
 *
 * Reverses the effect of chimera_setup_credential() by restoring the
 * server's original UID, GID, and supplementary groups. If the client
 * credential matched the server identity, the call is a no-op.
 *
 * @param cred  The client credential that was previously applied
 * @return 0 on success, errno value on failure
 */
static inline int
chimera_restore_privilege(const struct chimera_vfs_cred *cred)
{
    const struct chimera_vfs_cred *sc = chimera_vfs_get_server_cred();

    if (cred->flavor == CHIMERA_VFS_AUTH_UNIX) {
        /* restore base privileges */
        if ((cred->uid == sc->uid) &&
            (cred->gid == sc->gid)) {
            return 0;
        }
        if (seteuid(sc->uid) < 0) {
            return errno;
        }
        if (setegid(sc->gid) < 0) {
            return errno;
        }

        if (setgroups(0, NULL) < 0) {
            return errno;
        }
    }
    return 0;
} // chimera_restore_privilege
