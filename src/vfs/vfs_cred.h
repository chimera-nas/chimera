// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

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
    CHIMERA_VFS_AUTH_UNIX = 1,
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

