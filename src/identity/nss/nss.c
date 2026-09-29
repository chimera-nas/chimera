// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NSS identity module: resolves uids, gids and names through the host's
 * nsswitch (the getpw and getgr families).  Always present and walked first.  NSS has no
 * notion of a Windows SID, so every record it returns is SID-less; the engine
 * keeps such an answer to a numeric key provisional and lets a SID-bearing
 * module behind it (winbind) improve on it.
 */

#define _GNU_SOURCE
#include <string.h>
#include <pwd.h>
#include <grp.h>
#include <unistd.h>

#include "common/macros.h"
#include "common/platform.h"
#include "vfs/sdk/vfs_identity_module.h"

static enum chimera_vfs_identity_status
chimera_identity_nss_lookup(
    void                               *private_data,
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    struct passwd pw, *res = NULL;
    struct group  gr, *gres = NULL;
    char buf[16384];
    int rc;
    chimera_grouplist_t grps[CHIMERA_VFS_CRED_MAX_GIDS];
    int ng = CHIMERA_VFS_CRED_MAX_GIDS;
    int i;

    (void) private_data;

    switch (key) {
        case CHIMERA_VFS_IDENTITY_BY_UID:
            rc = getpwuid_r(id, &pw, buf, sizeof(buf), &res);
            break;
        case CHIMERA_VFS_IDENTITY_BY_NAME:
            rc = getpwnam_r(name, &pw, buf, sizeof(buf), &res);
            break;
        case CHIMERA_VFS_IDENTITY_BY_GID:
            rc = getgrgid_r(id, &gr, buf, sizeof(buf), &gres);
            if (rc != 0 || gres == NULL) {
                return CHIMERA_VFS_IDENTITY_NOT_MINE;
            }
            out->is_group  = 1;
            out->group.gid = gr.gr_gid;
            strncpy(out->group.groupname, gr.gr_name,
                    sizeof(out->group.groupname) - 1);
            out->group.groupname_len = (int) strlen(out->group.groupname);
            /* NSS supplies no SID; left empty so the algorithmic idmap is
             * used for this group. */
            return CHIMERA_VFS_IDENTITY_OK;
        default:
            /* NSS has no notion of a Windows SID. */
            return CHIMERA_VFS_IDENTITY_NOT_MINE;
    } /* switch */

    if (rc != 0 || res == NULL) {
        return CHIMERA_VFS_IDENTITY_NOT_MINE;
    }

    out->user.uid = pw.pw_uid;
    out->user.gid = pw.pw_gid;
    strncpy(out->user.username, pw.pw_name, sizeof(out->user.username) - 1);
    out->user.username_len = (int) strlen(out->user.username);

    /* Supplementary groups (getgrouplist includes the primary gid; storing it
     * twice is harmless for membership checks). */
    if (getgrouplist(pw.pw_name, pw.pw_gid, grps, &ng) < 0) {
        ng = CHIMERA_VFS_CRED_MAX_GIDS;
    }
    if (ng > CHIMERA_VFS_CRED_MAX_GIDS) {
        ng = CHIMERA_VFS_CRED_MAX_GIDS;
    }
    out->user.ngids = ng;
    for (i = 0; i < ng; i++) {
        out->user.gids[i] = grps[i];
    }

    /* NSS supplies no SID; left empty so the algorithmic idmap is used. */
    return CHIMERA_VFS_IDENTITY_OK;
} /* chimera_identity_nss_lookup */

SYMBOL_EXPORT struct chimera_vfs_identity_module identity_nss = {
    .sdk_version  = CHIMERA_VFS_IDENTITY_SDK_VERSION,
    .name         = "nss",
    .capabilities = CHIMERA_VFS_IDENTITY_CAP_LOOKUP,
    .lookup       = chimera_identity_nss_lookup,
};
