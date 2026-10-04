// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>

#include "smb_internal.h"
#include "smb_kerberos_identity.h"
#include "vfs/vfs_identity.h"

SYMBOL_EXPORT int
smb_kerberos_resolve_identity(
    struct chimera_vfs           *vfs,
    int                           mapper_required,
    int                           anonymous_fallback,
    const char                   *principal,
    struct smb_kerberos_identity *out)
{
    struct chimera_vfs_identity_result result;
    enum chimera_vfs_identity_status   status;
    int                                have_mapper;

    memset(out, 0, sizeof(*out));

    if (!principal || principal[0] == '\0') {
        chimera_smb_error("Kerberos logon refused: the accepted context names no principal");
        return -1;
    }

    have_mapper = vfs &&
        chimera_vfs_identity_has_capability(vfs, CHIMERA_VFS_IDENTITY_CAP_LOOKUP |
                                            CHIMERA_VFS_IDENTITY_CAP_PRINCIPAL);

    if (have_mapper || mapper_required) {
        if (!have_mapper) {
            chimera_smb_error("Kerberos logon refused for %s: winbind_enabled is set but no "
                              "identity module maps principals (built without libwbclient?)",
                              principal);
            return -1;
        }

        status = chimera_vfs_identity_lookup(vfs, CHIMERA_VFS_IDENTITY_BY_PRINCIPAL, 0,
                                             principal, &result);

        /* UNAVAILABLE distinguishes "the backend is down" from "nobody knows
         * this principal" in the log; either way the logon is refused. */
        if (status == CHIMERA_VFS_IDENTITY_UNAVAILABLE) {
            chimera_smb_error("Kerberos logon refused for %s: the identity backend that maps "
                              "principals is unavailable (winbindd down?)",
                              principal);
            return -1;
        }

        if (status != CHIMERA_VFS_IDENTITY_OK || result.is_group) {
            chimera_smb_error("Kerberos logon refused for %s: no identity module can map the "
                              "principal to a Unix identity",
                              principal);
            return -1;
        }

        out->uid   = result.user.uid;
        out->gid   = result.user.gid;
        out->ngids = result.user.ngids;
        if (out->ngids > CHIMERA_VFS_CRED_MAX_GIDS) {
            out->ngids = CHIMERA_VFS_CRED_MAX_GIDS;
        }
        memcpy(out->gids, result.user.gids, out->ngids * sizeof(uint32_t));
        snprintf(out->sid, sizeof(out->sid), "%s", result.user.sid);
        snprintf(out->group_sid, sizeof(out->group_sid), "%s", result.user.group_sid);

        chimera_smb_info("Kerberos principal %s mapped to uid=%u gid=%u ngids=%u",
                         principal, out->uid, out->gid, out->ngids);

        out->is_ad_user = 1;
        out->resolved   = 1;
        return 0;
    }

    if (!anonymous_fallback) {
        chimera_smb_error("Kerberos logon refused for %s: no identity source (no identity "
                          "module maps principals and kerberos_anonymous_fallback is off)",
                          principal);
        return -1;
    }

    /* Explicitly opted in: serve the principal as nobody.  Info level here; the
     * server logs the policy itself at startup. */
    chimera_smb_info("Kerberos principal %s served as uid/gid %u (kerberos_anonymous_fallback)",
                     principal, SMB_KERBEROS_NOBODY_ID);

    out->uid   = SMB_KERBEROS_NOBODY_ID;
    out->gid   = SMB_KERBEROS_NOBODY_ID;
    out->ngids = 0;
    smb_ntlm_synthesize_unix_sid(out->uid, out->sid, sizeof(out->sid));
    out->is_ad_user = 0;
    out->resolved   = 1;

    return 0;
} /* smb_kerberos_resolve_identity */
