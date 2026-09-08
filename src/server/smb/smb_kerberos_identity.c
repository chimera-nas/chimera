// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "smb_internal.h"
#include "smb_kerberos_identity.h"
#include "smb_wbclient.h"

SYMBOL_EXPORT int
smb_kerberos_resolve_identity(
    int                           winbind_enabled,
    const char                   *principal,
    struct smb_kerberos_identity *out)
{
    memset(out, 0, sizeof(*out));

    if (!principal || principal[0] == '\0') {
        chimera_smb_error("Kerberos logon refused: the accepted context names no principal");
        return -1;
    }

    if (winbind_enabled) {
        /* The ping distinguishes "winbindd is down" from "winbind does not know
         * this principal" in the log; either way the logon is refused. */
        if (!smb_wbclient_available()) {
            chimera_smb_error("Kerberos logon refused for %s: winbind_enabled is set but winbind "
                              "is unavailable (winbindd down, or built without libwbclient)",
                              principal);
            return -1;
        }

        if (smb_wbclient_map_principal(principal, &out->uid, &out->gid,
                                       &out->ngids, out->gids, out->sid) != 0) {
            chimera_smb_error("Kerberos logon refused for %s: winbind cannot map the principal "
                              "to a Unix identity",
                              principal);
            return -1;
        }

        out->is_ad_user = 1;
        return 0;
    }

    /* No identity source: serve the principal as nobody.  Error level on
     * purpose -- a production log must show why every Kerberos user shares one
     * uid, and this used to be a debug line nobody saw. */
    chimera_smb_error("Kerberos principal %s served as uid/gid %u: winbind_enabled is off",
                      principal, SMB_KERBEROS_NOBODY_ID);

    out->uid   = SMB_KERBEROS_NOBODY_ID;
    out->gid   = SMB_KERBEROS_NOBODY_ID;
    out->ngids = 0;
    smb_ntlm_synthesize_unix_sid(out->uid, out->sid, sizeof(out->sid));
    out->is_ad_user = 0;

    return 0;
} /* smb_kerberos_resolve_identity */
