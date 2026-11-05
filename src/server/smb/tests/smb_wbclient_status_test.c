// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Pins how every libwbclient error is classified for the identity resolver.
 * The classification decides whether a failure is cached as "this identity
 * does not exist" (NOT_FOUND) or fails closed as an outage (UNAVAILABLE), so
 * a code moved into the wrong bucket would silently turn an outage into
 * opaque, negatively cached ACL principals.
 */

#include <stdio.h>

#include "server/smb/smb_wbclient_status.h"

static int failures;

static void
expect(
    wbcErr                           err,
    enum chimera_vfs_identity_status want)
{
    enum chimera_vfs_identity_status got = smb_wbclient_err_to_status(err);

    if (got != want) {
        fprintf(stderr, "FAIL: wbcErr %d (%s) classified %d, want %d\n",
                (int) err, wbcErrorString(err), (int) got, (int) want);
        failures++;
    }
} /* expect */

int
main(void)
{
    expect(WBC_ERR_SUCCESS, CHIMERA_VFS_IDENTITY_OK);

    /* Winbind answered that the identity does not exist. */
    expect(WBC_ERR_INVALID_SID, CHIMERA_VFS_IDENTITY_NOT_FOUND);
    expect(WBC_ERR_DOMAIN_NOT_FOUND, CHIMERA_VFS_IDENTITY_NOT_FOUND);
    expect(WBC_ERR_UNKNOWN_USER, CHIMERA_VFS_IDENTITY_NOT_FOUND);
    expect(WBC_ERR_UNKNOWN_GROUP, CHIMERA_VFS_IDENTITY_NOT_FOUND);
    expect(WBC_ERR_NOT_MAPPED, CHIMERA_VFS_IDENTITY_NOT_FOUND);

    /* The lookup did not really run. */
    expect(WBC_ERR_NOT_IMPLEMENTED, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_UNKNOWN_FAILURE, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_NO_MEMORY, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_INVALID_PARAM, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_WINBIND_NOT_AVAILABLE, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_INVALID_RESPONSE, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_NSS_ERROR, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_AUTH_ERROR, CHIMERA_VFS_IDENTITY_UNAVAILABLE);
    expect(WBC_ERR_PWD_CHANGE_FAILED, CHIMERA_VFS_IDENTITY_UNAVAILABLE);

    /* A code a newer libwbclient adds, unknown to the table, is an outage. */
    expect((wbcErr) 1000, CHIMERA_VFS_IDENTITY_UNAVAILABLE);

    if (failures) {
        return 1;
    }
    fprintf(stderr, "  PASS: every wbcErr is classified as expected\n");
    return 0;
} /* main */
