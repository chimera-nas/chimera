// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "nfs_gss.h"
#include "vfs/sdk/vfs_cred.h"
#include <errno.h>

/* Server initialization rejects Kerberos configuration in this build, before
 * any listener is started. No identity is inferred from an unverified token. */
const struct evpl_rpc2_gss_provider chimera_nfs_gss_provider = {0};
int chimera_nfs_gss_init(const char *keytab)
{
    (void) keytab;
    errno = ENOTSUP;
    return -1;
}
void chimera_nfs_gss_set_principal_map(const struct chimera_server_config *config)
{
    (void) config;
}
void chimera_nfs_gss_map_principal(const char *principal, struct chimera_vfs_cred *cred)
{
    (void) principal;
    chimera_vfs_cred_init_anonymous(cred, CHIMERA_VFS_ANON_UID, CHIMERA_VFS_ANON_GID);
}
