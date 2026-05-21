// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "vfs_access.h"
#include "vfs_attrs.h"
#include "vfs_acl.h"
#include "vfs_cred.h"
#include "common/macros.h"

SYMBOL_EXPORT uint32_t
chimera_vfs_access_check(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        requested)
{
    const struct chimera_acl *acl = NULL;
    uint32_t                  mode;
    uint64_t                  owner_uid, owner_gid;
    int                       is_dir;

    /* AUTH_NONE means the caller did not establish an identity; treat as a
     * full grant (the protocol layer is responsible for export-level policy). */
    if (cred->flavor == CHIMERA_VFS_AUTH_NONE) {
        return requested;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) {
        acl = attr->va_acl;
    }

    mode      = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ? attr->va_mode : 0;
    owner_uid = (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ? attr->va_uid : 0;
    owner_gid = (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) ? attr->va_gid : 0;
    is_dir    = S_ISDIR(mode);

    return chimera_acl_access_check(acl, mode, owner_uid, owner_gid,
                                    cred, requested, is_dir);
} /* chimera_vfs_access_check */
