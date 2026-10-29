// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <stdio.h>
#include <stdlib.h>
#include "dfs_mock.h"

uint64_t
chimera_vfs_hash(
    const void *data,
    int         length)
{
    return XXH3_64bits(data, (size_t) length);
} /* chimera_vfs_hash */

uint32_t
chimera_vfs_access_check(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        requested)
{
    if (!cred || cred->uid == 0) {
        return requested;
    }
    uint32_t mode  = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ? attr->va_mode : 0;
    uint64_t owner = (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ? attr->va_uid : 0;
    uint64_t group = (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) ? attr->va_gid : 0;
    uint32_t bits  = mode & 0007;
    if (cred->uid == owner) {
        bits = (mode >> 6) & 0007;
    } else if (cred->gid == group) {
        bits = (mode >> 3) & 0007;
    }
    uint32_t granted = 0;
    if (bits & 4) {
        granted |= CHIMERA_ACE_READ_DATA;
    }
    if (bits & 2) {
        granted |= CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_APPEND_DATA;
    }
    if (bits & 1) {
        granted |= CHIMERA_ACE_EXECUTE;
    }
    return requested & granted;
} /* chimera_vfs_access_check */

int
chimera_vfs_delete_allowed(
    const struct chimera_vfs_attrs *parent,
    const struct chimera_vfs_attrs *child,
    const struct chimera_vfs_cred  *cred)
{
    (void) parent;
    (void) child;
    return cred == NULL || cred->uid == 0;
} /* chimera_vfs_delete_allowed */

uint32_t
chimera_vfs_killpriv_mode(
    const struct chimera_vfs_cred *cred,
    uint32_t                       mode)
{
    CHECK(!cred || cred->uid == 0);
    return mode;
} /* chimera_vfs_killpriv_mode */

int
chimera_vfs_resolve_set_time(
    const struct timespec *in,
    const struct timespec *now,
    struct timespec       *out)
{
    if (in->tv_nsec == CHIMERA_VFS_TIME_OMIT) {
        return 0;
    }
    *out = in->tv_nsec == CHIMERA_VFS_TIME_NOW ? *now : *in;
    return 1;
} /* chimera_vfs_resolve_set_time */

int
chimera_vfs_gate_needed_dac(
    uint64_t                       capabilities,
    const struct chimera_vfs_cred *cred)
{
    if (!cred || cred->flavor != CHIMERA_VFS_AUTH_UNIX || !cred->uid) {
        return 0;
    }
    return (capabilities & CHIMERA_VFS_CAP_DELEGATES_DAC) != 0;
} /* chimera_vfs_gate_needed_dac */
