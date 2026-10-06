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
    (void) attr;
    /* These ownership tests do not substitute for SDK permission tests. */
    CHECK(!cred);
    return requested;
} /* chimera_vfs_access_check */

int
chimera_vfs_delete_allowed(
    const struct chimera_vfs_attrs *parent,
    const struct chimera_vfs_attrs *child,
    const struct chimera_vfs_cred  *cred)
{
    (void) parent;
    (void) child;
    CHECK(!cred);
    return 1;
} /* chimera_vfs_delete_allowed */

uint32_t
chimera_vfs_killpriv_mode(
    const struct chimera_vfs_cred *cred,
    uint32_t                       mode)
{
    CHECK(!cred);
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
