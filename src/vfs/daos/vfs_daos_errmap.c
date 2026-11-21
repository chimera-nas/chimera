// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <stdio.h>
#include "vfs_daos.h"

enum chimera_vfs_error
dfs_errno_to_vfs(int rc)
{
    switch (rc) {
        case 0:
            return CHIMERA_VFS_OK;
#define MAP_ERROR(name) case name: return CHIMERA_VFS_ ## name
            MAP_ERROR(EPERM);
            MAP_ERROR(ENOENT);
            MAP_ERROR(EIO);
            MAP_ERROR(ENXIO);
            MAP_ERROR(EAGAIN);
            MAP_ERROR(EACCES);
            MAP_ERROR(EFAULT);
            MAP_ERROR(EBUSY);
            MAP_ERROR(EEXIST);
            MAP_ERROR(EXDEV);
            MAP_ERROR(ENOTDIR);
            MAP_ERROR(EISDIR);
            MAP_ERROR(EINVAL);
            MAP_ERROR(EMFILE);
            MAP_ERROR(EFBIG);
            MAP_ERROR(ENOSPC);
            MAP_ERROR(EROFS);
            MAP_ERROR(EMLINK);
            MAP_ERROR(ENAMETOOLONG);
            MAP_ERROR(ENOTEMPTY);
            MAP_ERROR(ELOOP);
            MAP_ERROR(EOVERFLOW);
            MAP_ERROR(EBADF);
            MAP_ERROR(EDQUOT);
            MAP_ERROR(ESTALE);
            MAP_ERROR(ENODATA);
            MAP_ERROR(ERANGE);
            MAP_ERROR(ENOTSUP);
#if EOPNOTSUPP != ENOTSUP
        case EOPNOTSUPP:
            return CHIMERA_VFS_ENOTSUP;
#endif /* if EOPNOTSUPP != ENOTSUP */
#undef MAP_ERROR
        default:
            fprintf(stderr, "vfs_daos: unclassified DFS status %d\n", rc);
            return CHIMERA_VFS_EIO;
    } /* switch */
} /* dfs_errno_to_vfs */
