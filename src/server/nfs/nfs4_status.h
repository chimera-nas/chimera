// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs_error.h"

static inline nfsstat4
chimera_nfs4_errno_to_nfsstat4(enum chimera_vfs_error err)
{
    switch (err) {
        case CHIMERA_VFS_OK:
            return NFS4_OK;
        case CHIMERA_VFS_EPERM:
            return NFS4ERR_PERM;
        case CHIMERA_VFS_ENOENT:
            return NFS4ERR_NOENT;
        case CHIMERA_VFS_EIO:
            return NFS4ERR_IO;
        case CHIMERA_VFS_ENXIO:
            return NFS4ERR_NXIO;
        case CHIMERA_VFS_EACCES:
            return NFS4ERR_ACCESS;
        case CHIMERA_VFS_EEXIST:
            return NFS4ERR_EXIST;
        case CHIMERA_VFS_EXDEV:
            return NFS4ERR_XDEV;
        case CHIMERA_VFS_ENOTDIR:
            return NFS4ERR_NOTDIR;
        case CHIMERA_VFS_EISDIR:
            return NFS4ERR_ISDIR;
        case CHIMERA_VFS_EINVAL:
            return NFS4ERR_INVAL;
        case CHIMERA_VFS_EFBIG:
            return NFS4ERR_FBIG;
        case CHIMERA_VFS_ENOSPC:
            return NFS4ERR_NOSPC;
        case CHIMERA_VFS_EROFS:
            return NFS4ERR_ROFS;
        case CHIMERA_VFS_EMLINK:
            return NFS4ERR_MLINK;
        case CHIMERA_VFS_ENAMETOOLONG:
            return NFS4ERR_NAMETOOLONG;
        case CHIMERA_VFS_ENOTEMPTY:
            return NFS4ERR_NOTEMPTY;
        case CHIMERA_VFS_EDQUOT:
            return NFS4ERR_DQUOT;
        case CHIMERA_VFS_ESTALE:
            return NFS4ERR_STALE;
        case CHIMERA_VFS_EBADCOOKIE:
            return NFS4ERR_NOT_SAME;
        case CHIMERA_VFS_EBADF:
            return NFS4ERR_BADHANDLE;
        case CHIMERA_VFS_ENOTSUP:
            return NFS4ERR_NOTSUPP;
        case CHIMERA_VFS_EOVERFLOW:
            return NFS4ERR_TOOSMALL;
        case CHIMERA_VFS_EFAULT:
            return NFS4ERR_SERVERFAULT;
        default:
            abort();
    } /* switch */
} /* chimera_nfs4_errno_to_nfsstat4 */