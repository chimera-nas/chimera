// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs_error.h"

static inline nfsstat3
chimera_vfs_error_to_nfsstat3(enum chimera_vfs_error err)
{
    switch (err) {
        case CHIMERA_VFS_OK:
            return NFS3_OK;
        case CHIMERA_VFS_EPERM:
            return NFS3ERR_PERM;
        case CHIMERA_VFS_ENOENT:
            return NFS3ERR_NOENT;
        case CHIMERA_VFS_EIO:
            return NFS3ERR_IO;
        case CHIMERA_VFS_ENXIO:
            return NFS3ERR_NXIO;
        case CHIMERA_VFS_EACCES:
            return NFS3ERR_ACCES;
        case CHIMERA_VFS_EEXIST:
            return NFS3ERR_EXIST;
        case CHIMERA_VFS_EXDEV:
            return NFS3ERR_XDEV;
        case CHIMERA_VFS_ENOTDIR:
            return NFS3ERR_NOTDIR;
        case CHIMERA_VFS_EISDIR:
            return NFS3ERR_ISDIR;
        case CHIMERA_VFS_EINVAL:
            return NFS3ERR_INVAL;
        case CHIMERA_VFS_EFBIG:
            return NFS3ERR_FBIG;
        case CHIMERA_VFS_ENOSPC:
            return NFS3ERR_NOSPC;
        case CHIMERA_VFS_EROFS:
            return NFS3ERR_ROFS;
        case CHIMERA_VFS_EMLINK:
            return NFS3ERR_MLINK;
        case CHIMERA_VFS_ENAMETOOLONG:
            return NFS3ERR_NAMETOOLONG;
        case CHIMERA_VFS_ENOTEMPTY:
            return NFS3ERR_NOTEMPTY;
        case CHIMERA_VFS_EDQUOT:
            return NFS3ERR_DQUOT;
        case CHIMERA_VFS_ESTALE:
            return NFS3ERR_STALE;
        case CHIMERA_VFS_EBADCOOKIE:
            return NFS3ERR_BAD_COOKIE;
        case CHIMERA_VFS_EBADF:
            return NFS3ERR_BADHANDLE;
        case CHIMERA_VFS_ENOTSUP:
            return NFS3ERR_NOTSUPP;
        case CHIMERA_VFS_EFAULT:
            return NFS3ERR_SERVERFAULT;
        case CHIMERA_VFS_EOVERFLOW:
            return NFS3ERR_TOOSMALL;
        case CHIMERA_VFS_EMFILE:
            return NFS3ERR_SERVERFAULT;
        default:
            return NFS3ERR_SERVERFAULT;
    } /* switch */
} /* chimera_vfs_error_to_nfsstat3 */


static inline int
nfs3_client_status_to_chimera_vfs_error(int status)
{
    switch (status) {
        case NFS3_OK:
            return CHIMERA_VFS_OK;
        case NFS3ERR_NOENT:
            return CHIMERA_VFS_ENOENT;
        case NFS3ERR_IO:
            return CHIMERA_VFS_EIO;
        case NFS3ERR_NXIO:
            return CHIMERA_VFS_ENXIO;
        case NFS3ERR_ACCES:
            return CHIMERA_VFS_EACCES;
        case NFS3ERR_EXIST:
            return CHIMERA_VFS_EEXIST;
        case NFS3ERR_XDEV:
            return CHIMERA_VFS_EXDEV;
        case NFS3ERR_NOTDIR:
            return CHIMERA_VFS_ENOTDIR;
        case NFS3ERR_ISDIR:
            return CHIMERA_VFS_EISDIR;
        case NFS3ERR_INVAL:
            return CHIMERA_VFS_EINVAL;
        case NFS3ERR_FBIG:
            return CHIMERA_VFS_EFBIG;
        case NFS3ERR_NOSPC:
            return CHIMERA_VFS_ENOSPC;
        case NFS3ERR_ROFS:
            return CHIMERA_VFS_EROFS;
        case NFS3ERR_MLINK:
            return CHIMERA_VFS_EMLINK;
        case NFS3ERR_NAMETOOLONG:
            return CHIMERA_VFS_ENAMETOOLONG;
        case NFS3ERR_NOTEMPTY:
            return CHIMERA_VFS_ENOTEMPTY;
        case NFS3ERR_DQUOT:
            return CHIMERA_VFS_EDQUOT;
        case NFS3ERR_STALE:
            return CHIMERA_VFS_ESTALE;
        case NFS3ERR_BAD_COOKIE:
            return CHIMERA_VFS_EBADCOOKIE;
        case NFS3ERR_BADHANDLE:
            return CHIMERA_VFS_EBADF;
        case NFS3ERR_NOTSUPP:
            return CHIMERA_VFS_ENOTSUP;
        case NFS3ERR_TOOSMALL:
            return CHIMERA_VFS_EOVERFLOW;
        case NFS3ERR_SERVERFAULT:
            return CHIMERA_VFS_EFAULT;
        default:
            return CHIMERA_VFS_EINVAL;
    } // switch
} // nfs3_client_status_to_chimera_vfs_error
