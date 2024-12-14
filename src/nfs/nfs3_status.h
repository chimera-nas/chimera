#pragma once

#include "vfs/vfs_error.h"
#include "nfs/nfs_internal.h"

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
        case CHIMERA_VFS_EBADF:
            return NFS3ERR_BADHANDLE;
        case CHIMERA_VFS_ENOTSUP:
            return NFS3ERR_NOTSUPP;
        case CHIMERA_VFS_EFAULT:
            return NFS3ERR_SERVERFAULT;
        case CHIMERA_VFS_EOVERFLOW:
            return NFS3ERR_TOOSMALL;
        case CHIMERA_VFS_EMFILE:
            chimera_nfs_abort("Too many open files");
            return NFS3ERR_SERVERFAULT;
        default:
            return NFS3ERR_SERVERFAULT;
    } /* switch */
} /* chimera_vfs_error_to_nfsstat3 */
