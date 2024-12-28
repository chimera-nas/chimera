#pragma once

#include <sys/stat.h>

#include "nfs3_xdr.h"
#include "vfs/vfs.h"

#define CHIMERA_NFS3_ATTR_MASK   ( \
            CHIMERA_VFS_ATTR_DEV | \
            CHIMERA_VFS_ATTR_INUM | \
            CHIMERA_VFS_ATTR_MODE | \
            CHIMERA_VFS_ATTR_NLINK | \
            CHIMERA_VFS_ATTR_UID | \
            CHIMERA_VFS_ATTR_GID | \
            CHIMERA_VFS_ATTR_RDEV | \
            CHIMERA_VFS_ATTR_SIZE | \
            CHIMERA_VFS_ATTR_ATIME | \
            CHIMERA_VFS_ATTR_MTIME | \
            CHIMERA_VFS_ATTR_CTIME)

#define CHIMERA_NFS3_FSSTAT_MASK ( \
            CHIMERA_VFS_ATTR_MASK_STATFS)

static ftype3
chimera_nfs3_type_from_vfs(uint16_t mode)
{
    switch (mode & S_IFMT) {
        case S_IFREG:  return NF3REG;
        case S_IFDIR:  return NF3DIR;
        case S_IFBLK:  return NF3BLK;
        case S_IFCHR:  return NF3CHR;
        case S_IFLNK:  return NF3LNK;
        case S_IFSOCK: return NF3SOCK;
        case S_IFIFO:  return NF3FIFO;
        default:       return NF3REG;
    } /* switch */
} /* chimera_nfs3_type_from_vfs */

static inline void
chimera_nfs3_sattr3_to_va(
    struct chimera_vfs_attrs *attr,
    struct sattr3            *sattr)
{
    attr->va_mask = 0;

    if (sattr->mode.set_it) {
        attr->va_mask |= CHIMERA_VFS_ATTR_MODE;
        attr->va_mode  = sattr->mode.mode;
    }

    if (sattr->uid.set_it) {
        attr->va_mask |= CHIMERA_VFS_ATTR_UID;
        attr->va_uid   = sattr->uid.uid;
    }

    if (sattr->gid.set_it) {
        attr->va_mask |= CHIMERA_VFS_ATTR_GID;
        attr->va_gid   = sattr->gid.gid;
    }

    if (sattr->size.set_it) {
        attr->va_mask |= CHIMERA_VFS_ATTR_SIZE;
        attr->va_size  = sattr->size.size;
    }

    if (sattr->atime.set_it == SET_TO_CLIENT_TIME) {
        attr->va_mask         |= CHIMERA_VFS_ATTR_ATIME;
        attr->va_atime.tv_sec  = sattr->atime.atime.seconds;
        attr->va_atime.tv_nsec = sattr->atime.atime.nseconds;
    } else if (sattr->atime.set_it == SET_TO_SERVER_TIME) {
        attr->va_mask         |= CHIMERA_VFS_ATTR_ATIME;
        attr->va_atime.tv_sec  = 0;
        attr->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    }

    if (sattr->mtime.set_it == SET_TO_CLIENT_TIME) {
        attr->va_mask         |= CHIMERA_VFS_ATTR_MTIME;
        attr->va_mtime.tv_sec  = sattr->mtime.mtime.seconds;
        attr->va_mtime.tv_nsec = sattr->mtime.mtime.nseconds;
    } else if (sattr->mtime.set_it == SET_TO_SERVER_TIME) {
        attr->va_mask         |= CHIMERA_VFS_ATTR_MTIME;
        attr->va_mtime.tv_sec  = 0;
        attr->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    }

} /* chimera_nfs3_sattr3_to_va */

static inline void
chimera_nfs3_marshall_attrs(
    const struct chimera_vfs_attrs *attr,
    struct fattr3                  *fattr)
{
    fattr->type           = chimera_nfs3_type_from_vfs(attr->va_mode);
    fattr->mode           = attr->va_mode & ~S_IFMT;
    fattr->nlink          = attr->va_nlink;
    fattr->uid            = attr->va_uid;
    fattr->gid            = attr->va_gid;
    fattr->size           = attr->va_size;
    fattr->used           = attr->va_space_used;
    fattr->rdev.specdata1 = (attr->va_rdev >> 32) & 0xFFFFFFFF;
    fattr->rdev.specdata2 = attr->va_rdev & 0xFFFFFFFF;
    fattr->fsid           = attr->va_dev;
    fattr->fileid         = attr->va_ino;
    fattr->atime.seconds  = attr->va_atime.tv_sec;
    fattr->atime.nseconds = attr->va_atime.tv_nsec;
    fattr->mtime.seconds  = attr->va_mtime.tv_sec;
    fattr->mtime.nseconds = attr->va_mtime.tv_nsec;
    fattr->ctime.seconds  = attr->va_ctime.tv_sec;
    fattr->ctime.nseconds = attr->va_ctime.tv_nsec;
} /* chimera_nfs4_marshall_attrs */
