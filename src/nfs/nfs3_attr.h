#pragma once

#include <sys/stat.h>

#include "nfs3_xdr.h"
#include "vfs/vfs.h"

static ftype3
chimera_nfs3_type_from_vfs(uint16_t mode)
{
    return (mode & S_IFMT) == S_IFDIR ? NF3DIR : NF3REG;
} /* chimera_nfs3_type_from_vfs */

static void
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
