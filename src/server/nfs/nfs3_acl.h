// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>

#include "nfs_common.h"
#include "vfs/vfs_attrs.h"

/*
 * Marshal a chimera_vfs_attrs into the NFSACL program's private nacl_post_op_attr
 * (a byte-identical clone of nfs3's post_op_attr -- see nfsacl.x for why the
 * type is prefixed).  Mirrors chimera_nfs3_set_post_op_attr: attributes follow
 * only when the full NFSv3 stat set is present.
 */
static inline void
chimera_nfs3_acl_set_post_op_attr(
    struct nacl_post_op_attr       *poa,
    const struct chimera_vfs_attrs *attr)
{
    struct nacl_fattr3 *f;

    if (!attr ||
        (attr->va_set_mask & CHIMERA_NFS3_ATTR_MASK) != CHIMERA_NFS3_ATTR_MASK) {
        poa->attributes_follow = 0;
        return;
    }

    poa->attributes_follow = 1;
    f                      = &poa->attributes;

    switch (attr->va_mode & S_IFMT) {
        case S_IFDIR:  f->type = NACL_NF3DIR;  break;
        case S_IFBLK:  f->type = NACL_NF3BLK;  break;
        case S_IFCHR:  f->type = NACL_NF3CHR;  break;
        case S_IFLNK:  f->type = NACL_NF3LNK;  break;
        case S_IFSOCK: f->type = NACL_NF3SOCK; break;
        case S_IFIFO:  f->type = NACL_NF3FIFO; break;
        default:       f->type = NACL_NF3REG;  break;
    } /* switch */

    f->mode           = attr->va_mode & ~S_IFMT;
    f->nlink          = attr->va_nlink;
    f->uid            = attr->va_uid;
    f->gid            = attr->va_gid;
    f->size           = attr->va_size;
    f->used           = attr->va_space_used;
    f->rdev.specdata1 = (attr->va_rdev >> 32) & 0xFFFFFFFF;
    f->rdev.specdata2 = attr->va_rdev & 0xFFFFFFFF;
    f->fsid           = attr->va_fsid;
    f->fileid         = attr->va_ino;
    f->atime.seconds  = attr->va_atime.tv_sec;
    f->atime.nseconds = attr->va_atime.tv_nsec;
    f->mtime.seconds  = attr->va_mtime.tv_sec;
    f->mtime.nseconds = attr->va_mtime.tv_nsec;
    f->ctime.seconds  = attr->va_ctime.tv_sec;
    f->ctime.nseconds = attr->va_ctime.tv_nsec;
} /* chimera_nfs3_acl_set_post_op_attr */
