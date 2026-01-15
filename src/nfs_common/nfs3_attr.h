// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <sys/stat.h>

#include "nfs3_xdr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_fh.h"

#define CHIMERA_NFS3_ATTR_MASK     ( \
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
            CHIMERA_VFS_ATTR_CTIME | \
            CHIMERA_VFS_ATTR_FSID)

#define CHIMERA_NFS3_ATTR_WCC_MASK ( \
            CHIMERA_VFS_ATTR_SIZE | \
            CHIMERA_VFS_ATTR_MTIME | \
            CHIMERA_VFS_ATTR_CTIME)

#define CHIMERA_NFS3_FSSTAT_MASK   ( \
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

static inline mode_t
chimera_nfs3_type_to_vfs(ftype3 type)
{
    switch (type) {
        case NF3REG:  return S_IFREG;
        case NF3DIR:  return S_IFDIR;
        case NF3BLK:  return S_IFBLK;
        case NF3CHR:  return S_IFCHR;
        case NF3LNK:  return S_IFLNK;
        case NF3SOCK: return S_IFSOCK;
        case NF3FIFO: return S_IFIFO;
        default:      return S_IFREG;
    } /* switch */
} /* chimera_nfs3_type_to_vfs */

static inline void
chimera_nfs3_sattr3_to_va(
    struct chimera_vfs_attrs *attr,
    struct sattr3            *sattr)
{
    attr->va_set_mask = 0;

    if (sattr->mode.set_it) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        attr->va_mode      = sattr->mode.mode;
    }

    if (sattr->uid.set_it) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
        attr->va_uid       = sattr->uid.uid;
    }

    if (sattr->gid.set_it) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
        attr->va_gid       = sattr->gid.gid;
    }

    if (sattr->size.set_it) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        attr->va_size      = sattr->size.size;
    }

    if (sattr->atime.set_it == SET_TO_CLIENT_TIME) {
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_ATIME;
        attr->va_atime.tv_sec  = sattr->atime.atime.seconds;
        attr->va_atime.tv_nsec = sattr->atime.atime.nseconds;
    } else if (sattr->atime.set_it == SET_TO_SERVER_TIME) {
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_ATIME;
        attr->va_atime.tv_sec  = 0;
        attr->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    }

    if (sattr->mtime.set_it == SET_TO_CLIENT_TIME) {
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
        attr->va_mtime.tv_sec  = sattr->mtime.mtime.seconds;
        attr->va_mtime.tv_nsec = sattr->mtime.mtime.nseconds;
    } else if (sattr->mtime.set_it == SET_TO_SERVER_TIME) {
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
        attr->va_mtime.tv_sec  = 0;
        attr->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
    }

} /* chimera_nfs3_sattr3_to_va */

static inline void
chimera_nfs_va_to_sattr3(
    struct sattr3                  *sattr,
    const struct chimera_vfs_attrs *attr)
{
    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        sattr->mode.set_it = 1;
        sattr->mode.mode   = attr->va_mode;
    } else {
        sattr->mode.set_it = 0;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) {
        sattr->uid.set_it = 1;
        sattr->uid.uid    = attr->va_uid;
    } else {
        sattr->uid.set_it = 0;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) {
        sattr->gid.set_it = 1;
        sattr->gid.gid    = attr->va_gid;
    } else {
        sattr->gid.set_it = 0;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        sattr->size.set_it = 1;
        sattr->size.size   = attr->va_size;
    } else {
        sattr->size.set_it = 0;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) {
        if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            sattr->atime.set_it = SET_TO_SERVER_TIME;
        } else {
            sattr->atime.set_it         = SET_TO_CLIENT_TIME;
            sattr->atime.atime.seconds  = attr->va_atime.tv_sec;
            sattr->atime.atime.nseconds = attr->va_atime.tv_nsec;
        }
    } else {
        sattr->atime.set_it = DONT_CHANGE;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) {
        if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            sattr->mtime.set_it = SET_TO_SERVER_TIME;
        } else {
            sattr->mtime.set_it         = SET_TO_CLIENT_TIME;
            sattr->mtime.mtime.seconds  = attr->va_mtime.tv_sec;
            sattr->mtime.mtime.nseconds = attr->va_mtime.tv_nsec;
        }
    } else {
        sattr->mtime.set_it = DONT_CHANGE;
    }

} /* chimera_nfs_va_to_sattr3 */

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
    fattr->fsid           = attr->va_fsid;
    fattr->fileid         = attr->va_ino;
    fattr->atime.seconds  = attr->va_atime.tv_sec;
    fattr->atime.nseconds = attr->va_atime.tv_nsec;
    fattr->mtime.seconds  = attr->va_mtime.tv_sec;
    fattr->mtime.nseconds = attr->va_mtime.tv_nsec;
    fattr->ctime.seconds  = attr->va_ctime.tv_sec;
    fattr->ctime.nseconds = attr->va_ctime.tv_nsec;
} /* chimera_nfs3_marshall_attrs */

static inline void
chimera_nfs3_marshall_wcc_attrs(
    const struct chimera_vfs_attrs *attr,
    struct wcc_attr                *wcc)
{
    wcc->size           = attr->va_size;
    wcc->mtime.seconds  = attr->va_mtime.tv_sec;
    wcc->mtime.nseconds = attr->va_mtime.tv_nsec;
    wcc->ctime.seconds  = attr->va_ctime.tv_sec;
    wcc->ctime.nseconds = attr->va_ctime.tv_nsec;
} /* chimera_nfs3_marshall_wcc_attrs */

static inline void
chimera_nfs3_unmarshall_wcc_attrs(
    const struct wcc_attr    *wcc,
    struct chimera_vfs_attrs *attr)
{
    attr->va_set_mask      = CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC;
    attr->va_size          = wcc->size;
    attr->va_mtime.tv_sec  = wcc->mtime.seconds;
    attr->va_mtime.tv_nsec = wcc->mtime.nseconds;
    attr->va_ctime.tv_sec  = wcc->ctime.seconds;
    attr->va_ctime.tv_nsec = wcc->ctime.nseconds;
} /* chimera_nfs3_unmarshall_wcc_attrs */

static inline void
chimera_nfs3_set_post_op_attr(
    struct post_op_attr            *post_op_attr,
    const struct chimera_vfs_attrs *attr)
{
    if (attr && (attr->va_set_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK) {
        post_op_attr->attributes_follow = 1;
        chimera_nfs3_marshall_attrs(attr, &post_op_attr->attributes);
    } else {
        post_op_attr->attributes_follow = 0;
    }
} /* chimera_nfs3_set_post_op_attr */

static inline void
chimera_nfs3_set_pre_op_attr(
    struct pre_op_attr             *pre_op_attr,
    const struct chimera_vfs_attrs *attr)
{

    if (attr && (attr->va_set_mask & CHIMERA_NFS3_ATTR_WCC_MASK) == CHIMERA_NFS3_ATTR_WCC_MASK) {
        pre_op_attr->attributes_follow = 1;
        chimera_nfs3_marshall_wcc_attrs(attr, &pre_op_attr->attributes);
    } else {
        pre_op_attr->attributes_follow = 0;
    }
} /* chimera_nfs3_set_pre_op_attr */

static inline void
chimera_nfs3_set_wcc_data(
    struct wcc_data                *wcc,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr)
{
    if (pre_attr && pre_attr->va_set_mask & CHIMERA_VFS_ATTR_ATOMIC) {
        chimera_nfs3_set_pre_op_attr(&wcc->before, pre_attr);
    } else {
        wcc->before.attributes_follow = 0;
    }

    if (post_attr && post_attr->va_set_mask & CHIMERA_VFS_ATTR_ATOMIC) {
        chimera_nfs3_set_post_op_attr(&wcc->after, post_attr);
    } else {
        wcc->after.attributes_follow = 0;
    }
} /* chimera_nfs3_set_wcc_data */

static inline void
chimera_nfs3_unmarshall_attrs(
    const struct fattr3      *fattr,
    struct chimera_vfs_attrs *attr)
{
    attr->va_mode          = chimera_nfs3_type_to_vfs(fattr->type) | (fattr->mode & ~S_IFMT);
    attr->va_nlink         = fattr->nlink;
    attr->va_uid           = fattr->uid;
    attr->va_gid           = fattr->gid;
    attr->va_size          = fattr->size;
    attr->va_space_used    = fattr->used;
    attr->va_dev           = fattr->fsid;
    attr->va_fsid          = fattr->fsid;
    attr->va_ino           = fattr->fileid;
    attr->va_rdev          = ((uint64_t) fattr->rdev.specdata1 << 32) | fattr->rdev.specdata2;
    attr->va_atime.tv_sec  = fattr->atime.seconds;
    attr->va_atime.tv_nsec = fattr->atime.nseconds;
    attr->va_mtime.tv_sec  = fattr->mtime.seconds;
    attr->va_mtime.tv_nsec = fattr->mtime.nseconds;
    attr->va_ctime.tv_sec  = fattr->ctime.seconds;
    attr->va_ctime.tv_nsec = fattr->ctime.nseconds;

    attr->va_set_mask |= CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FSID | CHIMERA_VFS_ATTR_ATOMIC;
} /* chimera_nfs3_unmarshall_attrs */

static inline void
chimera_nfs3_unmarshall_fh(
    const struct nfs_fh3     *fh,
    int                       server_index,
    const void               *parent_fh,
    struct chimera_vfs_attrs *attr)
{
    uint8_t fragment[CHIMERA_VFS_FH_SIZE];
    int     fragment_len;

    /* Build fh_fragment: [server_index][remote_fh_data] */
    fragment[0] = server_index;
    memcpy(fragment + 1, fh->data.data, fh->data.len);
    fragment_len = 1 + fh->data.len;

    attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
    attr->va_fh_len    = chimera_vfs_encode_fh_parent(parent_fh, fragment, fragment_len, attr->va_fh);
} /* chimera_nfs3_unmarshall_fh */


static inline void
chimera_nfs3_get_wcc_data(
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    struct wcc_data          *wcc)
{
    if (wcc->before.attributes_follow) {
        chimera_nfs3_unmarshall_wcc_attrs(&wcc->before.attributes, pre_attr);
    }
    if (wcc->after.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&wcc->after.attributes, post_attr);
        post_attr->va_set_mask |= CHIMERA_VFS_ATTR_ATOMIC;
    }
} /* chimera_nfs3_get_wcc_data */


