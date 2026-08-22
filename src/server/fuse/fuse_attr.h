// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <string.h>
#include <linux/fuse.h>

#include "vfs/sdk/vfs_attrs.h"

/* Attribute set carried on every stat-shaped FUSE reply. */
#define CHIMERA_FUSE_ATTR_MASK (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH)

static inline void
chimera_fuse_attr_from_vfs(
    struct fuse_attr               *fa,
    const struct chimera_vfs_attrs *va)
{
    memset(fa, 0, sizeof(*fa));

    fa->ino       = va->va_ino;
    fa->size      = va->va_size;
    fa->blocks    = va->va_space_used / 512;
    fa->atime     = va->va_atime.tv_sec;
    fa->atimensec = va->va_atime.tv_nsec;
    fa->mtime     = va->va_mtime.tv_sec;
    fa->mtimensec = va->va_mtime.tv_nsec;
    fa->ctime     = va->va_ctime.tv_sec;
    fa->ctimensec = va->va_ctime.tv_nsec;
    fa->mode      = va->va_mode;
    fa->nlink     = va->va_nlink;
    fa->uid       = va->va_uid;
    fa->gid       = va->va_gid;
    fa->rdev      = va->va_rdev;
    fa->blksize   = 4096;
} /* chimera_fuse_attr_from_vfs */

static inline void
chimera_fuse_statfs_from_vfs(
    struct fuse_kstatfs            *st,
    const struct chimera_vfs_attrs *va)
{
    memset(st, 0, sizeof(*st));

    st->bsize   = 4096;
    st->frsize  = 4096;
    st->blocks  = va->va_fs_space_total / 4096;
    st->bfree   = va->va_fs_space_free / 4096;
    st->bavail  = va->va_fs_space_avail / 4096;
    st->files   = va->va_fs_files_total;
    st->ffree   = va->va_fs_files_free;
    st->namelen = 255;
} /* chimera_fuse_statfs_from_vfs */

/*
 * Translate a fuse_setattr_in valid/values pair into a chimera set-attr.
 * FATTR_ATIME without FATTR_ATIME_NOW carries an explicit time; with it, the
 * chimera TIME_NOW sentinel asks the backend to stamp its own clock, which
 * maps UTIME_NOW exactly.
 */
static inline void
chimera_fuse_setattr_to_vfs(
    struct chimera_vfs_attrs     *va,
    const struct fuse_setattr_in *in)
{
    memset(va, 0, sizeof(*va));

    if (in->valid & FATTR_MODE) {
        va->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        va->va_mode      = in->mode;
    }

    if (in->valid & FATTR_UID) {
        va->va_set_mask |= CHIMERA_VFS_ATTR_UID;
        va->va_uid       = in->uid;
    }

    if (in->valid & FATTR_GID) {
        va->va_set_mask |= CHIMERA_VFS_ATTR_GID;
        va->va_gid       = in->gid;
    }

    if (in->valid & FATTR_SIZE) {
        va->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        va->va_size      = in->size;
    }

    if (in->valid & FATTR_ATIME) {
        va->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        if (in->valid & FATTR_ATIME_NOW) {
            va->va_atime.tv_nsec = CHIMERA_VFS_TIME_NOW;
        } else {
            va->va_atime.tv_sec  = in->atime;
            va->va_atime.tv_nsec = in->atimensec;
        }
    }

    if (in->valid & FATTR_MTIME) {
        va->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        if (in->valid & FATTR_MTIME_NOW) {
            va->va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
        } else {
            va->va_mtime.tv_sec  = in->mtime;
            va->va_mtime.tv_nsec = in->mtimensec;
        }
    }

    if (in->valid & FATTR_CTIME) {
        va->va_set_mask     |= CHIMERA_VFS_ATTR_CTIME;
        va->va_ctime.tv_sec  = in->ctime;
        va->va_ctime.tv_nsec = in->ctimensec;
    }
} /* chimera_fuse_setattr_to_vfs */
