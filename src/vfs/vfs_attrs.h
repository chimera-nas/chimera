// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdint.h>
#include <time.h>

#define CHIMERA_VFS_FH_SIZE             48

#define CHIMERA_VFS_ATTR_DEV            (1UL << 0)
#define CHIMERA_VFS_ATTR_INUM           (1UL << 1)
#define CHIMERA_VFS_ATTR_MODE           (1UL << 2)
#define CHIMERA_VFS_ATTR_NLINK          (1UL << 3)
#define CHIMERA_VFS_ATTR_UID            (1UL << 4)
#define CHIMERA_VFS_ATTR_GID            (1UL << 5)
#define CHIMERA_VFS_ATTR_RDEV           (1UL << 6)
#define CHIMERA_VFS_ATTR_SIZE           (1UL << 7)
#define CHIMERA_VFS_ATTR_ATIME          (1UL << 8)
#define CHIMERA_VFS_ATTR_MTIME          (1UL << 9)
#define CHIMERA_VFS_ATTR_CTIME          (1UL << 10)
#define CHIMERA_VFS_ATTR_SPACE_USED     (1UL << 11)

#define CHIMERA_VFS_ATTR_SPACE_AVAIL    (1UL << 12)
#define CHIMERA_VFS_ATTR_SPACE_FREE     (1UL << 13)
#define CHIMERA_VFS_ATTR_SPACE_TOTAL    (1UL << 14)
#define CHIMERA_VFS_ATTR_FILES_TOTAL    (1UL << 15)
#define CHIMERA_VFS_ATTR_FILES_FREE     (1UL << 16)
#define CHIMERA_VFS_ATTR_FILES_AVAIL    (1UL << 17)

#define CHIMERA_VFS_ATTR_FH             (1UL << 18)
#define CHIMERA_VFS_ATTR_ATOMIC         (1UL << 19)
#define CHIMERA_VFS_ATTR_FSID           (1UL << 20)

#define CHIMERA_VFS_ATTR_MASK_STAT      ( \
            CHIMERA_VFS_ATTR_DEV | \
            CHIMERA_VFS_ATTR_INUM | \
            CHIMERA_VFS_ATTR_MODE | \
            CHIMERA_VFS_ATTR_NLINK | \
            CHIMERA_VFS_ATTR_UID | \
            CHIMERA_VFS_ATTR_GID | \
            CHIMERA_VFS_ATTR_RDEV | \
            CHIMERA_VFS_ATTR_SIZE | \
            CHIMERA_VFS_ATTR_SPACE_USED | \
            CHIMERA_VFS_ATTR_ATIME | \
            CHIMERA_VFS_ATTR_MTIME | \
            CHIMERA_VFS_ATTR_CTIME)

#define CHIMERA_VFS_ATTR_MASK_STATFS    ( \
            CHIMERA_VFS_ATTR_SPACE_AVAIL | \
            CHIMERA_VFS_ATTR_SPACE_FREE | \
            CHIMERA_VFS_ATTR_SPACE_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_FREE | \
            CHIMERA_VFS_ATTR_FILES_AVAIL | \
            CHIMERA_VFS_ATTR_FSID)

#define CHIMERA_VFS_ATTR_MASK_CACHEABLE ( \
            CHIMERA_VFS_ATTR_MASK_STAT)

#define CHIMERA_VFS_TIME_NOW            ((1l << 30) - 3l)

struct chimera_vfs_attrs {
    uint64_t        va_req_mask;
    uint64_t        va_set_mask;

    uint64_t        va_dev;
    uint64_t        va_ino;
    uint64_t        va_mode;
    uint64_t        va_nlink;
    uint64_t        va_uid;
    uint64_t        va_gid;
    uint64_t        va_rdev;
    uint64_t        va_size;
    uint64_t        va_space_used;
    struct timespec va_atime;
    struct timespec va_mtime;
    struct timespec va_ctime;

    uint64_t        va_fs_space_avail;
    uint64_t        va_fs_space_free;
    uint64_t        va_fs_space_total;
    uint64_t        va_fs_space_used;
    uint64_t        va_fs_files_total;
    uint64_t        va_fs_files_free;
    uint64_t        va_fs_files_avail;
    uint64_t        va_fsid;

    uint32_t        va_fh_len;
    uint64_t        va_fh_hash;

    /* XXH3 uses SIMD memory loads that may read beyond the end
     * of the actual data, so we need to provide enough padding
     * to prevent this from causing compiler complaints?
     */
    uint8_t         va_fh[CHIMERA_VFS_FH_SIZE + 16];
};
