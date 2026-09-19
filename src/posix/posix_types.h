// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "common/platform.h"

/* Unix keeps its existing ABI. Windows uses full-width virtual metadata,
 * independent of the CRT's 32-bit inode/offset and second-only stat layout. */
#ifdef _WIN32
typedef struct chimera_posix_stat_info {
    uint64_t st_dev, st_ino, st_nlink, st_rdev;
    uint32_t st_mode, st_uid, st_gid;
    int64_t st_size, st_blocks;
    int32_t st_blksize;
    union { struct timespec st_atim; struct { time_t st_atime; long st_atime_ns; }; };
    union { struct timespec st_mtim; struct { time_t st_mtime; long st_mtime_ns; }; };
    union { struct timespec st_ctim; struct { time_t st_ctime; long st_ctime_ns; }; };
} chimera_posix_stat_t;

struct statvfs {
    uint64_t f_bsize, f_frsize, f_blocks, f_bfree, f_bavail;
    uint64_t f_files, f_ffree, f_favail, f_fsid, f_flag, f_namemax;
};
struct statfs {
    uint64_t f_type, f_bsize, f_frsize, f_blocks, f_bfree, f_bavail;
    uint64_t f_files, f_ffree, f_flags, f_namelen;
    struct { int32_t __val[2]; } f_fsid;
};
struct flock {
    short l_type, l_whence;
    int64_t l_start, l_len;
    int32_t l_pid;
};
#else
#include <sys/stat.h>
#include <sys/statvfs.h>
typedef struct stat chimera_posix_stat_t;
#endif
