// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "common/platform.h"

/* Unix keeps its existing ABI. Windows uses full-width virtual metadata,
 * independent of the CRT's 32-bit inode/offset and second-only stat layout. */
#ifdef _WIN32
/* These flags describe virtual descriptors, not CRT descriptors. Keep the
 * shared CRT open bits and allocate missing virtual flags above the CRT range. */
#include <fcntl.h>
#define O_ACCMODE 3
#define O_NONBLOCK 0x00100000
#define O_DIRECTORY 0x00200000
#define O_NOFOLLOW 0x00400000
#define O_SYNC 0x00800000
#define O_DIRECT 0x01000000
#define O_PATH 0x02000000
#define O_LARGEFILE 0x04000000
#define F_DUPFD 0
#define F_GETFD 1
#define F_SETFD 2
#define F_GETFL 3
#define F_SETFL 4
#define F_GETLK 5
#define F_SETLK 6
#define F_SETLKW 7
#define F_RDLCK 0
#define F_WRLCK 1
#define F_UNLCK 2
#define FD_CLOEXEC 1
#define F_ULOCK 0
#define F_LOCK 1
#define F_TLOCK 2
#define F_TEST 3
#define AT_FDCWD (-100)
#define AT_SYMLINK_NOFOLLOW 0x100
#define AT_REMOVEDIR 0x200
#define AT_EACCESS 0x200
#define AT_SYMLINK_FOLLOW 0x400
#define AT_EMPTY_PATH 0x1000
#define UTIME_NOW 1073741823L
#define UTIME_OMIT 1073741822L
#define SEEK_DATA 3
#define SEEK_HOLE 4

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
