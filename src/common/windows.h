// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

/* Native Win32/CRT boundary. Protocol/VFS mode bits describe Chimera objects;
 * they never imply Windows host impersonation or POSIX host permissions. */
#include <evpl/evpl_platform.h>
#include <ws2tcpip.h>
#include <bcrypt.h>
#include <io.h>
#include <direct.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <stdint.h>
#include <limits.h>
#include <string.h>
#include <malloc.h>
#include "common/compiler.h"

typedef intptr_t ssize_t;
typedef uint32_t uid_t;
typedef uint32_t gid_t;
typedef uint32_t mode_t;
typedef int pid_t;
typedef unsigned int useconds_t;
typedef int socklen_t;
struct iovec { void *iov_base; size_t iov_len; };
#define PATH_MAX 32768
#define NAME_MAX 255
#include "vfs/sdk/vfs_mode.h"
#define CLOCK_MONOTONIC 1
#define CLOCK_REALTIME 2
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#define strcasecmp _stricmp
#define strncasecmp _strnicmp
#define strdup _strdup
#define strtok_r strtok_s
#define alloca _alloca
#define close _close
#define read _read
#define write _write
#define unlink _unlink
#define rmdir _rmdir
#define getpid _getpid
#define fileno _fileno
#define isatty _isatty
#define fsync _commit
#define STDIN_FILENO 0
#define STDOUT_FILENO 1
#define STDERR_FILENO 2
#define F_OK 0
#define R_OK 4
#define W_OK 2
#define X_OK 0

static inline int chimera_clock_gettime(int clock, struct timespec *ts)
{
    if (clock == CLOCK_MONOTONIC) {
        LARGE_INTEGER now, frequency;
        QueryPerformanceCounter(&now);
        QueryPerformanceFrequency(&frequency);
        ts->tv_sec = (time_t) (now.QuadPart / frequency.QuadPart);
        ts->tv_nsec = (long) ((now.QuadPart % frequency.QuadPart) *
                             1000000000ULL / frequency.QuadPart);
    } else if (clock == CLOCK_REALTIME) {
        timespec_get(ts, TIME_UTC);
    } else {
        errno = EINVAL;
        return -1;
    }
    return 0;
}
#define clock_gettime chimera_clock_gettime
static inline int usleep(useconds_t us)
{
    Sleep((us + 999) / 1000);
    return 0;
}
static inline unsigned sleep(unsigned seconds)
{
    Sleep(seconds * 1000);
    return 0;
}
static inline struct tm *gmtime_r(const time_t *t, struct tm *out)
{
    return gmtime_s(out, t) ? NULL : out;
}
static inline struct tm *localtime_r(const time_t *t, struct tm *out)
{
    return localtime_s(out, t) ? NULL : out;
}
#define htobe16(x) _byteswap_ushort(x)
#define htobe32(x) _byteswap_ulong(x)
#define htobe64(x) _byteswap_uint64(x)
#define be16toh(x) _byteswap_ushort(x)
#define be32toh(x) _byteswap_ulong(x)
#define be64toh(x) _byteswap_uint64(x)
#define htole16(x) ((uint16_t)(x))
#define htole32(x) ((uint32_t)(x))
#define htole64(x) ((uint64_t)(x))
#define le16toh(x) ((uint16_t)(x))
#define le32toh(x) ((uint32_t)(x))
#define le64toh(x) ((uint64_t)(x))

/* Virtual special-device identifiers retain the complete major/minor pair. */
#define makedev(major_value, minor_value) (((uint64_t)(major_value) << 32) | (uint32_t)(minor_value))
#define major(device) ((uint32_t)((uint64_t)(device) >> 32))
#define minor(device) ((uint32_t)(device))
#ifndef SSIZE_MAX
#define SSIZE_MAX INTPTR_MAX
#endif

static inline char *strndup(const char *source, size_t limit)
{
    size_t len = strnlen(source, limit);
    char *copy = malloc(len + 1);
    if (copy) {
        memcpy(copy, source, len);
        copy[len] = 0;
    }
    return copy;
}
static inline int ftruncate(int fd, int64_t length)
{
    errno_t error = _chsize_s(fd, length);
    if (error) {
        errno = error;
        return -1;
    }
    return 0;
}
