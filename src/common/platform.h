// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Platform compatibility shims.
 *
 * Chimera is developed against glibc/Linux; macOS is supported for the
 * portable core (VFS framework, memfs/memkv/diskfs/cairn, the protocol
 * servers and the unit suites).  This header maps the handful of Linux
 * spellings that portable code reaches for onto their Darwin equivalents.
 *
 * Only interfaces with an honest one-to-one equivalent belong here.
 * Anything that has no counterpart -- io_uring, openat2, per-thread
 * setfsuid/setfsgid impersonation -- is gated out of the build instead
 * (see the CHIMERA_LINUX checks in the CMakeLists) rather than emulated.
 */

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <sys/types.h>
#include <sys/random.h>

#ifdef __APPLE__

#include <pthread.h>
#include <libkern/OSByteOrder.h>
#include <sys/param.h>
#include <sys/mount.h>  /* struct statfs; <sys/vfs.h> on glibc */

/*
 * Fixed-width byte-order helpers.  Linux provides these via <endian.h>;
 * macOS exposes the equivalent swaps through <libkern/OSByteOrder.h>.
 */
#define htobe16(x)            OSSwapHostToBigInt16(x)
#define htole16(x)            OSSwapHostToLittleInt16(x)
#define be16toh(x)            OSSwapBigToHostInt16(x)
#define le16toh(x)            OSSwapLittleToHostInt16(x)

#define htobe32(x)            OSSwapHostToBigInt32(x)
#define htole32(x)            OSSwapHostToLittleInt32(x)
#define be32toh(x)            OSSwapBigToHostInt32(x)
#define le32toh(x)            OSSwapLittleToHostInt32(x)

#define htobe64(x)            OSSwapHostToBigInt64(x)
#define htole64(x)            OSSwapHostToLittleInt64(x)
#define be64toh(x)            OSSwapBigToHostInt64(x)
#define le64toh(x)            OSSwapLittleToHostInt64(x)

/*
 * major()/minor()/makedev() live in <sys/sysmacros.h> on glibc and in
 * <sys/types.h> on Darwin, which is already included above.
 */

/*
 * Nanosecond timestamps in the *host* struct stat.  POSIX.1-2008 names them
 * st_atim/st_mtim/st_ctim (what glibc uses); Darwin predates the standard and
 * spells them st_*timespec.  Chimera's own struct chimera_stat always uses the
 * POSIX names and needs no shim -- only host struct stat access does.
 */
#define CHIMERA_STAT_ATIM(st) ((st).st_atimespec)
#define CHIMERA_STAT_MTIM(st) ((st).st_mtimespec)
#define CHIMERA_STAT_CTIM(st) ((st).st_ctimespec)

/*
 * getgrouplist() takes gid_t * on glibc and int * on Darwin.  gid_t is a
 * 32-bit unsigned int on both, so only the declared pointer type differs;
 * chimera_grouplist_t names whichever the host's prototype wants.
 */
typedef int chimera_grouplist_t;

#else /* ifdef __APPLE__ */

#include <endian.h>
#include <sys/sysmacros.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#include <unistd.h>

#define CHIMERA_STAT_ATIM(st) ((st).st_atim)
#define CHIMERA_STAT_MTIM(st) ((st).st_mtim)
#define CHIMERA_STAT_CTIM(st) ((st).st_ctim)

typedef gid_t chimera_grouplist_t;

#endif /* ifdef __APPLE__ */

/*
 * chimera's POSIX shim exposes a Linux-shaped fallocate(2) -- see
 * chimera_posix_fallocate_mode() -- so its mode bits are part of chimera's own
 * API and must be spelled the same everywhere.  glibc gets them from
 * <linux/falloc.h>; define them for libcs that have no such header.  Only the
 * two modes chimera_posix_fallocate_mode() accepts are declared: a caller that
 * needs another one wants the host fallocate(2), which is Linux-only anyway.
 */
#ifndef FALLOC_FL_KEEP_SIZE
#define FALLOC_FL_KEEP_SIZE    0x01
#endif /* ifndef FALLOC_FL_KEEP_SIZE */

#ifndef FALLOC_FL_PUNCH_HOLE
#define FALLOC_FL_PUNCH_HOLE   0x02
#endif /* ifndef FALLOC_FL_PUNCH_HOLE */

/*
 * Linux's coarse monotonic clock trades a tick of resolution for a read that
 * never leaves the vDSO.  Darwin has no equivalent; CLOCK_MONOTONIC carries the
 * same semantics at a higher (still cheap) cost, which is ample for the lease,
 * session and cache-expiry bookkeeping that reads it.
 */
#ifndef CLOCK_MONOTONIC_COARSE
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif /* ifndef CLOCK_MONOTONIC_COARSE */

/*
 * Fill a buffer with kernel-quality randomness.  Returns 0 on success and -1
 * (with errno set) otherwise.  Linux spells this getrandom(2), which may return
 * short; Darwin spells it getentropy(2), which is all-or-nothing and caps len at
 * 256 bytes -- comfortably above every chimera call site.
 */
static inline int
chimera_getrandom(
    void  *buf,
    size_t len)
{
#ifdef __APPLE__
    return getentropy(buf, len);
#else  /* ifdef __APPLE__ */
    return getrandom(buf, len, 0) == (ssize_t) len ? 0 : -1;
#endif /* ifdef __APPLE__ */
} /* chimera_getrandom */

/*
 * Per-thread identifier, used only to annotate log lines.
 *
 * Linux gettid() returns the kernel task id; the raw syscall is used so this
 * header does not oblige every includer to define _GNU_SOURCE.  Darwin's
 * equivalent is the 64-bit mach thread id from pthread_threadid_np(), likewise
 * unique per thread within the process.
 */
static inline uint64_t
chimera_gettid(void)
{
#ifdef __APPLE__
    uint64_t tid = 0;

    pthread_threadid_np(NULL, &tid);
    return tid;
#else  /* ifdef __APPLE__ */
    return (uint64_t) syscall(SYS_gettid);
#endif /* ifdef __APPLE__ */
} /* chimera_gettid */
