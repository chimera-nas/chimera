// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Compiled implementations of the SDK helpers declared in sdk/vfs_utils.h
 * and sdk/vfs_attrs.h.  These live in the VFS core (rather than as header
 * inlines) so that backend modules incorporate no VFS code at build time;
 * see the SDK boundary rules in sdk/chimera_vfs_sdk.h.
 */

#ifndef XXH_INLINE_ALL
#define XXH_INLINE_ALL
#endif /* ifndef XXH_INLINE_ALL */
#include <xxhash.h>

#include "sdk/vfs_utils.h"
#include "sdk/vfs_attrs.h"
#include "vfs/vfs.h"
#include "vfs_clock.h"
#include "common/macros.h"

SYMBOL_EXPORT uint64_t
chimera_vfs_hash(
    const void *data,
    int         len)
{
    /* Mask the MSB to ensure the result is non-negative when interpreted as
     * a signed 64-bit value.  NFS readdir cookies are derived from this hash
     * and the Linux kernel rejects negative loff_t values in nfs_llseek_dir(),
     * which breaks seekdir()/telldir() for cookies with bit 63 set. */
    return XXH3_64bits(data, len) & INT64_MAX;
} /* chimera_vfs_hash */

SYMBOL_EXPORT void
chimera_vfs_realtime(struct timespec *ts)
{
    uint64_t ns;

    if (unlikely(!chimera_vfs_clock.initialized)) {
        clock_gettime(CLOCK_REALTIME, ts);
        return;
    }

    ns          = chimera_vfs_wall_ns();
    ts->tv_sec  = ns / 1000000000ULL;
    ts->tv_nsec = ns % 1000000000ULL;
} /* chimera_vfs_realtime */

SYMBOL_EXPORT enum chimera_tcp_flavor
chimera_vfs_request_tcp_flavor(const struct chimera_vfs_request *request)
{
    return request->thread->vfs->tcp_flavor;
} /* chimera_vfs_request_tcp_flavor */

SYMBOL_EXPORT int
chimera_vfs_resolve_set_time(
    const struct timespec *in,
    const struct timespec *now,
    struct timespec       *out)
{
    if (in->tv_nsec == CHIMERA_VFS_TIME_NOW) {
        *out = *now;
        return 1;
    } else if (in->tv_nsec != CHIMERA_VFS_TIME_OMIT) {
        *out = *in;
        return 1;
    }

    return 0;
} /* chimera_vfs_resolve_set_time */

SYMBOL_EXPORT int
chimera_vfs_relatime_needs_update(
    const struct timespec *atime,
    const struct timespec *mtime,
    const struct timespec *ctime,
    const struct timespec *now)
{
    if (chimera_vfs_timespec_ge(mtime, atime)) {
        return 1;
    }
    if (chimera_vfs_timespec_ge(ctime, atime)) {
        return 1;
    }
    if (now->tv_sec - atime->tv_sec >= CHIMERA_VFS_RELATIME_PERIOD_SEC) {
        return 1;
    }
    return 0;
} /* chimera_vfs_relatime_needs_update */
