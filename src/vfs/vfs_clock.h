// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/atomic.h"
#include "common/export.h"
#include <stdint.h>
#include <time.h>
#include "stopwatch.h"

/* Process-wide stopwatch context. Monotonic ticks serve cache TTLs and
 * deadlines; stopwatch's separate wall clock supplies file timestamps. */
struct chimera_vfs_clock {
    struct stopwatch_context ctx;
    struct stopwatch         base_sw;
    int                      initialized;
};

/* Exported data (see common/macros.h). */
#ifdef CHIMERA_VFS_BUILD
#define CHIMERA_VFS_DATA CHIMERA_DATA_EXPORT
#else // ifdef CHIMERA_VFS_BUILD
#define CHIMERA_VFS_DATA CHIMERA_DATA_IMPORT
#endif // ifdef CHIMERA_VFS_BUILD

extern CHIMERA_VFS_DATA struct chimera_vfs_clock chimera_vfs_clock;

SYMBOL_EXPORT void chimera_vfs_clock_init(
    void);

SYMBOL_EXPORT void chimera_vfs_clock_shutdown(
    void);

/* Monotonic time in stopwatch ticks since init. */
static inline uint64_t
chimera_vfs_now_ticks(void)
{
    return stopwatch_read_ticks(&chimera_vfs_clock.ctx, &chimera_vfs_clock.base_sw);
} /* chimera_vfs_now_ticks */

static inline uint64_t
chimera_vfs_ns_to_ticks(uint64_t ns)
{
    return stopwatch_ns_to_ticks(&chimera_vfs_clock.ctx, ns);
} /* chimera_vfs_ns_to_ticks */

static inline uint64_t
chimera_vfs_ticks_to_ns(uint64_t ticks)
{
    return stopwatch_ticks_to_ns(&chimera_vfs_clock.ctx, ticks);
} /* chimera_vfs_ticks_to_ns */

/* Monotonic nanoseconds elapsed since a tick stamp taken with
 * chimera_vfs_now_ticks(). Clamped to 0 if the stamp is in the future. */
static inline uint64_t
chimera_vfs_elapsed_ns(uint64_t since_ticks)
{
    uint64_t now = chimera_vfs_now_ticks();

    return now > since_ticks ? chimera_vfs_ticks_to_ns(now - since_ticks) : 0;
} /* chimera_vfs_elapsed_ns */

/* File timestamps use stopwatch_realtime through the module-facing SDK
 * helper chimera_vfs_realtime, implemented in vfs_sdk_utils.c. */
