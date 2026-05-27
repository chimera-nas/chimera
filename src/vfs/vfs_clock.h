// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <time.h>
#include "stopwatch.h"

/*
 * Process-wide VFS clock backed by the stopwatch TSC timer.
 *
 * Monotonic time is read directly from the stopwatch (a single unfenced rdtsc
 * on the TSC path) and is used for cache TTLs, lease deadlines, etc.
 *
 * Wall-clock time (for file timestamps) is reconstructed without a per-call
 * clock_gettime: at init we capture base_wall_ns paired with a base stopwatch,
 * and a correction delta_ns = real_now - (base_wall + elapsed_ticks). The
 * delta is refreshed lazily and inline: whenever a caller finds more than
 * ~1s of ticks have elapsed since the last refresh, it re-reads CLOCK_REALTIME
 * and updates the delta. No background thread, no lock; the delta/last-refresh
 * accesses are plain 64-bit reads/writes (relaxed atomics == one mov on
 * x86-64). A stale delta only costs up to ~1s of drift, corrected on the next
 * crossing.
 */
struct chimera_vfs_clock {
    struct stopwatch_context ctx;
    struct stopwatch         base_sw;         /* started at init */
    uint64_t                 base_wall_ns;    /* CLOCK_REALTIME ns at init */
    uint64_t                 delta_ns;        /* wall-vs-tsc correction (relaxed) */
    uint64_t                 last_refresh;    /* ticks at last delta refresh (relaxed) */
    uint64_t                 refresh_interval;/* ticks between refreshes (~1s) */
    int                      initialized;
};

extern struct chimera_vfs_clock chimera_vfs_clock;

void chimera_vfs_clock_init(
    void);

void chimera_vfs_clock_shutdown(
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

/* Recompute the wall-vs-tsc correction from CLOCK_REALTIME. Called inline from
 * the hot path at most ~once/sec (per the refresh interval). Racing callers
 * simply recompute the same value; the relaxed stores need no lock. */
static inline void
chimera_vfs_clock_refresh(uint64_t now_ticks)
{
    struct timespec ts;
    uint64_t        actual, elapsed;

    clock_gettime(CLOCK_REALTIME, &ts);
    actual  = (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
    elapsed = chimera_vfs_ticks_to_ns(now_ticks);

    __atomic_store_n(&chimera_vfs_clock.delta_ns, actual - chimera_vfs_clock.base_wall_ns - elapsed,
                     __ATOMIC_RELAXED);
    __atomic_store_n(&chimera_vfs_clock.last_refresh, now_ticks, __ATOMIC_RELAXED);
} /* chimera_vfs_clock_refresh */

/* Current wall-clock time in nanoseconds, reconstructed from the TSC. */
static inline uint64_t
chimera_vfs_wall_ns(void)
{
    uint64_t now_ticks = chimera_vfs_now_ticks();

    if (unlikely(now_ticks - __atomic_load_n(&chimera_vfs_clock.last_refresh, __ATOMIC_RELAXED) >
                 chimera_vfs_clock.refresh_interval)) {
        chimera_vfs_clock_refresh(now_ticks);
    }

    return chimera_vfs_clock.base_wall_ns + chimera_vfs_ticks_to_ns(now_ticks) +
           __atomic_load_n(&chimera_vfs_clock.delta_ns, __ATOMIC_RELAXED);
} /* chimera_vfs_wall_ns */

/* Fill ts with the current wall-clock time. Equivalent to
 * clock_gettime(CLOCK_REALTIME, ts) but without the per-call syscall/read;
 * falls back to clock_gettime if the clock has not been initialized yet. */
static inline void
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
