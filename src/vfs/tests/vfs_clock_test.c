// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/platform.h"
#include "common/thread.h"
#include <stdio.h>
#include <time.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_clock.h"
#include "vfs/sdk/vfs_utils.h"

static uint64_t
to_ns(const struct timespec *ts)
{
    return (uint64_t) ts->tv_sec * 1000000000ULL + ts->tv_nsec;
} /* to_ns */

/* The fallback must read the actual system clock. The TSC path permits
 * bounded estimation error but must not go backwards across refreshes. */
static void *
check_realtime(void *arg)
{
    struct timespec before, actual, after;
    uint64_t        previous = 0;
    int             tsc      = chimera_vfs_clock.initialized && chimera_vfs_clock.ctx.use_tsc;

    (void) arg;

    for (int i = 0; i < 10000; i++) {
        assert(clock_gettime(CLOCK_REALTIME, &before) == 0);
        chimera_vfs_realtime(&actual);
        assert(clock_gettime(CLOCK_REALTIME, &after) == 0);
        if (tsc) {
            assert(to_ns(&actual) >= previous);
            assert(to_ns(&actual) + 100000000ULL >= to_ns(&before));
            assert(to_ns(&actual) <= to_ns(&after) + 100000000ULL);
            previous = to_ns(&actual);
        } else if (to_ns(&actual) < to_ns(&before) || to_ns(&actual) > to_ns(&after)) {
            fprintf(stderr, "VFS realtime outside system clock bounds: %llu <= %llu <= %llu\n",
                    (unsigned long long) to_ns(&before),
                    (unsigned long long) to_ns(&actual),
                    (unsigned long long) to_ns(&after));
            assert(0);
        }
    }

    return NULL;
} /* check_realtime */

int
main(void)
{
    evpl_native_thread_t readers[4];

    check_realtime(NULL);
    chimera_vfs_clock_init();
    check_realtime(NULL);
    chimera_thread_sleep_us(1100000);
    for (int i = 0; i < 4; i++) {
        assert(evpl_native_thread_create(&readers[i], NULL, check_realtime, NULL) == 0);
    }
    for (int i = 0; i < 4; i++) {
        assert(evpl_native_thread_join(readers[i], NULL) == 0);
    }
    chimera_vfs_clock_shutdown();
    check_realtime(NULL);
    return 0;
} /* main */
