// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <time.h>
#include "common/platform.h"
#include "common/atomic.h"

/*
 * A flood guard for a message that comes in bursts -- every request failing
 * the same way while a dependency is down.  Give each message site its own
 * zero-initialised struct chimera_ratelimit (a static is fine: it is
 * lock-free).  chimera_ratelimit_allow() returns 1 at most once per
 * `interval_ns`, setting *suppressed to how many calls it refused since the
 * last one it allowed, so the message can say how many it stands for.
 */
struct chimera_ratelimit {
    uint64_t next_ns;
    uint64_t suppressed;
};

static inline int
chimera_ratelimit_allow(
    struct chimera_ratelimit *rl,
    uint64_t                  interval_ns,
    uint64_t                 *suppressed)
{
    struct timespec ts;
    uint64_t        now, next;

    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    now  = (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
    next = chimera_atomic_load_n(&rl->next_ns, CHIMERA_MEMORY_RELAXED);

    if (now < next ||
        !chimera_atomic_compare_exchange_n(&rl->next_ns, &next,
                                           now + interval_ns, 0,
                                           CHIMERA_MEMORY_RELAXED,
                                           CHIMERA_MEMORY_RELAXED)) {
        chimera_atomic_fetch_add(&rl->suppressed, 1, CHIMERA_MEMORY_RELAXED);
        return 0;
    }

    *suppressed = chimera_atomic_exchange_n(&rl->suppressed, 0,
                                            CHIMERA_MEMORY_RELAXED);
    return 1;
} /* chimera_ratelimit_allow */
