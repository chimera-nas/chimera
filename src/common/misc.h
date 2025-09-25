// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <sys/random.h>
#include "common/logging.h"
#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)
#endif /* ifndef likely */

#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif /* ifndef unlikely */


static inline uint64_t
chimera_rand64(void)
{
    uint64_t v;
    ssize_t  rc;

    rc = getrandom(&v, sizeof v, 0);

    chimera_abort_if(rc != sizeof v, "common", __FILE__, __LINE__, "getrandom failed");

    return v;
} /* chimera_rand64 */

#define NT_EPOCH_DELTA 11644473600ULL

static inline uint64_t
chimera_nt_time(const struct timespec *ts)
{
    uint64_t nt_now = ts->tv_sec;

    nt_now += NT_EPOCH_DELTA;
    nt_now *= 10000000ULL;
    nt_now += ts->tv_nsec / 100ULL;

    return nt_now;
} /* chimera_nt_time */

static inline void
chimera_nt_to_epoch(
    uint64_t         nt_now,
    struct timespec *ts)
{
    ts->tv_sec  = (nt_now - NT_EPOCH_DELTA) / 10000000ULL;
    ts->tv_nsec = (nt_now % 10000000ULL) * 100ULL;
} /* chimera_nt_to_epoch */

static inline uint64_t
chimera_get_elapsed_ns(
    struct timespec *end,
    struct timespec *start)
{
    uint64_t end_ns   = end->tv_sec * 1000000000 + end->tv_nsec;
    uint64_t start_ns = start->tv_sec * 1000000000 + start->tv_nsec;

    return end_ns - start_ns;
} /* chimera_get_elapsed_ns */

static inline int
chimera_timespec_cmp(
    const struct timespec *a,
    const struct timespec *b)
{
    if (a->tv_sec < b->tv_sec) {
        return -1;
    } else if (a->tv_sec > b->tv_sec) {
        return 1;
    } else {
        if (a->tv_nsec < b->tv_nsec) {
            return -1;
        } else if (a->tv_nsec > b->tv_nsec) {
            return 1;
        } else {
            return 0;
        }
    }
} /* chimera_timespec_cmp */

static inline int
chimera_memequal(
    const void *a,
    uint64_t    alen,
    const void *b,
    uint64_t    blen)
{
    const uint64_t *a64 = a;
    const uint64_t *b64 = b;
    const uint8_t  *a8;
    const uint8_t  *b8;
    uint64_t        len64, len8, i;

    if (alen != blen) {
        return 0;
    }

    len64 = alen >> 3;
    len8  = alen & 7;

    a8 = (const uint8_t *) &a64[len64];
    b8 = (const uint8_t *) &b64[len64];

    for (i = 0; i < len64; i++) {
        if (a64[i] != b64[i]) {
            return 0;
        }
    }

    for (i = 0; i < len8; i++) {
        if (a8[i] != b8[i]) {
            return 0;
        }
    }

    return 1;
} /* chimera_memequal */
