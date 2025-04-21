#pragma once

#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)
#endif /* ifndef likely */

#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif /* ifndef unlikely */

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
