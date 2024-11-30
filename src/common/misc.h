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