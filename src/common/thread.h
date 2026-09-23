// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <evpl/evpl_platform.h>
#include <time.h>
#include <errno.h>
#ifndef _WIN32
#include <unistd.h>
#endif // ifndef _WIN32

/* Absolute UTC deadlines, matching pthread_cond_timedwait on Unix. */
static inline int
chimera_cond_timedwait(
    evpl_cond_t           *cond,
    evpl_mutex_t          *mutex,
    const struct timespec *deadline)
{
#ifdef _WIN32
    struct timespec now;
    int64_t         ns;
    uint64_t        ms;
    DWORD           error;

    timespec_get(&now, TIME_UTC);
    ns = ((int64_t) deadline->tv_sec - now.tv_sec) * 1000000000 +
        deadline->tv_nsec - now.tv_nsec;
    ms = ns > 0 ? ((uint64_t) ns + 999999) / 1000000 : 0;
    if (SleepConditionVariableSRW(cond, mutex,
                                  ms >= INFINITE ? INFINITE - 1 : (DWORD) ms, 0)) {
        return 0;
    }
    error = GetLastError();
    return error == ERROR_TIMEOUT ? ETIMEDOUT : (int) error;
#else // ifdef _WIN32
    return pthread_cond_timedwait(cond, mutex, deadline);
#endif // ifdef _WIN32
} // chimera_cond_timedwait

static inline int
chimera_rwlock_destroy(evpl_rwlock_t *lock)
{
#ifdef _WIN32
    (void) lock;
    return 0;
#else // ifdef _WIN32
    return pthread_rwlock_destroy(lock);
#endif // ifdef _WIN32
} // chimera_rwlock_destroy

static inline void
chimera_thread_sleep_us(unsigned int delay)
{
#ifdef _WIN32
    Sleep((delay + 999) / 1000);
#else // ifdef _WIN32
    usleep(delay);
#endif // ifdef _WIN32
} // chimera_thread_sleep_us
