// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Process-wide pthread_create() interposer with bounded EAGAIN retry.
 *
 * pthread_create() can fail transiently with EAGAIN under resource pressure
 * (RLIMIT_NPROC, cgroup pids/memory limits, kernel threads-max) during the
 * thread-creation spikes a chimera process produces at startup -- e.g. the
 * delegation/identity/log pools plus liburcu's per-CPU call_rcu reclaim
 * workers, multiplied across the many in-process server/client instances a
 * parallel (-j) CI run brings up at once.
 *
 * chimera's own thread spawns already ride out that spike via
 * chimera_pthread_create() (src/common/pthread_util.h).  liburcu does NOT:
 * its call_rcu_data_init() spawns the reclaim worker with a bare
 * pthread_create() and, on any nonzero return, calls urcu_die() which abort()s
 * the entire process ("Unrecoverable error: Resource temporarily
 * unavailable").  liburcu exposes no hook to retry that spawn, no way to hand
 * it a pre-created thread, and lazily creates additional workers (the default
 * worker on first call_rcu(), the per-CPU workers via
 * create_all_cpu_call_rcu_data()) that all share the same no-retry path.
 * Capping the configured reclaim-worker count only lowers the odds; it is not a
 * fix, because a single EAGAIN on any one of those spawns still aborts.
 *
 * The only place to make that spawn EAGAIN-tolerant for every caller (server,
 * client, posix-direct) and every worker count is the pthread_create() symbol
 * itself.  We therefore define a default-visibility pthread_create() in
 * chimera_common (every chimera library/binary links it), which interposes the
 * libc symbol process-wide and retries EAGAIN before delegating to the real
 * implementation resolved via dlsym(RTLD_NEXT).  RTLD_NEXT chains correctly
 * through any other interposer ahead of libc -- notably AddressSanitizer's
 * pthread_create() interceptor in debug builds -- so thread tracking is
 * preserved.  The retry is a no-op for the overwhelming non-EAGAIN case, so
 * normal thread creation is unaffected.
 *
 * Note: the whole library is built with -fvisibility=hidden, so this symbol
 * MUST carry default visibility (SYMBOL_EXPORT) or it will not interpose.
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <errno.h>
#include <dlfcn.h>
#include <unistd.h>

#include "macros.h"

typedef int (*chimera_pthread_create_fn)(
    pthread_t *,
    const pthread_attr_t *,
    void *(*) (void *),
    void *);

/*
 * Resolve the next pthread_create in the search order (the real libc one, or an
 * upstream interceptor such as ASan's).  dlsym() is itself thread-safe and the
 * resolved pointer is idempotent, so a benign data race on first use just
 * resolves the same value twice.
 */
static chimera_pthread_create_fn
chimera_real_pthread_create(void)
{
    static chimera_pthread_create_fn real;

    if (!real) {
        real = (chimera_pthread_create_fn) dlsym(RTLD_NEXT, "pthread_create");
    }

    return real;
} /* chimera_real_pthread_create */

SYMBOL_EXPORT int
pthread_create(
    pthread_t            *thread,
    const pthread_attr_t *attr,
    void *(*start_routine )(void *),
    void                 *arg)
{
    chimera_pthread_create_fn real  = chimera_real_pthread_create();
    useconds_t                delay = 1000;
    int                       attempt;
    int                       rc;

    if (!real) {
        /* Should never happen, but never recurse into ourselves. */
        return EAGAIN;
    }

    for (attempt = 0; ; attempt++) {
        rc = real(thread, attr, start_routine, arg);

        if (rc != EAGAIN || attempt >= 100) {
            return rc;
        }

        usleep(delay);

        if (delay < 100000) {
            delay *= 2;
        }
    }
} /* pthread_create */
