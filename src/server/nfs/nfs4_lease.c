// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <pthread.h>
#include <stdatomic.h>
#include <time.h>

#include "nfs4_lease.h"
#include "nfs4_recovery.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "common/misc.h"
#include "evpl/evpl.h"

#define NFS_LEASE_SWEEP_INTERVAL_US 1000000   /* 1 Hz */

/* True once `deadline` (CLOCK_MONOTONIC) has elapsed. */
static inline bool
nfs_deleg_deadline_passed(const struct timespec *deadline)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    if (now.tv_sec != deadline->tv_sec) {
        return now.tv_sec > deadline->tv_sec;
    }
    return now.tv_nsec >= deadline->tv_nsec;
} /* nfs_deleg_deadline_passed */

/* If any of `uc`'s delegations has an outstanding recall that has gone
 * unanswered past its deadline, the client's callback path is unusable: mark
 * it DOWN so a subsequent RENEW returns NFS4ERR_CB_PATH_DOWN (RFC 7530
 * §16.30.4).  Blocked acquirers separately revoke the lease lazily on retry. */
static void
nfs_deleg_recall_timeout_check(struct nfs_client *uc)
{
    struct nfs_delegation *deleg;
    bool                   timed_out = false;

    pthread_mutex_lock(&uc->lock);
    LL_FOREACH2(uc->delegations, deleg, next_in_client)
    {
        if (deleg->lease_held &&
            deleg->lease.break_state == CHIMERA_VFS_BREAK_BREAKING &&
            nfs_deleg_deadline_passed(&deleg->lease.break_deadline)) {
            timed_out = true;
            break;
        }
    }
    pthread_mutex_unlock(&uc->lock);

    if (timed_out) {
        atomic_store_explicit(&uc->cb_path.cb_state, NFS4_CB_DOWN,
                              memory_order_release);
    }
} /* nfs_deleg_recall_timeout_check */

void
nfs_lease_sweep_once(struct chimera_server_nfs_shared *shared)
{
#ifndef __clang_analyzer__
    struct nfs4_client_table *table = &shared->nfs4_shared_clients;
    struct nfs4_client       *cur, *tmp;
    uint64_t                  now_ns = nfs_lease_now_ns();
    uint64_t                  lease_ns;

    lease_ns = (uint64_t) shared->nfs_lease_time_s * 1000000000ULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_ITER(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, cur, tmp)
    {
        struct nfs_client *uc = cur->unified;
        uint64_t           last;

        if (!uc) {
            continue;
        }

        last = atomic_load_explicit((_Atomic uint64_t *) &uc->last_touch_ns,
                                    memory_order_relaxed);

        if (last == 0) {
            /* Newly created client that hasn't been touched yet: give it
             * one sweep grace by stamping now. */
            atomic_store_explicit((_Atomic uint64_t *) &uc->last_touch_ns,
                                  now_ns, memory_order_relaxed);
            continue;
        }

        if (now_ns - last > lease_ns) {
            uc->expired = 1;
        }

        /* Detect delegation recalls the client never answered. */
        nfs_deleg_recall_timeout_check(uc);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);
#endif /* ifndef __clang_analyzer__ */

    /* Phase 5: piggyback grace-window expiry on the lease tick. */
    nfs_recovery_sweep_once(&shared->nfs4_recovery);
} /* nfs_lease_sweep_once */

static void
nfs_lease_sweeper_fire(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct nfs_lease_sweeper *sweeper =
        container_of(timer, struct nfs_lease_sweeper, timer);

    nfs_lease_sweep_once(sweeper->thread->shared);
} /* nfs_lease_sweeper_fire */

void
nfs_lease_sweeper_init(
    struct nfs_lease_sweeper         *sweeper,
    struct chimera_server_nfs_thread *thread)
{
    sweeper->thread = thread;
    evpl_add_timer(thread->evpl, &sweeper->timer,
                   nfs_lease_sweeper_fire,
                   NFS_LEASE_SWEEP_INTERVAL_US);
} /* nfs_lease_sweeper_init */

void
nfs_lease_sweeper_destroy(struct nfs_lease_sweeper *sweeper)
{
    if (sweeper->thread) {
        evpl_remove_timer(sweeper->thread->evpl, &sweeper->timer);
        sweeper->thread = NULL;
    }
} /* nfs_lease_sweeper_destroy */
