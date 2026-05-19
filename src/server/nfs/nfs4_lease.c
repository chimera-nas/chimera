// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <pthread.h>
#include <stdatomic.h>

#include "nfs4_lease.h"
#include "nfs4_recovery.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "common/misc.h"
#include "evpl/evpl.h"

#define NFS_LEASE_SWEEP_INTERVAL_US 1000000   /* 1 Hz */

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
