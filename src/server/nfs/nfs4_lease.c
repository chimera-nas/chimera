// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdatomic.h>
#include <stdlib.h>
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

/* True once `deadline` (stopwatch ticks) has elapsed. */
static inline bool
nfs_deleg_deadline_passed(uint64_t deadline)
{
    return chimera_vfs_now_ticks() >= deadline;
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

    evpl_mutex_lock(&uc->lock);
    LL_FOREACH2(uc->delegations, deleg, next_in_client)
    {
        if (deleg->lease_held &&
            deleg->claim.break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
            nfs_deleg_deadline_passed(deleg->claim.break_deadline)) {
            timed_out = true;
            break;
        }
    }
    evpl_mutex_unlock(&uc->lock);

    if (timed_out) {
        atomic_store_explicit(&uc->cb_path.cb_state, NFS4_CB_DOWN,
                              memory_order_release);
    }
} /* nfs_deleg_recall_timeout_check */

/* What the sweep does to a client once the table lock is dropped. */
enum nfs_lease_action {
    /* Revoke its state, and drop its stable-storage record. */
    NFS_LEASE_EXPIRE,
    /* Revoke its state, but keep the stable-storage record: a newer record
    * for the same owner holds the slot and may already have written it. */
    NFS_LEASE_EXPIRE_KEEP_RECORD,
    /* Its tombstone was retired: destroy the state hierarchy. */
    NFS_LEASE_RETIRE,
};

struct nfs_lease_victim {
    struct nfs_client *uc;
    enum nfs_lease_action action;
};

static void
nfs_lease_victim_add(
    struct nfs_lease_victim **victims,
    size_t                   *count,
    size_t                   *capacity,
    struct nfs_client        *uc,
    enum nfs_lease_action     action)
{
    if (*count == *capacity) {
        size_t                   new_capacity = *capacity ? *capacity * 2 : 8;
        struct nfs_lease_victim *new_victims;

        new_victims = realloc(*victims, new_capacity * sizeof(*new_victims));
        chimera_nfs_abort_if(new_victims == NULL,
                             "expired client list realloc failed");
        *victims  = new_victims;
        *capacity = new_capacity;
    }
    (*victims)[*count].uc     = uc;
    (*victims)[*count].action = action;
    (*count)++;
} /* nfs_lease_victim_add */

void
nfs_lease_sweep_once(struct chimera_server_nfs_thread *thread)
{
#ifndef __clang_analyzer__
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs4_client_table         *table  = &shared->nfs4_shared_clients;
    struct nfs4_client               *cur, *tmp;
    struct nfs_lease_victim          *victims         = NULL;
    size_t                            victim_count    = 0;
    size_t                            victim_capacity = 0;
    uint64_t                          now_ns          = nfs_lease_now_ns();
    uint64_t                          lease_ns;

    uint64_t                          courtesy_ns;

    lease_ns    = (uint64_t) shared->nfs_lease_time_s * 1000000000ULL;
    courtesy_ns = (uint64_t) shared->nfs_courtesy_time_s * 1000000000ULL;

    evpl_mutex_lock(&table->nfs4_ct_lock);

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

        if (!uc->expired) {
            uint8_t courtesy = atomic_load_explicit(&uc->courtesy,
                                                    memory_order_relaxed);
            uint8_t reclaim = atomic_load_explicit(&uc->reclaim_pending,
                                                   memory_order_acquire);
            bool    reap = false;

            /* `now_ns` is sampled once before the table lock is taken, while
             * `last` (a client's last_touch_ns) is stamped by EXCHANGE_ID /
             * SEQUENCE / RENEW on other threads.  A client created or touched
             * after that sample therefore has last > now_ns, and `now_ns - last`
             * is unsigned -- so a naive `now_ns - last > lease_ns` underflows to
             * a huge value and spuriously treats a brand-new client as lapsed.
             * Guard the subtraction: a client whose last touch is at or after
             * `now_ns` is freshly active, never lapsed. */
            bool lease_lapsed = now_ns > last && (now_ns - last) > lease_ns;

            if (reclaim) {
                /* A conflicting acquirer reclaimed one of this courtesy
                 * client's leases: tear it down now. */
                reap = true;
            } else if (!cur->nfs4_client_confirmed) {
                /* RFC 8881 §18.35.3: an unconfirmed EXCHANGE_ID record that
                 * is not confirmed by CREATE_SESSION within a lease period is
                 * stale.  Courtesy only applies after the client has proven
                 * possession of the clientid. */
                if (lease_lapsed) {
                    reap = true;
                }
            } else if (courtesy) {
                /* Already in courtesy: reap once the courtesy window closes. */
                if (now_ns >= uc->courtesy_deadline_ns) {
                    reap = true;
                }
            } else if (lease_lapsed) {
                /* Lease lapsed for the first time: enter courtesy rather than
                 * revoking, so the client resumes seamlessly if it returns and
                 * a conflicting request can reclaim its state on demand
                 * (RFC 8881 §8.3 courteous server). */
                uc->courtesy_deadline_ns = now_ns + courtesy_ns;
                atomic_store_explicit(&uc->courtesy, 1, memory_order_release);
            }

            if (reap) {
                struct nfs4_client *holder;

                uc->expired = 1;

                if (cur->nfs4_client_confirmed) {
                    /* The lease is cancelled: take the record out of owner
                     * matching so the client registers a new clientid.  The
                     * tombstone's hold restarts now, so it is not retired
                     * while the teardown below may still be running. */
                    nfs4_client_fence_locked(table, cur);
                    cur->nfs4_client_fenced_ns = now_ns ? now_ns : 1;
                }

                HASH_FIND(nfs4_client_hh_by_owner,
                          table->nfs4_ct_clients_by_owner,
                          cur->nfs4_client_owner, cur->nfs4_client_owner_len,
                          holder);

                nfs_lease_victim_add(&victims, &victim_count, &victim_capacity,
                                     uc, holder && holder != cur ?
                                     NFS_LEASE_EXPIRE_KEEP_RECORD :
                                     NFS_LEASE_EXPIRE);
            }
        } else if (cur->nfs4_client_fenced_ns &&
                   now_ns > cur->nfs4_client_fenced_ns &&
                   now_ns - cur->nfs4_client_fenced_ns >= lease_ns) {
            /* The tombstone has answered NFS4ERR_EXPIRED for a full lease,
             * which is long enough for the client to have seen it.  Without
             * this a cancelled client would leak its record, since it is never
             * matched again. */
            nfs_lease_victim_add(&victims, &victim_count, &victim_capacity,
                                 nfs4_client_retire_locked(table, cur),
                                 NFS_LEASE_RETIRE);
            continue;
        }

        /* Keep the client's stable-storage record within half a lease of the
         * truth: the next boot compares its renewal stamp with this boot's
         * last heartbeat to tell a client that was alive at the crash from one
         * whose lease had already lapsed.  Issued under the table lock so the
         * client cannot be freed under the serialize; the KV write itself is
         * asynchronous and copies its input. */
        if (nfs_recovery_persist_due(&shared->nfs4_recovery, uc, now_ns)) {
            nfs_recovery_persist(thread->vfs_thread, &shared->nfs4_recovery,
                                 uc);
        }

        /* Detect delegation recalls the client never answered. */
        nfs_deleg_recall_timeout_check(uc);
    }

    evpl_mutex_unlock(&table->nfs4_ct_lock);

    for (size_t i = 0; i < victim_count; i++) {
        struct nfs_client *uc = victims[i].uc;

        if (victims[i].action == NFS_LEASE_RETIRE) {
            nfs_client_destroy(uc, &shared->nfs4_state_table,
                               thread->vfs_thread, false);
            continue;
        }

        nfs_client_expire_state(uc, &shared->nfs4_state_table,
                                thread->vfs_thread);
        /* Its lease is gone for good: drop the stable-storage record too, so
         * the next reboot does not wait on a client that cannot reclaim.  The
         * record is keyed by owner, so leave it alone when a newer record for
         * the owner exists: that one persists or forgets it. */
        if (victims[i].action == NFS_LEASE_EXPIRE) {
            nfs_recovery_forget(thread->vfs_thread, &shared->nfs4_recovery,
                                uc->owner_string, uc->owner_len);
        }
    }
    free(victims);
#endif /* ifndef __clang_analyzer__ */

    /* Record that this instance was alive (see nfs_recovery_heartbeat). */
    nfs_recovery_heartbeat(&thread->shared->nfs4_recovery, thread->vfs_thread);

    /* Phase 5: piggyback grace-window expiry on the lease tick. */
    nfs_recovery_sweep_once(&thread->shared->nfs4_recovery, thread->vfs_thread);
} /* nfs_lease_sweep_once */

static void
nfs_lease_sweeper_fire(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct nfs_lease_sweeper *sweeper =
        container_of(timer, struct nfs_lease_sweeper, timer);

    nfs_lease_sweep_once(sweeper->thread);
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
