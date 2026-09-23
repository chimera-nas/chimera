// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Shared recycle pool for the fungible, hot-path RCU caches (attribute, name
 * and reverse-path-lookup).  Each such cache hands a displaced entry to its
 * shard's RCU domain; once the grace period is over the entry is recycled
 * rather than freed.
 *
 * The pool decouples the producer -- the liburcu call_rcu reclaim workers, one
 * per CPU, or in a build without liburcu the worker thread that displaced the
 * entry, which by then already holds the proof that no reader can reach it --
 * from the consumers (per-request inserts on the VFS worker threads):
 *
 *   - The depot is striped, one stack per stripe on its own
 *     cache line.  A stripe is owned by a worker thread (assigned once at thread
 *     init), NOT by a CPU: each entry records the stripe of the thread that
 *     allocated it (home_stripe), and the grace-period reclaim worker returns it
 *     to that stripe.  So an entry's whole recycle lifecycle stays on its owner
 *     thread's stripe even though the worker migrates across CPUs and the
 *     reclaim worker runs on an unrelated CPU -- contention-free in the common
 *     case, and crucially no stranding.  Retire pushes with
 *     chimera_rcu_stack_push (wait-free under urcu, a short mutex without it).
 *   - Each VFS worker thread keeps a thread-local magazine (a plain LIFO, no
 *     atomics).  Alloc pops from the magazine; on a miss it refills up to
 *     CHIMERA_RCU_MAGAZINE_CAP entries from this thread's depot stripe under a
 *     single pop-lock acquisition (bounded work -- it never walks the whole
 *     stack).  On a cold stripe it falls back to calloc (one-time; entries are
 *     recycled forever after).
 *
 * Entries are never returned to the heap during runtime -- they cycle through
 * depot -> magazine -> in-use -> (grace period) -> depot.
 */

#include "common/compiler.h"
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else // ifdef _WIN32
#include <unistd.h>
#endif // ifdef _WIN32
#include "common/chimera_rcu.h"
#include "common/chimera_rcu_stack.h"

#include "vfs/vfs.h" /* enum chimera_rcu_pool_id, struct chimera_rcu_magazine */

struct chimera_rcu_pool;

/*
 * Embedded as the FIRST member of every recyclable cache entry, so a
 * struct chimera_rcu_node * aliases the entry pointer.  `rcu` and `pool` are
 * live during the retire window; `wfs`/`mag_next` alias because an entry is on
 * a depot stripe or in a magazine, never both.
 */
struct chimera_rcu_node {
    chimera_rcu_head         rcu;   /* deferred-free grace-period linkage */
    struct chimera_rcu_pool *pool;  /* pool this entry returns to */
    uint32_t                 home_stripe; /* depot stripe of the allocating thread */
    union {
        struct chimera_rcu_stack_node wfs; /* linked on a depot stripe */
        struct chimera_rcu_node      *mag_next; /* linked on a thread magazine */
    };
};

/* One depot stripe per slot (the count is sized to the CPU count, but a stripe
 * is owned by a worker thread, see header comment), each on its own cache line
 * to avoid false sharing of the adjacent pop locks. */
struct CHIMERA_ALIGNED(64) chimera_rcu_depot {
    struct chimera_rcu_stack stack;
};

struct chimera_rcu_pool {
    struct chimera_rcu_depot *depots; /* [n_stripes] */
    uint32_t                  stripe_mask;
    uint32_t                  n_stripes;
    size_t                    entry_size;
    int                       id;
};

static inline uint32_t
chimera_rcu_round_pow2(uint32_t v)
{
    if (v < 1) {
        v = 1;
    }
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
} /* chimera_rcu_round_pow2 */

static inline void
chimera_rcu_pool_init(
    struct chimera_rcu_pool *pool,
    int                      id,
    size_t                   entry_size)
{
    long     ncpu = chimera_cpu_count();
    uint32_t n    = chimera_rcu_round_pow2(ncpu > 0 ? (uint32_t) ncpu : 1);
    uint32_t i;
    size_t   bytes = (size_t) n * sizeof(struct chimera_rcu_depot);

    pool->n_stripes   = n;
    pool->stripe_mask = n - 1;
    pool->entry_size  = entry_size;
    pool->id          = id;

    pool->depots = chimera_aligned_alloc(64, bytes);
    memset(pool->depots, 0, bytes);

    for (i = 0; i < n; i++) {
        chimera_rcu_stack_init(&pool->depots[i].stack);
    }
} /* chimera_rcu_pool_init */

/*
 * call_rcu callback: a retired entry has cleared its grace period; return it to
 * the depot stripe of the thread that allocated it (home_stripe), NOT the CPU
 * this reclaim worker happens to run on.  This is the crux: the per-CPU
 * reclaim worker runs on an arbitrary CPU relative to the (unpinned, migrating)
 * worker thread that will re-allocate the entry, so keying the stripe off the
 * current CPU stranded entries in stripes the consumer never popped.  Routing
 * by home_stripe returns the entry to the exact stripe its owner refills from.
 * Wait-free; safe from any reclaim worker.
 */
static inline void
chimera_rcu_pool_retire(chimera_rcu_head *head)
{
    struct chimera_rcu_node *node = container_of(head, struct chimera_rcu_node, rcu);

    chimera_rcu_stack_node_init(&node->wfs);
    chimera_rcu_stack_push(&node->pool->depots[node->home_stripe].stack, &node->wfs);
} /* chimera_rcu_pool_retire */

/*
 * Obtain a recycled (or freshly calloc'd) entry for `pool`, using the calling
 * thread's magazine.  Returns a node whose entry fields the caller overwrites.
 */
static inline struct chimera_rcu_node *
chimera_rcu_pool_alloc(
    struct chimera_rcu_magazine *mag,
    struct chimera_rcu_pool     *pool)
{
    struct chimera_rcu_node *node;
    uint32_t                 stripe = mag->stripe & pool->stripe_mask;

    if (!mag->head) {
        /* Refill up to the cap from this THREAD's stable stripe under one
         * pop-lock acquisition.  Bounded work -- we never walk the whole stack. */
        struct chimera_rcu_stack      *depot = &pool->depots[stripe].stack;
        struct chimera_rcu_stack_node *wn;

        chimera_rcu_stack_pop_lock(depot);
        while (mag->count < CHIMERA_RCU_MAGAZINE_CAP) {
            wn = chimera_rcu_stack_pop_locked(depot);
            if (!wn) {
                break;
            }
            node           = container_of(wn, struct chimera_rcu_node, wfs);
            node->mag_next = mag->head;
            mag->head      = node;
            mag->count++;
        }
        chimera_rcu_stack_pop_unlock(depot);
    }

    if (mag->head) {
        node      = mag->head;
        mag->head = node->mag_next;
        mag->count--;
    } else {
        node = calloc(1, pool->entry_size);
    }

    node->pool        = pool;
    node->home_stripe = stripe; /* retire returns the entry to this thread's stripe */
    return node;
} /* chimera_rcu_pool_alloc */

/* Push every entry in a thread magazine back to a depot (at thread destroy). */
static inline void
chimera_rcu_magazine_drain(struct chimera_rcu_magazine *mag)
{
    struct chimera_rcu_node *node;

    while (mag->head) {
        node      = mag->head;
        mag->head = node->mag_next;
        chimera_rcu_stack_node_init(&node->wfs);
        chimera_rcu_stack_push(&node->pool->depots[node->home_stripe].stack, &node->wfs);
    }
    mag->count = 0;
} /* chimera_rcu_magazine_drain */

/*
 * Free every entry on the depot and destroy it.  Call only after
 * chimera_rcu_barrier()
 * (so all pending retires have landed) and after all thread magazines have been
 * drained.  In-use entries still held in cache slots are freed by the cache.
 */
static inline void
chimera_rcu_pool_destroy(struct chimera_rcu_pool *pool)
{
    chimera_rcu_stack_batch        batch;
    struct chimera_rcu_stack_node *wn, *wn_safe;
    struct chimera_rcu_node       *node;
    uint32_t                       i;

    for (i = 0; i < pool->n_stripes; i++) {
        batch = chimera_rcu_stack_pop_all(&pool->depots[i].stack);
        if (batch) {
            chimera_rcu_stack_for_each_safe(batch, wn, wn_safe)
            {
                node = container_of(wn, struct chimera_rcu_node, wfs);
                free(node);
            }
        }
        chimera_rcu_stack_destroy(&pool->depots[i].stack);
    }

    chimera_aligned_free(pool->depots);
} /* chimera_rcu_pool_destroy */
