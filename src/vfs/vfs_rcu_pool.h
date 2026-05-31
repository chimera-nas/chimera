// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Shared recycle pool for the fungible, hot-path RCU caches (attribute, name
 * and reverse-path-lookup).  Each such cache retires a displaced entry with
 * call_rcu; after a grace period the entry is recycled rather than freed.
 *
 * The pool decouples the producer (call_rcu reclaim workers, possibly one per
 * CPU) from the consumers (per-request inserts on the VFS worker threads):
 *
 *   - A single per-pool depot is a liburcu wait-free stack.  Retire pushes onto
 *     it with cds_wfs_push (wait-free, multi-producer -- no lock, so multiple
 *     per-CPU reclaim workers never contend).
 *   - Each VFS worker thread keeps a thread-local magazine (a plain LIFO with
 *     no atomics).  Alloc pops from the magazine; on a miss it refills a whole
 *     batch from the depot in one shot (cds_wfs_pop_all_blocking takes the
 *     stack's internal mutex once, amortized over the batch), capping the
 *     magazine so one thread cannot hoard the pool.  On a cold pool it falls
 *     back to calloc (a one-time cost; entries are recycled forever after).
 *
 * Entries are never returned to the heap during runtime -- they cycle through
 * depot -> magazine -> in-use -> (grace period) -> depot.
 */

#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <urcu.h>
#include <urcu/urcu-memb.h>
#include <urcu/wfstack.h>

#include "vfs/vfs.h" /* enum chimera_rcu_pool_id, struct chimera_rcu_magazine */

struct chimera_rcu_pool;

/*
 * Embedded as the FIRST member of every recyclable cache entry, so a
 * struct chimera_rcu_node * aliases the entry pointer.  `rcu` and `pool` are
 * live during the retire window; `wfs`/`mag_next` alias because an entry is on
 * the depot or in a magazine, never both.
 */
struct chimera_rcu_node {
    struct rcu_head          rcu;   /* deferred-free grace-period linkage */
    struct chimera_rcu_pool *pool;  /* depot this entry returns to */
    union {
        struct cds_wfs_node      wfs;      /* linked on the pool depot */
        struct chimera_rcu_node *mag_next; /* linked on a thread magazine */
    };
};

struct chimera_rcu_pool {
    struct cds_wfs_stack depot;
    size_t               entry_size;
    int                  id;
};

static inline void
chimera_rcu_pool_init(
    struct chimera_rcu_pool *pool,
    int                      id,
    size_t                   entry_size)
{
    cds_wfs_init(&pool->depot);
    pool->id         = id;
    pool->entry_size = entry_size;
} /* chimera_rcu_pool_init */

/*
 * call_rcu callback: a retired entry has cleared its grace period; return it to
 * its pool depot.  Wait-free; safe from any (per-CPU) reclaim worker.
 */
static inline void
chimera_rcu_pool_retire(struct rcu_head *head)
{
    struct chimera_rcu_node *node = caa_container_of(head, struct chimera_rcu_node, rcu);

    cds_wfs_node_init(&node->wfs);
    cds_wfs_push(&node->pool->depot, &node->wfs);
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
    struct cds_wfs_head     *batch;
    struct cds_wfs_node     *wn, *wn_safe;

    if (!mag->head) {
        /* Refill a batch from the depot under one internal-mutex acquisition;
         * keep up to the cap, push any surplus back. */
        batch = cds_wfs_pop_all_blocking(&pool->depot);
        if (batch) {
            cds_wfs_for_each_blocking_safe(batch, wn, wn_safe)
            {
                node = caa_container_of(wn, struct chimera_rcu_node, wfs);
                if (mag->count < CHIMERA_RCU_MAGAZINE_CAP) {
                    node->mag_next = mag->head;
                    mag->head      = node;
                    mag->count++;
                } else {
                    cds_wfs_node_init(&node->wfs);
                    cds_wfs_push(&pool->depot, &node->wfs);
                }
            }
        }
    }

    if (mag->head) {
        node      = mag->head;
        mag->head = node->mag_next;
        mag->count--;
    } else {
        node = calloc(1, pool->entry_size);
    }

    node->pool = pool;
    return node;
} /* chimera_rcu_pool_alloc */

/* Push every entry in a thread magazine back to its depot (at thread destroy). */
static inline void
chimera_rcu_magazine_drain(struct chimera_rcu_magazine *mag)
{
    struct chimera_rcu_node *node;

    while (mag->head) {
        node      = mag->head;
        mag->head = node->mag_next;
        cds_wfs_node_init(&node->wfs);
        cds_wfs_push(&node->pool->depot, &node->wfs);
    }
    mag->count = 0;
} /* chimera_rcu_magazine_drain */

/*
 * Free every entry on the depot and destroy it.  Call only after rcu_barrier()
 * (so all pending retires have landed) and after all thread magazines have been
 * drained.  In-use entries still held in cache slots are freed by the cache.
 */
static inline void
chimera_rcu_pool_destroy(struct chimera_rcu_pool *pool)
{
    struct cds_wfs_head     *batch;
    struct cds_wfs_node     *wn, *wn_safe;
    struct chimera_rcu_node *node;

    batch = cds_wfs_pop_all_blocking(&pool->depot);
    if (batch) {
        cds_wfs_for_each_blocking_safe(batch, wn, wn_safe)
        {
            node = caa_container_of(wn, struct chimera_rcu_node, wfs);
            free(node);
        }
    }
    cds_wfs_destroy(&pool->depot);
} /* chimera_rcu_pool_destroy */
