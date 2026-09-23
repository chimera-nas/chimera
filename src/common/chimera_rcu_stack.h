// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * The one concurrent container chimera takes from liburcu-cds: the wait-free
 * stack behind the recycle pool's depot stripes.  With urcu present this is
 * cds_wfs verbatim.  Without it, a mutex-guarded LIFO stands in -- which costs
 * little, because the pool already batches: a push happens once per retired
 * entry and a pop happens once per magazine refill, under a lock the depot
 * takes anyway.
 */

#include "common/chimera_rcu.h"

#ifdef CHIMERA_HAVE_URCU

#include <urcu/wfstack.h>

#define chimera_rcu_stack_node cds_wfs_node
#define chimera_rcu_stack      cds_wfs_stack
typedef struct cds_wfs_head *chimera_rcu_stack_batch;

#define chimera_rcu_stack_init(s)       cds_wfs_init(s)
#define chimera_rcu_stack_destroy(s)    cds_wfs_destroy(s)
#define chimera_rcu_stack_node_init(n)  cds_wfs_node_init(n)
#define chimera_rcu_stack_push(s, n)    cds_wfs_push(s, n)
#define chimera_rcu_stack_pop_lock(s)   cds_wfs_pop_lock(s)
#define chimera_rcu_stack_pop_locked(s) __cds_wfs_pop_blocking(s)
#define chimera_rcu_stack_pop_unlock(s) cds_wfs_pop_unlock(s)
#define chimera_rcu_stack_pop_all(s)    cds_wfs_pop_all_blocking(s)

#define chimera_rcu_stack_for_each_safe(batch, node, tmp) \
        cds_wfs_for_each_blocking_safe(batch, node, tmp)

#else /* !CHIMERA_HAVE_URCU */

#include "common/thread.h"
#include <stddef.h>

struct chimera_rcu_stack_node {
    struct chimera_rcu_stack_node *next;
};

struct chimera_rcu_stack {
    struct chimera_rcu_stack_node *head;
    evpl_mutex_t                   lock;
};

typedef struct chimera_rcu_stack_node *chimera_rcu_stack_batch;

static inline void
chimera_rcu_stack_init(struct chimera_rcu_stack *stack)
{
    stack->head = NULL;
    evpl_mutex_init(&stack->lock, NULL);
} /* chimera_rcu_stack_init */

static inline void
chimera_rcu_stack_destroy(struct chimera_rcu_stack *stack)
{
    evpl_mutex_destroy(&stack->lock);
} /* chimera_rcu_stack_destroy */

static inline void
chimera_rcu_stack_node_init(struct chimera_rcu_stack_node *node)
{
    node->next = NULL;
} /* chimera_rcu_stack_node_init */

static inline void
chimera_rcu_stack_push(
    struct chimera_rcu_stack      *stack,
    struct chimera_rcu_stack_node *node)
{
    evpl_mutex_lock(&stack->lock);
    node->next  = stack->head;
    stack->head = node;
    evpl_mutex_unlock(&stack->lock);
} /* chimera_rcu_stack_push */

/*
 * The pop side mirrors cds_wfs: the caller takes the pop lock once and then
 * pops a bounded run under it, so a magazine refill is one acquisition rather
 * than one per entry.
 */
static inline void
chimera_rcu_stack_pop_lock(struct chimera_rcu_stack *stack)
{
    evpl_mutex_lock(&stack->lock);
} /* chimera_rcu_stack_pop_lock */

static inline void
chimera_rcu_stack_pop_unlock(struct chimera_rcu_stack *stack)
{
    evpl_mutex_unlock(&stack->lock);
} /* chimera_rcu_stack_pop_unlock */

static inline struct chimera_rcu_stack_node *
chimera_rcu_stack_pop_locked(struct chimera_rcu_stack *stack)
{
    struct chimera_rcu_stack_node *node = stack->head;

    if (node) {
        stack->head = node->next;
    }

    return node;
} /* chimera_rcu_stack_pop_locked */

static inline struct chimera_rcu_stack_node *
chimera_rcu_stack_pop_all(struct chimera_rcu_stack *stack)
{
    struct chimera_rcu_stack_node *batch;

    evpl_mutex_lock(&stack->lock);
    batch       = stack->head;
    stack->head = NULL;
    evpl_mutex_unlock(&stack->lock);

    return batch;
} /* chimera_rcu_stack_pop_all */

#define chimera_rcu_stack_for_each_safe(batch, node, tmp)                 \
        for ((node) = (batch); (node) && ((tmp) = (node)->next, 1);           \
             (node) = (tmp))

#endif /* CHIMERA_HAVE_URCU */
