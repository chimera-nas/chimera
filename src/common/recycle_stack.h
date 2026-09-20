// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#ifdef _WIN32
#include "common/thread.h"

typedef struct chimera_stack_node {
    struct chimera_stack_node *next;
} chimera_stack_node;
typedef chimera_stack_node chimera_stack_batch;
typedef struct chimera_stack {
    evpl_mutex_t        lock;
    chimera_stack_node *head;
} chimera_stack;

static inline void
chimera_stack_init(chimera_stack *s)
{
    evpl_mutex_init(&s->lock, NULL);
    s->head = NULL;
} // chimera_stack_init
static inline void
chimera_stack_destroy(chimera_stack *s)
{
    evpl_mutex_destroy(&s->lock);
} // chimera_stack_destroy
static inline void
chimera_stack_node_init(chimera_stack_node *n)
{
    n->next = NULL;
} // chimera_stack_node_init
static inline void
chimera_stack_push(
    chimera_stack      *s,
    chimera_stack_node *n)
{
    evpl_mutex_lock(&s->lock);
    n->next = s->head;
    s->head = n;
    evpl_mutex_unlock(&s->lock);
} // chimera_stack_push
static inline void
chimera_stack_pop_lock(chimera_stack *s)
{
    evpl_mutex_lock(&s->lock);
} // chimera_stack_pop_lock
static inline void
chimera_stack_pop_unlock(chimera_stack *s)
{
    evpl_mutex_unlock(&s->lock);
} // chimera_stack_pop_unlock
/* Caller holds the consumer lock for the entire magazine refill. */
static inline chimera_stack_node *
chimera_stack_pop_locked(chimera_stack *s)
{
    chimera_stack_node *n = s->head;

    if (n) {
        s->head = n->next;
    }
    return n;
} // chimera_stack_pop_locked
static inline chimera_stack_batch *
chimera_stack_pop_all(chimera_stack *s)
{
    chimera_stack_node *head;

    evpl_mutex_lock(&s->lock);
    head    = s->head;
    s->head = NULL;
    evpl_mutex_unlock(&s->lock);
    return head;
} // chimera_stack_pop_all
#define chimera_stack_for_each_safe(batch, node, next_node) \
        for ((node) = (batch); (node) && (((next_node) = (node)->next), 1); (node) = (next_node))
#else // ifdef _WIN32
#include <urcu/wfstack.h>
typedef struct cds_wfs_node chimera_stack_node;
typedef struct cds_wfs_stack chimera_stack;
typedef struct cds_wfs_head chimera_stack_batch;
#define chimera_stack_init          cds_wfs_init
#define chimera_stack_destroy       cds_wfs_destroy
#define chimera_stack_node_init     cds_wfs_node_init
#define chimera_stack_push          cds_wfs_push
#define chimera_stack_pop_lock      cds_wfs_pop_lock
#define chimera_stack_pop_unlock    cds_wfs_pop_unlock
#define chimera_stack_pop_locked    __cds_wfs_pop_blocking
#define chimera_stack_pop_all       cds_wfs_pop_all_blocking
#define chimera_stack_for_each_safe cds_wfs_for_each_blocking_safe
#endif // ifdef _WIN32
