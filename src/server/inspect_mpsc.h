// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include <stdatomic.h>
#include <stdint.h>

struct chimera_inspect_mpsc_node {
    struct chimera_inspect_mpsc_node *next;
};

struct chimera_inspect_mpsc_queue {
    _Atomic(struct chimera_inspect_mpsc_node *) head;
};

#define CHIMERA_INSPECT_MPSC_CLOSED ((struct chimera_inspect_mpsc_node *) (uintptr_t) 1)

static inline void
chimera_inspect_mpsc_init(struct chimera_inspect_mpsc_queue *queue)
{
    atomic_init(&queue->head, NULL);
} // chimera_inspect_mpsc_init

static inline int
chimera_inspect_mpsc_push(
    struct chimera_inspect_mpsc_queue *queue,
    struct chimera_inspect_mpsc_node  *node)
{
    struct chimera_inspect_mpsc_node *head = atomic_load_explicit(&queue->head, memory_order_relaxed);

    do {
        if (head == CHIMERA_INSPECT_MPSC_CLOSED) {
            return 0;
        }
        node->next = head;
    } while (!atomic_compare_exchange_weak_explicit(&queue->head, &head, node,
                                                    memory_order_release, memory_order_relaxed));
    return 1;
} // chimera_inspect_mpsc_push

static inline struct chimera_inspect_mpsc_node *
chimera_inspect_mpsc_drain(struct chimera_inspect_mpsc_queue *queue)
{
    struct chimera_inspect_mpsc_node *nodes = atomic_exchange_explicit(&queue->head, NULL, memory_order_acquire);

    if (nodes == CHIMERA_INSPECT_MPSC_CLOSED) {
        return NULL;
    }
    struct chimera_inspect_mpsc_node *reverse = NULL;
    while (nodes) {
        struct chimera_inspect_mpsc_node *next = nodes->next;
        nodes->next = reverse;
        reverse     = nodes;
        nodes       = next;
    }
    return reverse;
} // chimera_inspect_mpsc_drain

static inline struct chimera_inspect_mpsc_node *
chimera_inspect_mpsc_close(struct chimera_inspect_mpsc_queue *queue)
{
    struct chimera_inspect_mpsc_node *nodes = atomic_exchange_explicit(&queue->head,
                                                                       CHIMERA_INSPECT_MPSC_CLOSED,
                                                                       memory_order_acq_rel);
    struct chimera_inspect_mpsc_node *reverse = NULL;

    if (nodes == CHIMERA_INSPECT_MPSC_CLOSED) {
        return NULL;
    }
    while (nodes) {
        struct chimera_inspect_mpsc_node *next = nodes->next;
        nodes->next = reverse;
        reverse     = nodes;
        nodes       = next;
    }
    return reverse;
} // chimera_inspect_mpsc_close

static inline int
chimera_inspect_mpsc_is_closed(struct chimera_inspect_mpsc_queue *queue)
{
    return atomic_load_explicit(&queue->head, memory_order_relaxed) == CHIMERA_INSPECT_MPSC_CLOSED;
} // chimera_inspect_mpsc_is_closed
