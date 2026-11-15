// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "server/inspect_mpsc.h"

#define PRODUCERS          4
#define ITEMS_PER_PRODUCER 10000
#define ITEM_COUNT         (PRODUCERS * ITEMS_PER_PRODUCER)

struct test_node {
    struct chimera_inspect_mpsc_node node;
    unsigned int                     id;
};

struct test_state {
    struct chimera_inspect_mpsc_queue queue;
    atomic_uint                       producers_done;
    atomic_uint                       consumed;
    atomic_uint                      *seen;
};
struct producer_arg {
    struct test_state *state;
    unsigned int       index;
};

static void
check(
    int         condition,
    const char *message)
{
    if (!condition) {
        fprintf(stderr, "inspection mpsc: %s\n", message);
        abort();
    }
} /* check */

static void *
producer(void *data)
{
    struct producer_arg *arg   = data;
    struct test_state   *state = arg->state;

    for (unsigned int i = 0; i < ITEMS_PER_PRODUCER; i++) {
        unsigned int      id   = arg->index * ITEMS_PER_PRODUCER + i;
        struct test_node *node = calloc(1, sizeof(*node));
        check(node != NULL, "allocate producer node");
        node->id = id;
        check(chimera_inspect_mpsc_push(&state->queue, &node->node), "concurrent producer push succeeds");
    }
    atomic_fetch_add_explicit(&state->producers_done, 1, memory_order_relaxed);
    return NULL;
} /* producer */

static void *
consumer(void *data)
{
    struct test_state *state = data;

    while (atomic_load_explicit(&state->consumed, memory_order_relaxed) < ITEM_COUNT) {
        struct chimera_inspect_mpsc_node *nodes = chimera_inspect_mpsc_drain(&state->queue);
        if (!nodes) {
            sched_yield();
            continue;
        }
        while (nodes) {
            struct test_node *node = (struct test_node *) nodes;
            nodes = nodes->next;
            check(node->id < ITEM_COUNT, "node id in range");
            check(atomic_fetch_add_explicit(&state->seen[node->id], 1, memory_order_relaxed) == 0,
                  "node delivered exactly once");
            atomic_fetch_add_explicit(&state->consumed, 1, memory_order_relaxed);
            free(node);
        }
    }
    return NULL;
} /* consumer */

int
main(void)
{
    struct chimera_inspect_mpsc_queue basic;
    struct test_node                  a = { .id = 1 }, b = { .id = 2 };

    chimera_inspect_mpsc_init(&basic);
    check(chimera_inspect_mpsc_push(&basic, &a.node), "first push succeeds");
    check(chimera_inspect_mpsc_push(&basic, &b.node), "second push succeeds");
    struct chimera_inspect_mpsc_node *nodes = chimera_inspect_mpsc_drain(&basic);
    check(nodes == &a.node && nodes->next == &b.node, "drain preserves FIFO order");
    check(chimera_inspect_mpsc_close(&basic) == NULL, "close empty queue");
    check(chimera_inspect_mpsc_is_closed(&basic), "closed state visible");
    check(!chimera_inspect_mpsc_push(&basic, &a.node), "push after close is rejected");

    struct test_state                 state = { 0 };
    state.seen = calloc(ITEM_COUNT, sizeof(*state.seen));
    check(state.seen != NULL, "allocate seen table");
    for (unsigned int i = 0; i < ITEM_COUNT; i++) {
        atomic_init(&state.seen[i], 0);
    }
    chimera_inspect_mpsc_init(&state.queue);
    atomic_init(&state.producers_done, 0);
    atomic_init(&state.consumed, 0);
    pthread_t           producers[PRODUCERS], consumer_thread;
    struct producer_arg args[PRODUCERS];
    check(pthread_create(&consumer_thread, NULL, consumer, &state) == 0, "start consumer");
    for (unsigned int i = 0; i < PRODUCERS; i++) {
        args[i] = (struct producer_arg) { .state = &state, .index = i };
        check(pthread_create(&producers[i], NULL, producer, &args[i]) == 0, "start producer");
    }
    for (unsigned int i = 0; i < PRODUCERS; i++) {
        pthread_join(producers[i], NULL);
    }
    pthread_join(consumer_thread, NULL);
    check(atomic_load_explicit(&state.producers_done, memory_order_relaxed) == PRODUCERS,
          "all producers completed");
    check(atomic_load_explicit(&state.consumed, memory_order_relaxed) == ITEM_COUNT,
          "all pushed nodes drained");
    check(chimera_inspect_mpsc_close(&state.queue) == NULL, "concurrent queue drained before close");
    free(state.seen);
    return 0;
} /* main */
