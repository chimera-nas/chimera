// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Keep allocations pending deliberately: no journal thread can accidentally
 * make a committed-only free-space counter pass this test. Four owners with
 * pre-existing claims compete for two blocks of data headroom. */
#ifdef NDEBUG
#undef NDEBUG
#endif /* ifdef NDEBUG */
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include "space_map.h"
#include "common/logging.h"

#define OWNERS 4

struct gate {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    int             ready;
};

struct owner {
    struct gate          *gate;
    struct sm_reservation claim;
    uint64_t              floor;
    uint64_t              offset;
    uint32_t              device;
    int                   status;
};

static void *
allocate(void *arg)
{
    struct owner *owner = arg;
    struct gate  *gate  = owner->gate;

    pthread_mutex_lock(&gate->lock);
    if (++gate->ready == OWNERS) {
        pthread_cond_broadcast(&gate->cond);
    }
    while (gate->ready < OWNERS) {
        pthread_cond_wait(&gate->cond, &gate->lock);
    }
    pthread_mutex_unlock(&gate->lock);
    owner->status = space_map_bump_alloc(&owner->claim, NULL, SM_BLOCK_SIZE,
                                         owner->floor, &owner->device, &owner->offset);
    return NULL;
} /* allocate */

int
main(void)
{
    struct sm_device_cfg  cfg = { .size = 128ULL << 20, .role = SM_DEV_LOCAL };
    struct space_map     *sm;
    struct owner          owners[OWNERS] = { 0 };
    struct sm_reservation metadata       = { 0 };
    struct gate           gate           = { .lock = PTHREAD_MUTEX_INITIALIZER,
                                             .cond = PTHREAD_COND_INITIALIZER };
    pthread_t             threads[OWNERS];
    uint64_t              initial, offset, available;
    uint32_t              device;
    int                   successes = 0;

    chimera_log_disable();
    sm      = space_map_create(&cfg, 1, 4ULL << 20);
    initial = sm->available_bytes;
    assert(initial == space_map_free_bytes(sm));
    for (int i = 0; i < OWNERS; i++) {
        owners[i].gate  = &gate;
        owners[i].floor = initial - 2 * SM_BLOCK_SIZE;
        assert(space_map_reserve_chunk(sm, &owners[i].claim, SM_DEV_LOCAL,
                                       SM_BLOCK_SIZE, SM_RESERVATION_CHUNK, i) == 0);
    }
    /* A speculative claim is not a data allocation. */
    assert(sm->available_bytes == initial);
    for (int i = 0; i < OWNERS; i++) {
        assert(pthread_create(&threads[i], NULL, allocate, &owners[i]) == 0);
    }
    for (int i = 0; i < OWNERS; i++) {
        assert(pthread_join(threads[i], NULL) == 0);
        assert(owners[i].status == 0 || owners[i].status == -1);
        successes += owners[i].status == 0;
    }
    assert(successes == 2);
    assert(sm->available_bytes == initial - 2 * SM_BLOCK_SIZE);
    assert(space_map_free_bytes(sm) == initial);

    /* Metadata can use the reserve, even while those data allocations have
     * not retired. Retiring an allocation must not charge it twice. */
    assert(space_map_reservation_alloc(sm, &metadata, NULL, SM_DEV_LOCAL,
                                       SM_BLOCK_SIZE, SM_RESERVATION_CHUNK, 0, 0,
                                       &device, &offset) == 0);
    available = initial - 3 * SM_BLOCK_SIZE;
    assert(sm->available_bytes == available);
    for (int i = 0; i < OWNERS; i++) {
        if (owners[i].status == 0) {
            space_map_alloc_apply(sm, owners[i].device, owners[i].offset, SM_BLOCK_SIZE);
        }
    }
    space_map_alloc_apply(sm, device, offset, SM_BLOCK_SIZE);
    assert(sm->available_bytes == available);
    assert(space_map_free_bytes(sm) == available);

    for (int i = 0; i < OWNERS; i++) {
        if (owners[i].status == 0) {
            space_map_free_apply(sm, owners[i].device, owners[i].offset, SM_BLOCK_SIZE);
        }
        space_map_release_reservation(sm, &owners[i].claim);
    }
    space_map_free_apply(sm, device, offset, SM_BLOCK_SIZE);
    assert(sm->available_bytes == initial);

    /* Losing a spatial claim after admission refunds the charge. */
    assert(space_map_bump_alloc(&metadata, NULL, 2 * SM_RESERVATION_CHUNK,
                                0, &device, &offset) == 1);
    assert(sm->available_bytes == initial);
    assert(space_map_bump_alloc(&metadata, NULL, SM_BLOCK_SIZE,
                                0, &device, &offset) == 0);
    assert(sm->available_bytes == initial - SM_BLOCK_SIZE);
    space_map_alloc_discard(sm, device, offset, SM_BLOCK_SIZE);
    assert(sm->available_bytes == initial);
    assert(space_map_free_bytes(sm) == initial);
    space_map_release_reservation(sm, &metadata);
    space_map_destroy(sm);
    pthread_cond_destroy(&gate.cond);
    pthread_mutex_destroy(&gate.lock);
    puts("PASS: pending allocations, concurrent admission, metadata reserve, commit and abort");
    return 0;
} /* main */
