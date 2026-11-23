// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Allocator internals: the common wrapper and the backend interface.
 *
 * Every backend sits under the wrapper, which enforces the parts of the public
 * contract no backend should reimplement: the closed status set, max_pending,
 * ESHUTDOWN, and delivery of each result exactly once, never inline, on the
 * caller's home loop with no lock held.  This interface is internal and may
 * evolve; it is not an exported ABI.
 */

#include <stdbool.h>
#include <stdint.h>
#include <jansson.h>

#include "common/thread.h"   /* evpl_mutex_t, evpl_thread_id_t */
#include "evpl/evpl.h"
#include "clientid_allocator.h"
#include "clientid_format.h"

/* Allocator-private encoder.  Only allocator backends mint client IDs; no
 * other layer may build one from fields. */
static inline uint64_t
clientid_type1_encode(
    uint32_t node_id,
    uint32_t instance,
    uint32_t generation,
    uint32_t sequence)
{
    return ((uint64_t) CLIENTID_TYPE_SUPERVISOR << CLIENTID_TYPE_SHIFT) |
           ((uint64_t) 0 << CLIENTID_T1_VERSION_SHIFT) |
           ((uint64_t) node_id << CLIENTID_T1_NODE_SHIFT) |
           ((uint64_t) instance << CLIENTID_T1_INSTANCE_SHIFT) |
           ((uint64_t) generation << CLIENTID_T1_GENERATION_SHIFT) |
           (uint64_t) sequence;
} /* clientid_type1_encode */

struct clientid_req;

struct clientid_backend_ops {
    const char        *name;
    const uint8_t     *types;           /* Types this backend implements */
    int                ntypes;
    /* The backend may call clientid_allocator_complete() from a thread other
     * than the submitting one.  The wrapper then registers a doorbell on each
     * home loop so the result can be handed back. */
    bool               cross_thread;
    /* kwargs names whose values are secrets, redacted in logs (NULL-ended). */
    const char *const *secret_keys;
    int                (*supported)(
        uint8_t       type,
        const json_t *kwargs);
    int                (*create)(
        uint8_t       type,
        const json_t *kwargs,
        void        **state);
    /* Must lead to exactly one clientid_allocator_complete(req, ...). */
    void               (*start)(
        void                *state,
        struct clientid_req *req);
    void               (*destroy)(
        void *state);
};

/* Called by a backend, from any thread, exactly once per start(). */
SYMBOL_EXPORT void
clientid_allocator_complete(
    struct clientid_req *req,
    int                  status,
    uint64_t             clientid);

#ifdef CHIMERA_CLIENTID_TEST_BACKEND
/* Test hook: make the next `count` cross-thread hand-back signals fail with
 * `err`, to exercise the wrapper's retry and abort path.  Returns how many
 * injected failures of the previous setting were still unused. */
SYMBOL_EXPORT unsigned
clientid_allocator_test_fail_signals(
    struct clientid_allocator *allocator,
    unsigned                   count,
    int                        err);
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */

/* Local backend helpers, also used by the test backend. */
struct clientid_local;

int
clientid_local_supported(
    uint8_t       type,
    const json_t *kwargs);

int
clientid_local_create(
    uint8_t                 type,
    const json_t           *kwargs,
    struct clientid_local **out);

/* Mint the next ID.  Returns 0 or ENOSPC. */
int
clientid_local_mint(
    struct clientid_local *local,
    uint64_t              *id);

uint64_t
clientid_local_remaining(
    struct clientid_local *local);

void
clientid_local_destroy(
    struct clientid_local *local);

/* Test hook: move the next sequence (to exercise exhaustion and warnings). */
void
clientid_local_set_next_sequence(
    struct clientid_local *local,
    uint32_t               sequence);

SYMBOL_EXPORT void
clientid_allocator_local_set_next_sequence(
    struct clientid_allocator *allocator,
    uint32_t                   sequence);

extern const struct clientid_backend_ops clientid_local_ops;
#ifdef CHIMERA_CLIENTID_TEST_BACKEND
extern const struct clientid_backend_ops clientid_test_ops;
#endif /* ifdef CHIMERA_CLIENTID_TEST_BACKEND */
