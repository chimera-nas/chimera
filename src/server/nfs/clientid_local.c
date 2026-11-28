// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The "local" client ID allocator backend: in-memory, one per process.
 *
 * Type 1 / Version 0 (supervisor grant):
 *   kwargs { version: 0, node_id: 1..1023, instance: 0..15,
 *            generation: 1..1048575 }
 *   The supervisor owns Node ID, Instance and Generation: it must hand out a
 *   unique (Node ID, Instance) per running process and a strictly increasing,
 *   durably recorded Generation on every start.  This backend checks ranges
 *   only, never advances Generation, and fails with ENOSPC once Sequence
 *   0xFFFFFF has been issued; it warns when 90% and 99% of the sequence space
 *   is used.
 */

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "common/misc.h"
#include "clientid_allocator_internal.h"
#include "clientid_format.h"
#include "nfs_internal.h"

struct clientid_local {
    evpl_mutex_t lock;
    uint8_t      type;
    uint32_t     node;
    uint32_t     instance;
    uint32_t     generation;
    uint32_t     sequence;      /* next to issue */
    uint32_t     sequence_first;
    uint32_t     sequence_max;
    uint32_t     warn90;
    uint32_t     warn99;
    bool         warned90;
    bool         warned99;
    bool         exhausted;
};

static const uint8_t clientid_local_types[] = { CLIENTID_TYPE_SUPERVISOR };

static int
clientid_local_check_keys(
    const json_t      *kwargs,
    const char *const *allowed)
{
    const char *key;
    json_t     *value;

    json_object_foreach((json_t *) kwargs, key, value)
    {
        bool known = false;

        (void) value;
        for (int i = 0; allowed[i]; i++) {
            if (strcmp(key, allowed[i]) == 0) {
                known = true;
                break;
            }
        }
        if (!known) {
            chimera_nfs_error("clientid allocator: unknown key \"%s\" in \"kwargs\"", key);
            return EINVAL;
        }
    }
    return 0;
} /* clientid_local_check_keys */

static int
clientid_local_get(
    const json_t *kwargs,
    const char   *key,
    bool          required,
    uint64_t      min,
    uint64_t      max,
    uint64_t     *out)
{
    json_t    *v = json_object_get(kwargs, key);
    json_int_t i;

    if (!v) {
        if (required) {
            chimera_nfs_error("clientid allocator: kwargs \"%s\" is required", key);
            return EINVAL;
        }
        return ENOENT;
    }
    if (!json_is_integer(v)) {
        chimera_nfs_error("clientid allocator: kwargs \"%s\" must be an integer", key);
        return EINVAL;
    }
    i = json_integer_value(v);
    if (i < 0 || (uint64_t) i < min || (uint64_t) i > max) {
        chimera_nfs_error("clientid allocator: kwargs \"%s\" = %lld is out of range "
                          "[%llu, %llu]", key, (long long) i,
                          (unsigned long long) min, (unsigned long long) max);
        return EINVAL;
    }
    *out = (uint64_t) i;
    return 0;
} /* clientid_local_get */

static int
clientid_local_version(const json_t *kwargs)
{
    uint64_t v;
    int      rc = clientid_local_get(kwargs, "version", true, 0, 3, &v);

    if (rc) {
        return rc;
    }
    if (v != 0) {
        chimera_nfs_error("clientid allocator: layout version %llu is reserved",
                          (unsigned long long) v);
        return ENOTSUP;
    }
    return 0;
} /* clientid_local_version */

int
clientid_local_supported(
    uint8_t       type,
    const json_t *kwargs)
{
    static const char *const t1_keys[] = { "version", "node_id", "instance", "generation", NULL };
    uint64_t                 v;
    int                      rc;

    if (type == CLIENTID_TYPE_SUPERVISOR) {
        rc = clientid_local_check_keys(kwargs, t1_keys);
        if (!rc) {
            rc = clientid_local_version(kwargs);
        }
        if (!rc) {
            rc = clientid_local_get(kwargs, "node_id", true, CLIENTID_T1_NODE_MIN,
                                    CLIENTID_T1_NODE_MAX, &v);
        }
        if (!rc) {
            rc = clientid_local_get(kwargs, "instance", true, 0,
                                    CLIENTID_T1_INSTANCE_MAX, &v);
        }
        if (!rc) {
            rc = clientid_local_get(kwargs, "generation", true,
                                    CLIENTID_T1_GENERATION_MIN,
                                    CLIENTID_T1_GENERATION_MAX, &v);
        }
        return rc;
    }

    return ENOTSUP;
} /* clientid_local_supported */

static void
clientid_local_set_thresholds(struct clientid_local *l)
{
    uint64_t span = (uint64_t) l->sequence_max - l->sequence_first + 1;

    l->warn90   = l->sequence_first + (uint32_t) (span * 90 / 100);
    l->warn99   = l->sequence_first + (uint32_t) (span * 99 / 100);
    l->warned90 = l->sequence >= l->warn90;
    l->warned99 = l->sequence >= l->warn99;
} /* clientid_local_set_thresholds */

int
clientid_local_create(
    uint8_t                 type,
    const json_t           *kwargs,
    struct clientid_local **out)
{
    struct clientid_local *l;
    uint64_t               node = 0, instance = 0, generation = 0;
    int                    rc;

    rc = clientid_local_supported(type, kwargs);
    if (rc) {
        return rc;
    }

    l = calloc(1, sizeof(*l));
    if (!l) {
        return ENOMEM;
    }
    l->type = type;

    if (type == CLIENTID_TYPE_SUPERVISOR) {
        /* supported() has validated these; check again rather than read an
         * unset value if that ever changes. */
        rc = clientid_local_get(kwargs, "node_id", true, CLIENTID_T1_NODE_MIN,
                                CLIENTID_T1_NODE_MAX, &node);
        if (!rc) {
            rc = clientid_local_get(kwargs, "instance", true, 0,
                                    CLIENTID_T1_INSTANCE_MAX, &instance);
        }
        if (!rc) {
            rc = clientid_local_get(kwargs, "generation", true,
                                    CLIENTID_T1_GENERATION_MIN,
                                    CLIENTID_T1_GENERATION_MAX, &generation);
        }
        if (rc) {
            free(l);
            return rc;
        }
        l->node           = (uint32_t) node;
        l->instance       = (uint32_t) instance;
        l->generation     = (uint32_t) generation;
        l->sequence_first = CLIENTID_T1_SEQUENCE_MIN;
        l->sequence_max   = CLIENTID_T1_SEQUENCE_MAX;
        chimera_nfs_info("NFSv4 client IDs: Type 1 (supervisor grant) node %u instance %u "
                         "generation %u; "
                         "uniqueness across restarts and nodes relies on the supervisor",
                         l->node, l->instance, l->generation);
    }

    l->sequence = l->sequence_first;
    clientid_local_set_thresholds(l);
    evpl_mutex_init(&l->lock, NULL);

    *out = l;
    return 0;
} /* clientid_local_create */

int
clientid_local_mint(
    struct clientid_local *l,
    uint64_t              *id)
{
    uint32_t seq;
    bool     w90 = false, w99 = false, last = false;

    evpl_mutex_lock(&l->lock);
    if (l->exhausted) {
        evpl_mutex_unlock(&l->lock);
        return ENOSPC;
    }

    seq = l->sequence;
    if (seq == l->sequence_max) {
        l->exhausted = true;  /* last ID issued; Generation is never advanced */
        last         = true;
    } else {
        l->sequence++;
    }
    if (!l->warned90 && seq >= l->warn90) {
        l->warned90 = true;
        w90         = true;
    }
    if (!l->warned99 && seq >= l->warn99) {
        l->warned99 = true;
        w99         = true;
    }

    *id = clientid_type1_encode(l->node, l->instance, l->generation, seq);
    evpl_mutex_unlock(&l->lock);

    if (w90) {
        chimera_nfs_error("WARNING: 90%% of this run's NFSv4 client ID space (Type %u) "
                          "is used; new mounts will fail with NFS4ERR_SERVERFAULT once "
                          "it runs out, until Chimera restarts", l->type);
    }
    if (w99) {
        chimera_nfs_error("WARNING: 99%% of this run's NFSv4 client ID space (Type %u) "
                          "is used; restart Chimera soon", l->type);
    }
    if (last) {
        chimera_nfs_error("NFSv4 client ID space (Type %u) exhausted: new SETCLIENTID "
                          "and EXCHANGE_ID fail with NFS4ERR_SERVERFAULT until Chimera "
                          "restarts; existing clients are unaffected", l->type);
    }
    return 0;
} /* clientid_local_mint */

uint64_t
clientid_local_remaining(struct clientid_local *l)
{
    uint64_t r;

    evpl_mutex_lock(&l->lock);
    r = l->exhausted ? 0 : (uint64_t) l->sequence_max - l->sequence + 1;
    evpl_mutex_unlock(&l->lock);
    return r;
} /* clientid_local_remaining */

void
clientid_local_set_next_sequence(
    struct clientid_local *l,
    uint32_t               sequence)
{
    evpl_mutex_lock(&l->lock);
    l->sequence  = sequence;
    l->exhausted = false;
    l->warned90  = false;
    l->warned99  = false;
    evpl_mutex_unlock(&l->lock);
} /* clientid_local_set_next_sequence */

void
clientid_local_destroy(struct clientid_local *l)
{
    if (!l) {
        return;
    }
    evpl_mutex_destroy(&l->lock);
    free(l);
} /* clientid_local_destroy */

/* ------------------------------------------------------------------ *
*  backend ops                                                       *
* ------------------------------------------------------------------ */

static int
clientid_local_ops_create(
    uint8_t       type,
    const json_t *kwargs,
    void        **state)
{
    return clientid_local_create(type, kwargs, (struct clientid_local **) state);
} /* clientid_local_ops_create */

static void
clientid_local_ops_start(
    void                *state,
    struct clientid_req *req)
{
    uint64_t id = 0;
    int      rc = clientid_local_mint(state, &id);

    /* Completes on the submitting thread; the wrapper still defers the
     * caller's callback to the next pass of its loop. */
    clientid_allocator_complete(req, rc, id);
} /* clientid_local_ops_start */

static void
clientid_local_ops_destroy(void *state)
{
    clientid_local_destroy(state);
} /* clientid_local_ops_destroy */

const struct clientid_backend_ops clientid_local_ops = {
    .name         = "local",
    .types        = clientid_local_types,
    .ntypes       = (int) (sizeof(clientid_local_types) / sizeof(clientid_local_types[0])),
    .cross_thread = false,
    .secret_keys  = NULL,
    .supported    = clientid_local_supported,
    .create       = clientid_local_ops_create,
    .start        = clientid_local_ops_start,
    .destroy      = clientid_local_ops_destroy,
};
