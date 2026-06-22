// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <strings.h>
#include <jansson.h>

#include "evpl/evpl.h"
#include "common/tcp_flavor.h"

/*
 * Read the TCP transport flavor from the top-level "common" section.  This is
 * the shared setting honored by both the server (listen sockets) and the
 * client (outbound connections).  Defaults to CHIMERA_TCP_FLAVOR_PLAIN when
 * absent or unrecognized.  `root` is the parsed top-level config (may be NULL).
 */
static inline enum chimera_tcp_flavor
chimera_common_tcp_flavor(json_t *root)
{
    json_t *common, *val;
    const char *s;

    if (!root) {
        return CHIMERA_TCP_FLAVOR_PLAIN;
    }

    common = json_object_get(root, "common");
    if (!json_is_object(common)) {
        return CHIMERA_TCP_FLAVOR_PLAIN;
    }

    val = json_object_get(common, "tcp_flavor");
    if (!json_is_string(val)) {
        return CHIMERA_TCP_FLAVOR_PLAIN;
    }

    s = json_string_value(val);
    if (strcasecmp(s, "xlio") == 0) {
        return CHIMERA_TCP_FLAVOR_XLIO;
    } else if (strcasecmp(s, "io_uring") == 0) {
        return CHIMERA_TCP_FLAVOR_IO_URING;
    }

    return CHIMERA_TCP_FLAVOR_PLAIN;
} /* chimera_common_tcp_flavor */

/*
 * Parse a size from a JSON value.  A bare integer is taken as a byte count; a
 * string may carry a single K/M/G suffix (1024-based, optional trailing
 * "iB"/"B"), e.g. 2097152 or "2M" or "1G".  Returns 0 and stores the byte
 * count on success, -1 if the value is not a usable size.
 */
static inline int
chimera_parse_size(
    const json_t *val,
    uint64_t     *out)
{
    if (json_is_integer(val)) {
        json_int_t v = json_integer_value(val);

        if (v < 0) {
            return -1;
        }
        *out = (uint64_t) v;
        return 0;
    }

    if (json_is_string(val)) {
        const char        *s    = json_string_value(val);
        char              *end  = NULL;
        uint64_t           mult = 1;
        unsigned long long n    = strtoull(s, &end, 0);

        if (end == s) {
            return -1;
        }

        while (*end == ' ') {
            end++;
        }

        if (*end) {
            switch (*end) {
                case 'k': case 'K': mult = 1024ULL; break;
                case 'm': case 'M': mult = 1024ULL * 1024; break;
                case 'g': case 'G': mult = 1024ULL * 1024 * 1024; break;
                default: return -1;
            } /* switch */
            end++;
            if (*end == 'i') {
                end++;
            }
            if (*end == 'B' || *end == 'b') {
                end++;
            }
            if (*end != '\0') {
                return -1;
            }
        }

        *out = (uint64_t) n * mult;
        return 0;
    }

    return -1;
} /* chimera_parse_size */

/*
 * Apply the shared "common" config section -- parsed identically by the server
 * and the client -- onto an evpl global config, before evpl_init().  Recognised
 * keys: huge_pages (bool), huge_page_size (size), slab_size (size),
 * preallocate_slabs/threads (int), rdmacm_tos (int, RoCEv2 ToS byte =
 * DSCP << 2; e.g. 104 for DSCP 26).  A missing
 * "common" section, missing keys, or malformed values leave the corresponding
 * evpl defaults untouched.  `root` is the parsed top-level config object (may be
 * NULL).
 */
static inline void
chimera_apply_common_config(
    json_t                    *root,
    struct evpl_global_config *cfg)
{
    json_t  *common, *val;
    uint64_t size;

    /* Only enable XLIO in libevpl when the common tcp_flavor selects it;
     * libevpl otherwise spins up XLIO polling in every worker thread. Done
     * unconditionally (chimera_common_tcp_flavor handles a missing section). */
    evpl_global_config_set_xlio_enabled(cfg,
                                        chimera_common_tcp_flavor(root) == CHIMERA_TCP_FLAVOR_XLIO);

    if (!root) {
        return;
    }

    common = json_object_get(root, "common");
    if (!json_is_object(common)) {
        return;
    }

    val = json_object_get(common, "huge_pages");
    if (json_is_boolean(val)) {
        evpl_global_config_set_huge_pages(cfg, json_boolean_value(val));
    } else if (json_is_integer(val)) {
        evpl_global_config_set_huge_pages(cfg, json_integer_value(val) != 0);
    }

    val = json_object_get(common, "huge_page_size");
    if (val && chimera_parse_size(val, &size) == 0) {
        evpl_global_config_set_huge_page_size(cfg, size);
    }

    val = json_object_get(common, "slab_size");
    if (val && chimera_parse_size(val, &size) == 0) {
        evpl_global_config_set_slab_size(cfg, size);
    }

    val = json_object_get(common, "buffer_size");
    if (val && chimera_parse_size(val, &size) == 0) {
        evpl_global_config_set_buffer_size(cfg, size);
    }

    val = json_object_get(common, "preallocate_slabs");
    if (json_is_integer(val)) {
        evpl_global_config_set_preallocate_slabs(cfg, json_integer_value(val));
    }

    val = json_object_get(common, "preallocate_threads");
    if (json_is_integer(val)) {
        evpl_global_config_set_preallocate_threads(cfg, json_integer_value(val));
    }

    /* RoCEv2 traffic class: stamp this ToS byte on every RDMA QP so the fabric's
     * lossless priority (PFC) actually carries chimera traffic.  ToS = DSCP << 2,
     * so DSCP 26 -> 104.  Without it QPs default to ToS 0 (DSCP 0) and land in
     * the switch's default, lossy class regardless of PFC config. */
    val = json_object_get(common, "rdmacm_tos");
    if (json_is_integer(val)) {
        evpl_global_config_set_rdmacm_tos(cfg, (uint8_t) json_integer_value(val));
    }
} /* chimera_apply_common_config */

/*
 * Delegation-pool settings parsed from the shared top-level "common" section.
 * These are VFS-level parameters (the sync/async delegation thread pools live in
 * the VFS core), so they are honored identically by the server (daemon) and the
 * client (fio engine, client library).  Each field defaults to -1, meaning "not
 * set in the common section" -- the caller then leaves its own default (or a
 * value parsed from a legacy server/config section) untouched.
 */
struct chimera_common_delegation {
    int sync_delegation;            /* 0/1, or -1 if unset */
    int sync_delegation_threads;    /* >=0, or -1 if unset */
    int async_delegation;           /* 0/1, or -1 if unset */
    int async_delegation_threads;   /* >=0, or -1 if unset */
};

/*
 * Populate `out` from the "common" section's delegation keys (sync_delegation,
 * sync_delegation_threads, async_delegation, async_delegation_threads).  A
 * missing section or key leaves the corresponding field at -1.  `root` is the
 * parsed top-level config object (may be NULL).
 */
static inline void
chimera_common_delegation_config(
    json_t                           *root,
    struct chimera_common_delegation *out)
{
    json_t *common, *val;

    out->sync_delegation          = -1;
    out->sync_delegation_threads  = -1;
    out->async_delegation         = -1;
    out->async_delegation_threads = -1;

    if (!root) {
        return;
    }

    common = json_object_get(root, "common");
    if (!json_is_object(common)) {
        return;
    }

    val = json_object_get(common, "sync_delegation");
    if (json_is_boolean(val)) {
        out->sync_delegation = json_is_true(val);
    }

    val = json_object_get(common, "sync_delegation_threads");
    if (json_is_integer(val)) {
        out->sync_delegation_threads = (int) json_integer_value(val);
    }

    val = json_object_get(common, "async_delegation");
    if (json_is_boolean(val)) {
        out->async_delegation = json_is_true(val);
    }

    val = json_object_get(common, "async_delegation_threads");
    if (json_is_integer(val)) {
        out->async_delegation_threads = (int) json_integer_value(val);
    }
} /* chimera_common_delegation_config */

/*
 * Return the path configured in the shared "common" section's "metrics_file"
 * key, or NULL if the section or key is absent (or not a string).  This is the
 * file into which a process dumps a final Prometheus scrape at shutdown -- the
 * same exposition served live at :PORT/metrics -- so that metrics survive
 * process exit even when a run is too short to scrape while it is running.  The
 * returned pointer is owned by `root` and is only valid until json_decref(root);
 * copy it if it must outlive the parsed config.
 */
static inline const char *
chimera_common_metrics_file(json_t *root)
{
    json_t *common, *val;

    if (!root) {
        return NULL;
    }

    common = json_object_get(root, "common");
    if (!json_is_object(common)) {
        return NULL;
    }

    val = json_object_get(common, "metrics_file");
    if (json_is_string(val)) {
        return json_string_value(val);
    }

    return NULL;
} /* chimera_common_metrics_file */

/*
 * Number of liburcu call_rcu reclaim worker threads, from the shared "common"
 * section's "rcu_reclaim_threads" key.  This is a VFS-level setting (the workers
 * reclaim retired entries from the VFS fungible caches), so it is honored
 * identically by the server and the client.  Returns the configured value
 * (0 == one worker per CPU, the default reclaim parallelism; a positive value
 * caps the count) or -1 when the section or key is absent, meaning "unset" so
 * the caller leaves its own default in place.  `root` may be NULL.
 */
static inline int
chimera_common_rcu_reclaim_threads(json_t *root)
{
    json_t *common, *val;

    if (!root) {
        return -1;
    }

    common = json_object_get(root, "common");
    if (!json_is_object(common)) {
        return -1;
    }

    val = json_object_get(common, "rcu_reclaim_threads");
    if (json_is_integer(val)) {
        return (int) json_integer_value(val);
    }

    return -1;
} /* chimera_common_rcu_reclaim_threads */

/*
 * Whether the VFS attribute cache is enabled, from the shared "common" section's
 * "attr_cache" boolean key.  The attr cache is a VFS-level facility used by both
 * the server and the client, so this is honored identically by both.  When
 * disabled the cache is not instantiated and nothing is inserted into or looked
 * up from it (the cache helpers no-op on a NULL cache).  Defaults to enabled
 * (returns 1) when the section or key is absent; `root` may be NULL.
 */
static inline int
chimera_common_attr_cache_enabled(json_t *root)
{
    json_t *common, *val;

    if (!root) {
        return 1;
    }

    common = json_object_get(root, "common");
    if (!json_is_object(common)) {
        return 1;
    }

    val = json_object_get(common, "attr_cache");
    if (json_is_boolean(val)) {
        return json_is_true(val) ? 1 : 0;
    }

    return 1;
} /* chimera_common_attr_cache_enabled */
