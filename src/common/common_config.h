// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <jansson.h>

#include "evpl/evpl.h"

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
 * keys: huge_pages (bool), huge_page_size (size), slab_size (size).  A missing
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

    val = json_object_get(common, "preallocate_slabs");
    if (json_is_integer(val)) {
        evpl_global_config_set_preallocate_slabs(cfg, json_integer_value(val));
    }

    val = json_object_get(common, "preallocate_threads");
    if (json_is_integer(val)) {
        evpl_global_config_set_preallocate_threads(cfg, json_integer_value(val));
    }
} /* chimera_apply_common_config */
