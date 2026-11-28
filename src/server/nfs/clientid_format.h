// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * NFSv4 client ID layout and the shared, read-only client ID codec.
 *
 * A client ID (clientid4) is an opaque 64-bit identity at ordinary call
 * sites: registry, session, state, recovery and reply-cache code store,
 * compare, hash, persist and restore the full value and never mask or shift
 * it.  When metadata is genuinely wanted (logs, diagnostics, observing
 * recovery) they may call clientid_decode().  A successful decode does not
 * authenticate an ID or prove where it came from, and its result must never
 * drive routing, ownership, restore/expire decisions, allocator counters or
 * stateid identity.  Decoding failure must never gate a lookup or a
 * hydration: IDs in older or unknown formats remain valid identities.
 *
 * The only globally defined field is Type, bits 63..60 of the host uint64_t
 * (the first nibble on the wire).  Each Type defines the other 60 bits.
 *
 * Type registry (a number is never reused, even after its Type is retired):
 *
 *   0      reserved, never emitted
 *   1      supervisor grant (proof of concept, not for production; the only
 *          Type minted)
 *   2      reserved for the design's best-effort production Type; not
 *          implemented, and never given to any other scheme
 *   3..14  unassigned; assigned by a change to the allocator design doc
 *   15     reserved (possible future extension marker)
 *
 * Type 1 / Version 0:
 *
 *    63..60  59..58   57..48       47..44       43..24          23..0
 *   +-------+-------+------------+------------+---------------+-------------+
 *   |Type 1 | Ver 0 | Node ID 10 | Instance 4 | Generation 20 | Sequence 24 |
 *   +-------+-------+------------+------------+---------------+-------------+
 *                     1..1023      0..15        1..1048575      1..16777215
 *
 * The two-bit Version is Type-1-specific.  Generation 0 is reserved, so a
 * client ID minted before the allocator (node_id << 48 | counter, which
 * decodes as Generation 0 whenever its top nibble happens to be 1) can never
 * equal a Type 1 ID.
 */

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#define CLIENTID_TYPE_SHIFT          60
#define CLIENTID_TYPE_RESERVED       0
#define CLIENTID_TYPE_SUPERVISOR     1

#define CLIENTID_T1_VERSION_SHIFT    58
#define CLIENTID_T1_VERSION_MASK     0x3u
#define CLIENTID_T1_NODE_SHIFT       48
#define CLIENTID_T1_INSTANCE_SHIFT   44
#define CLIENTID_T1_GENERATION_SHIFT 24
#define CLIENTID_T1_NODE_MIN         1u
#define CLIENTID_T1_NODE_MAX         0x3FFu
#define CLIENTID_T1_INSTANCE_MAX     0xFu
#define CLIENTID_T1_GENERATION_MIN   1u      /* 0 is reserved */
#define CLIENTID_T1_GENERATION_MAX   0xFFFFFu
#define CLIENTID_T1_SEQUENCE_MIN     1u
#define CLIENTID_T1_SEQUENCE_MAX     0xFFFFFFu

struct clientid_type1_v0_info {
    uint16_t node_id;     /* 1..1023: original minting host */
    uint8_t  instance;    /* 0..15: original server instance */
    uint32_t generation;  /* 20 bits: startup generation */
    uint32_t sequence;    /* 24 bits: allocation sequence */
};

struct clientid_decoded {
    uint8_t type;
    /* Version is interpreted only by the selected Type's decoder. */
    union {
        struct clientid_type1_v0_info type1_v0;
    } payload;
    uint8_t version;      /* Defined for a successfully decoded Type 1 ID. */
};

/* Pure and side-effect free.  0 = decoded; ENOTSUP = reserved or unknown
 * Type, or a Version the Type does not define; EINVAL = a value the
 * recognised format can never hold (Node ID 0, Generation 0, Sequence 0), or
 * a NULL `out`.  Always a positive errno. */
static inline int
clientid_decode(
    uint64_t                 clientid,
    struct clientid_decoded *out)
{
    struct clientid_type1_v0_info *t1;

    if (!out) {
        return EINVAL;
    }

    out->type      = (uint8_t) (clientid >> CLIENTID_TYPE_SHIFT);
    out->version   = 0;
    t1             = &out->payload.type1_v0;
    t1->node_id    = 0;
    t1->instance   = 0;
    t1->generation = 0;
    t1->sequence   = 0;

    /* Dispatch on the Type bits only; each Type owns its Version field. */
    if (out->type != CLIENTID_TYPE_SUPERVISOR) {
        return ENOTSUP;
    }

    out->version = (uint8_t) ((clientid >> CLIENTID_T1_VERSION_SHIFT) & CLIENTID_T1_VERSION_MASK);
    if (out->version != 0) {
        return ENOTSUP;
    }

    t1->node_id    = (uint16_t) ((clientid >> CLIENTID_T1_NODE_SHIFT) & CLIENTID_T1_NODE_MAX);
    t1->instance   = (uint8_t) ((clientid >> CLIENTID_T1_INSTANCE_SHIFT) & CLIENTID_T1_INSTANCE_MAX);
    t1->generation = (uint32_t) ((clientid >> CLIENTID_T1_GENERATION_SHIFT) & CLIENTID_T1_GENERATION_MAX);
    t1->sequence   = (uint32_t) (clientid & CLIENTID_T1_SEQUENCE_MAX);

    if (t1->node_id < CLIENTID_T1_NODE_MIN ||
        t1->generation < CLIENTID_T1_GENERATION_MIN ||
        t1->sequence < CLIENTID_T1_SEQUENCE_MIN) {
        return EINVAL;
    }
    return 0;
} /* clientid_decode */

/* Human-readable form for logs and diagnostics.  Never branch on it. */
static inline int
clientid_format(
    uint64_t clientid,
    char    *buf,
    size_t   len)
{
    struct clientid_decoded d;
    int                     rc = clientid_decode(clientid, &d);

    if (rc == ENOTSUP) {
        return snprintf(buf, len, "0x%016llx (Type %u, not decodable here)",
                        (unsigned long long) clientid, d.type);
    }
    if (rc == EINVAL) {
        return snprintf(buf, len, "0x%016llx (Type 1, invalid fields; possibly a "
                        "pre-allocator ID)", (unsigned long long) clientid);
    }
    return snprintf(buf, len, "0x%016llx (Type 1 Version 0: node %u instance %u "
                    "generation %u sequence %u)", (unsigned long long) clientid,
                    d.payload.type1_v0.node_id, d.payload.type1_v0.instance,
                    d.payload.type1_v0.generation, d.payload.type1_v0.sequence);
} /* clientid_format */
