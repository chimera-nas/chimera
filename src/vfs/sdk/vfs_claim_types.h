// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Chimera VFS module SDK: the module-facing slice of the claim vocabulary.
 *
 * The full claim model (admission, triggers, breaks) lives in the VFS core
 * (vfs_claim_types.h / vfs_claim.h) and is deliberately NOT part of the
 * module contract -- a backend never inspects a claim.  What a backend does
 * see is here:
 *
 *   - the claim capability masks, which are the wire vocabulary of the
 *     CAP_LEASE backend arbitration ops (lease_acquire.rev_used/bind_deny);
 *   - the two backend lease wire shapes (AGGREGATE / RANGE);
 *   - the owner/actor identity blocks, embedded by value in
 *     struct chimera_vfs_request and its lease_acquire payload;
 *   - the pending-acquire ticket, embedded by value in the request so the
 *     I/O lease path never touches the heap (backends treat it as opaque
 *     storage);
 *   - the range-overlap helper shared by the core and CAP_LEASE arbiters.
 *
 * struct chimera_vfs_claim and struct chimera_vfs_claim_conflict remain
 * opaque to modules: forward declarations only.
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

struct chimera_vfs_claim;
struct chimera_vfs_claim_conflict;
struct chimera_vfs_file_state;
struct chimera_vfs_open_handle;

/* -------------------------------------------------------------------- */
/* Capability bits                                                      */
/* -------------------------------------------------------------------- */

/* Eight capabilities.  Cache bits (CR/CW) are deliberately split from the
 * data-access bits (R/W): an SMB R-lease survives a foreign W-open until the
 * first actual write, while an NFSv4 R-delegation (which carries real I/O
 * rights) is recalled at open admission -- inexpressible with one R bit. */
#define CHIMERA_CLAIM_R              0x01u /* read file data through the server      */
#define CHIMERA_CLAIM_W              0x02u /* write file data through the server     */
#define CHIMERA_CLAIM_D              0x04u /* delete/rename the name (SMB share dim) */
#define CHIMERA_CLAIM_CR             0x08u /* cache reads (serve reads locally)      */
#define CHIMERA_CLAIM_CW             0x10u /* cache writes (absorb writes locally)   */
#define CHIMERA_CLAIM_H              0x20u /* retain handle across close             */
#define CHIMERA_CLAIM_LR             0x40u /* hold a shared byte-range lock          */
#define CHIMERA_CLAIM_LW             0x80u /* hold an exclusive byte-range lock      */

#define CHIMERA_CLAIM_CACHE_BITS     (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | \
                                      CHIMERA_CLAIM_H)
#define CHIMERA_CLAIM_LOCK_BITS      (CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW)
#define CHIMERA_CLAIM_DATA_BITS      (CHIMERA_CLAIM_R | CHIMERA_CLAIM_W)

/* -------------------------------------------------------------------- */
/* Protocols and owner identity                                         */
/* -------------------------------------------------------------------- */

#define CHIMERA_CLAIM_PROTO_NLM      1
#define CHIMERA_CLAIM_PROTO_NFSV4    2
#define CHIMERA_CLAIM_PROTO_SMB2     3
#define CHIMERA_CLAIM_PROTO_INTERNAL 4
#define CHIMERA_CLAIM_PROTO_POSIX    5
#define CHIMERA_CLAIM_PROTO_FUSE     6
#define CHIMERA_CLAIM_PROTO_S3       7

/* Owner identity flag: the identity is node-scoped (an SMB zero-GUID
 * session_id fallback); such owners never coalesce across nodes. */
#define CHIMERA_CLAIM_OWNER_UNSTABLE 0x01

/* 128-bit-safe half-open range overlap shared by the core and CAP_LEASE
 * arbiters (UINT64_MAX = to-EOF, 0 = genuine zero-byte range). */
static inline bool
chimera_vfs_claim_range_overlap_i(
    uint64_t a_off,
    uint64_t a_len,
    uint64_t b_off,
    uint64_t b_len)
{
    __uint128_t a_end = (a_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) a_off + a_len;
    __uint128_t b_end = (b_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) b_off + b_len;

    return a_off < b_end && b_off < a_end;
} /* chimera_vfs_claim_range_overlap_i */

/* Backend lease wire shapes (CHIMERA_VFS_OP_LEASE_ACQUIRE/_RELEASE): the
 * revocable per-node AGGREGATE token vs a binding per-owner RANGE record. */
#define CHIMERA_VFS_LEASE_AGGREGATE 1
#define CHIMERA_VFS_LEASE_RANGE     2

/* Identity block.  client_key is the node-local fast compare; the canonical
 * cluster-stable bytes (co_ownerid, ClientGuid, caller_name) are registered
 * separately by the protocol layer for projection serialization.  key[16] is
 * the KEY circle (SMB LeaseKey / ParentLeaseKey); all-zero means "no key".
 * The KEY circle is deliberately NOT nested inside CLIENT: SameLeaseKey
 * coalesces across ClientGuids (MS-SMB2; WPTS SameLeaseKey). */
struct chimera_claim_owner {
    uint8_t  proto;
    uint8_t  flags;
    uint64_t client_key;
    uint64_t owner_lo;
    uint64_t owner_hi;
    uint8_t  key[16];
};

/* Actor: who is performing an I/O or a mutating operation.  Presented to the
 * trigger engine and the I/O predicate.  op_handle is the HOLDER-circle
 * anchor for self-exemption of metadata mutations through a claim's own
 * handle (R57). */
struct chimera_claim_actor {
    struct chimera_claim_owner      owner;
    struct chimera_vfs_open_handle *op_handle;
};

static inline bool
chimera_claim_owner_equal(
    const struct chimera_claim_owner *a,
    const struct chimera_claim_owner *b)
{
    return a->proto == b->proto &&
           a->client_key == b->client_key &&
           a->owner_lo == b->owner_lo &&
           a->owner_hi == b->owner_hi;
} /* chimera_claim_owner_equal */

static inline bool
chimera_claim_owner_has_key(const struct chimera_claim_owner *o)
{
    static const uint8_t zero[16] = { 0 };

    return memcmp(o->key, zero, 16) != 0;
} /* chimera_claim_owner_has_key */

/* same_key: same 16-byte nonzero key.  A zero key never matches anything,
 * including itself -- keyless holders fall back to the HOLDER/OWNER circles. */
static inline bool
chimera_claim_owner_same_key(
    const struct chimera_claim_owner *a,
    const struct chimera_claim_owner *b)
{
    return chimera_claim_owner_has_key(a) &&
           memcmp(a->key, b->key, 16) == 0;
} /* chimera_claim_owner_same_key */

static inline bool
chimera_claim_owner_same_client(
    const struct chimera_claim_owner *a,
    const struct chimera_claim_owner *b)
{
    return a->proto == b->proto && a->client_key == b->client_key;
} /* chimera_claim_owner_same_client */

/* -------------------------------------------------------------------- */
/* Acquire results and the pending-acquire ticket                       */
/* -------------------------------------------------------------------- */

enum chimera_vfs_claim_result {
    CHIMERA_CLAIM_GRANTED  = 0,
    CHIMERA_CLAIM_DENIED   = 1,
    CHIMERA_CLAIM_BREAKING = 2,
};

typedef void (*chimera_vfs_claim_acquire_cb_t)(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *granted,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data);

/* Caller-allocated pending-acquire ticket.  wait_hard additionally keeps a
 * ticket queued on a HARD (binding) conflict -- blocking byte-range locks
 * (generalizing the old kind==RANGE test); ACCESS/CACHE denials never
 * queue.  Embedded by value in struct chimera_vfs_request (io_lease_ticket)
 * so the implicit I/O lease path never touches the heap; modules treat it
 * as opaque storage. */
struct chimera_vfs_pending_acquire {
    struct chimera_vfs_claim           *claim;
    chimera_vfs_claim_acquire_cb_t      cb;
    void                               *private_data;
    struct chimera_vfs_file_state      *file;
    bool                                queued;
    bool                                wait;      /* wait on BREAKING      */
    bool                                wait_hard; /* also wait on DENIED   */
    struct chimera_vfs_pending_acquire *prev;
    struct chimera_vfs_pending_acquire *next;
};
