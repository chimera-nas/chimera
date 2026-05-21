// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>

#include "nfs4_xdr.h"

/*
 * Server-private encoding of stateid.other (12 bytes):
 *
 *   bytes 0-3 : server-instance epoch (big-endian) -- see below
 *   byte 4    : version (high 6 bits) | type (low 2 bits)
 *   byte 5    : shard index (0..NFS_STATE_NUM_SHARDS-1)
 *   bytes 6-8 : slot_idx (24-bit, big-endian)
 *   bytes 9-11: generation (24-bit, big-endian)
 *
 * The epoch is a per-server-instance value (see nfs_state_table.epoch),
 * placed first on purpose: a stateid minted by a previous server instance
 * carries a different epoch, so the lookup returns NFS4ERR_STALE_STATEID
 * (server reboot) instead of NFS4ERR_BAD_STATEID (RFC 7530 §9.1.4.2).  This is
 * the convention pynfs's makeStaleId() probes -- it rewrites the leading bytes
 * with an "old" epoch while makeBadID() preserves them and corrupts the rest.
 *
 * Special stateids defined by RFC 7530 §9.1.4 (all-zero, all-ones, etc.) are
 * detected by inspecting the entire {seqid, other} blob before any decode.
 */

#define NFS4_STATEID_VERSION       0x1
#define NFS4_STATEID_TYPE_OPEN     0x0
#define NFS4_STATEID_TYPE_LOCK     0x1
#define NFS4_STATEID_TYPE_MASK     0x3

#define NFS4_STATEID_VERSION_SHIFT 2

#define NFS_STATE_NUM_SHARDS       64

struct nfs4_stateid_view {
    uint32_t epoch;
    uint8_t  version;
    uint8_t  type;
    uint8_t  shard;
    uint32_t slot_idx;       /* 24-bit */
    uint32_t generation;     /* 24-bit */
};

static inline void
nfs4_stateid_encode(
    struct stateid4 *out,
    uint32_t         seqid,
    uint8_t          type,
    uint8_t          shard,
    uint32_t         slot_idx,
    uint32_t         generation,
    uint32_t         epoch)
{
    uint8_t *p = (uint8_t *) out->other;

    out->seqid = seqid;

    p[0] = (uint8_t) (epoch >> 24);
    p[1] = (uint8_t) (epoch >> 16);
    p[2] = (uint8_t) (epoch >> 8);
    p[3] = (uint8_t) (epoch);
    p[4] = (NFS4_STATEID_VERSION << NFS4_STATEID_VERSION_SHIFT) |
        (type & NFS4_STATEID_TYPE_MASK);
    p[5]  = shard;
    p[6]  = (uint8_t) (slot_idx >> 16);
    p[7]  = (uint8_t) (slot_idx >> 8);
    p[8]  = (uint8_t) (slot_idx);
    p[9]  = (uint8_t) (generation >> 16);
    p[10] = (uint8_t) (generation >> 8);
    p[11] = (uint8_t) (generation);
} /* nfs4_stateid_encode */

static inline void
nfs4_stateid_decode(
    struct nfs4_stateid_view *out,
    const struct stateid4    *sid)
{
    const uint8_t *p = (const uint8_t *) sid->other;

    out->epoch = ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) |
        ((uint32_t) p[2] << 8) | p[3];
    out->version    = p[4] >> NFS4_STATEID_VERSION_SHIFT;
    out->type       = p[4] & NFS4_STATEID_TYPE_MASK;
    out->shard      = p[5];
    out->slot_idx   = ((uint32_t) p[6] << 16) | ((uint32_t) p[7] << 8) | p[8];
    out->generation = ((uint32_t) p[9] << 16) | ((uint32_t) p[10] << 8) | p[11];
} /* nfs4_stateid_decode */

/*
 * RFC 7530 §9.1.4.2 seqid validation for a (non-special) NFSv4.0 stateid: the
 * presented seqid is compared against the state's current seqid.  A smaller
 * seqid is a stale reference to a superseded stateid (NFS4ERR_OLD_STATEID); a
 * larger one was never issued (NFS4ERR_BAD_STATEID).  A zero seqid means "use
 * the most current" and always matches.  4.1+ does not carry this seqid
 * coupling, so callers gate this on minorversion 0.
 */
static inline nfsstat4
nfs4_stateid_check_seqid(
    uint32_t cur_seqid,
    uint32_t sid_seqid)
{
    if (sid_seqid == 0) {
        return NFS4_OK;
    }
    if (sid_seqid < cur_seqid) {
        return NFS4ERR_OLD_STATEID;
    }
    if (sid_seqid > cur_seqid) {
        return NFS4ERR_BAD_STATEID;
    }
    return NFS4_OK;
} /* nfs4_stateid_check_seqid */

static inline int
nfs4_stateid_is_special(const struct stateid4 *sid)
{
    /* All-zero stateid (anonymous) and all-ones (read bypass) are special
     * per RFC 7530 §9.1.4.3.  Detected by looking at both seqid and other. */
    static const uint8_t zero[NFS4_OTHER_SIZE] = { 0 };
    static const uint8_t ones[NFS4_OTHER_SIZE] = {
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff
    };

    if (sid->seqid == 0 && memcmp(sid->other, zero, NFS4_OTHER_SIZE) == 0) {
        return 1;
    }
    if (sid->seqid == 0xffffffff &&
        memcmp(sid->other, ones, NFS4_OTHER_SIZE) == 0) {
        return 1;
    }
    return 0;
} /* nfs4_stateid_is_special */
