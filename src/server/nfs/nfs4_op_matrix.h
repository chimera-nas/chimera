// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>

#include "nfs4_xdr.h"

#define NFS4_OP_MIN                 3 /* OP_ACCESS */
#define NFS4_OP_MAX                 75 /* OP_REMOVEXATTR */

/* Per-minor-version support bits. */
#define NFS4_OP_V40                 0x1
#define NFS4_OP_V41                 0x2
#define NFS4_OP_V42                 0x4

/* Behavioral flags applied during dispatch. */
#define NFS4_OP_FLAG_MUST_BE_FIRST  0x01 /* SEQUENCE in 4.1+: must be at op_index 0 */
#define NFS4_OP_FLAG_NO_REQ_SESSION 0x02 /* allowed in 4.1+ even without a preceding SEQUENCE */

struct nfs4_op_info {
    uint8_t minors;
    uint8_t flags;
};

static const struct nfs4_op_info nfs4_op_support[NFS4_OP_MAX + 1] = {
    /* Common 4.0/4.1/4.2 ops */
    [OP_ACCESS] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_CLOSE] =                { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_COMMIT] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_CREATE] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_DELEGPURGE] =           { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_DELEGRETURN] =          { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_GETATTR] =              { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_GETFH] =                { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_LINK] =                 { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_LOCK] =                 { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_LOCKT] =                { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_LOCKU] =                { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_LOOKUP] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_LOOKUPP] =              { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_NVERIFY] =              { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_OPEN] =                 { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_OPENATTR] =             { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_OPEN_DOWNGRADE] =       { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_PUTFH] =                { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_PUTPUBFH] =             { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_PUTROOTFH] =            { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_READ] =                 { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_READDIR] =              { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_READLINK] =             { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_REMOVE] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_RENAME] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_RESTOREFH] =            { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_SAVEFH] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_SECINFO] =              { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_SETATTR] =              { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_VERIFY] =               { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },
    [OP_WRITE] =                { NFS4_OP_V40 | NFS4_OP_V41 | NFS4_OP_V42, 0
    },

    /* 4.0-only ops (RFC 7530); MNI in 4.1+ per RFC 5661 / 8881. */
    [OP_OPEN_CONFIRM] =         { NFS4_OP_V40,                             0
    },
    [OP_RENEW] =                { NFS4_OP_V40,                             0
    },
    [OP_SETCLIENTID] =          { NFS4_OP_V40,                             0
    },
    [OP_SETCLIENTID_CONFIRM] =  { NFS4_OP_V40,                             0
    },
    [OP_RELEASE_LOCKOWNER] =    { NFS4_OP_V40,                             0
    },

    /* 4.1+ ops. */
    [OP_BACKCHANNEL_CTL] =      { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_BIND_CONN_TO_SESSION] = { NFS4_OP_V41 | NFS4_OP_V42,               NFS4_OP_FLAG_NO_REQ_SESSION
    },
    [OP_EXCHANGE_ID] =          { NFS4_OP_V41 | NFS4_OP_V42,               NFS4_OP_FLAG_NO_REQ_SESSION
    },
    [OP_CREATE_SESSION] =       { NFS4_OP_V41 | NFS4_OP_V42,               NFS4_OP_FLAG_NO_REQ_SESSION
    },
    [OP_DESTROY_SESSION] =      { NFS4_OP_V41 | NFS4_OP_V42,               NFS4_OP_FLAG_NO_REQ_SESSION
    },
    [OP_FREE_STATEID] =         { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    /* Directory delegations are distinct from OPEN file delegations and are
     * not implemented.  Leave OP_GET_DIR_DELEGATION unsupported so clients
     * receive NFS4ERR_NOTSUPP instead of passing dispatch with no handler. */
    [OP_GETDEVICEINFO] =        { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_GETDEVICELIST] =        { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_LAYOUTCOMMIT] =         { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_LAYOUTGET] =            { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_LAYOUTRETURN] =         { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    /* LAYOUTSTATS is a 4.2 operation (RFC 7862) but flex-files (RFC 8435, a 4.1
     * layout type) clients emit it, so accept it on 4.1 too. */
    [OP_LAYOUTSTATS] =          { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_SECINFO_NO_NAME] =      { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_SEQUENCE] =             { NFS4_OP_V41 | NFS4_OP_V42,
                                  NFS4_OP_FLAG_MUST_BE_FIRST | NFS4_OP_FLAG_NO_REQ_SESSION },
    [OP_SET_SSV] =              { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    [OP_TEST_STATEID] =         { NFS4_OP_V41 | NFS4_OP_V42,               0
    },
    /* Explicit post-OPEN delegation requests are not implemented.  OPEN may
     * still grant file delegations when nfs4_delegations is enabled. */
    [OP_DESTROY_CLIENTID] =     { NFS4_OP_V41 | NFS4_OP_V42,               NFS4_OP_FLAG_NO_REQ_SESSION
    },
    [OP_RECLAIM_COMPLETE] =     { NFS4_OP_V41 | NFS4_OP_V42,               0
    },

    /* 4.2-only ops. */
    [OP_ALLOCATE] =             { NFS4_OP_V42,                             0
    },
    [OP_COPY] =                 { NFS4_OP_V42,                             0
    },
    [OP_COPY_NOTIFY] =          { NFS4_OP_V42,                             0
    },
    [OP_DEALLOCATE] =           { NFS4_OP_V42,                             0
    },
    [OP_IO_ADVISE] =            { NFS4_OP_V42,                             0
    },
    [OP_LAYOUTERROR] =          { NFS4_OP_V42,                             0
    },
    [OP_OFFLOAD_CANCEL] =       { NFS4_OP_V42,                             0
    },
    [OP_OFFLOAD_STATUS] =       { NFS4_OP_V42,                             0
    },
    [OP_READ_PLUS] =            { NFS4_OP_V42,                             0
    },
    [OP_SEEK] =                 { NFS4_OP_V42,                             0
    },
    [OP_WRITE_SAME] =           { NFS4_OP_V42,                             0
    },
    [OP_CLONE] =                { NFS4_OP_V42,                             0
    },

    /* RFC 8276 extended attribute ops (NFSv4.2 only) */
    [OP_GETXATTR] =             { NFS4_OP_V42,                             0
    },
    [OP_SETXATTR] =             { NFS4_OP_V42,                             0
    },
    [OP_LISTXATTRS] =           { NFS4_OP_V42,                             0
    },
    [OP_REMOVEXATTR] =          { NFS4_OP_V42,                             0
    },
};

/**
 * Check whether `op` is permitted at this point in the compound, given the
 * negotiated `minor` and the current `op_index` / `seen_sequence` state.
 *
 * Returns one of:
 *   NFS4_OK                       — op may proceed
 *   NFS4ERR_OP_ILLEGAL            — op is not part of this minor's protocol
 *   NFS4ERR_NOTSUPP               — op is in range but not in the matrix
 *   NFS4ERR_MINOR_VERS_MISMATCH   — minor itself is unrecognized (>2)
 *   NFS4ERR_SEQUENCE_POS          — SEQUENCE used outside position 0
 *   NFS4ERR_OP_NOT_IN_SESSION     — 4.1+ op issued before SEQUENCE
 */
static inline nfsstat4
nfs4_op_check_minor(
    uint32_t op,
    uint8_t  minor,
    uint32_t op_index,
    bool     seen_sequence)
{
    const struct nfs4_op_info *info;
    uint8_t                    minor_bit;

    if (minor > 2) {
        return NFS4ERR_MINOR_VERS_MISMATCH;
    }

    if (op == OP_ILLEGAL) {
        return NFS4ERR_OP_ILLEGAL;
    }

    if (op < NFS4_OP_MIN || op > NFS4_OP_MAX) {
        return NFS4ERR_OP_ILLEGAL;
    }

    info = &nfs4_op_support[op];

    if (info->minors == 0) {
        return NFS4ERR_NOTSUPP;
    }

    minor_bit = (minor == 0) ? NFS4_OP_V40 :
        (minor == 1) ? NFS4_OP_V41 : NFS4_OP_V42;

    if (!(info->minors & minor_bit)) {
        return NFS4ERR_OP_ILLEGAL;
    }

    if (minor >= 1) {
        if (info->flags & NFS4_OP_FLAG_MUST_BE_FIRST) {
            if (op_index != 0) {
                return NFS4ERR_SEQUENCE_POS;
            }
        } else if (!(info->flags & NFS4_OP_FLAG_NO_REQ_SESSION)) {
            if (!seen_sequence) {
                return NFS4ERR_OP_NOT_IN_SESSION;
            }
        }
    }

    return NFS4_OK;
} /* nfs4_op_check_minor */
