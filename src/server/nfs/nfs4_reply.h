// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_common.h"
#include "nfs4_session.h"
#include "vfs/vfs_idmap.h"

/* Keep these limits aligned with the READ buffers and the generated
 * send_reply_NFSPROC4_COMPOUND adapter, respectively. */
#define CHIMERA_NFS4_READ_MAX_IOV          256
#define CHIMERA_NFS4_REPLY_MAX_IOV         260

/* Largest optional delegation arm, including its bounded ACL WHO. */
#define CHIMERA_NFS4_OPEN_DELEGATION_BYTES (64 + CHIMERA_IDMAP_WHO_MAX)

/* Reply storage and the transport share the request's arena. Reserve the
 * transport descriptors before admitting any operation, including standalone
 * protocol operations and single-operation VFS spans. No arena allocation
 * performed after accepted completion may consume this reservation. */
static inline uint64_t
chimera_nfs4_reply_transport_bytes(const struct nfs_request *req)
{
    uint64_t bytes = CHIMERA_NFS4_REPLY_MAX_IOV * sizeof(struct evpl_iovec);

    if (req->conn && req->conn->rdma) {
        uint64_t segments = sizeof(((struct evpl_rpc2_rdma_segment_list *) 0)->segments) /
            sizeof(struct evpl_rpc2_rdma_segment);
        uint64_t iov_size = (sizeof(struct evpl_iovec) + 7) & ~UINT64_C(7);

        /* cursor_move partitions a source sequence among at most 'segments'
         * remote targets, duplicating at most one view per boundary. Only the
         * first READ uses the RPC/RDMA Write chunk; later READs remain inline.
         * The Reply chunk can carry all generated reply vectors. Reduced
         * replies also allocate one inline RDMA-header descriptor. Rounding
         * every descriptor covers separately allocated, aligned view arrays. */
        bytes += (CHIMERA_NFS4_READ_MAX_IOV + CHIMERA_NFS4_REPLY_MAX_IOV +
                  2 * (segments - 1) + 1) * iov_size;
    }

    return bytes;
} /* chimera_nfs4_reply_transport_bytes */

static inline bool
chimera_nfs4_reply_fits(
    const struct nfs_request *req,
    uint64_t                  bytes)
{
    const xdr_dbuf *dbuf    = req->encoding->dbuf;
    uint64_t        reserve = chimera_nfs4_reply_transport_bytes(req);
    uint64_t        available;

    if (dbuf->used < 0 || dbuf->used > dbuf->size || bytes > UINT64_MAX - 7) {
        return false;
    }
    available = (uint64_t) (dbuf->size - dbuf->used);
    bytes     = (bytes + 7) & ~UINT64_C(7);
    return reserve <= available && bytes <= available - reserve;
} /* chimera_nfs4_reply_fits */

static inline uint64_t
chimera_nfs4_reply_pad(uint64_t bytes)
{
    return (bytes + 3) & ~UINT64_C(3);
} /* chimera_nfs4_reply_pad */

/* Keep space for the next operation's size error. SETATTR carries an empty
 * bitmap even on failure. This lookahead applies across VFS span boundaries. */
static inline uint32_t
chimera_nfs4_reply_error_bytes(uint32_t opcode)
{
    return opcode == OP_SETATTR ? 12 : 8;
} /* chimera_nfs4_reply_error_bytes */

static inline nfsstat4
chimera_nfs4_reply_check(
    const struct nfs_request *req,
    uint32_t                  index,
    uint64_t                  bytes,
    uint64_t                  chunk_bytes,
    bool                      success)
{
    const struct evpl_rpc2_encoding *encoding = req->encoding;
    const struct nfs4_session       *session  = req->replay_session ? req->replay_session : req->session;
    uint64_t                         overhead = encoding->reply_overhead ? encoding->reply_overhead : 24;

    if (success && index + 1 < req->args_compound->num_argarray) {
        bytes += chimera_nfs4_reply_error_bytes(req->args_compound->argarray[index + 1].argop);
    }
    bytes += overhead;
    if (req->seen_sequence && session) {
        const struct channel_attrs4 *attrs = &session->nfs4_session_fore_attrs;
        if (bytes > attrs->ca_maxresponsesize) {
            return NFS4ERR_REP_TOO_BIG;
        }
        if (req->args_compound->argarray[0].opsequence.sa_cachethis &&
            bytes > attrs->ca_maxresponsesize_cached) {
            return NFS4ERR_REP_TOO_BIG_TO_CACHE;
        }
    }
    if (encoding->reply_chunk_present && bytes - chunk_bytes > 512 &&
        bytes - chunk_bytes > encoding->reply_chunk_capacity) {
        return NFS4ERR_RESOURCE;
    }
    return NFS4_OK;
} /* chimera_nfs4_reply_check */

/* Success bounds for operations that can mutate state. Read-only variable
 * replies use their empty form here and their actual length at completion.
 * The generated sizer keeps fixed protocol fields out of a second schema. */
static inline uint64_t
chimera_nfs4_reply_op_bound(
    const struct nfs_request *req,
    const struct nfs_argop4  *argop)
{
    struct nfs_resop4 result = { .resop = argop->argop };
    uint64_t          extra  = 0;

    switch (argop->argop) {
        case OP_OPEN: {
            uint32_t want = argop->opopen.share_access & OPEN4_SHARE_ACCESS_WANT_DELEG_MASK;
            /* A v4.0 owner replay may carry an existing grant and attrset.
             * Session OPENs need only reserve their mandatory decline;
             * optional grants are admitted after accepted finish. */
            if (!req->minorversion) {
                extra = 12 + CHIMERA_NFS4_OPEN_DELEGATION_BYTES;
            } else {
                if (argop->opopen.openhow.opentype == OPEN4_CREATE &&
                    argop->opopen.openhow.how.mode != EXCLUSIVE4) {
                    extra = 12; /* at most three applied attribute words */
                }
                if (want != OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE) {
                    extra += want == OPEN4_SHARE_ACCESS_WANT_NO_DELEG ||
                        want == OPEN4_SHARE_ACCESS_WANT_CANCEL ? 4 : 8;
                }
            }
            break;
        }
        case OP_CREATE:
        case OP_SETATTR:
            extra = 12; /* chimera_nfs4_mask2attr emits at most three words */
            break;
        case OP_LAYOUTGET:
            /* At most eight layouts: layout4 plus flex body and wrapped FH.
             * This covers native block/SCSI bodies and DS materialization. */
            extra = 8 * (28 + 84 + chimera_nfs4_reply_pad(CHIMERA_NFS_FH_MAX));
            break;
        case OP_LAYOUTCOMMIT:
            result.oplayoutcommit.locr_resok4.locr_newsize.ns_sizechanged = true;
            break;
        case OP_LAYOUTRETURN:
            result.oplayoutreturn.lorr_stateid.lrs_present = true;
            break;
        case OP_TEST_STATEID:
            extra = (uint64_t) argop->optest_stateid.num_ts_stateids * 4;
            break;
        case OP_EXCHANGE_ID:
        case OP_CREATE_SESSION:
            return 256; /* bounded server identity / two channel_attrs4 */
        default:
            break;
    } // switch
    int length = marshall_length_nfs_resop4(&result);
    return (length < 0 ? 8 : (uint64_t) length) + extra;
} /* chimera_nfs4_reply_op_bound */
