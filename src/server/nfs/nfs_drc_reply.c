// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "nfs_drc_reply.h"
#include "nfs_common.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_program.h"

static uint32_t
nfs_drc_be32(const uint8_t *p)
{
    return ((uint32_t) p[0] << 24) |
           ((uint32_t) p[1] << 16) |
           ((uint32_t) p[2] << 8) |
           (uint32_t) p[3];
} /* nfs_drc_be32 */

/* Copy the RPC reply out of the outgoing message, skipping rpc_offset bytes of
 * transport framing (record marker or RPC-over-RDMA header).  What is stored is
 * therefore transport-independent, which is what lets a reply captured over one
 * transport be replayed at all -- and is required for RDMA, whose header is
 * variable-length.  Returns the number of bytes written, or 0. */
uint32_t
nfs_drc_copy_rpc_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    uint32_t                 rpc_offset,
    uint8_t                 *buf,
    uint32_t                 buf_len)
{
    uint32_t skip = rpc_offset;
    uint32_t off  = 0;
    int      i;

    for (i = 0; i < niov; i++) {
        const uint8_t *src = iov[i].data;
        uint32_t       n   = iov[i].length;

        if (skip) {
            if (skip >= n) {
                skip -= n;
                continue;
            }
            src += skip;
            n   -= skip;
            skip = 0;
        }

        if (off + n > buf_len) {
            return 0;
        }
        memcpy(buf + off, src, n);
        off += n;
    }

    return off;
} /* nfs_drc_copy_rpc_reply */

/*
 * buf is the RPC reply message itself -- no transport framing.  The capture
 * callbacks strip that off before storing (see the rpc_offset argument of
 * evpl_rpc2_reply_capture_cb_t), because the framing is rebuilt for the
 * retransmit anyway and differs per transport: a 4-byte record marker over a
 * stream, a variable-length RPC-over-RDMA header over RDMA.  This parser used
 * to begin by validating a record marker, which made a cached reply captured
 * over RDMA unparseable -- and its callers turn that into a fatal abort.
 */
bool
nfs_drc_reply_body_offset(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t      *offset)
{
    uint32_t v, verf_len, pad;
    uint32_t pos = 0;

    /* xid(4) mtype(4) reply_stat(4) verf_flavor(4) verf_len(4) accept_stat(4) */
    if (len < 24) {
        return false;
    }

    pos += 4; /* xid */

    v = nfs_drc_be32(buf + pos); /* mtype */
    if (v != 1) { /* REPLY */
        return false;
    }
    pos += 4;

    v = nfs_drc_be32(buf + pos); /* reply stat */
    if (v != 0) { /* MSG_ACCEPTED */
        return false;
    }
    pos += 4;

    pos     += 4; /* verifier flavor */
    verf_len = nfs_drc_be32(buf + pos);
    pos     += 4;

    pad = (4 - (verf_len & 3)) & 3;
    if (pos + verf_len + pad + 4 > len) {
        return false;
    }
    pos += verf_len + pad;

    v = nfs_drc_be32(buf + pos); /* accept stat */
    if (v != 0) { /* SUCCESS */
        return false;
    }
    pos += 4;

    *offset = pos;
    return true;
} /* nfs_drc_reply_body_offset */

int
nfs_drc_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_encoding        *encoding,
    const uint8_t                    *cached,
    uint32_t                          cached_len)
{
    uint32_t           body_offset, body_len, reserve;
    struct evpl_iovec *msg_iov;
    int                niov;

    if (!nfs_drc_reply_body_offset(cached, cached_len, &body_offset)) {
        return -1;
    }

    body_len = cached_len - body_offset;
    reserve  = encoding->program->reserve;

    msg_iov = xdr_dbuf_alloc_space(sizeof(*msg_iov), encoding->dbuf);
    if (!msg_iov) {
        return -1;
    }

    niov = evpl_iovec_alloc(thread->evpl, body_len + reserve, 8, 1, 0, msg_iov);
    if (niov != 1) {
        return -1;
    }

    memcpy((uint8_t *) msg_iov->data + reserve, cached + body_offset, body_len);

    return evpl_rpc2_send_reply_dispatch(thread->evpl,
                                         encoding,
                                         NULL,
                                         msg_iov,
                                         1,
                                         body_len + reserve);
} /* nfs_drc_send_cached_reply */
