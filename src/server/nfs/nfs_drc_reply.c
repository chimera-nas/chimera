// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "nfs_drc_reply.h"
#include "nfs_common.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_program.h"


/* Copy the procedure results out of what the capture callback was shown,
 * skipping body_offset bytes of reserved headroom.  What is stored is then the
 * answer alone: no transport framing, no RPC header, and no security-layer
 * reframing -- all three of which belong to a particular send rather than to
 * the answer, and are rebuilt for the retransmit.  Returns the number of bytes
 * written, or 0. */
uint32_t
nfs_drc_copy_rpc_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    uint32_t                 body_offset,
    uint8_t                 *buf,
    uint32_t                 buf_len)
{
    uint32_t skip = body_offset;
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

int
nfs_drc_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_encoding        *encoding,
    const uint8_t                    *cached,
    uint32_t                          cached_len)
{
    uint32_t           reserve;
    struct evpl_iovec *msg_iov;
    int                niov;

    reserve = encoding->program->reserve;

    msg_iov = xdr_dbuf_alloc_space(sizeof(*msg_iov), encoding->dbuf);
    if (!msg_iov) {
        return -1;
    }

    niov = evpl_iovec_alloc(thread->evpl, cached_len + reserve, 8, 1, 0, msg_iov);
    if (niov != 1) {
        return -1;
    }

    memcpy((uint8_t *) msg_iov->data + reserve, cached, cached_len);

    /* Through the ordinary reply path, so the RPC header, the security layer
     * and the transport framing are all built for THIS call rather than
     * recovered from the one that first produced the answer. */
    return evpl_rpc2_send_reply_dispatch(thread->evpl,
                                         encoding,
                                         NULL,
                                         msg_iov,
                                         1,
                                         cached_len + reserve);
} /* nfs_drc_send_cached_reply */
