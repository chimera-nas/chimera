// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <limits.h>

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
    const struct evpl_iovec           *iov,
    int                                niov,
    uint32_t                           body_offset,
    const struct evpl_rpc2_rdma_chunk *write_chunk,
    uint8_t                           *buf,
    uint32_t                           buf_len)
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

        if (n > buf_len - off) {
            return 0;
        }
        memcpy(buf + off, src, n);
        off += n;
    }

    if (skip) {
        return 0;
    }
    if (write_chunk && write_chunk->length) {
        uint32_t position = write_chunk->xdr_position;
        uint32_t left     = write_chunk->length;
        uint64_t padded   = ((uint64_t) left + 3) & ~UINT64_C(3);

        if (position > off || padded > buf_len - off) {
            return 0;
        }
        memmove(buf + position + padded, buf + position, off - position);
        off += padded;
        for (i = 0; i < write_chunk->niov && left; i++) {
            uint32_t n = write_chunk->iov[i].length;
            if (n > left) {
                n = left;
            }
            memcpy(buf + position, write_chunk->iov[i].data, n);
            position += n;
            left     -= n;
        }
        if (left) {
            return 0;
        }
        memset(buf + position, 0, padded - write_chunk->length);
    }
    return off;
} /* nfs_drc_copy_rpc_reply */

static uint32_t
nfs_drc_word(const uint8_t *p)
{
    return (uint32_t) p[0] << 24 | (uint32_t) p[1] << 16 |
           (uint32_t) p[2] << 8 | p[3];
} /* nfs_drc_word */

/* Locate the first successful READ in the saved logical COMPOUND. Decode
 * preceding results with the generated codec, reusing scratch after each op.
 * READ is the only zero-copy result arm, and is inspected before decoding, so
 * no buffer references escape this scan. Empty READ still consumes the offer.
 * Keeping the cache in normal XDR form also supports persistent entries and
 * retries that switch between stream and RDMA transports. */
static int
nfs4_cached_read(
    struct evpl_rpc2_encoding *encoding,
    const uint8_t             *cached,
    uint32_t                   cached_len,
    uint32_t                  *position,
    uint32_t                  *length)
{
    struct nfs_resop4 result;
    xdr_dbuf          scratch   = *encoding->dbuf;
    void             *allocated = NULL;
    uint64_t          offset;
    uint32_t          count;
    int               rc = -1;

    *position = *length = 0;
    if (cached_len < 12) {
        return -1;
    }
    offset = 8 + (((uint64_t) nfs_drc_word(cached + 4) + 3) & ~UINT64_C(3));
    if (offset + 4 > cached_len) {
        return -1;
    }
    count   = nfs_drc_word(cached + offset);
    offset += 4;
    for (uint32_t i = 0; i < count; i++) {
        if (offset + 8 > cached_len) {
            goto out;
        }
        if (nfs_drc_word(cached + offset) == OP_READ &&
            nfs_drc_word(cached + offset + 4) == NFS4_OK) {
            if (offset + 16 > cached_len) {
                goto out;
            }
            *position = offset + 16;
            *length   = nfs_drc_word(cached + offset + 12);
            if (*position + (((uint64_t) *length + 3) & ~UINT64_C(3)) > cached_len) {
                goto out;
            }
            rc = 0;
            goto out;
        }
        struct evpl_iovec input = { .data = (void *) (cached + offset), .length = cached_len - offset };
        scratch.used = allocated ? 0 : encoding->dbuf->used;
        int               consumed = unmarshall_nfs_resop4(&result, &input, 1, NULL, &scratch);
        if (consumed < 0 && !allocated) {
            /* A retry may have filled its request arena with arguments. Each
             * allocated result object needs at least one XDR word; the result
             * union bounds its size. This deliberately loose bound is used
             * only when the existing arena cannot decode one prefix op. */
            uint64_t capacity = (uint64_t) cached_len * sizeof(result) / 4 + 8;
            if (capacity > INT_MAX || !(allocated = malloc(capacity))) {
                goto out;
            }
            scratch.buffer = allocated;
            scratch.size   = capacity;
            scratch.used   = 0;
            consumed       = unmarshall_nfs_resop4(&result, &input, 1, NULL, &scratch);
        }
        if (consumed <= 0 || (uint64_t) consumed > cached_len - offset) {
            goto out;
        }
        offset += consumed;
    }
    rc = offset == cached_len ? 0 : -1;
 out:
    free(allocated);
    return rc;
} /* nfs4_cached_read */

int
nfs_drc_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_encoding        *encoding,
    const uint8_t                    *cached,
    uint32_t                          cached_len)
{
    uint32_t           reserve;
    struct evpl_iovec *msg_iov;
    int                niov, reply_niov = 1;
    uint32_t           position = 0, payload = 0, padded = 0;

    if (encoding->program->version == 4 && encoding->write_chunk &&
        (encoding->write_chunk->max_length || encoding->write_chunk->num_segments) &&
        nfs4_cached_read(encoding, cached, cached_len, &position, &payload)) {
        return evpl_rpc2_send_reply_system_error(thread->evpl, encoding);
    }

    reserve = encoding->program->reserve;

    msg_iov = xdr_dbuf_alloc_space(3 * sizeof(*msg_iov), encoding->dbuf);
    if (!msg_iov) {
        return evpl_rpc2_send_reply_system_error(thread->evpl, encoding);
    }

    niov = evpl_iovec_alloc(thread->evpl, cached_len + reserve, 8, 1, 0, msg_iov);
    if (niov != 1) {
        return evpl_rpc2_send_reply_system_error(thread->evpl, encoding);
    }

    memcpy((uint8_t *) msg_iov->data + reserve, cached, cached_len);

    if (payload) {
        struct evpl_rpc2_rdma_chunk *chunk = encoding->write_chunk;
        padded = (payload + 3) & ~3U;
        evpl_iovec_clone_segment(&msg_iov[2], &msg_iov[0], reserve + position, payload);
        evpl_iovec_clone_segment(&msg_iov[1], &msg_iov[0], reserve + position + padded,
                                 cached_len - position - padded);
        msg_iov[0].length   = reserve + position;
        chunk->iov          = &msg_iov[2];
        chunk->niov         = 1;
        chunk->length       = payload;
        chunk->xdr_position = position;
        reply_niov          = 2;
    }

    /* Through the ordinary reply path, so the RPC header, the security layer
     * and the transport framing are all built for THIS call rather than
     * recovered from the one that first produced the answer. */
    return evpl_rpc2_send_reply_dispatch(thread->evpl,
                                         encoding,
                                         NULL,
                                         msg_iov,
                                         reply_niov,
                                         cached_len + reserve - padded);
} /* nfs_drc_send_cached_reply */
