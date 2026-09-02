// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdbool.h>
#include <stdint.h>

struct chimera_server_nfs_thread;
struct evpl_rpc2_encoding;
struct evpl_iovec;

/*
 * Shared helpers for the NFSv3/NFSv4 duplicate-request caches.
 *
 * The rpc2 reply_capture_cb hands over the whole outgoing message plus the
 * length of the transport framing in front of the RPC reply.  Both DRCs store
 * what is behind that framing -- [RPC reply header][procedure result body] --
 * and cache only the procedure-result body, re-emitting it through the rpc2
 * reply path on replay.  The RPC header (xid, verifier) and the framing are
 * regenerated for the current connection, which may differ from the one the
 * reply was first captured on: a different transport, or, after a restart, a
 * brand-new connection entirely.
 *
 * Keeping the framing instead would tie an entry to its original transport.
 * It used to, and a reply captured over RPC-over-RDMA was then unparseable on
 * replay -- fatal for the NFSv4.1 session cache, and the reason the NFSv3
 * cache disabled itself on RDMA connections outright.
 */

/* Parse a captured RPC reply and yield the offset of the procedure-result body
 * within it.  Returns false unless the buffer is a well-formed MSG_ACCEPTED /
 * SUCCESS reply (the only kind worth replaying). */
bool
nfs_drc_reply_body_offset(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t      *offset);

/*
 * Copy the RPC reply out of a captured outgoing message, skipping rpc_offset
 * bytes of transport framing.  See evpl_rpc2_reply_capture_cb_t for what that
 * offset is; storing what it skips would tie a cached reply to the transport
 * it was captured on.  Returns bytes written, or 0 if buf_len is too small.
 */
uint32_t
nfs_drc_copy_rpc_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    uint32_t                 rpc_offset,
    uint8_t                 *buf,
    uint32_t                 buf_len);

/* Strip the cached reply down to its procedure-result body and send it as the
 * reply for `encoding`'s request.  Returns 0 on success. */
int
nfs_drc_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_encoding        *encoding,
    const uint8_t                    *cached,
    uint32_t                          cached_len);
