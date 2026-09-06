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
 * What is cached is the procedure's results and nothing else.  The rpc2
 * capture callback hands those over directly; everything the wire form adds
 * around them -- transport framing, the RPC header, and the RPCSEC_GSS
 * reframing under integrity or privacy -- is rebuilt by the ordinary reply
 * path when the entry is replayed, for the call being answered then.
 *
 * Each of those three has to be rebuilt, and for a different reason.  Framing
 * differs per transport, and keeping it once made a reply captured over
 * RPC-over-RDMA unparseable on replay -- fatal for the NFSv4.1 session cache,
 * and why the NFSv3 cache disabled itself on RDMA connections outright.  The
 * header carries the retransmit's own xid and verifier.  And a wrapped reply
 * is bound to the sequence number of the call it answered (RFC 2203 sec
 * 5.3.3.2/5.3.3.3), so replaying one wrapped is replaying it around the wrong
 * call -- and, sent back through the reply path, wraps it a second time, which
 * is what the client's decoder then chokes on.
 */

/*
 * Copy the procedure results out of what the capture callback was shown,
 * skipping body_offset bytes of reserved headroom.  See
 * evpl_rpc2_reply_capture_cb_t.  Returns bytes written, or 0 if buf_len is too
 * small.
 */
uint32_t
nfs_drc_copy_rpc_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    uint32_t                 body_offset,
    uint8_t                 *buf,
    uint32_t                 buf_len);

/* Send the cached results as the reply for `encoding`'s request.  Returns 0 on
 * success. */
int
nfs_drc_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_encoding        *encoding,
    const uint8_t                    *cached,
    uint32_t                          cached_len);
