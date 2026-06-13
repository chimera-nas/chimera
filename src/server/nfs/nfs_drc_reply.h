// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdbool.h>
#include <stdint.h>

struct chimera_server_nfs_thread;
struct evpl_rpc2_encoding;

/*
 * Shared helpers for the NFSv3/NFSv4 duplicate-request caches.
 *
 * A captured reply (from the rpc2 reply_capture_cb) is the full on-wire TCP
 * reply: [record-mark(4)][RPC reply header][procedure result body].  Both DRCs
 * cache only the procedure-result body and re-emit it through the rpc2 reply
 * path on replay, so the RPC header (xid, verifier) and transport framing are
 * regenerated for the current connection -- which may differ from the one the
 * reply was first captured on (a different transport, or, after a restart, a
 * brand-new connection entirely).
 */

/* Parse a captured on-wire reply and yield the offset of the procedure-result
 * body within it.  Returns false unless the buffer is a well-formed
 * MSG_ACCEPTED / SUCCESS reply (the only kind worth replaying). */
bool
nfs_drc_reply_body_offset(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t      *offset);

/* Strip the cached reply down to its procedure-result body and send it as the
 * reply for `encoding`'s request.  Returns 0 on success. */
int
nfs_drc_send_cached_reply(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_encoding        *encoding,
    const uint8_t                    *cached,
    uint32_t                          cached_len);
