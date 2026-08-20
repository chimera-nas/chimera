// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdbool.h>
#include <stdint.h>

/* nfs3_xdr.h defines xdr_iovec (used by the minorversion peek below). */
#include "nfs3_xdr.h"

struct chimera_server_nfs_shared;
struct COMPOUND4args;

/*
 * NFSv4.0 duplicate-request cache.
 *
 * NFSv4.0 has no sessions, so its only exactly-once mechanism for non-idempotent
 * COMPOUNDs is a server reply cache.  This wraps the generated NFS_V4 call
 * dispatcher and runs minorversion-0 COMPOUNDs through the shared connectionless
 * DRC (nfs3_drc.{c,h}) -- the same client-keyed, lazily-hydrated cache the NFSv3
 * DRC uses -- so a 4.0 client's retransmit replays instead of re-executing, even
 * after it reconnects to a different node.  4.1+ COMPOUNDs (handled by the
 * session reply cache) and NULL pass straight through.
 *
 * Installed only when server.nfs4_drc is set, exactly as the NFSv3 DRC is gated
 * on server.nfs3_drc; persistence to the KV store additionally requires a
 * persistent kv_module.
 */
void
nfs4_v40_drc_install(
    struct chimera_server_nfs_shared *shared,
    int                               persist);

/*
 * Is this COMPOUND worth caching -- does it carry an operation that must not be
 * re-executed on a retransmit?  Read-only compounds are not cached, so they can
 * never be answered from the cache either; see the comment on the definition.
 *
 * Called after the arguments are decoded, from chimera_nfs4_compound.
 */
bool
nfs4_v40_drc_compound_cacheable(
    const struct COMPOUND4args *args);

/*
 * Read the COMPOUND4args minorversion straight off the wire, before the request
 * is decoded.  False when it cannot be determined from the first iovec, which
 * the caller must treat as "do not cache" rather than as minorversion 0.
 *
 * Exposed for unit tests (test_nfs_persist): tag_len is attacker-controlled and
 * the offset arithmetic has to stay overflow-free.
 */
bool
nfs4_v40_peek_minorversion(
    const xdr_iovec *iov,
    int              niov,
    uint32_t        *out_mv);
