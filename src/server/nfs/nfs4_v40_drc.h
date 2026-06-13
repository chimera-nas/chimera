// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct chimera_server_nfs_shared;

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
 * The in-memory cache is always on (it also fixes replay across a client's own
 * reconnect to the same node); persistence to the KV store is gated by
 * server.nfs4_drc + a persistent kv_module, matching the 4.1 reply cache.
 */
void
nfs4_v40_drc_install(
    struct chimera_server_nfs_shared *shared,
    int                               persist);
