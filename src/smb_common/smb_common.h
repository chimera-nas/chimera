// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Shared foundation for the SMB2 server (src/server/smb) and the SMB2 client
 * VFS module (src/vfs/smb), in the same relationship src/nfs_common has with
 * the NFS server and the NFS proxy: neither side depends on the other, both
 * depend on this.
 *
 * What belongs here is the part of SMB2 that is not a matter of policy -- the
 * wire format, and the cryptography and codecs whose outputs the two sides must
 * agree on BYTE FOR BYTE or the protocol fails.  Before this component existed
 * the client mirrored the server's signing, encryption and compression by hand,
 * and a mirror that drifts does not produce a compile error or a failed
 * assertion; it produces a session that negotiates cleanly and then fails every
 * MAC or tag check, which is the most expensive kind of bug to chase.
 *
 * What does NOT belong here is anything either side decides for itself:
 * negotiation policy (the server selects, the client offers), session and
 * connection state, and the direction a key is used in -- that last one is a
 * parameter to the shared derivation, not a reason to have two copies of it.
 */

#include "common/logging.h"
#include "common/macros.h"

/* Logging domain for code that belongs to neither side.  The server logs under
 * "smb" and the client under "smbclient"; shared code cannot claim either, so
 * it logs under "smb2" -- the protocol rather than an endpoint. */
#define chimera_smb2_debug(...) chimera_debug("smb2", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_smb2_info(...)  chimera_info("smb2", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_smb2_error(...) chimera_error("smb2", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_smb2_fatal(...) chimera_fatal("smb2", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_smb2_abort_if(cond, ...) \
        chimera_abort_if(cond, "smb2", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_smb2_abort(...) \
        chimera_abort("smb2", __FILE__, __LINE__, __VA_ARGS__)
