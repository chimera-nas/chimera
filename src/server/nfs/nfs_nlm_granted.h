// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include "nfs4_xdr.h"
#include "nlm4_xdr.h"

struct chimera_server_nfs_shared;
struct nlm_granter;

/* RFC 1813 / XNFS blocking-lock grant delivery.
 *
 * When a blocking NLM LOCK that conflicts is finally granted (the VFS pending
 * pump fires its acquire callback with GRANTED), the server must call the
 * waiting client back with NLMPROC4_GRANTED so its F_SETLKW state machine
 * completes.  The grant is delivered asynchronously on a dedicated evpl_thread
 * (the "granter"): for each job the granter resolves the client's NLM callback
 * port via the client's portmapper (prog 100021, vers 4), connects, sends the
 * GRANTED call carrying the original cookie + lock, and retransmits on a timer
 * until the client acks (or a retry cap is reached).
 *
 * Grant jobs are fully self-contained snapshots (no pointer back into the
 * nlm_lock_entry / RPC context), so the lock entry can be released by CANCEL,
 * UNLOCK, disconnect, or shutdown without any use-after-free in the granter. */

/* A blocking-lock grant to deliver to a waiting client.  All fields are copied
 * by value; nothing here aliases server lock state. */
struct nlm_grant_request {
    char     client_addr[80];           /* client IP (no port); portmap target */
    char     caller_name[LM_MAXSTRLEN + 1];
    uint8_t  cookie[LM_MAXSTRLEN];
    uint32_t cookie_len;
    uint8_t  fh[NFS4_FHSIZE];           /* wrapped wire FH (as the client sent) */
    uint32_t fh_len;
    uint8_t  oh[LM_MAXSTRLEN];
    uint32_t oh_len;
    int32_t  svid;
    uint64_t offset;
    uint64_t length;                    /* UINT64_MAX == to EOF (wire convention) */
    int      exclusive;
};

/* Lazily create the granter for this server (idempotent; thread-safe under
 * nlm_state.mutex by the caller).  Returns NULL on failure (grant delivery is
 * then skipped -- the lock is still held, the client just retransmits). */
struct nlm_granter *
nlm_granter_get_or_create(
    struct chimera_server_nfs_shared *shared);

/* Hand a grant job to the granter.  Copies *req; the caller retains ownership
 * of req (typically a stack buffer).  Safe to call from any server thread. */
void
nlm_granter_submit(
    struct nlm_granter             *granter,
    const struct nlm_grant_request *req);

/* Stop and free the granter (called from nfs_server_destroy).  Joins the
 * granter thread, aborting any in-flight grant jobs cleanly.  NULL-safe. */
void
nlm_granter_destroy(
    struct nlm_granter *granter);
