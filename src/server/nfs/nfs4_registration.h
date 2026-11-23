// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * SETCLIENTID / EXCHANGE_ID registration driven by the client ID allocator.
 *
 * nfs4_registration_run() classifies the request.  When the RFC case needs a
 * fresh client ID it reserves the owner, asks the allocator (asynchronously),
 * classifies again with the new ID when the allocator answers, finalizes the
 * registration on the original request's thread, removes the reservation and
 * replies to the original request and to every identical request that was
 * coalesced onto it -- each on the thread that received it.  Allocator
 * failures map to NFS4ERR_DELAY (transient) or NFS4ERR_SERVERFAULT
 * (permanent).
 */

#include <stdbool.h>
#include "nfs4_session.h"

struct chimera_server_nfs_thread;
struct nfs_request;
struct nfs_argop4;
struct nfs_resop4;

void
nfs4_registration_run(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop,
    const struct nfs4_reg_args       *args);

/* Fill and send the reply for one request (implemented by the handlers).
 * `primary` is the request that started the registration; per-registration
 * side effects that must happen once (recovery persistence) run only there. */
void
chimera_nfs4_setclientid_reply(
    struct chimera_server_nfs_thread     *thread,
    struct nfs_request                   *req,
    struct nfs_argop4                    *argop,
    struct nfs_resop4                    *resop,
    const struct nfs4_setclientid_result *scid);

void
chimera_nfs4_exchange_id_reply(
    struct chimera_server_nfs_thread     *thread,
    struct nfs_request                   *req,
    struct nfs_argop4                    *argop,
    struct nfs_resop4                    *resop,
    const struct nfs4_exchange_id_result *eid,
    bool                                  primary);
