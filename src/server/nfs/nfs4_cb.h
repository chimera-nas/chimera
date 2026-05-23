// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * NFSv4.1 callback (backchannel) channel: the server sends CB_COMPOUND CALLs to
 * the client over the connection the client established (RFC 8881 §2.10.3.1).
 */

struct chimera_server_nfs_thread;

#include <stdint.h>

/*
 * Two-stage, globally-correct layout recall.  Recall the layout for fh from
 * EVERY client that holds one (found via the server-wide layout table) and
 * defer the conflicting operation: resume(resume_arg) runs once every holder
 * has returned its layout (LAYOUTRETURN) or confirmed it no longer holds it
 * (NOMATCHING / transport failure / revoke).  If no layout is held, resume()
 * runs immediately.  Must be called on the connection's own thread.
 */
void
chimera_nfs4_cb_recall_and_wait(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void (                           *resume )(void *arg),
    void                             *resume_arg);
