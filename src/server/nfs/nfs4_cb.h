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

/*
 * Recall the layout `holder` from its client.  The CB_LAYOUTRECALL rides the
 * client's backchannel connection, which is owned by a single thread's evpl
 * (evpl sends are not cross-thread safe).  When called off that owner thread,
 * this self-bounces: it pins `holder` with a ref, queues it on the owner's
 * cb_layoutrecall_queue, and rings cb_doorbell -- the doorbell drain re-enters
 * this function on the owner thread and sends inline.  `holder` is pinned by
 * the caller across the call.
 */
struct nfs_layout_state;
void
nfs4_cb_recall_holder(
    struct chimera_server_nfs_thread *thread,
    struct nfs_layout_state          *holder);

/*
 * Run deferred-operation resumes that were bounced to `thread` (their home
 * thread) by nfs4_cb_resume_bounce.  Called from the cb_doorbell drain.
 */
void
nfs4_cb_drain_resume_queue(
    struct chimera_server_nfs_thread *thread);
