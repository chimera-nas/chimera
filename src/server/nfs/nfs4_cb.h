// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * NFSv4.1 callback (backchannel) channel: the server sends CB_COMPOUND CALLs to
 * the client over the connection the client established (RFC 8881 §2.10.3.1).
 */

struct chimera_server_nfs_thread;

#include <stdbool.h>
#include <stdint.h>

#include "vfs/vfs_ref.h"

struct chimera_server_nfs_shared;
struct prometheus_counter;
struct prometheus_counter_series;
struct prometheus_counter_instance;

/* chimera_nfs4_layout_recall{cause} and chimera_nfs4_layout_revoke{why}. */
enum nfs4_layout_recall_cause {
    NFS4_LAYOUT_RECALL_CONFLICT,  /* a conflicting op (SETATTR size) */
    NFS4_LAYOUT_RECALL_UNLINKED,  /* the unlinked file lost its last open */
    NFS4_LAYOUT_RECALL_UMOUNT,    /* the file's mount is being unmounted */
    NFS4_LAYOUT_RECALL_NUM
};

enum nfs4_layout_revoke_why {
    NFS4_LAYOUT_REVOKE_DEADLINE,  /* recall unanswered for two leases */
    NFS4_LAYOUT_REVOKE_REFUSED,   /* the client answered the recall with an error */
    NFS4_LAYOUT_REVOKE_NUM
};

struct nfs4_layout_metrics {
    struct prometheus_counter          *recall;
    struct prometheus_counter_series   *recall_series[NFS4_LAYOUT_RECALL_NUM];
    struct prometheus_counter_instance *recalls[NFS4_LAYOUT_RECALL_NUM];
    struct prometheus_counter          *revoke;
    struct prometheus_counter_series   *revoke_series[NFS4_LAYOUT_REVOKE_NUM];
    struct prometheus_counter_instance *revokes[NFS4_LAYOUT_REVOKE_NUM];
};

void
nfs4_layout_metrics_init(
    struct chimera_server_nfs_shared *shared);

void
nfs4_layout_metrics_destroy(
    struct chimera_server_nfs_shared *shared);

/*
 * The VFS's recall callback, registered with every layout's reference on its
 * file (chimera_vfs_recallable.fn; `arg` is the server's nfs_layout_table).
 * Runs on the VFS close thread, outside every VFS lock, when an unlinked file
 * has no real open left or its mount is going.  Starts a recall of every layout
 * on the file and returns at once: nothing waits for the clients, and the
 * overdue sweep bounds a holder that never answers.
 */
void
nfs4_layout_vfs_recall(
    const uint8_t                 *fh,
    uint16_t                       fh_len,
    enum chimera_vfs_recall_reason reason,
    void                          *arg);

/*
 * Two-stage, globally-correct layout recall.  Recall the layout for fh from
 * EVERY client that holds one (found via the server-wide layout table) and
 * defer the conflicting operation: resume(resume_arg) runs once every holder
 * has returned its layout (LAYOUTRETURN) or confirmed it no longer holds it
 * (NOMATCHING / transport failure / revoke).  Holders are recalled in batches
 * until all have been asked; a call that overlaps a recall already active on
 * fh joins it as one more waiter and sends no second CB_LAYOUTRECALL.  If no
 * layout is held, resume() runs immediately; otherwise it runs later, on this
 * thread, from its cb_doorbell drain.  Must be called on the connection's own thread.
 */
void
chimera_nfs4_cb_recall_and_wait(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void (                           *resume )(
        void *arg),
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

/*
 * Withdraw the resume a chimera_nfs4_cb_recall_and_wait(fh, ..., resume_arg)
 * parked and that has not fired yet.  True: withdrawn, resume will never run.
 * False: it already fired, and its resume has run or is queued for this
 * thread's cb_doorbell drain, so on false the caller must keep the operation
 * alive until that resume has run.  Cancel at most once, and only while the
 * operation is still pending.  Call on the thread that parked it.
 */
bool
chimera_nfs4_cb_recall_cancel(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void                             *resume_arg);

/* Revoke each recalled layout still held two lease periods after its recall.
 * Called from the per-thread lease sweep. */
void
nfs4_cb_revoke_overdue_layouts(
    struct chimera_server_nfs_thread *thread);
