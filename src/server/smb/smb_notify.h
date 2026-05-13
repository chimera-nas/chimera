// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>
#include "vfs/vfs_notify.h"

struct chimera_smb_request;
struct chimera_smb_conn;
struct chimera_smb_open_file;
struct chimera_smb_tree;
struct chimera_server_smb_thread;

/*
 * Hard cap on the size of a single CHANGE_NOTIFY response buffer.
 * Sized to fit a comfortable batch of events without letting a hostile
 * client (or misbehaving Windows version) demand multi-megabyte
 * server-side allocations.  Beyond this cap the server escalates to
 * STATUS_NOTIFY_ENUM_DIR and the client rescans.
 */
#define CHIMERA_SMB_NOTIFY_MAX_RESP 65536

/*
 * Parked CHANGE_NOTIFY request.
 *
 * When a CHANGE_NOTIFY arrives and no events are pending, the request
 * is "parked" — we store enough state to build an async response later
 * when events arrive.
 */
struct chimera_smb_notify_request {
    struct chimera_smb_conn           *conn;
    struct chimera_server_smb_thread  *thread;
    struct chimera_smb_open_file      *open_file;
    struct chimera_smb_tree           *tree;        /* for open_file refcount release */
    uint64_t                           message_id;
    uint64_t                           async_id;
    uint64_t                           session_id;
    uint32_t                           tree_id;
    uint16_t                           credit_charge;
    uint16_t                           credit_request;
    uint32_t                           output_buffer_length;
    uint32_t                           completion_filter;
    uint16_t                           flags;
    int                                watch_tree;
    /* Signing state captured at park time so async responses can be
    * properly signed even after the originating request is gone. */
    int                                signed_session;
    uint8_t                            signing_key[16];
    /* Set under state->lock when callback has queued this nr to
     * thread->notify_ready.  Cleared by send_response or by close
     * when reaping the request from the ready queue. */
    int                                on_ready_queue;
    struct chimera_smb_notify_request *next;          /* parked list linkage */
    struct chimera_smb_notify_request *prev;          /* parked list linkage */
    struct chimera_smb_notify_request *ready_next;    /* doorbell ready queue */
};

/*
 * Per-open-file notify state.
 * Attached to chimera_smb_open_file when a CHANGE_NOTIFY is issued.
 *
 * `lock` brackets transitions between (drain ring + observe pending) and
 * (claim pending in callback).  Without it the drain/park sequence in the
 * change_notify handler races against a concurrent VFS callback: the
 * callback can run between drain and park, see pending == NULL, return,
 * and the freshly-parked request never gets woken up.
 */
struct chimera_smb_notify_state {
    pthread_mutex_t                    lock;
    struct chimera_vfs_notify_watch   *watch;
    struct chimera_smb_notify_request *pending;  /* parked request, or NULL */
};

/*
 * VFS notify callback — called when events arrive on a watched directory.
 * May be called from any thread.  Queues the pending request to the SMB
 * thread's doorbell for safe processing.
 */
void
chimera_smb_notify_callback(
    struct chimera_vfs_notify_watch *watch,
    void                            *private_data);

/*
 * Initialize the per-thread notify doorbell.
 * Must be called during SMB thread setup.
 */
void
chimera_smb_notify_thread_init(
    struct chimera_server_smb_thread *thread);

/*
 * Tear down the per-thread notify doorbell.
 */
void
chimera_smb_notify_thread_destroy(
    struct chimera_server_smb_thread *thread);

/*
 * Send an interim STATUS_PENDING response for a parked request.
 * Called from the CHANGE_NOTIFY handler on the SMB thread.
 */
void
chimera_smb_notify_send_interim(
    struct chimera_smb_notify_request *nr);

/*
 * Send async CHANGE_NOTIFY response for a parked request.
 * Called from the SMB thread context.
 */
void
chimera_smb_notify_send_response(
    struct chimera_smb_notify_request *nr);

/*
 * Cancel a parked CHANGE_NOTIFY request and send STATUS_CANCELLED to
 * the client.  Use this on explicit SMB2_CLOSE.
 */
void
chimera_smb_notify_cancel(
    struct chimera_smb_notify_request *nr);

/*
 * Drop a parked CHANGE_NOTIFY request without sending a reply.  Used
 * during connection teardown where the bind is going away and any
 * send would race the destroy.
 */
void
chimera_smb_notify_drop(
    struct chimera_smb_notify_request *nr);

/*
 * Clean up notify state for an open file being closed.
 */
void
chimera_smb_notify_close(
    struct chimera_vfs_notify       *vfs_notify,
    struct chimera_smb_notify_state *state);

/*
 * Serialize VFS notify events into FILE_NOTIFY_INFORMATION records.
 * Returns the number of bytes written to `out`.  RENAMED events produce
 * two records (OLD_NAME + NEW_NAME).  Events are serialized atomically:
 * if a record from event N does not fit, all of event N is rolled back
 * and `*events_consumed` reports N.  Callers MUST check this against
 * `nevents` and escalate to STATUS_NOTIFY_ENUM_DIR when not all events
 * fit, otherwise un-serialized events are silently dropped.
 * `events_consumed` may be NULL if the caller has externally guaranteed
 * the buffer is large enough.
 */
struct chimera_smb_iconv_ctx;

int
chimera_smb_notify_serialize_events(
    struct chimera_smb_iconv_ctx    *iconv_ctx,
    struct chimera_vfs_notify_event *events,
    int                              nevents,
    uint8_t                         *out,
    int                              out_size,
    int                             *events_consumed);

/*
 * Map an SMB2 CHANGE_NOTIFY CompletionFilter (Windows-style bits) to
 * the chimera VFS event mask.  Exposed so the async response builder
 * can re-filter ring contents against the *current request's* filter
 * — the per-watch filter on the VFS side may have been broader when
 * earlier events were enqueued.
 */
uint32_t
chimera_smb_map_completion_filter(
    uint32_t cf);

/*
 * Allocate a notify request.
 */
struct chimera_smb_notify_request *
chimera_smb_notify_request_alloc(
    void);

/*
 * Free a notify request, releasing the held open_file reference.
 */
void
chimera_smb_notify_request_free(
    struct chimera_smb_notify_request *nr);
