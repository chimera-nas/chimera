// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#include "nfs4_xdr.h"

struct chimera_server_nfs_thread;
struct nfs_client;
struct nfs_delegation;
struct nfs_request;
struct nfs4_session;
struct nfs4_cb_path;
struct evpl;
struct evpl_doorbell;
struct evpl_rpc2_conn;
struct chimera_vfs_lease;

/* Marker stored at offset 0 of struct nfs4_cb_client and set as the
 * private_data of a 4.0 outbound callback connection, so the shared rpc2
 * disconnect notify can recognise the conn and invalidate the channel.  Shares
 * the offset-0 magic convention used by nfs4_session / nlm_client. */
#define NFS4_CB_CLIENT_MAGIC 0x4E464342U /* "NFCB" */

/*
 * NFSv4 delegation callback channel.
 *
 * Delivers CB_RECALL (and the CB_NULL liveness probe) to a client's callback
 * service.  For 4.0 this is a server-initiated connection to the address the
 * client gave in SETCLIENTID; for 4.1 it rides the session backchannel on the
 * fore connection.  A channel is owned by exactly one NFS thread (the one that
 * established it, which is also the thread that owns the client's fore
 * connection); all sends happen on that thread.  Recalls raised on other
 * threads are marshalled across via the owner thread's doorbell.
 */
struct nfs4_cb_client {
    /* Must be first: lets the rpc2 disconnect notify identify a 4.0 callback
     * conn from its private_data (offset-0 magic convention). */
    uint32_t                          magic;
    struct chimera_server_nfs_thread *owner_thread;
    /* 4.0: the server-owned outbound connection.  Set to NULL by
     * nfs4_cb_conn_lost() when the connection drops, so sends stop using a
     * freed conn.  Unused for 4.1 (see `session`). */
    struct evpl_rpc2_conn            *conn;
    /* 4.1: the session whose fore connection carries the backchannel (a held
     * ref).  The live conn is re-read from session->nfs4_session_backchannel_conn
     * at send time rather than cached, since that conn is freed on disconnect
     * and re-bound on reconnect.  NULL for 4.0. */
    struct nfs4_session              *session;
    /* Back-pointer to the owning path so a connection loss can mark the path
     * NFS4_CB_DOWN (stops further grants). */
    struct nfs4_cb_path              *cb_path;
    /* Private copy of the NFS_V4_CB program with rpc2.program patched to the
     * client's transient callback program number (cb_program from
     * SETCLIENTID / CREATE_SESSION).  Heap-stable so async reply dispatch is
     * safe. */
    struct NFS_V4_CB                  cb_prog;
    uint8_t                           minorversion;
    uint8_t                           owns_conn;   /* 1 => 4.0 owned conn */
    uint32_t                          cb_ident;    /* 4.0 callback_ident */
    uint8_t                           sessionid[NFS4_SESSIONID_SIZE]; /* 4.1 */
    _Atomic uint32_t                  cb_seq;      /* 4.1 CB_SEQUENCE slot-0 seqid */
};

/* Invalidate a 4.0 callback channel whose outbound connection has dropped.
 * Called from the rpc2 disconnect notify on the channel's owner thread (the
 * only thread that sends on it), so simply clearing the conn is race-free. */
void nfs4_cb_conn_lost(
    struct nfs4_cb_client *chan);

struct nfs4_cb_path;

/* Tear down a client's callback channel (free the channel struct, disconnect a
 * 4.0 outbound connection).  Called from nfs_client_destroy. */
void nfs4_cb_path_teardown(
    struct nfs4_cb_path *cb);

/* Per-thread doorbell + recall queue setup/teardown. */
void nfs4_cb_thread_init(
    struct chimera_server_nfs_thread *thread);
void nfs4_cb_thread_destroy(
    struct chimera_server_nfs_thread *thread);

/*
 * Ensure the calling client's callback path is usable, kicking a CB_NULL
 * probe if it has not been validated yet.  Must be called on the thread that
 * owns the client's fore connection (i.e. from the OPEN handler).  Returns
 * true only when the path is confirmed UP -- the OPEN handler grants a
 * delegation only in that case (RFC 7530 §10.1).
 */
bool nfs4_cb_ensure_probe(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client                *client,
    struct nfs_request               *req);

/*
 * Begin recalling `deleg` by sending CB_RECALL to its client.  Safe to call
 * from any thread; the work is marshalled to the channel's owner thread.
 * Takes a transient ref on `deleg` for the in-flight recall.  If no usable
 * callback path exists, the delegation's lease is revoked so the conflicting
 * acquirer can proceed.
 */
void nfs4_cb_recall(
    struct nfs_delegation *deleg);

/* vfs_state break_cb wired onto every delegation's CACHING lease. */
void nfs4_delegation_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data);

/*
 * CB_GETATTR (RFC 8881 §20.1): query the holder of a write delegation for the
 * file's current change/size during another client's GETATTR.  Resumed
 * asynchronously: `resume` is invoked on the REQUESTER's thread once the holder
 * has replied (or the query failed).  `status` is 0 on success.  The caller
 * passes a +1 ref on `deleg`; this module releases it when the query completes.
 */
typedef void (*nfs4_cb_getattr_resume_t)(
    void    *priv,
    int      status,
    bool     got_change,
    uint64_t change,
    bool     got_size,
    uint64_t size);

void nfs4_cb_getattr(
    struct chimera_server_nfs_thread *requester_thread,
    struct nfs_delegation            *deleg,
    void                             *priv,
    nfs4_cb_getattr_resume_t          resume);

/* Return the write delegation held on `fh` by a client other than
 * `querying_client_id` (with a +1 ref the caller must release via the state
 * table), or NULL if none.  Used by GETATTR to decide whether to CB_GETATTR. */
struct nfs_delegation *
nfs4_find_conflicting_write_deleg(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    uint64_t                          querying_client_id);

