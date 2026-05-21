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
struct evpl;
struct evpl_doorbell;
struct evpl_rpc2_conn;
struct chimera_vfs_lease;

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
    struct chimera_server_nfs_thread *owner_thread;
    struct evpl_rpc2_conn            *conn;
    /* Private copy of the NFS_V4_CB program with rpc2.program patched to the
     * client's transient callback program number (cb_program from
     * SETCLIENTID / CREATE_SESSION).  Heap-stable so async reply dispatch is
     * safe. */
    struct NFS_V4_CB                  cb_prog;
    uint8_t                           minorversion;
    uint8_t                           owns_conn;   /* 1 => disconnect on free (4.0) */
    uint32_t                          cb_ident;    /* 4.0 callback_ident */
    uint8_t                           sessionid[NFS4_SESSIONID_SIZE]; /* 4.1 */
    _Atomic uint32_t                  cb_seq;      /* 4.1 CB_SEQUENCE slot-0 seqid */
};

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
