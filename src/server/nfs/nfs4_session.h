// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdatomic.h>
#include <stdbool.h>
#include <uthash.h>

#include "nfs4_xdr.h"
#include "nfs_internal.h"

struct evpl_rpc2_conn;
struct chimera_vfs_thread;
struct nfs_client;
struct nfs_state_table;
struct nfs_request;

/* RPC slot table caps (NFS4.1 SEQUENCE replay cache).  These are *server*
 * ceilings on what a client can negotiate at CREATE_SESSION; the negotiated
 * value is min(client_request, server_cap). */
#define NFS4_MAX_REPLY_CACHE_SLOTS    64u
#define NFS4_MAX_CACHED_RESPONSE_SIZE (64u * 1024u)
#define NFS4_MAX_REPLY_CACHE_BYTES    (4u * 1024u * 1024u)

enum nfs4_slot_state {
    NFS4_SLOT_UNUSED      = 0,
    NFS4_SLOT_IN_PROGRESS = 1,
    NFS4_SLOT_CACHED      = 2, /* completed; cachethis=true; bytes held */
    NFS4_SLOT_COMPLETED   = 3, /* completed; cachethis=false; no bytes */
};

/* Set on req->replay_action by the SEQUENCE handler so the compound
 * dispatcher knows whether to execute the compound normally or to
 * short-circuit and replay a cached reply. */
#define NFS4_REPLAY_ACTION_NONE       0
#define NFS4_REPLAY_ACTION_NEW        1
#define NFS4_REPLAY_ACTION_FROM_CACHE 2

struct nfs4_replay_slot {
    uint32_t seqid;                    /* last completed seqid (in CACHED/COMPLETED) */
    uint32_t inflight_seqid; /* seqid being processed when IN_PROGRESS */
    uint8_t  state;                    /* enum nfs4_slot_state */
    uint8_t  cachethis;      /* sa_cachethis from current inflight */
    uint32_t cached_len;     /* bytes in cached_buf (CACHED only) */
    void    *cached_buf;     /* RPC reply (header+body); malloc'd */
};

/* Magic stored in nfs4_session.magic so that a connection's private_data
 * (which may hold either an nlm_client* or an nfs4_session*) can be safely
 * type-checked in the disconnect handler.  Must match the layout of
 * nlm_client (uint32_t magic at offset 0). */
#define NFS4_SESSION_MAGIC 0x4E465353U /* "NFSS" */

/* Identity of the RPC principal that established a client record, captured at
 * EXCHANGE_ID time.  Used for the RFC 8881 §18.35.4 record-matching cases,
 * which distinguish callers by principal as well as by boot verifier. */
struct nfs4_client_principal {
    uint32_t    flavor;
    uint32_t    uid;
    uint32_t    gid;
    const char *machinename;
    uint32_t    machinename_len;
};

struct nfs4_client {
    uint64_t              nfs4_client_id;
    uint32_t              nfs4_client_owner_len;
    uint32_t              nfs4_client_refcnt;
    uint32_t              nfs4_client_proto;
    uint64_t              nfs4_client_verifier;
    struct UT_hash_handle nfs4_client_hh_by_owner;
    struct UT_hash_handle nfs4_client_hh_by_id;
    uint8_t               nfs4_client_owner[NFS4_OPAQUE_LIMIT];
    char                  nfs4_client_domain[NFS4_OPAQUE_LIMIT];
    char                  nfs4_client_name[NFS4_OPAQUE_LIMIT];
    /* RFC 8881 §18.35: a record is "unconfirmed" until the client proves
     * possession of the clientid via a successful CREATE_SESSION; only then
     * is it "confirmed". */
    uint8_t               nfs4_client_confirmed;
    /* Principal that created the record (see nfs4_client_principal). */
    uint32_t              nfs4_client_princ_flavor;
    uint32_t              nfs4_client_princ_uid;
    uint32_t              nfs4_client_princ_gid;
    uint32_t              nfs4_client_princ_mach_len;
    char                  nfs4_client_princ_mach[NFS4_OPAQUE_LIMIT];
    /* Client-reboot (RFC 8881 §18.35.4 case 5): when a confirmed record is
     * replaced by a new EXCHANGE_ID from the same principal with a new boot
     * verifier, the new (unconfirmed) record records the old clientid here.
     * The old record stays live until this new one is confirmed at
     * CREATE_SESSION, at which point the old record and its sessions are torn
     * down.  0 means "supersedes nothing". */
    uint64_t              nfs4_client_supersedes_id;
    /* Unified state hierarchy.  Created when this nfs4_client is first
     * registered; freed when the nfs4_client is unregistered or the table
     * is torn down.  See nfs4_state.h. */
    struct nfs_client    *unified;
};

/* Outcome of the EXCHANGE_ID record-matching state machine.  The handler acts
 * on `status`; on NFS4_OK it returns `clientid`/`confirmed` to the client.
 * `destroy_unified`, if non-NULL, is a superseded client's state hierarchy the
 * caller must tear down with nfs_client_destroy AFTER dropping the table lock
 * (nfs4_client_exchange_id only unhashes/frees the bookkeeping struct). */
struct nfs4_exchange_id_result {
    nfsstat4           status;
    uint64_t           clientid;
    uint32_t           confirmed;
    struct nfs_client *destroy_unified;
};

struct nfs4_session {
    /* magic MUST be the first member -- see NFS4_SESSION_MAGIC comment. */
    uint32_t                 magic;
    /* Cached pointer to the owning client's unified state hierarchy.  Set
     * at session creation; cleared at table teardown (when the unified
     * client is destroyed).  Borrowed reference: lifetime matches the
     * nfs4_client this session belongs to. */
    struct nfs_client       *client_unified;
    _Atomic uint32_t         refcount;
    _Atomic bool             destroyed;
    uint8_t                  nfs4_session_id[NFS4_SESSIONID_SIZE];
    uint64_t                 nfs4_session_clientid;
    /* Guards mutable session-level state: the 4.1 replay slot table and
     * its byte accounting.  Stateid lookups don't need this -- they go
     * through the per-shard rwlocks in nfs_state_table. */
    pthread_mutex_t          nfs4_session_lock;
    uint32_t                 nfs4_session_implicit;
    struct nfs4_client      *nfs4_session_client;
    struct channel_attrs4    nfs4_session_fore_attrs;
    struct channel_attrs4    nfs4_session_back_attrs;
    uint32_t                 nfs4_session_fore_attrs_rdma_ird;
    uint32_t                 nfs4_session_back_attrs_rdma_ird;
    /* NFS4.1 SEQUENCE replay cache (per-session slot table).  Distinct
     * mechanism from the 4.0 per-owner replay in nfs_open_owner.replay /
     * nfs_lock_owner.replay -- the two coexist by minor version.  Implicit
     * sessions (4.0 SETCLIENTID path) leave replay_max_slots = 0. */
    uint32_t                 replay_max_slots;
    uint32_t                 replay_maxresp_cached;
    size_t                   replay_bytes_in_use;
    struct nfs4_replay_slot *replay_slots;
    struct UT_hash_handle    nfs4_session_hh;
};

struct nfs4_client_table {
    struct nfs4_client  *nfs4_ct_clients_by_owner;
    struct nfs4_client  *nfs4_ct_clients_by_id;
    struct nfs4_session *nfs4_ct_sessions;
    uint64_t             nfs4_ct_next_client_id;
    pthread_mutex_t      nfs4_ct_lock;
};

void
nfs4_client_table_init(
    struct nfs4_client_table *table);

void
nfs4_client_table_free(
    struct nfs4_client_table *table);

/* Walk all clients in the table and tear down each one's `unified` state
 * hierarchy via nfs_client_destroy.  Must be called while the VFS thread is
 * still live (handles get released through it).  After this returns, every
 * client->unified pointer is NULL and the state slot table is empty. */
void
nfs4_client_table_destroy_unified(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread);

uint64_t
nfs4_client_register(
    struct nfs4_client_table *table,
    const void               *owner,
    int                       owner_len,
    uint64_t                  verifier,
    uint32_t                  proto,
    const char               *nii_domain,
    const char               *nii_name);

/* RFC 8881 §18.35.4 EXCHANGE_ID client-record matching state machine.  Runs
 * entirely under the table lock and fills *out (see nfs4_exchange_id_result).
 * `update` reflects EXCHGID4_FLAG_UPD_CONFIRMED_REC_A. */
void
nfs4_client_exchange_id(
    struct nfs4_client_table           *table,
    const void                         *owner,
    int                                 owner_len,
    uint64_t                            verifier,
    const struct nfs4_client_principal *principal,
    bool                                update,
    uint8_t                             minorversion,
    struct nfs4_exchange_id_result     *out);

/* Mark a client record confirmed (first successful CREATE_SESSION).  If the
 * record supersedes an older one (client reboot), the older record and its
 * sessions are torn down and its state hierarchy is returned via
 * *destroy_unified for teardown outside the table lock.  Returns false when no
 * such client exists (caller maps to NFS4ERR_STALE_CLIENTID). */
bool
nfs4_client_confirm(
    struct nfs4_client_table *table,
    uint64_t                  client_id,
    struct nfs_client       **destroy_unified);

/* Unregister a client and tear down its unified state hierarchy.
 *
 * `state_table` and `vfs_thread` may be NULL only when the caller has
 * already invoked nfs4_client_table_destroy_unified (e.g. at shutdown);
 * otherwise both must be valid so that any state slots / dup'd VFS
 * handles still rooted under client->unified are released. */
void
nfs4_client_unregister(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread,
    uint64_t                   client_id);

struct nfs4_session *
nfs4_create_session(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     implicit,
    uint32_t                     replay_max_slots,
    uint32_t                     replay_maxresp_cached,
    const struct channel_attrs4 *fore_attrs,
    const struct channel_attrs4 *back_attrs);

/* SEQUENCE replay slot state machine.  See plan in
 * /root/.claude/plans/lets-plan-out-the-steady-steele.md for the full
 * state diagram.  Returns NFS4_OK on success (NEW request or REPLAY hit),
 * otherwise an NFS4ERR_* code (BADSLOT / SEQ_MISORDERED / RETRY_UNCACHED_REP).
 * On NFS4_OK with *out_is_replay=true the caller must short-circuit the
 * compound and replay slot->cached_buf.
 *
 * On NFS4_OK the slot pointer and slot id are stashed on req for the
 * compound dispatcher to consume at finalize time.  cachethis is recorded
 * for the in-flight request and consulted only at finalize. */
nfsstat4 nfs4_replay_slot_acquire(
    struct nfs4_session *session,
    uint32_t             slotid,
    uint32_t             seqid,
    bool                 cachethis,
    struct nfs_request  *req,
    bool                *out_is_replay);

/* Called by the compound dispatcher just before sending the reply.  In
 * Phase 1 this advances IN_PROGRESS -> COMPLETED.  Phase 2 will optionally
 * capture the encoded reply bytes and transition to CACHED. */
void nfs4_replay_slot_finalize(
    struct nfs_request *req);

struct nfs4_session *
nfs4_session_lookup(
    struct nfs4_client_table *table,
    const void               *sessionid);

void
nfs4_destroy_session(
    struct nfs4_client_table *table,
    const void               *session_id);

struct nfs4_session *
nfs4_session_find_by_clientid(
    struct nfs4_client_table *table,
    uint64_t                  client_id);

/*
 * Reference counting for nfs4_session.
 *
 * A session is referenced by:
 *   - the client table (1 ref, dropped by nfs4_destroy_session or
 *     nfs4_client_table_free)
 *   - every evpl_rpc2_conn whose private_data points to it (1 ref each,
 *     installed by nfs4_session_bind_conn, dropped by
 *     nfs4_session_unbind_conn from the disconnect notify)
 *
 * nfs4_session_lookup() and nfs4_session_find_by_clientid() both return
 * a session with +1 ref.  Callers must release that ref with
 * nfs4_session_put() (either directly, or by binding it to a conn and then
 * putting the lookup ref).
 *
 * nfs4_session_bind_conn() takes its own +1 ref; it does NOT consume the
 * caller's ref.
 */
void nfs4_session_get(
    struct nfs4_session *session);
void nfs4_session_put(
    struct nfs4_session *session);
void nfs4_session_bind_conn(
    struct evpl_rpc2_conn *conn,
    struct nfs4_session   *session);
void nfs4_session_unbind_conn(
    struct evpl_rpc2_conn *conn);

static inline bool
nfs4_session_is_live(struct nfs4_session *session)
{
    return session &&
           session->magic == NFS4_SESSION_MAGIC &&
           !atomic_load_explicit(&session->destroyed, memory_order_acquire);
} // nfs4_session_is_live
