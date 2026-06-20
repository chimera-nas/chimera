// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>
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

/*
 * Per-slot state lives in a single atomic word so the SEQUENCE hot path needs
 * no per-session lock: bits [1:0] hold enum nfs4_slot_state, bits [33:2] hold
 * the 32-bit seqid (the in-flight seqid while IN_PROGRESS, otherwise the last
 * completed seqid).  calloc() zero-init == (seqid 0, NFS4_SLOT_UNUSED).
 *
 * Distinct slots are independent (RFC 5661: a client has at most one
 * outstanding request per slot id), so concurrent SEQUENCEs touch different
 * words and never contend.  The only same-slot race is a retransmit arriving on
 * another connection/thread; it is arbitrated by the CAS in
 * nfs4_replay_slot_acquire.  cached_buf/cached_len are written by the (rare)
 * cachethis capture path on the owning thread and published via the release
 * store in nfs4_replay_slot_finalize.
 */
struct nfs4_replay_slot {
    _Atomic uint64_t state_word;       /* (seqid << NFS4_SLOT_SEQID_SHIFT) | state */
    uint32_t         cached_len;       /* bytes in cached_buf (CACHED only) */
    void            *cached_buf;       /* RPC reply (header+body); malloc'd */
    /* Diagnostics for a wedged compound: when the slot entered IN_PROGRESS
     * and when a stuck-slot report was last logged for it.  Written only by
     * the CAS winner / the retry path observing it, both monotonic-seconds;
     * plain (non-atomic) is fine for a rate-limited log. */
    time_t           in_progress_since;
    time_t           last_stuck_report;
};

#define NFS4_SLOT_STATE_MASK  0x3u
#define NFS4_SLOT_SEQID_SHIFT 2

/* Coarse monotonic seconds for the slot-stuck diagnostics; off every hot
 * comparison except the IN_PROGRESS transitions and the already-failing
 * retry path. */
static inline time_t
nfs4_slot_now(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return ts.tv_sec;
} /* nfs4_slot_now */

static inline uint64_t
nfs4_slot_word(
    uint32_t             seqid,
    enum nfs4_slot_state state)
{
    return ((uint64_t) seqid << NFS4_SLOT_SEQID_SHIFT) | (uint64_t) state;
} /* nfs4_slot_word */

static inline enum nfs4_slot_state
nfs4_slot_state(struct nfs4_replay_slot *slot)
{
    return (enum nfs4_slot_state) (atomic_load_explicit(&slot->state_word,
                                                        memory_order_acquire) &
                                   NFS4_SLOT_STATE_MASK);
} /* nfs4_slot_state */

static inline uint32_t
nfs4_slot_seqid(struct nfs4_replay_slot *slot)
{
    return (uint32_t) (atomic_load_explicit(&slot->state_word,
                                            memory_order_acquire) >>
                       NFS4_SLOT_SEQID_SHIFT);
} /* nfs4_slot_seqid */

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

/* SP4_MACH_CRED enforcement: 1 if `p` may operate on client `c` (no protection,
 * or the bound machine principal matches); 0 -> reject with NFS4ERR_WRONG_CRED. */
struct nfs4_client;
int
nfs4_client_mach_cred_ok(
    const struct nfs4_client           *c,
    const struct nfs4_client_principal *p);

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
    /* Principal that created the record (see nfs4_client_principal).  Under
     * SP4_MACH_CRED this is the bound "machine credential": state-management
     * operations must be issued with this same (RPCSEC_GSS) principal. */
    uint32_t              nfs4_client_princ_flavor;
    uint32_t              nfs4_client_princ_uid;
    uint32_t              nfs4_client_princ_gid;
    uint32_t              nfs4_client_princ_mach_len;
    char                  nfs4_client_princ_mach[NFS4_OPAQUE_LIMIT];
    /* Negotiated state-protection mode (RFC 8881 §2.10.8.3): SP4_NONE (0,
     * default), SP4_MACH_CRED, or SP4_SSV.  When SP4_MACH_CRED, the enforced
     * state-management ops require the bound principal above. */
    uint32_t              nfs4_client_sp_how;
    /* Client-reboot (RFC 8881 §18.35.4 case 5): when a confirmed record is
     * replaced by a new EXCHANGE_ID from the same principal with a new boot
     * verifier, the new (unconfirmed) record records the old clientid here.
     * The old record stays live until this new one is confirmed at
     * CREATE_SESSION, at which point the old record and its sessions are torn
     * down.  0 means "supersedes nothing". */
    uint64_t              nfs4_client_supersedes_id;
    /* CREATE_SESSION sequencing + single-entry reply cache (RFC 8881
     * §18.36.4).  cs_seqid is the last csa_sequence processed, initialised to
     * eir_sequenceid - 1 (= 0).  When cs_have_reply is set, the cached fields
     * below reproduce the reply for a retransmit of cs_seqid. */
    uint32_t              nfs4_client_cs_seqid;
    uint8_t               nfs4_client_cs_have_reply;
    nfsstat4              nfs4_client_cs_reply_status;
    uint8_t               nfs4_client_cs_reply_sessionid[NFS4_SESSIONID_SIZE];
    uint32_t              nfs4_client_cs_reply_sequence;
    uint32_t              nfs4_client_cs_reply_flags;
    /* Negotiated channel attrs.  Stored with ca_rdma_ird cleared (the pointer
     * would otherwise dangle once the session is freed); the RDMA ird is not
     * preserved across a CREATE_SESSION retransmit, which no client relies on. */
    struct channel_attrs4 nfs4_client_cs_reply_fore;
    struct channel_attrs4 nfs4_client_cs_reply_back;
    /* Set once the client has issued RECLAIM_COMPLETE; a second one is
     * NFS4ERR_COMPLETE_ALREADY (RFC 8881 §18.51.4). */
    uint8_t               nfs4_client_reclaim_complete;
    /* NFSv4.0 SETCLIENTID/SETCLIENTID_CONFIRM handshake (RFC 7530 §16.33/16.34).
     * When a SETCLIENTID leaves a confirm pending on this record, the 8-byte
     * verifier the client must echo back is stored here and scid_confirm_valid
     * is set.  For a *confirmed* record, scid_pending_id names a separate
     * superseding unconfirmed record (a reboot with a new verifier created a
     * new clientid); 0 = none. */
    uint8_t               nfs4_client_scid_confirm[NFS4_VERIFIER_SIZE];
    uint8_t               nfs4_client_scid_confirm_valid;
    uint64_t              nfs4_client_scid_pending_id;
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
    uint32_t                          magic;
    /* Cached pointer to the owning client's unified state hierarchy.  Set
     * at session creation; cleared at table teardown (when the unified
     * client is destroyed).  Borrowed reference: lifetime matches the
     * nfs4_client this session belongs to. */
    struct nfs_client                *client_unified;
    _Atomic uint32_t                  refcount;
    _Atomic bool                      destroyed;
    uint8_t                           nfs4_session_id[NFS4_SESSIONID_SIZE];
    uint64_t                          nfs4_session_clientid;
    uint32_t                          nfs4_session_implicit;
    struct nfs4_client               *nfs4_session_client;
    struct channel_attrs4             nfs4_session_fore_attrs;
    struct channel_attrs4             nfs4_session_back_attrs;
    uint32_t                          nfs4_session_fore_attrs_rdma_ird;
    uint32_t                          nfs4_session_back_attrs_rdma_ird;
    /* NFS4.1 SEQUENCE replay cache (per-session slot table).  Distinct
     * mechanism from the 4.0 per-owner replay in nfs_open_owner.replay /
     * nfs_lock_owner.replay -- the two coexist by minor version.  Implicit
     * sessions (4.0 SETCLIENTID path) leave replay_max_slots = 0. */
    uint32_t                          replay_max_slots;
    uint32_t                          replay_maxresp_cached;
    _Atomic size_t                    replay_bytes_in_use;
    struct nfs4_replay_slot          *replay_slots;
    /* NFSv4.1 backchannel for delegation recalls (RFC 8881 §2.10.3.1).
     * Set at CREATE_SESSION when CREATE_SESSION4_FLAG_CONN_BACK_CHAN is
     * requested: cb_program is the client's callback program number and
     * backchannel_conn is the fore connection the session was created on,
     * which doubles as the server->client callback transport.  The conn is
     * a borrowed pointer kept live by the session<->conn binding; it is
     * cleared by nfs4_session_unbind_conn on disconnect. */
    uint32_t                          nfs4_session_cb_program;
    struct evpl_rpc2_conn            *nfs4_session_backchannel_conn;
    /* The thread that owns nfs4_session_backchannel_conn's evpl (set whenever
     * the backchannel conn is (re)assigned, on that conn's own handler thread).
     * Callbacks must be sent from this thread -- evpl sends are not cross-thread
     * safe -- so cross-thread recalls marshal to it (nfs4_cb_recall_holder). */
    struct chimera_server_nfs_thread *nfs4_session_backchannel_owner;
    /* CREATE_SESSION4_FLAG_PERSIST was advertised for this session (server
     * nfs4_drc on AND client requested it): the per-slot reply cache is
     * written through to the KV store so retransmits replay across a restart. */
    bool                              nfs4_session_persist;
    struct UT_hash_handle             nfs4_session_hh;
};

struct nfs4_client_table {
    struct nfs4_client  *nfs4_ct_clients_by_owner;
    struct nfs4_client  *nfs4_ct_clients_by_id;
    struct nfs4_session *nfs4_ct_sessions;
    /* Low 48 bits of the next clientid; the high 16 are this instance's node_id
     * (see nfs4_make_clientid) so peers sharing one store never collide. */
    uint64_t             nfs4_ct_next_client_id;
    uint16_t             nfs4_ct_node_id;
    /* Monotonic source for SETCLIENTID setclientid_confirm verifiers; every
     * value handed out is unique for the life of the table. */
    uint64_t             nfs4_ct_next_confirm;
    pthread_mutex_t      nfs4_ct_lock;
};

/* A clientid carries the minting instance's node_id in its high 16 bits and a
 * per-instance counter in the low 48, so two instances over one backing store
 * never hand out the same value (and `clientid >> 48` is the owning node). */
#define NFS4_CLIENTID_COUNTER_MASK 0xFFFFFFFFFFFFULL
#define nfs4_make_clientid(node_id, counter) \
        (((uint64_t) (node_id) << 48) | ((counter)&NFS4_CLIENTID_COUNTER_MASK))

void
nfs4_client_table_init(
    struct nfs4_client_table *table,
    uint16_t                  node_id);

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

/* Outcome of the NFSv4.0 SETCLIENTID record-matching state machine
 * (RFC 7530 §16.33.5).  On NFS4_OK the handler returns `clientid` and the
 * `confirm` verifier the client must echo in SETCLIENTID_CONFIRM. */
struct nfs4_setclientid_result {
    nfsstat4           status;
    uint64_t           clientid;
    uint8_t            confirm[NFS4_VERIFIER_SIZE];
    /* A replaced unconfirmed record's state hierarchy, to be torn down with
     * nfs_client_destroy after the table lock is dropped (NULL if none). */
    struct nfs_client *destroy_unified;
};

/* RFC 7530 §16.33.5 SETCLIENTID record matching.  Records (or updates) an
 * unconfirmed record for the client owner and fills *out.  May return
 * NFS4ERR_CLID_INUSE when a confirmed record owned by a different principal
 * still holds open/lock state. */
void
nfs4_client_setclientid(
    struct nfs4_client_table           *table,
    const void                         *owner,
    int                                 owner_len,
    uint64_t                            verifier,
    const struct nfs4_client_principal *principal,
    uint8_t                             minorversion,
    struct nfs4_setclientid_result     *out);

/* RFC 7530 §16.34 SETCLIENTID_CONFIRM.  Confirms the (unconfirmed) record
 * named by `clientid`/`confirm`.  On a client reboot this promotes the
 * superseding record and tears the old one down, returning its unified state
 * hierarchy via *destroy_unified for teardown outside the table lock.
 * Returns NFS4ERR_STALE_CLIENTID (no such clientid) or NFS4ERR_CLID_INUSE
 * (confirm verifier mismatch) on failure. */
nfsstat4
nfs4_client_setclientid_confirm(
    struct nfs4_client_table *table,
    uint64_t                  clientid,
    const uint8_t            *confirm,
    struct nfs_client       **destroy_unified);

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

/* CREATE_SESSION sequencing decision (RFC 8881 §18.36.4). */
enum nfs4_cs_action {
    NFS4_CS_NEW,    /* fresh request -- caller creates the session */
    NFS4_CS_REPLAY, /* retransmit -- caller replays the cached reply */
    NFS4_CS_ERROR,  /* sequencing error -- caller returns `status` (uncached) */
};

struct nfs4_cs_classify {
    enum nfs4_cs_action   action;
    nfsstat4              status;   /* ERROR status, or REPLAY cached status */
    uint32_t              confirmed; /* record confirmed? (NEW only) */
    uint32_t              principal_ok; /* current principal matches record? (NEW only) */
    /* REPLAY cached reply fields. */
    uint8_t               sessionid[NFS4_SESSIONID_SIZE];
    uint32_t              sequence;
    uint32_t              flags;
    struct channel_attrs4 fore;
    struct channel_attrs4 back;
};

/* Classify an incoming CREATE_SESSION against the client's sequence state and
 * reply cache.  Looks up `client_id`; on NFS4_CS_NEW it also reports whether
 * the record is confirmed and whether `principal` matches the record's
 * principal so the caller can apply the RFC 8881 §18.36.4 principal rule. */
void
nfs4_client_create_session_classify(
    struct nfs4_client_table           *table,
    uint64_t                            client_id,
    uint32_t                            csa_sequence,
    const struct nfs4_client_principal *principal,
    struct nfs4_cs_classify            *out);

/* Record the reply for the just-processed CREATE_SESSION (advances cs_seqid
 * and populates the reply cache).  `status` is the op result; the resok fields
 * are ignored when status != NFS4_OK. */
void
nfs4_client_create_session_cache(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     csa_sequence,
    nfsstat4                     status,
    const uint8_t               *sessionid,
    uint32_t                     sequence,
    uint32_t                     flags,
    const struct channel_attrs4 *fore,
    const struct channel_attrs4 *back);

/* Mark a client's RECLAIM_COMPLETE.  Returns true if the client had already
 * completed reclaim (caller returns NFS4ERR_COMPLETE_ALREADY), false on the
 * first call (now recorded). */
bool
nfs4_client_mark_reclaim_complete(
    struct nfs4_client_table *table,
    uint64_t                  client_id);

bool
nfs4_client_reclaim_complete(
    struct nfs4_client_table *table,
    uint64_t                  client_id);

/* DESTROY_CLIENTID (RFC 8881 §18.50): returns NFS4ERR_STALE_CLIENTID if no
 * such client, NFS4ERR_CLIENTID_BUSY if it still owns sessions, else removes
 * the record (tearing down its unified state hierarchy) and returns NFS4_OK. */
nfsstat4
nfs4_client_destroy_clientid(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread,
    uint64_t                   client_id);

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
    const struct channel_attrs4 *back_attrs,
    const uint8_t               *restore_sessionid);

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

/* Record the delegation callback path on a client's unified state record.
 * 4.0 supplies a full cb_client4 (program + netid/addr); 4.1 supplies only
 * the program (netid/addr empty -- the backchannel rides the fore conn).
 * Resets the CB_NULL probe state so the path is re-validated before the next
 * delegation grant. */
void
nfs4_client_set_cb_path(
    struct nfs4_client_table *table,
    uint64_t                  client_id,
    uint32_t                  cb_program,
    uint32_t                  cb_ident,
    uint8_t                   minorversion,
    const char               *netid,
    int                       netid_len,
    const char               *addr,
    int                       addr_len);

/* Record the callback-channel RPC auth (CREATE_SESSION csa_sec_parms /
 * BACKCHANNEL_CTL).  flavor is an ONC RPC flavor (AUTH_NONE=0, AUTH_SYS=1);
 * uid/gid apply to AUTH_SYS.  cb_program, if non-zero, updates the program. */
void
nfs4_client_set_cb_sec(
    struct nfs4_client_table *table,
    uint64_t                  client_id,
    uint32_t                  cb_program,
    uint32_t                  flavor,
    uint32_t                  uid,
    uint32_t                  gid);

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
