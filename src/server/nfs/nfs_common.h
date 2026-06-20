// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <utlist.h>

#include "nfs.h"
#include "nfs_fh_wrap.h"
#include "evpl/evpl_rpc2.h"
#include "portmap_xdr.h"
#include "vfs/vfs_cred.h"
#include "nfs_gss.h"
#include "nfs_mount_xdr.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"
#include "nlm4_xdr.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_layout_table.h"
#include "nfs4_recovery.h"
#include "nfs3_drc.h"
#include "nfs_nlm_state.h"
#include "nfs_nsm_state.h"
#include "sm_inter_xdr.h"

struct chimera_server_nfs_thread;

/*
 * pNFS device cache for backend-SOURCED layouts (CHIMERA_VFS_CAP_LAYOUT_SOURCE).
 *
 * A sourcing backend mints deviceids and returns their descriptors alongside
 * the layout in chimera_vfs_get_layout().  GETDEVICEINFO arrives later with only
 * a deviceid (no file handle), so LAYOUTGET caches each returned descriptor here
 * and GETDEVICEINFO answers from the cache.  Bounded and fixed-size: the device
 * population is tiny and stable.  (Orchestrated flex devices live in the
 * chimera_vfs_pnfs table instead and never enter this cache.)
 */
#define NFS_PNFS_DEVCACHE_MAX 64

struct nfs_pnfs_devcache_entry {
    uint8_t                          deviceid[CHIMERA_VFS_DEVICEID_SIZE];
    uint8_t                          valid;
    struct chimera_vfs_layout_device device;
};

struct nfs_pnfs_devcache {
    pthread_mutex_t                lock;
    uint32_t                       count;
    struct nfs_pnfs_devcache_entry entries[NFS_PNFS_DEVCACHE_MAX];
};
struct nfs4_range_lease;

struct nfs_nfs3_readdir_cursor {
    uint64_t       count;
    struct entry3 *entries;
    struct entry3 *last;
};

struct nfs_nfs3_readdirplus_cursor {
    uint64_t           count;
    uint64_t           dircount;
    struct entryplus3 *entries;
    struct entryplus3 *last;
};

struct nfs_nfs4_readdir_cursor {
    uint64_t       count;
    struct entry4 *entries;
    struct entry4 *last;
};

struct nfs_request {
    struct chimera_server_nfs_thread *thread;
    struct nfs4_session              *session;
    struct chimera_vfs_cred           cred;
    /* The caller's credential as mapped from the RPC auth, before any export
     * squash.  req->cred is recomputed from this each time a file handle is
     * decoded, so squashing is not applied cumulatively when an NFSv4 compound
     * switches between exports with different policies. */
    struct chimera_vfs_cred           orig_cred;
    /* RPC principal of the caller, captured at compound entry for EXCHANGE_ID
     * client-record matching (RFC 8881 §18.35.4).  machinename points into the
     * request message buffer, valid for the lifetime of the request. */
    uint32_t                          principal_flavor;
    uint32_t                          principal_uid;
    uint32_t                          principal_gid;
    const char                       *principal_machinename;
    uint32_t                          principal_machinename_len;
    uint8_t                           fh[NFS4_FHSIZE];
    int                               fhlen;
    uint8_t                           saved_fh[NFS4_FHSIZE];
    int                               saved_fhlen;
    /* Export the current/saved file handle belongs to, recovered from the
     * wire handle on receive (and inherited by child handles minted in the
     * reply).  Drives per-request squash attribution and is re-stamped into
     * every wire handle this request emits.  0 = none (e.g. the NFSv4 pseudo
     * root). */
    uint16_t                          export_id;
    uint16_t                          saved_export_id;
    /* The request's RPC security flavor as a CHIMERA_NFS_SEC_* bit, derived
     * from the RPC credential at request entry.  Checked against the target
     * export's sec_allowed mask. */
    uint32_t                          sec_bit;
    struct chimera_vfs_open_handle   *handle;
    int                               index;
    uint8_t                           minorversion;     /* COMPOUND4args.minorversion */
    bool                              seen_sequence;    /* set once OP_SEQUENCE has run in this compound */
    /* NFS4.1 "current stateid" (RFC 8881 §16.2.3.1.2): a per-COMPOUND value
     * set by stateid-returning ops (OPEN/LOCK/...) and substituted into
     * later ops that present the special current-stateid value. */
    bool                              current_stateid_valid;
    struct stateid4                   current_stateid;
    /* SAVEFH/RESTOREFH save and restore the current stateid alongside the
     * current filehandle (RFC 8881 §16.2.3.1.2). */
    bool                              saved_current_stateid_valid;
    struct stateid4                   saved_current_stateid;
    /* In-flight state ref for ops that acquire from the unified state table.
     * Released by the completion handler.  Phase 2.  Type is one of
     * NFS4_SLOT_TYPE_OPEN / NFS4_SLOT_TYPE_LOCK. */
    void                             *nfs_state_ref;
    uint8_t                           nfs_state_type;
    /* Set when a READ/WRITE authorized by a delegation stateid is served via an
     * on-the-fly open: the on-the-fly I/O must carry the delegation holder's
     * own lease owner so the VFS I/O path does not treat the client's own I/O
     * as a foreign actor and recall the client's own delegation. */
    bool                              io_owner_from_deleg;
    /* In-flight vfs_state byte-range lease for an async NFSv4 LOCK.
     * Allocated at lock dispatch, linked onto the lock_state on grant,
     * freed on denial.  See nfs4_proc_lock.c. */
    struct nfs4_range_lease          *nfs_inflight_range;
    /* Per-owner seqid bookkeeping for the 4.0 OPEN flow.  Populated at
     * entry to chimera_nfs4_open after the seqid is classified NEW; nil on
     * 4.1+ and on replay/bad-seqid short-circuits.  All OPEN response
     * paths route through chimera_nfs4_open_complete, which advances the
     * owner seqid + caches the reply iff this is non-NULL and the status
     * is in nfs4_seqid_should_advance(). */
    struct nfs_open_owner            *open_4_0_owner;
    /* Per-owner seqid bookkeeping for the 4.0 LOCK flow.  For
     * new_lock_owner=true both open_owner (open_seqid) and lock_owner
     * (lock_seqid) advance; for new_lock_owner=false only the lock_owner
     * advances.  Set in chimera_nfs4_lock after classification; consumed
     * by chimera_nfs4_lock_finish. */
    struct nfs_open_owner            *lock_4_0_open_owner;
    struct nfs_lock_owner            *lock_4_0_lock_owner;
    struct evpl_rpc2_conn            *conn;
    struct evpl_rpc2_encoding        *encoding;
    struct nlm_lock_entry            *nlm_pending_entry; /* in-flight NLM lock/test */
    /* NFS4.1 SEQUENCE replay slot tracking.  Set by chimera_nfs4_sequence
     * when a SEQUENCE op is processed; consumed by the compound dispatcher
     * at completion time (nfs4_replay_slot_finalize) and on replay
     * short-circuit. */
    struct nfs4_replay_slot          *replay_slot;
    uint32_t                          replay_slot_id;
    uint8_t                           replay_action;
    struct nfs_request               *next;
    /* Park slot used while this request is awaiting a 4.0 callback-channel
     * CB_NULL probe completion (see nfs4_callback.c probe-defer path).
     * Owned by chan->owner_thread; no extra synchronization needed. */
    struct nfs_request               *probe_next;
    union {
        struct mountargs3       *args_mount;
        struct ACCESS3args      *args_access;
        struct LOOKUP3args      *args_lookup;
        struct CREATE3args      *args_create;
        struct GETATTR3args     *args_getattr;
        struct FSSTAT3args      *args_fsstat;
        struct READ3args        *args_read;
        struct READDIR3args     *args_readdir;
        struct READDIRPLUS3args *args_readdirplus;
        struct FSINFO3args      *args_fsinfo;
        struct WRITE3args       *args_write;
        struct COMMIT3args      *args_commit;
        struct RMDIR3args       *args_rmdir;
        struct REMOVE3args      *args_remove;
        struct MKDIR3args       *args_mkdir;
        struct SYMLINK3args     *args_symlink;
        struct SETATTR3args     *args_setattr;
        struct READLINK3args    *args_readlink;
        struct MKNOD3args       *args_mknod;
        struct WRITE4args       *args_write4;
    };
    struct COMPOUND4args *args_compound;
    union {
        struct READLINK3res    res_readlink;
        struct READDIR3res     res_readdir;
        struct READDIRPLUS3res res_readdirplus;
        struct COMPOUND4res    res_compound;
    };
    union {
        struct nfs_nfs3_readdir_cursor     readdir3_cursor;
        struct nfs_nfs3_readdirplus_cursor readdirplus3_cursor;
        struct nfs_nfs4_readdir_cursor     readdir4_cursor;
    };

};

/*
 * Per-export access-control options.
 *
 * options : RO/RW access mode (CHIMERA_NFS_EXPORT_OPT_*).
 * squash  : credential squashing policy (CHIMERA_NFS_SQUASH_*).  Default is
 *           SQUASH_NONE (no squashing), preserving chimera's historical
 *           pass-through behavior; root_squash / all_squash are opt-in.
 * anonuid/anongid : identity that squashed callers are mapped to.
 * id      : stable 16-bit identifier embedded into every wire file handle minted
 *           for this export, so each request can be re-attributed to its export
 *           (and thus its squash policy) without trusting client-supplied paths.
 *
 * Per-client (IP/network) access control is tracked separately (issue #69).
 *
 * The CHIMERA_NFS_EXPORT_OPT_* / CHIMERA_NFS_SQUASH_* option constants are
 * declared in the public nfs.h (included below) so the daemon config parser can
 * reach them.
 */
#define CHIMERA_NFS_MAX_EXPORTS 4096

struct chimera_nfs_export {
    char                       name[CHIMERA_VFS_NAME_MAX];
    char                       path[CHIMERA_VFS_PATH_MAX];
    uint16_t                   id;
    uint32_t                   options;
    uint32_t                   squash;
    uint32_t                   anonuid;
    uint32_t                   anongid;
    uint32_t                   sec_allowed;  /* CHIMERA_NFS_SEC_* mask; 0 = any */
    struct chimera_nfs_export *prev;
    struct chimera_nfs_export *next;
};

/*
 * In-memory rmtab-style mount-entry table (RFC 1813 App. I).  MOUNTPROC3_MNT
 * records an entry, UMNT/UMNTALL remove them, and DUMP enumerates them
 * (showmount -a/-d).  Advisory only and not persisted across restart.
 * hostname is the caller's address (port stripped); directory is the
 * client-requested export path.
 */
struct chimera_nfs_mount_entry {
    char                            hostname[64];
    char                            directory[MNTPATHLEN + 1];
    struct chimera_nfs_mount_entry *prev;
    struct chimera_nfs_mount_entry *next;
};

/* Prometheus instances for the NFS4.1 SEQUENCE replay cache.  All
 * counters are best-effort (not atomic); contention is low because
 * SEQUENCE handling is per-session under that session's lock. */
struct nfs4_replay_metrics {
    struct prometheus_counter          *counter;
    struct prometheus_counter_series   *hit_series;
    struct prometheus_counter_series   *seq_misordered_series;
    struct prometheus_counter_series   *bad_slot_series;
    struct prometheus_counter_series   *retry_uncached_series;
    struct prometheus_counter_instance *hit;
    struct prometheus_counter_instance *seq_misordered;
    struct prometheus_counter_instance *bad_slot;
    struct prometheus_counter_instance *retry_uncached;
    struct prometheus_gauge            *bytes_gauge;
    struct prometheus_gauge_series     *bytes_series;
    struct prometheus_gauge_instance   *bytes_in_use;
};

/* The NFSv4.0 reply cache is now an instance of the shared connectionless DRC
 * (struct nfs3_drc); see nfs4_v40_drc.{c,h}. */

/* uthash-tracked in-flight 4.1 session hydrate (defined in nfs4_drc.c). */
struct nfs4_drc_hydra;

struct chimera_server_nfs_shared {

    const struct chimera_server_config *config;
    struct chimera_vfs                 *vfs;

    char                                portmap_hostname[16];

    struct PORTMAP_V2                   portmap_v2;
    struct PORTMAP_V3                   portmap_v3;
    struct PORTMAP_V4                   portmap_v4;
    struct NFS_MOUNT_V3                 mount_v3;
    struct NFS_V3                       nfs_v3;
    struct NFS_V4                       nfs_v4;
    struct NFS_V4_CB                    nfs_v4_cb;
    struct NLM_V4                       nlm_v4;
    struct nlm_state                    nlm_state;
    struct SM_INTER_V1                  nsm_v1;
    struct nsm_state                    nsm_state;

    struct chimera_nfs_export          *exports;
    pthread_mutex_t                     exports_lock;
    int                                 num_exports;
    /* Direct id -> export index for O(1) per-request attribution.  Slot 0 is
     * unused (id 0 is reserved as "invalid"); ids are assigned sequentially
     * starting at 1.  Reads on the request hot path are lockless (entries are
     * stable once published; removal is a rare REST operation guarded by
     * exports_lock). */
    uint16_t                            next_export_id;
    struct chimera_nfs_export          *exports_by_id[CHIMERA_NFS_MAX_EXPORTS];

    /* File-handle signing.  fh_sign gates the SipHash MAC appended to every
     * wire file handle (default on); fh_key is the 128-bit server secret used
     * for that MAC, generated once and persisted under state_dir so handles
     * survive a restart (a config-supplied key overrides). */
    int                                 fh_sign;
    uint8_t                             fh_key[16];

    struct chimera_nfs_mount_entry     *mount_entries;
    pthread_mutex_t                     mount_entries_lock;
    int                                 num_mount_entries;
    struct evpl_endpoint               *nfs_endpoint;
    struct evpl_endpoint               *mount_endpoint;
    struct evpl_endpoint               *portmap_endpoint;
    struct evpl_endpoint               *nfs_rdma_endpoint;
    struct evpl_endpoint               *nlm_endpoint;
    struct evpl_endpoint               *nsm_endpoint;

    struct evpl_rpc2_server            *portmap_server;
    struct evpl_rpc2_server            *mount_server;
    struct evpl_rpc2_server            *nfs_server;
    struct evpl_rpc2_server            *nfs_rdma_server;
    struct evpl_rpc2_server            *nlm_server;
    struct evpl_rpc2_server            *nsm_server;

    uint64_t                            nfs_verifier;

    /* Stable identity of this server instance among any peers sharing the same
     * backing KV store.  Namespaces every persisted record + the clientid /
     * stateid epoch so N instances over one store never collide and each
     * reloads only its own state.  From server.nfs4_node_id, else derived from
     * the machine name (see nfs_server_init).  Range 1..0xFFFE. */
    uint16_t                            node_id;

    /* Lease management (Phase 3).  Set from defaults at init; future
     * config knobs would override these in nfs_server_init. */
    uint32_t                            nfs_lease_time_s;
    uint32_t                            nfs_grace_time_s;
    uint32_t                            nfs_courtesy_time_s;

    struct nfs4_client_table            nfs4_shared_clients;
    struct nfs_state_table              nfs4_state_table;
    struct nfs_layout_table             nfs4_layout_table;   /* fh -> layout holders */
    struct nfs_pnfs_devcache            nfs4_pnfs_devcache;  /* sourced deviceid -> descriptor */
    struct nfs_recovery                 nfs4_recovery;

    struct prometheus_histogram        *op_histogram;
    struct prometheus_metrics          *metrics;
    struct nfs4_replay_metrics          replay_metrics;
    struct nfs3_drc                     v40_drc;
    struct nfs3_drc                     nfs3_drc;

    /* Dedup / negative-cache for in-flight lazy 4.1 session hydrates, keyed by
     * sessionid (see nfs4_drc_session_hydrate). */
    pthread_mutex_t                     nfs4_drc_hydra_lock;
    struct nfs4_drc_hydra              *nfs4_drc_hydra;
};

/* Forward decl for the per-thread lease sweeper (defined in nfs4_lease.h). */
struct nfs_lease_sweeper;

struct chimera_server_nfs_thread {
    struct evpl_rpc2_thread          *rpc2_thread;
    struct chimera_server_nfs_shared *shared;
    struct chimera_vfs_thread        *vfs_thread;
    struct chimera_vfs               *vfs;
    struct evpl                      *evpl;
    struct nfs_lease_sweeper         *lease_sweeper;
    struct evpl_rpc2_thread          *nfs_server_thread;
    struct evpl_rpc2_thread          *mount_server_thread;
    struct evpl_rpc2_thread          *portmap_server_thread;
    int                               active;
    int                               again;
    int                               active_requests;
    struct nfs_request               *free_requests;

    /* Delegation callback recall marshalling.  A recall is triggered by a
     * conflicting op on an arbitrary thread, but the callback connection is
     * owned by one thread's evpl and evpl sends are not cross-thread safe.
     * break_cb pushes the delegation onto the owner thread's queue (under
     * cb_recall_lock) and rings cb_doorbell; the owner thread drains the
     * queue and sends CB_RECALL.  See nfs4_callback.c. */
    struct evpl_doorbell              cb_doorbell;
    pthread_mutex_t                   cb_recall_lock;
    struct nfs_delegation            *cb_recall_queue; /* via deleg->recall_qnext */
    /* Cross-thread pNFS CB_LAYOUTRECALL marshalling (same rationale as
     * cb_recall_queue, but for layout holders).  Via layout->recall_qnext. */
    struct nfs_layout_state          *cb_layoutrecall_queue;
    /* Deferred-op resumes bounced back to their home thread: a layout recall
     * completes (LAYOUTRETURN) on the backchannel owner thread, but the deferred
     * op's request/iovecs are owned by the thread that received it.  See
     * nfs4_cb_resume_bounce / nfs4_cb_drain_resume_queue in nfs4_cb.c. */
    struct nfs4_cb_resume_ctx        *cb_resume_queue;
    /* Cross-thread CB_GETATTR work (request to the deleg holder's thread, and
     * the response back to the requester's thread).  Protected by
     * cb_recall_lock and drained by cb_doorbell.  See nfs4_callback.c. */
    struct nfs4_cb_getattr           *cb_getattr_queue;
    /* Callback channels whose last reference was dropped off the owner thread
     * (e.g. the lease sweeper expiring a client).  The owner thread frees them
     * from the cb_doorbell drain so the free cannot race in-flight CB reply
     * handling on this thread.  Linked via nfs4_cb_client->teardown_next,
     * protected by cb_recall_lock.  See nfs4_callback.c. */
    struct nfs4_cb_client            *cb_teardown_queue;
    uint8_t                           cb_doorbell_armed;
};

static inline struct nfs_request *
nfs_request_alloc(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_conn            *conn,
    struct evpl_rpc2_encoding        *encoding)
{
    struct nfs_request *req;

    if (thread->free_requests) {
        req = thread->free_requests;
        LL_DELETE(thread->free_requests, req);
    } else {
        req         = calloc(1, sizeof(*req));
        req->thread = thread;
    }


    req->conn     = conn;
    req->encoding = encoding;

    req->replay_slot    = NULL;
    req->replay_slot_id = 0;
    req->replay_action  = NFS4_REPLAY_ACTION_NONE;

    thread->active_requests++;

    return req;
} /* nfs_request_alloc */

static inline void
nfs_request_free(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    thread->active_requests--;
    LL_PREPEND(thread->free_requests, req);
} /* nfs_request_free */

/*
 * Map RPC2 credentials to VFS credentials.
 *
 * This converts the protocol-specific evpl_rpc2_cred to the
 * protocol-independent chimera_vfs_cred used by the VFS layer.
 *
 * For AUTH_SYS, UNIX credentials are extracted directly.
 * For AUTH_NONE or unknown, anonymous credentials are used.
 *
 * @param vfs_cred    Output VFS credentials
 * @param rpc_cred    Input RPC credentials
 */
static inline void
chimera_nfs_map_cred(
    struct chimera_vfs_cred     *vfs_cred,
    const struct evpl_rpc2_cred *rpc_cred)
{
    if (!rpc_cred || rpc_cred->flavor == EVPL_RPC2_AUTH_NONE) {
        /* Anonymous credentials */
        chimera_vfs_cred_init_anonymous(vfs_cred,
                                        CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
    } else if (rpc_cred->flavor == EVPL_RPC2_AUTH_SYS) {
        /* UNIX credentials */
        chimera_vfs_cred_init_unix(vfs_cred,
                                   rpc_cred->authsys.uid,
                                   rpc_cred->authsys.gid,
                                   rpc_cred->authsys.num_gids,
                                   rpc_cred->authsys.gids);
    } else if (rpc_cred->flavor == EVPL_RPC2_AUTH_RPCSEC_GSS) {
        /* RPCSEC_GSS: map the authenticated principal to a UNIX identity. */
        chimera_nfs_gss_map_principal(rpc_cred->gss.principal, vfs_cred);
    } else {
        /* Unknown auth flavor - use anonymous */
        chimera_vfs_cred_init_anonymous(vfs_cred,
                                        CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
    }
} /* chimera_nfs_map_cred */

/*
 * Classify an RPC credential into a CHIMERA_NFS_SEC_* bit for per-export
 * security-flavor policy.  AUTH_SYS and AUTH_NONE both map to "sys";
 * RPCSEC_GSS maps to krb5 / krb5i / krb5p by its negotiated GSS service.
 */
static inline uint32_t
chimera_nfs_sec_bit(const struct evpl_rpc2_cred *rpc_cred)
{
    if (rpc_cred && rpc_cred->flavor == EVPL_RPC2_AUTH_RPCSEC_GSS) {
        switch (rpc_cred->gss.service) {
            case EVPL_RPC2_GSS_SVC_INTEGRITY:
                return CHIMERA_NFS_SEC_KRB5I;
            case EVPL_RPC2_GSS_SVC_PRIVACY:
                return CHIMERA_NFS_SEC_KRB5P;
            default:
                return CHIMERA_NFS_SEC_KRB5;
        } /* switch */
    }
    return CHIMERA_NFS_SEC_SYS;
} /* chimera_nfs_sec_bit */

/*
 * True if an export permits the given security-flavor bit.  An export with no
 * explicit policy (sec_allowed == 0) permits everything.
 */
static inline int
chimera_nfs_export_sec_ok(
    const struct chimera_nfs_export *export,
    uint32_t sec_bit)
{
    if (!export || export->sec_allowed == 0) {
        return 1;
    }
    return (export->sec_allowed & sec_bit) != 0;
} /* chimera_nfs_export_sec_ok */

/*
 * Map the RPC credential into a request and snapshot it as the pre-squash
 * original.  Use this from every proc/compound entry instead of calling
 * chimera_nfs_map_cred directly so that file-handle decoding can recompute the
 * effective (squashed) credential from a stable baseline.
 */
static inline void
chimera_nfs_map_cred_req(
    struct nfs_request          *req,
    const struct evpl_rpc2_cred *rpc_cred)
{
    chimera_nfs_map_cred(&req->cred, rpc_cred);
    req->orig_cred = req->cred;
    req->sec_bit   = chimera_nfs_sec_bit(rpc_cred);
} /* chimera_nfs_map_cred_req */

/*
 * Squash a credential according to an export's policy.
 *
 *   SQUASH_ALL  : every caller is mapped to the export's anon identity.
 *   SQUASH_ROOT : a UNIX uid 0 is mapped to anon; everyone else passes through.
 *   SQUASH_NONE : credentials pass through unchanged.
 *
 * chimera_vfs_cred_init_anonymous() also clears the supplementary groups, which
 * is the required behavior for a squashed caller.
 */
static inline void
chimera_nfs_squash_cred(
    struct chimera_vfs_cred *cred,
    const struct chimera_nfs_export *export)
{
    if (!export) {
        return;
    }

    switch (export->squash) {
        case CHIMERA_NFS_SQUASH_ALL:
            chimera_vfs_cred_init_anonymous(cred, export->anonuid, export->anongid);
            break;
        case CHIMERA_NFS_SQUASH_ROOT:
            if (cred->flavor == CHIMERA_VFS_AUTH_UNIX && cred->uid == 0) {
                chimera_vfs_cred_init_anonymous(cred, export->anonuid, export->anongid);
            }
            break;
        case CHIMERA_NFS_SQUASH_NONE:
        default:
            break;
    } /* switch */
} /* chimera_nfs_squash_cred */

/*
 * Decode an incoming wire file handle: authenticate it, recover the inner VFS
 * handle into vfs_fh/vfs_len and the owning export id into req->export_id, and
 * apply that export's squash policy to req->cred.
 *
 * Returns CHIMERA_NFS_FH_OK on success or CHIMERA_NFS_FH_BADHANDLE if the
 * handle is malformed or fails authentication.  A well-formed handle naming an
 * unknown export still succeeds (export_id is set, squash is a no-op); the VFS
 * layer rejects the inner handle with a stale error in that case.
 */
static inline int
chimera_nfs_fh_decode(
    struct nfs_request *req,
    const void         *wire,
    int                 wirelen,
    uint8_t            *vfs_fh,
    int                *vfs_len)
{
    struct chimera_server_nfs_shared *shared = req->thread->shared;
    const struct chimera_nfs_export *export;
    int                               rc;

    rc = chimera_nfs_fh_unwrap(wire, wirelen, &req->export_id, vfs_fh, vfs_len,
                               shared->fh_key, shared->fh_sign);

    if (rc != CHIMERA_NFS_FH_OK) {
        return rc;
    }

    /* Recompute the effective credential from the pre-squash original so that
     * switching exports within a compound applies the new export's policy
     * rather than stacking on a previous squash. */
    export = chimera_nfs_get_export_by_id(shared, req->export_id);

    /* Enforce the export's security-flavor policy: a handle that authenticates
     * fine but arrives under a flavor the export does not permit is rejected
     * (NFS4ERR_WRONGSEC), which drives the client to renegotiate via SECINFO. */
    if (!chimera_nfs_export_sec_ok(export, req->sec_bit)) {
        return CHIMERA_NFS_FH_WRONGSEC;
    }

    req->cred = req->orig_cred;
    chimera_nfs_squash_cred(&req->cred, export);

    return CHIMERA_NFS_FH_OK;
} /* chimera_nfs_fh_decode */

/*
 * Set the request's current export (when known directly, e.g. resolved by name
 * at the NFSv4 pseudo-fs or NFSv3 MOUNT) and apply its squash policy.  Child
 * handles minted in the reply inherit this export id.
 */
static inline void
chimera_nfs_set_export(
    struct nfs_request *req,
    const struct chimera_nfs_export *export)
{
    req->export_id = export ? export->id : 0;
    req->cred      = req->orig_cred;
    chimera_nfs_squash_cred(&req->cred, export);
} /* chimera_nfs_set_export */

/*
 * Encode a VFS file handle for the wire, stamping it with the request's current
 * export id (inherited by child handles minted in a reply) and signing it if
 * enabled.  out must hold at least CHIMERA_NFS_FH_MAX bytes.
 */
static inline void
chimera_nfs_fh_encode(
    struct nfs_request *req,
    const uint8_t      *vfs_fh,
    int                 vfs_len,
    uint8_t            *out,
    int                *outlen)
{
    struct chimera_server_nfs_shared *shared = req->thread->shared;

    chimera_nfs_fh_wrap(out, outlen, req->export_id, vfs_fh, vfs_len,
                        shared->fh_key, shared->fh_sign);
} /* chimera_nfs_fh_encode */

/*
 * Populate `out` (capacity >= 4) with the secinfo4 entries advertising an
 * export's allowed security flavors.  An export with no explicit policy
 * (sec_allowed == 0) advertises everything chimera supports.  Returns the
 * number of entries written, in the server's preference order.
 */
static inline int
chimera_nfs_fill_secinfo(
    struct secinfo4 *out,
    uint32_t         sec_allowed)
{
    /* Kerberos v5 mechanism OID 1.2.840.113554.1.2.2 (RFC 1964). */
    static const uint8_t krb5_oid[9] = {
        0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02
    };
    uint32_t             mask = sec_allowed ? sec_allowed : CHIMERA_NFS_SEC_ALL;
    int                  n    = 0;

    static const struct {
        uint32_t bit;
        uint32_t service;
    } gss_flavors[] = {
        { CHIMERA_NFS_SEC_KRB5,  RPC_GSS_SVC_NONE        },
        { CHIMERA_NFS_SEC_KRB5I, RPC_GSS_SVC_INTEGRITY   },
        { CHIMERA_NFS_SEC_KRB5P, RPC_GSS_SVC_PRIVACY     },
    };
    unsigned             i;

    if (mask & CHIMERA_NFS_SEC_SYS) {
        out[n++].flavor = AUTH_SYS;
    }
    for (i = 0; i < sizeof(gss_flavors) / sizeof(gss_flavors[0]); i++) {
        if (mask & gss_flavors[i].bit) {
            out[n].flavor                   = RPCSEC_GSS;
            out[n].flavor_info.oid.oid.len  = sizeof(krb5_oid);
            out[n].flavor_info.oid.oid.data = (void *) krb5_oid;
            out[n].flavor_info.qop          = 0;
            out[n].flavor_info.service      = gss_flavors[i].service;
            n++;
        }
    }
    return n;
} /* chimera_nfs_fill_secinfo */
