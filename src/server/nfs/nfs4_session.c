// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <uuid/uuid.h>
#include "prometheus-c.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs_internal.h"
#include "nfs_nlm_state.h"
#include "nfs_common.h"
#include "evpl/evpl_rpc2.h"
#include "vfs/vfs_release.h"

/* Bump a replay-cache counter on the shared struct.  Safe when the
 * thread/shared are not set up (e.g. in the unit test). */
static inline void
nfs4_replay_metric_inc(
    struct nfs_request *req,
    struct prometheus_counter_instance *(*field)(struct nfs4_replay_metrics *))
{
    struct nfs4_replay_metrics         *rm;
    struct prometheus_counter_instance *inst;

    if (!req || !req->thread || !req->thread->shared) {
        return;
    }
    rm   = &req->thread->shared->replay_metrics;
    inst = field(rm);
    if (inst) {
        prometheus_counter_increment(inst);
    }
} /* nfs4_replay_metric_inc */

/* Adjust the replay-cache bytes gauge.  Safe when not set up. */
static inline void
nfs4_replay_bytes_delta(
    struct nfs_request *req,
    int64_t             delta)
{
    struct prometheus_gauge_instance *g;

    if (!req || !req->thread || !req->thread->shared) {
        return;
    }
    g = req->thread->shared->replay_metrics.bytes_in_use;
    if (g) {
        prometheus_gauge_add(g, delta);
    }
} /* nfs4_replay_bytes_delta */

static struct prometheus_counter_instance *
replay_field_hit(struct nfs4_replay_metrics *rm)
{
    return rm->hit;
} /* replay_field_hit */
static struct prometheus_counter_instance *
replay_field_seq_misordered(struct nfs4_replay_metrics *rm)
{
    return rm->seq_misordered;
} /* replay_field_seq_misordered */
static struct prometheus_counter_instance *
replay_field_bad_slot(struct nfs4_replay_metrics *rm)
{
    return rm->bad_slot;
} /* replay_field_bad_slot */
static struct prometheus_counter_instance *
replay_field_retry_uncached(struct nfs4_replay_metrics *rm)
{
    return rm->retry_uncached;
} /* replay_field_retry_uncached */

void
nfs4_client_table_init(struct nfs4_client_table *table)
{
    table->nfs4_ct_clients_by_owner = NULL;
    table->nfs4_ct_clients_by_id    = NULL;
    table->nfs4_ct_sessions         = NULL;
    table->nfs4_ct_next_client_id   = 1;

    pthread_mutex_init(&table->nfs4_ct_lock, NULL);
} /* nfs4_client_table_init */

void
nfs4_client_table_free(struct nfs4_client_table *table)
{

#ifndef __clang_analyzer__

    struct nfs4_client  *cur, *tmp;
    struct nfs4_session *sess, *sesstmp;


    /* HASH_DEL blows clangs mind so we disable this block under analyzer */

    HASH_ITER(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, cur, tmp)
    {
        HASH_DELETE(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                    cur);
        HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, cur);
        free(cur);
    }

    HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, sess, sesstmp)
    {
        HASH_DELETE(nfs4_session_hh, table->nfs4_ct_sessions, sess);
        atomic_store_explicit(&sess->destroyed, true, memory_order_release);
        /* Drop the table's ref.  By this point in shutdown evpl_destroy()
         * has already drained all conns and fired NOTIFY_DISCONNECTED for
         * each, so no conns hold refs and refcount is exactly 1 -- the
         * session is freed here. */
        nfs4_session_put(sess);
    }

#endif /* ifndef __clang_analyzer__ */

} /* nfs4_client_table_free */

uint64_t
nfs4_client_register(
    struct nfs4_client_table *table,
    const void               *owner,
    int                       owner_len,
    uint64_t                  verifier,
    uint32_t                  proto,
    const char               *nii_domain,
    const char               *nii_name)
{
    struct nfs4_client *client;
    uint64_t            id;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
              owner, owner_len, client);

    if (client) {
        if (verifier != client->nfs4_client_verifier) {
            /* XXX handle this */
            chimera_nfs_info("Client has a new verifier!");
        }
    } else {
        client = calloc(1, sizeof(*client));

        client->nfs4_client_id = table->nfs4_ct_next_client_id++;

        memcpy(client->nfs4_client_owner, owner, owner_len);
        client->nfs4_client_owner_len = owner_len;
        client->nfs4_client_verifier  = verifier;
        client->nfs4_client_refcnt    = 1;

        /* Bring up the unified state hierarchy in lockstep with the
         * registration record.  minor is encoded as proto/40 today
         * (4.0 only path uses different proto values); pass 0 here and
         * let Phase 4 thread the real minor through. */
        client->unified = nfs_client_alloc(client->nfs4_client_id,
                                           owner, owner_len, verifier, 0);

        if (nii_domain) {
            snprintf(client->nfs4_client_domain,
                     sizeof(client->nfs4_client_domain), "%s", nii_domain);
        } else {
            strncpy(client->nfs4_client_domain, "unidentified",
                    NFS4_OPAQUE_LIMIT);
        }

        if (nii_name) {
            snprintf(client->nfs4_client_name,
                     sizeof(client->nfs4_client_name),
                     "%s", nii_name);
        } else {
            strncpy(client->nfs4_client_name, "unidentified", NFS4_OPAQUE_LIMIT)
            ;
        }


        HASH_ADD(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                 nfs4_client_owner, client->nfs4_client_owner_len, client);
        HASH_ADD(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
                 nfs4_client_id, sizeof(client->nfs4_client_id), client);

        chimera_nfs_info("NFS4 Registered new client %lu (%s %s)",
                         client->nfs4_client_id,
                         client->nfs4_client_domain,
                         client->nfs4_client_name);
    }

    id = client->nfs4_client_id;

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return id;
} /* nfs4_client_register */

void
nfs4_client_unregister(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread,
    uint64_t                   client_id)
{
    struct nfs4_client *client;
    struct nfs_client  *unified = NULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);

    if (client) {
        chimera_nfs_info("NFS4 Unregistered client %lu", client_id);
        HASH_DELETE(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
                    client);
        HASH_DELETE(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, client);
        unified         = client->unified;
        client->unified = NULL;
        free(client);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    /* Tear down the unified state hierarchy outside the table lock so
     * nfs_client_destroy can take per-client / per-owner locks without
     * inversion against the table lock.  RFC 8881 §18.50.4 requires that
     * the client have no remaining state at DESTROY_CLIENTID time -- if
     * any survived, this releases their slots + dup'd VFS handles. */
    if (unified) {
        nfs_client_destroy(unified, state_table, vfs_thread);
    }
} /* nfs4_client_unregister */

struct nfs4_session *
nfs4_create_session(
    struct nfs4_client_table    *table,
    uint64_t                     client_id,
    uint32_t                     implicit,
    uint32_t                     replay_max_slots,
    uint32_t                     replay_maxresp_cached,
    const struct channel_attrs4 *fore_attrs,
    const struct channel_attrs4 *back_attrs)
{
    struct nfs4_client  *client  = NULL;
    struct nfs4_session *session = NULL;
    char                 session_id_str[80];

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);

    if (client) {
        session = calloc(1, sizeof(*session));

        session->magic          = NFS4_SESSION_MAGIC;
        session->client_unified = client->unified;
        /* refcount: 1 for the hash table, 1 for the caller (so the session
         * cannot be destroyed by another thread before the caller binds it
         * to a conn). */
        atomic_init(&session->refcount, 2);
        atomic_init(&session->destroyed, false);

        uuid_generate(session->nfs4_session_id);

        session->nfs4_session_implicit = implicit;
        session->nfs4_session_clientid = client_id;

        pthread_mutex_init(&session->nfs4_session_lock, NULL);

        /* NFS4.1 SEQUENCE replay cache slot table.  Implicit (NFS4.0)
         * sessions get zero slots -- v4.0 uses per-owner replay caches in
         * nfs_open_owner / nfs_lock_owner instead. */
        session->replay_max_slots      = implicit ? 0 : replay_max_slots;
        session->replay_maxresp_cached = replay_maxresp_cached;
        session->replay_bytes_in_use   = 0;
        session->replay_slots          = NULL;

        if (session->replay_max_slots) {
            session->replay_slots = calloc(session->replay_max_slots,
                                           sizeof(*session->replay_slots));
        }

        if (fore_attrs) {
            session->nfs4_session_fore_attrs = *fore_attrs;
            if (fore_attrs->num_ca_rdma_ird) {
                session->nfs4_session_fore_attrs_rdma_ird    = fore_attrs->ca_rdma_ird[0];
                session->nfs4_session_fore_attrs.ca_rdma_ird = &session->nfs4_session_fore_attrs_rdma_ird;
            }

        }

        if (back_attrs) {
            session->nfs4_session_back_attrs = *back_attrs;
            if (back_attrs->num_ca_rdma_ird) {
                session->nfs4_session_back_attrs_rdma_ird    = back_attrs->ca_rdma_ird[0];
                session->nfs4_session_back_attrs.ca_rdma_ird = &session->nfs4_session_back_attrs_rdma_ird;
            }
        }

        HASH_ADD(nfs4_session_hh, table->nfs4_ct_sessions,
                 nfs4_session_id, NFS4_SESSIONID_SIZE, session);

    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    if (!client) {
        return NULL;
    }

    uuid_unparse(session->nfs4_session_id, session_id_str);

    chimera_nfs_info("NFS4 Created new session %s for client %lu",
                     session_id_str, client_id);

    return session;

} /* nfs4_create_session */

struct nfs4_session *
nfs4_session_lookup(
    struct nfs4_client_table *table,
    const void               *sessionid)
{
    struct nfs4_session *session = NULL;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_session_hh, table->nfs4_ct_sessions,
              sessionid, NFS4_SESSIONID_SIZE, session);

    if (session) {
        /* take a +1 ref for the caller while holding the table lock so a
         * concurrent nfs4_destroy_session cannot race the free. */
        atomic_fetch_add_explicit(&session->refcount, 1,
                                  memory_order_acq_rel);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return session;
} /* nfs4_session_lookup */

struct nfs4_session *
nfs4_session_find_by_clientid(
    struct nfs4_client_table *table,
    uint64_t                  client_id)
{
    struct nfs4_session *session = NULL;
    struct nfs4_session *cur, *tmp;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_ITER(nfs4_session_hh, table->nfs4_ct_sessions, cur, tmp)
    {
        if (cur->nfs4_session_clientid == client_id) {
            session = cur;
            break;
        }
    }

    if (session) {
        /* take a +1 ref for the caller while holding the table lock. */
        atomic_fetch_add_explicit(&session->refcount, 1,
                                  memory_order_acq_rel);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    return session;
} /* nfs4_session_find_by_clientid */

void
nfs4_destroy_session(
    struct nfs4_client_table *table,
    const void               *session_id)
{
    struct nfs4_session *session = NULL;
    char                 session_id_str[80];

    uuid_unparse(session_id, session_id_str);

    chimera_nfs_info("NFS4 Destroying session %s", session_id_str);

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_session_hh, table->nfs4_ct_sessions, session_id,
              NFS4_SESSIONID_SIZE, session);

    if (session) {
        HASH_DELETE(nfs4_session_hh, table->nfs4_ct_sessions, session);
        atomic_store_explicit(&session->destroyed, true,
                              memory_order_release);
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);

    if (session) {
        /* Drop the hash table's ref.  The session is freed only when the
         * last conn that still caches it in private_data also drops its
         * ref (in the disconnect notify). */
        nfs4_session_put(session);
    }

} /* nfs4_destroy_session */

void
nfs4_client_table_destroy_unified(
    struct nfs4_client_table  *table,
    struct nfs_state_table    *state_table,
    struct chimera_vfs_thread *vfs_thread)
{
#ifndef __clang_analyzer__
    struct nfs4_client *cur, *tmp;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_ITER(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id, cur, tmp)
    {
        if (cur->unified) {
            nfs_client_destroy(cur->unified, state_table, vfs_thread);
            cur->unified = NULL;
        }
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);
#endif /* ifndef __clang_analyzer__ */
} /* nfs4_client_table_destroy_unified */

void
nfs4_session_get(struct nfs4_session *session)
{
    atomic_fetch_add_explicit(&session->refcount, 1, memory_order_acq_rel);
} /* nfs4_session_get */

void
nfs4_session_put(struct nfs4_session *session)
{
    uint32_t prev;

    prev = atomic_fetch_sub_explicit(&session->refcount, 1,
                                     memory_order_acq_rel);

    chimera_nfs_abort_if(prev == 0,
                         "nfs4_session_put: refcount underflow on session %p",
                         session);

    if (prev == 1) {
        /* Last ref -- safe to tear down. */
        if (session->replay_slots) {
            uint32_t i;
            for (i = 0; i < session->replay_max_slots; i++) {
                free(session->replay_slots[i].cached_buf);
            }
            free(session->replay_slots);
            session->replay_slots = NULL;
        }
        pthread_mutex_destroy(&session->nfs4_session_lock);
        session->magic = 0;
        free(session);
    }
} /* nfs4_session_put */

void
nfs4_session_bind_conn(
    struct evpl_rpc2_conn *conn,
    struct nfs4_session   *session)
{
    void    *existing;
    uint32_t magic = 0;

    existing = evpl_rpc2_conn_get_private_data(conn);

    if (existing) {
        memcpy(&magic, existing, sizeof(magic));
    }

    if (magic == NFS4_SESSION_MAGIC) {
        if (existing == session) {
            /* Already bound to this session. */
            return;
        }
        /* Bound to a different session -- unbind the old one. */
        nfs4_session_put((struct nfs4_session *) existing);
    } else if (magic == NLM_CLIENT_MAGIC) {
        /* NLM and NFS4 should never share a conn (different ports). */
        chimera_nfs_abort(
            "nfs4_session_bind_conn: conn %p already holds an NLM client",
            conn);
    }

    nfs4_session_get(session);
    evpl_rpc2_conn_set_private_data(conn, session);
} /* nfs4_session_bind_conn */

void
nfs4_session_unbind_conn(struct evpl_rpc2_conn *conn)
{
    void                *existing;
    uint32_t             magic = 0;
    struct nfs4_session *session;

    existing = evpl_rpc2_conn_get_private_data(conn);

    if (!existing) {
        return;
    }

    memcpy(&magic, existing, sizeof(magic));

    if (magic != NFS4_SESSION_MAGIC) {
        return;
    }

    session = (struct nfs4_session *) existing;
    evpl_rpc2_conn_set_private_data(conn, NULL);
    nfs4_session_put(session);
} /* nfs4_session_unbind_conn */

/*
 * Reply-capture callback invoked from inside evpl_rpc2_send_reply, just
 * before the encoded reply iovecs are queued onto the wire (and the
 * request -- including the encoding's dbuf -- is freed).  We have one
 * chance to copy the bytes out for later replay.
 *
 * private_data is the struct nfs_request whose SEQUENCE handler armed
 * the capture (only when sa_cachethis was set and the slot is in
 * IN_PROGRESS for this request).
 *
 * If the reply exceeds the per-slot or per-session byte cap, the
 * capture is silently demoted: cached_buf stays NULL and finalize will
 * transition the slot to COMPLETED instead of CACHED.  A future retry
 * will then return NFS4ERR_RETRY_UNCACHED_REP, which RFC 5661 2.10.6.1
 * explicitly permits (the server may override sa_cachethis).
 */
static void
nfs4_replay_capture_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    int                      total_length,
    void                    *private_data)
{
    struct nfs_request      *req     = private_data;
    struct nfs4_session     *session = req->session;
    struct nfs4_replay_slot *slot    = req->replay_slot;
    uint8_t                 *buf;
    size_t                   offset = 0;
    int                      i;

    if (!slot || !session || total_length <= 0) {
        return;
    }

    if ((uint32_t) total_length > session->replay_maxresp_cached) {
        return;
    }

    pthread_mutex_lock(&session->nfs4_session_lock);

    if (session->replay_bytes_in_use + (size_t) total_length >
        NFS4_MAX_REPLY_CACHE_BYTES) {
        pthread_mutex_unlock(&session->nfs4_session_lock);
        return;
    }

    buf = malloc(total_length);
    if (!buf) {
        pthread_mutex_unlock(&session->nfs4_session_lock);
        return;
    }

    /* Concatenate iovecs into a single contiguous buffer.  Loses
     * zerocopy on replay; acceptable since cachethis=true is rare for
     * iovec-heavy ops (READ/READLINK) and replay is rare in general. */
    for (i = 0; i < niov; i++) {
        memcpy(buf + offset, iov[i].data, iov[i].length);
        offset += iov[i].length;
    }

    slot->cached_buf              = buf;
    slot->cached_len              = total_length;
    session->replay_bytes_in_use += total_length;

    pthread_mutex_unlock(&session->nfs4_session_lock);

    /* Update gauge after dropping the session lock. */
    nfs4_replay_bytes_delta(req, total_length);
} /* nfs4_replay_capture_reply */

/*
 * Install the capture callback on the encoding so it fires from inside
 * the next send_reply call.  Caller already holds the session lock and
 * has set req->replay_slot. */
static inline void
nfs4_replay_arm_capture(struct nfs_request *req)
{
    if (req->encoding) {
        req->encoding->reply_capture_cb      = nfs4_replay_capture_reply;
        req->encoding->reply_capture_private = req;
    }
} /* nfs4_replay_arm_capture */

nfsstat4
nfs4_replay_slot_acquire(
    struct nfs4_session *session,
    uint32_t             slotid,
    uint32_t             seqid,
    bool                 cachethis,
    struct nfs_request  *req,
    bool                *out_is_replay)
{
    struct nfs4_replay_slot *slot;
    nfsstat4                 status      = NFS4_OK;
    uint32_t                 freed_bytes = 0;

    *out_is_replay = false;

    if (slotid >= session->replay_max_slots) {
        nfs4_replay_metric_inc(req, replay_field_bad_slot);
        return NFS4ERR_BADSLOT;
    }

    slot = &session->replay_slots[slotid];

    pthread_mutex_lock(&session->nfs4_session_lock);

    switch (slot->state) {
        case NFS4_SLOT_UNUSED:
            /* First-ever use.  RFC 5661 sets csr_sequence = 1 at
             * CREATE_SESSION; the client's first SEQUENCE on a slot must
             * therefore use seqid = 1. */
            if (seqid != 1) {
                status = NFS4ERR_SEQ_MISORDERED;
                break;
            }
            slot->state          = NFS4_SLOT_IN_PROGRESS;
            slot->inflight_seqid = seqid;
            slot->cachethis      = cachethis ? 1 : 0;
            req->replay_slot     = slot;
            req->replay_slot_id  = slotid;
            req->replay_action   = NFS4_REPLAY_ACTION_NEW;
            if (cachethis) {
                nfs4_replay_arm_capture(req);
            }
            break;

        case NFS4_SLOT_CACHED:
        case NFS4_SLOT_COMPLETED:
            if (seqid == (uint32_t) (slot->seqid + 1)) {
                /* Normal advance.  Drop any prior cached reply -- the
                 * client has acknowledged it by moving on. */
                if (slot->state == NFS4_SLOT_CACHED &&
                    slot->cached_buf) {
                    freed_bytes                   = slot->cached_len;
                    session->replay_bytes_in_use -= slot->cached_len;
                    free(slot->cached_buf);
                    slot->cached_buf = NULL;
                    slot->cached_len = 0;
                }
                slot->state          = NFS4_SLOT_IN_PROGRESS;
                slot->inflight_seqid = seqid;
                slot->cachethis      = cachethis ? 1 : 0;
                req->replay_slot     = slot;
                req->replay_slot_id  = slotid;
                req->replay_action   = NFS4_REPLAY_ACTION_NEW;
                if (cachethis) {
                    nfs4_replay_arm_capture(req);
                }
            } else if (seqid == slot->seqid) {
                /* Retransmit. */
                if (slot->state == NFS4_SLOT_CACHED && slot->cached_buf) {
                    req->replay_slot    = slot;
                    req->replay_slot_id = slotid;
                    req->replay_action  = NFS4_REPLAY_ACTION_FROM_CACHE;
                    *out_is_replay      = true;
                } else {
                    /* Original ran with cachethis=false (or was demoted
                     * for size).  We cannot replay the bytes. */
                    status = NFS4ERR_RETRY_UNCACHED_REP;
                }
            } else {
                status = NFS4ERR_SEQ_MISORDERED;
            }
            break;

        case NFS4_SLOT_IN_PROGRESS:
            /* Original compound is still running.  Whether seqid matches
             * (true retry) or not (client jumped ahead), we cannot
             * produce a reply yet.  Both paths return errors. */
            if (seqid == slot->inflight_seqid) {
                status = NFS4ERR_RETRY_UNCACHED_REP;
            } else {
                status = NFS4ERR_SEQ_MISORDERED;
            }
            break;

        default:
            status = NFS4ERR_SERVERFAULT;
            break;
    } /* switch */

    pthread_mutex_unlock(&session->nfs4_session_lock);

    /* Update metrics outside the session lock to avoid serializing the
     * shared prometheus counters on per-session traffic. */
    if (freed_bytes) {
        nfs4_replay_bytes_delta(req, -(int64_t) freed_bytes);
    }
    if (*out_is_replay) {
        nfs4_replay_metric_inc(req, replay_field_hit);
    } else if (status == NFS4ERR_SEQ_MISORDERED) {
        nfs4_replay_metric_inc(req, replay_field_seq_misordered);
    } else if (status == NFS4ERR_RETRY_UNCACHED_REP) {
        nfs4_replay_metric_inc(req, replay_field_retry_uncached);
    }
    return status;
} /* nfs4_replay_slot_acquire */

void
nfs4_replay_slot_finalize(struct nfs_request *req)
{
    struct nfs4_session     *session = req->session;
    struct nfs4_replay_slot *slot    = req->replay_slot;

    if (!slot || !session) {
        return;
    }

    /* NB: at this point req->encoding has already been freed inside
     * send_reply_NFSPROC4_COMPOUND -- do not dereference it. */

    /* If the session has been destroyed under us, the slot table may
     * still be valid (it lives until the last conn ref is dropped) but
     * caching the reply is pointless. */
    if (atomic_load_explicit(&session->destroyed, memory_order_acquire)) {
        req->replay_slot = NULL;
        return;
    }

    pthread_mutex_lock(&session->nfs4_session_lock);

    /* If the capture callback successfully copied bytes into the slot,
     * transition to CACHED so a retransmit can replay.  Otherwise (no
     * cachethis, or demoted for size) transition to COMPLETED -- a
     * retransmit will get NFS4ERR_RETRY_UNCACHED_REP. */
    if (slot->state == NFS4_SLOT_IN_PROGRESS) {
        slot->seqid          = slot->inflight_seqid;
        slot->inflight_seqid = 0;
        if (slot->cached_buf) {
            slot->state = NFS4_SLOT_CACHED;
        } else {
            slot->state = NFS4_SLOT_COMPLETED;
        }
    }

    pthread_mutex_unlock(&session->nfs4_session_lock);

    req->replay_slot = NULL;
} /* nfs4_replay_slot_finalize */