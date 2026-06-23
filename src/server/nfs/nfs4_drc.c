// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "nfs4_drc.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_kv_keys.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

#define NFS4_DRC_SESSION_MAGIC   0x3153534Eu /* "NSS1" */
#define NFS4_DRC_REPLY_MAGIC     0x3150524Eu /* "NRP1" */

/* Session-record value: fixed header before the variable principal+owner. */
#define NFS4_DRC_SESSION_HDR_LEN 100u
/* Reply-record value: magic + seqid + cached_len, before the bytes. */
#define NFS4_DRC_REPLY_HDR_LEN   12u

/* ------------------------------------------------------------------ *
*  fire-and-forget KV write contexts                                 *
* ------------------------------------------------------------------ */

struct nfs4_drc_kv_ctx {
    uint8_t  key[CHIMERA_KV_REPLY_KEY_LEN];
    uint32_t key_len;
    uint8_t *value;     /* malloc'd; NULL for deletes */
    uint32_t value_len;
};

static void
nfs4_drc_kv_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs4_drc_kv_ctx *ctx = private_data;

    (void) error_code;
    free(ctx->value);
    free(ctx);
} /* nfs4_drc_kv_done */

/* ------------------------------------------------------------------ *
*  reply cache write-through                                         *
* ------------------------------------------------------------------ */

/* Reply value layout: magic(4) seqid(4) cached_len(4) bytes[cached_len]. */
uint32_t
nfs4_drc_reply_serialize(
    uint8_t    *buf,
    uint32_t    buf_size,
    uint32_t    seqid,
    const void *data,
    uint32_t    data_len)
{
    uint32_t p = 0;

    if (buf_size < NFS4_DRC_REPLY_HDR_LEN + data_len) {
        return 0;
    }
    nfs_kv_put_le32(buf, &p, NFS4_DRC_REPLY_MAGIC);
    nfs_kv_put_le32(buf, &p, seqid);
    nfs_kv_put_le32(buf, &p, data_len);
    memcpy(buf + p, data, data_len);
    return p + data_len;
} /* nfs4_drc_reply_serialize */

int
nfs4_drc_reply_parse(
    const uint8_t  *buf,
    uint32_t        len,
    uint32_t       *out_seqid,
    const uint8_t **out_data,
    uint32_t       *out_data_len)
{
    uint32_t data_len;

    if (len < NFS4_DRC_REPLY_HDR_LEN ||
        nfs_kv_le32(buf) != NFS4_DRC_REPLY_MAGIC) {
        return -1;
    }
    *out_seqid = nfs_kv_le32(buf + 4);
    data_len   = nfs_kv_le32(buf + 8);
    if (NFS4_DRC_REPLY_HDR_LEN + data_len > len) {
        return -1;
    }
    *out_data     = buf + NFS4_DRC_REPLY_HDR_LEN;
    *out_data_len = data_len;
    return 0;
} /* nfs4_drc_reply_parse */

void
nfs4_drc_persist_reply(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs4_session       *session,
    uint32_t                   slotid,
    uint32_t                   seqid,
    const void                *buf,
    uint32_t                   len)
{
    struct nfs4_drc_kv_ctx *ctx;

    if (!buf || len == 0 || len > session->replay_maxresp_cached) {
        return;
    }

    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_reply_key(ctx->key, session->nfs4_session_id,
                                    slotid, seqid);

    ctx->value     = malloc(NFS4_DRC_REPLY_HDR_LEN + len);
    ctx->value_len = nfs4_drc_reply_serialize(ctx->value,
                                              NFS4_DRC_REPLY_HDR_LEN + len,
                                              seqid, buf, len);

    chimera_vfs_put_key(vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len, nfs4_drc_kv_done, ctx);
} /* nfs4_drc_persist_reply */

void
nfs4_drc_delete_reply(
    struct chimera_vfs_thread *vfs_thread,
    const uint8_t             *sessionid,
    uint32_t                   slotid,
    uint32_t                   seqid)
{
    struct nfs4_drc_kv_ctx *ctx = malloc(sizeof(*ctx));

    ctx->value     = NULL;
    ctx->value_len = 0;
    ctx->key_len   = nfs_kv_reply_key(ctx->key, sessionid, slotid, seqid);

    chimera_vfs_delete_key(vfs_thread, ctx->key, ctx->key_len,
                           nfs4_drc_kv_done, ctx);
} /* nfs4_drc_delete_reply */

/* ------------------------------------------------------------------ *
*  session metadata persistence                                      *
* ------------------------------------------------------------------ */

/*
 * Session value layout (all LE): magic(4) clientid(8) verifier(8)
 * princ_flavor/uid/gid(4*3) replay_max_slots(4) replay_maxresp_cached(4)
 * cb_program(4) flags(4) fore[6*4] back[6*4] mach_len(2) owner_len(2)
 * mach[mach_len] owner[owner_len].  The fixed header is NFS4_DRC_SESSION_HDR_LEN.
 */
uint32_t
nfs4_drc_session_serialize(
    uint8_t                              *buf,
    uint32_t                              buf_size,
    const struct nfs4_drc_session_record *r)
{
    const struct channel_attrs4 *fore = &r->fore;
    const struct channel_attrs4 *back = &r->back;
    uint32_t                     p    = 0;

    if (buf_size < NFS4_DRC_SESSION_HDR_LEN + r->mach_len + r->owner_len) {
        return 0;
    }

    nfs_kv_put_le32(buf, &p, NFS4_DRC_SESSION_MAGIC);
    nfs_kv_put_le64(buf, &p, r->clientid);
    nfs_kv_put_le64(buf, &p, r->verifier);
    nfs_kv_put_le32(buf, &p, r->princ_flavor);
    nfs_kv_put_le32(buf, &p, r->princ_uid);
    nfs_kv_put_le32(buf, &p, r->princ_gid);
    nfs_kv_put_le32(buf, &p, r->replay_max_slots);
    nfs_kv_put_le32(buf, &p, r->replay_maxresp_cached);
    nfs_kv_put_le32(buf, &p, r->cb_program);
    nfs_kv_put_le32(buf, &p, r->flags);
    nfs_kv_put_le32(buf, &p, fore->ca_headerpadsize);
    nfs_kv_put_le32(buf, &p, fore->ca_maxrequestsize);
    nfs_kv_put_le32(buf, &p, fore->ca_maxresponsesize);
    nfs_kv_put_le32(buf, &p, fore->ca_maxresponsesize_cached);
    nfs_kv_put_le32(buf, &p, fore->ca_maxoperations);
    nfs_kv_put_le32(buf, &p, fore->ca_maxrequests);
    nfs_kv_put_le32(buf, &p, back->ca_headerpadsize);
    nfs_kv_put_le32(buf, &p, back->ca_maxrequestsize);
    nfs_kv_put_le32(buf, &p, back->ca_maxresponsesize);
    nfs_kv_put_le32(buf, &p, back->ca_maxresponsesize_cached);
    nfs_kv_put_le32(buf, &p, back->ca_maxoperations);
    nfs_kv_put_le32(buf, &p, back->ca_maxrequests);
    buf[p]     = r->mach_len & 0xff;
    buf[p + 1] = (r->mach_len >> 8) & 0xff;
    p         += 2;
    buf[p]     = r->owner_len & 0xff;
    buf[p + 1] = (r->owner_len >> 8) & 0xff;
    p         += 2;
    memcpy(buf + p, r->mach, r->mach_len);
    p += r->mach_len;
    memcpy(buf + p, r->owner, r->owner_len);
    p += r->owner_len;

    return p;
} /* nfs4_drc_session_serialize */

int
nfs4_drc_session_deserialize(
    const uint8_t                  *buf,
    uint32_t                        len,
    struct nfs4_drc_session_record *out)
{
    uint32_t p = 4;

    if (len < NFS4_DRC_SESSION_HDR_LEN ||
        nfs_kv_le32(buf) != NFS4_DRC_SESSION_MAGIC) {
        return -1;
    }

    memset(out, 0, sizeof(*out));
    out->clientid                       = nfs_kv_le64(buf + p); p += 8;
    out->verifier                       = nfs_kv_le64(buf + p); p += 8;
    out->princ_flavor                   = nfs_kv_le32(buf + p); p += 4;
    out->princ_uid                      = nfs_kv_le32(buf + p); p += 4;
    out->princ_gid                      = nfs_kv_le32(buf + p); p += 4;
    out->replay_max_slots               = nfs_kv_le32(buf + p); p += 4;
    out->replay_maxresp_cached          = nfs_kv_le32(buf + p); p += 4;
    out->cb_program                     = nfs_kv_le32(buf + p); p += 4;
    out->flags                          = nfs_kv_le32(buf + p); p += 4;
    out->fore.ca_headerpadsize          = nfs_kv_le32(buf + p); p += 4;
    out->fore.ca_maxrequestsize         = nfs_kv_le32(buf + p); p += 4;
    out->fore.ca_maxresponsesize        = nfs_kv_le32(buf + p); p += 4;
    out->fore.ca_maxresponsesize_cached = nfs_kv_le32(buf + p); p += 4;
    out->fore.ca_maxoperations          = nfs_kv_le32(buf + p); p += 4;
    out->fore.ca_maxrequests            = nfs_kv_le32(buf + p); p += 4;
    out->back.ca_headerpadsize          = nfs_kv_le32(buf + p); p += 4;
    out->back.ca_maxrequestsize         = nfs_kv_le32(buf + p); p += 4;
    out->back.ca_maxresponsesize        = nfs_kv_le32(buf + p); p += 4;
    out->back.ca_maxresponsesize_cached = nfs_kv_le32(buf + p); p += 4;
    out->back.ca_maxoperations          = nfs_kv_le32(buf + p); p += 4;
    out->back.ca_maxrequests            = nfs_kv_le32(buf + p); p += 4;
    out->mach_len                       = (uint16_t) buf[p] | ((uint16_t) buf[p + 1] << 8); p += 2;
    out->owner_len                      = (uint16_t) buf[p] | ((uint16_t) buf[p + 1] << 8); p += 2;

    if (out->mach_len > NFS4_OPAQUE_LIMIT || out->owner_len > NFS4_OPAQUE_LIMIT ||
        p + out->mach_len + out->owner_len > len) {
        return -1;
    }
    memcpy(out->mach, buf + p, out->mach_len);
    p += out->mach_len;
    memcpy(out->owner, buf + p, out->owner_len);

    return 0;
} /* nfs4_drc_session_deserialize */

void
nfs4_drc_persist_session(
    struct chimera_vfs_thread          *vfs_thread,
    struct nfs4_session                *session,
    const struct nfs4_client_principal *principal,
    uint32_t                            cb_program,
    uint32_t                            flags)
{
    struct nfs_client             *uc = session->client_unified;
    struct nfs4_drc_kv_ctx        *ctx;
    struct nfs4_drc_session_record rec;

    if (!uc) {
        return;
    }

    memset(&rec, 0, sizeof(rec));
    rec.clientid              = uc->client_id;
    rec.verifier              = uc->verifier;
    rec.princ_flavor          = principal ? principal->flavor : 0;
    rec.princ_uid             = principal ? principal->uid : 0;
    rec.princ_gid             = principal ? principal->gid : 0;
    rec.replay_max_slots      = session->replay_max_slots;
    rec.replay_maxresp_cached = session->replay_maxresp_cached;
    rec.cb_program            = cb_program;
    rec.flags                 = flags;
    rec.fore                  = session->nfs4_session_fore_attrs;
    rec.back                  = session->nfs4_session_back_attrs;
    rec.mach_len              = principal ? principal->machinename_len : 0;
    if (rec.mach_len > NFS4_OPAQUE_LIMIT) {
        rec.mach_len = NFS4_OPAQUE_LIMIT;
    }
    if (rec.mach_len) {
        memcpy(rec.mach, principal->machinename, rec.mach_len);
    }
    rec.owner_len = uc->owner_len;
    memcpy(rec.owner, uc->owner_string, uc->owner_len);

    ctx            = malloc(sizeof(*ctx));
    ctx->key_len   = nfs_kv_session_key(ctx->key, session->nfs4_session_id);
    ctx->value     = malloc(NFS4_DRC_SESSION_HDR_LEN + rec.mach_len + rec.owner_len);
    ctx->value_len = nfs4_drc_session_serialize(
        ctx->value, NFS4_DRC_SESSION_HDR_LEN + rec.mach_len + rec.owner_len, &rec);

    chimera_vfs_put_key(vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len, nfs4_drc_kv_done, ctx);
} /* nfs4_drc_persist_session */

/* ------------------------------------------------------------------ *
*  DESTROY_SESSION cleanup: drop the session record + its replies     *
* ------------------------------------------------------------------ */

struct nfs4_drc_forget_ctx {
    struct chimera_vfs_thread *vfs_thread;
    uint8_t                    sessionid[NFS4_SESSIONID_SIZE];
    uint8_t                    start[CHIMERA_KV_REPLY_KEY_LEN];
    /* Collected reply keys to delete after the scan (bounded by slot count). */
    uint8_t                    keys[NFS4_MAX_REPLY_CACHE_SLOTS][CHIMERA_KV_REPLY_KEY_LEN];
    uint32_t                   key_lens[NFS4_MAX_REPLY_CACHE_SLOTS];
    uint32_t                   nkeys;
};

static int
nfs4_drc_forget_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs4_drc_forget_ctx *ctx = private_data;

    (void) value;
    (void) value_len;

    /* Stop once we leave this session's reply entries (keys are ordered:
     * hdr(3) + sessionid(16) + slot + seqid). */
    if (key_len < CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE ||
        memcmp((const uint8_t *) key + CHIMERA_KV_HDR_LEN,
               ctx->sessionid, NFS4_SESSIONID_SIZE) != 0) {
        return 1;
    }

    if (ctx->nkeys < NFS4_MAX_REPLY_CACHE_SLOTS &&
        key_len <= CHIMERA_KV_REPLY_KEY_LEN) {
        memcpy(ctx->keys[ctx->nkeys], key, key_len);
        ctx->key_lens[ctx->nkeys] = key_len;
        ctx->nkeys++;
    }
    return 0;
} /* nfs4_drc_forget_scan_cb */

static void
nfs4_drc_forget_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs4_drc_forget_ctx *ctx = private_data;
    uint32_t                    i;

    (void) error_code;

    for (i = 0; i < ctx->nkeys; i++) {
        struct nfs4_drc_kv_ctx *dctx = malloc(sizeof(*dctx));

        dctx->value     = NULL;
        dctx->value_len = 0;
        dctx->key_len   = ctx->key_lens[i];
        memcpy(dctx->key, ctx->keys[i], ctx->key_lens[i]);
        chimera_vfs_delete_key(ctx->vfs_thread, dctx->key, dctx->key_len,
                               nfs4_drc_kv_done, dctx);
    }

    free(ctx);
} /* nfs4_drc_forget_complete */

void
nfs4_drc_forget_session(
    struct chimera_vfs_thread *vfs_thread,
    const uint8_t             *sessionid)
{
    struct nfs4_drc_forget_ctx *ctx;
    struct nfs4_drc_kv_ctx     *sctx;
    uint32_t                    slen;

    /* Delete the session-metadata record (fire-and-forget). */
    sctx            = malloc(sizeof(*sctx));
    sctx->value     = NULL;
    sctx->value_len = 0;
    sctx->key_len   = nfs_kv_session_key(sctx->key, sessionid);
    chimera_vfs_delete_key(vfs_thread, sctx->key, sctx->key_len,
                           nfs4_drc_kv_done, sctx);

    /* Then scan + delete this session's reply entries. */
    ctx             = calloc(1, sizeof(*ctx));
    ctx->vfs_thread = vfs_thread;
    memcpy(ctx->sessionid, sessionid, NFS4_SESSIONID_SIZE);
    slen = nfs_kv_reply_key(ctx->start, sessionid, 0, 0);

    /* No end key + flags 0: the search returns key-ordered results and the
     * callback stops once the sessionid in the key no longer matches. */
    chimera_vfs_search_keys(vfs_thread, ctx->start, slen,
                            NULL, 0, 0,
                            nfs4_drc_forget_scan_cb,
                            nfs4_drc_forget_complete, ctx);
} /* nfs4_drc_forget_session */

/* ------------------------------------------------------------------ *
*  cold-start reload                                                 *
* ------------------------------------------------------------------ */

/* Reconstruct (idempotently) a confirmed client record with its original
 * clientid so the post-restart EXCHANGE_ID from the same owner resolves to it.
 * Runs under the table lock. */
static void
nfs4_drc_ensure_client(
    struct nfs4_client_table *table,
    uint64_t                  client_id,
    uint64_t                  verifier,
    const uint8_t            *owner,
    uint16_t                  owner_len,
    uint32_t                  princ_flavor,
    uint32_t                  princ_uid,
    uint32_t                  princ_gid,
    const uint8_t            *princ_mach,
    uint16_t                  princ_mach_len,
    uint64_t                  boot_id)
{
    struct nfs4_client *client;

    pthread_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);
    if (client) {
        pthread_mutex_unlock(&table->nfs4_ct_lock);
        return;
    }

    client                        = calloc(1, sizeof(*client));
    client->nfs4_client_id        = client_id;
    client->nfs4_client_owner_len = owner_len;
    client->nfs4_client_refcnt    = 1;
    client->nfs4_client_verifier  = verifier;
    client->nfs4_client_confirmed = 1;
    memcpy(client->nfs4_client_owner, owner, owner_len);

    client->nfs4_client_princ_flavor   = princ_flavor;
    client->nfs4_client_princ_uid      = princ_uid;
    client->nfs4_client_princ_gid      = princ_gid;
    client->nfs4_client_princ_mach_len = princ_mach_len;
    if (princ_mach_len) {
        memcpy(client->nfs4_client_princ_mach, princ_mach, princ_mach_len);
    }
    strncpy(client->nfs4_client_domain, "recovered", NFS4_OPAQUE_LIMIT);
    strncpy(client->nfs4_client_name, "recovered", NFS4_OPAQUE_LIMIT);

    client->unified            = nfs_client_alloc(client_id, owner, owner_len, verifier, 1);
    client->unified->confirmed = 1;
    client->unified->boot_id   = boot_id;

    HASH_ADD(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
             nfs4_client_owner, client->nfs4_client_owner_len, client);
    HASH_ADD(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
             nfs4_client_id, sizeof(client->nfs4_client_id), client);

    /* Keep the id allocator ahead of every restored clientid.  The low 48 bits
     * are the per-instance counter (the high 16 are this node's node_id), and
     * next_client_id holds just that counter -- so compare/advance on the low
     * bits, not the whole 64-bit value. */
    {
        uint64_t counter = client_id & NFS4_CLIENTID_COUNTER_MASK;

        if (table->nfs4_ct_next_client_id <= counter) {
            table->nfs4_ct_next_client_id = counter + 1;
        }
    }

    pthread_mutex_unlock(&table->nfs4_ct_lock);
} /* nfs4_drc_ensure_client */

void
nfs4_drc_reconstruct_session(
    struct nfs4_client_table             *table,
    const uint8_t                        *sessionid,
    const struct nfs4_drc_session_record *rec,
    uint64_t                              boot_id)
{
    struct nfs4_session *session;

    nfs4_drc_ensure_client(table, rec->clientid, rec->verifier,
                           rec->owner, rec->owner_len,
                           rec->princ_flavor, rec->princ_uid, rec->princ_gid,
                           rec->mach, rec->mach_len, boot_id);

    session = nfs4_create_session(table, rec->clientid, 0,
                                  rec->replay_max_slots, rec->replay_maxresp_cached,
                                  &rec->fore, &rec->back, sessionid);
    if (session) {
        session->nfs4_session_persist = true;
        /* Drop the +1 caller ref; the hash holds the session. */
        nfs4_session_put(session);
    }
} /* nfs4_drc_reconstruct_session */

void
nfs4_drc_repopulate_slot(
    struct nfs4_session *session,
    uint32_t             slotid,
    uint32_t             seqid,
    const void          *data,
    uint32_t             len)
{
    struct nfs4_replay_slot *slot;

    if (slotid >= session->replay_max_slots ||
        len > session->replay_maxresp_cached) {
        return;
    }

    slot = &session->replay_slots[slotid];
    if (slot->cached_buf) {
        return;  /* a higher seqid already won this slot */
    }

    slot->cached_buf = malloc(len);
    if (!slot->cached_buf) {
        return;  /* OOM: leave the slot uncached (degrade to a cache miss) */
    }
    memcpy(slot->cached_buf, data, len);
    slot->cached_len = len;
    atomic_fetch_add_explicit(&session->replay_bytes_in_use, len,
                              memory_order_relaxed);
    atomic_store_explicit(&slot->state_word,
                          nfs4_slot_word(seqid, NFS4_SLOT_CACHED),
                          memory_order_release);
} /* nfs4_drc_repopulate_slot */

/* ------------------------------------------------------------------ *
 *  lazy per-session hydrate                                          *
 * ------------------------------------------------------------------ *
 *
 * A 4.1 session's metadata + reply slots are keyed by its (globally unique)
 * sessionid, with no node_id, so they survive a client failing over to a
 * different node.  Rather than bulk-reload every session at boot (which would
 * reconstruct sessions that may never reconnect, and is wrong when the session
 * belongs to a still-live peer), each node reconstructs a session lazily, the
 * first time a client presents an unknown sessionid here (SEQUENCE /
 * BIND_CONN_TO_SESSION).  The caller holds the client off with NFS4ERR_DELAY
 * until the async KV scan settles, then the retry finds the live session.
 *
 * The hydra set deduplicates concurrent first-contacts on the same sessionid
 * (so two threads never double-reconstruct) and negatively caches a genuinely
 * unknown sessionid (so a bad id is not rescanned on every retry).
 */

enum {
    NFS4_DRC_HYDRA_INFLIGHT = 0, /* KV scan in progress -> reply DELAY        */
    NFS4_DRC_HYDRA_DONE     = 1, /* scan settled; if still not live -> ABSENT */
};

struct nfs4_drc_hydra {
    uint8_t        sessionid[NFS4_SESSIONID_SIZE];
    uint8_t        state;
    UT_hash_handle hh;
};

void
nfs4_drc_hydra_init(struct chimera_server_nfs_shared *shared)
{
    pthread_mutex_init(&shared->nfs4_drc_hydra_lock, NULL);
    shared->nfs4_drc_hydra = NULL;
} /* nfs4_drc_hydra_init */

void
nfs4_drc_hydra_destroy(struct chimera_server_nfs_shared *shared)
{
#ifndef __clang_analyzer__ /* HASH_ITER trips the analyzer's alias checker */
    struct nfs4_drc_hydra *h, *tmp;

    HASH_ITER(hh, shared->nfs4_drc_hydra, h, tmp)
    {
        HASH_DEL(shared->nfs4_drc_hydra, h);
        free(h);
    }
#endif /* ifndef __clang_analyzer__ */
    pthread_mutex_destroy(&shared->nfs4_drc_hydra_lock);
} /* nfs4_drc_hydra_destroy */

struct nfs4_drc_hydrate_ctx {
    struct chimera_server_nfs_thread *thread;
    uint8_t                           sessionid[NFS4_SESSIONID_SIZE];
    uint8_t                           sstart[CHIMERA_KV_SESSION_KEY_LEN];
    uint8_t                           rstart[CHIMERA_KV_REPLY_KEY_LEN];
    bool                              found;
    uint32_t                          nreplies;
};

static int
nfs4_drc_hydrate_session_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs4_drc_hydrate_ctx      *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct nfs4_client_table         *table  = &thread->shared->nfs4_shared_clients;
    struct nfs4_drc_session_record    rec;

    if (key_len < CHIMERA_KV_SESSION_PREFIX_LEN ||
        memcmp((const uint8_t *) key + CHIMERA_KV_HDR_LEN, ctx->sessionid,
               NFS4_SESSIONID_SIZE) != 0) {
        return 1;  /* past this session's metadata record */
    }
    if (nfs4_drc_session_deserialize(value, value_len, &rec) != 0) {
        return 0;  /* skip a corrupt record */
    }

    nfs4_drc_reconstruct_session(table, ctx->sessionid, &rec,
                                 thread->shared->nfs4_recovery.current_boot_id);
    ctx->found = true;
    return 0;
} /* nfs4_drc_hydrate_session_cb */

static int
nfs4_drc_hydrate_reply_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs4_drc_hydrate_ctx      *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    const uint8_t                    *k      = key;
    const uint8_t                    *cached_data;
    struct nfs4_session              *session;
    uint32_t                          slotid, seqid, cached_len;

    if (key_len < CHIMERA_KV_REPLY_KEY_LEN ||
        memcmp(k + CHIMERA_KV_HDR_LEN, ctx->sessionid,
               NFS4_SESSIONID_SIZE) != 0) {
        return 1;  /* past this session's reply band */
    }
    if (nfs4_drc_reply_parse(value, value_len, &seqid, &cached_data,
                             &cached_len) != 0) {
        return 0;
    }

    slotid = nfs_kv_le32(k + CHIMERA_KV_HDR_LEN + NFS4_SESSIONID_SIZE);

    session = nfs4_session_lookup(&thread->shared->nfs4_shared_clients,
                                  ctx->sessionid);
    if (!session) {
        /* Orphan (no session record reconstructed): drop it. */
        nfs4_drc_delete_reply(thread->vfs_thread, ctx->sessionid, slotid, seqid);
        return 0;
    }

    nfs4_drc_repopulate_slot(session, slotid, seqid, cached_data, cached_len);
    ctx->nreplies++;

    nfs4_session_put(session);
    return 0;
} /* nfs4_drc_hydrate_reply_cb */

static void
nfs4_drc_hydrate_reply_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs4_drc_hydrate_ctx      *ctx    = private_data;
    struct chimera_server_nfs_shared *shared = ctx->thread->shared;
    struct nfs4_drc_hydra            *h;

    (void) error_code;

    /* Reconstruction settled: flip the hydra entry to DONE so a retry that
     * still cannot find the session in memory resolves to ABSENT (not a
     * perpetual DELAY) and a bad sessionid is not rescanned. */
    pthread_mutex_lock(&shared->nfs4_drc_hydra_lock);
    HASH_FIND(hh, shared->nfs4_drc_hydra, ctx->sessionid, NFS4_SESSIONID_SIZE, h);
    if (h) {
        h->state = NFS4_DRC_HYDRA_DONE;
    }
    pthread_mutex_unlock(&shared->nfs4_drc_hydra_lock);

    if (ctx->found) {
        chimera_nfs_info("NFS4.1 DRC: hydrated session + %u reply record(s) "
                         "for a returning client", ctx->nreplies);
    }
    free(ctx);
} /* nfs4_drc_hydrate_reply_complete */

static void
nfs4_drc_hydrate_session_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs4_drc_hydrate_ctx *ctx = private_data;

    (void) error_code;

    /* Session (and its client) are in place; now repopulate its reply slots.
     * The [hdr] + sessionid prefix selects exactly this session's reply band;
     * the callback stops once a key leaves it. */
    nfs_kv_reply_key(ctx->rstart, ctx->sessionid, 0, 0);
    chimera_vfs_search_keys(ctx->thread->vfs_thread,
                            ctx->rstart, CHIMERA_KV_REPLY_KEY_LEN,
                            NULL, 0, 0,
                            nfs4_drc_hydrate_reply_cb,
                            nfs4_drc_hydrate_reply_complete, ctx);
} /* nfs4_drc_hydrate_session_complete */

enum nfs4_drc_hydrate_result
nfs4_drc_session_hydrate(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *sessionid)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_recovery              *rec    = &shared->nfs4_recovery;
    struct nfs4_drc_hydra            *h;
    struct nfs4_drc_hydrate_ctx      *ctx;

    /* Nothing is persisted unless the 4.1 reply cache is on and the backend is
     * durable -- an unknown session is then simply unknown. */
    if (!rec->nfs4_drc || rec->persistence_disabled) {
        return NFS4_DRC_HYDRATE_ABSENT;
    }

    pthread_mutex_lock(&shared->nfs4_drc_hydra_lock);
    HASH_FIND(hh, shared->nfs4_drc_hydra, sessionid, NFS4_SESSIONID_SIZE, h);
    if (h) {
        enum nfs4_drc_hydrate_result r =
            (h->state == NFS4_DRC_HYDRA_DONE) ? NFS4_DRC_HYDRATE_ABSENT
                                              : NFS4_DRC_HYDRATE_INFLIGHT;
        pthread_mutex_unlock(&shared->nfs4_drc_hydra_lock);
        return r;
    }
    h = calloc(1, sizeof(*h));
    memcpy(h->sessionid, sessionid, NFS4_SESSIONID_SIZE);
    h->state = NFS4_DRC_HYDRA_INFLIGHT;
    HASH_ADD(hh, shared->nfs4_drc_hydra, sessionid, NFS4_SESSIONID_SIZE, h);
    pthread_mutex_unlock(&shared->nfs4_drc_hydra_lock);

    /* First contact for this sessionid: scan its band out of the KV store.
     * Phase 1 reconstructs the session (+ client); phase 2 repopulates its
     * reply slots; then the hydra entry flips to DONE. */
    ctx         = calloc(1, sizeof(*ctx));
    ctx->thread = thread;
    memcpy(ctx->sessionid, sessionid, NFS4_SESSIONID_SIZE);
    nfs_kv_session_key(ctx->sstart, sessionid);

    chimera_vfs_search_keys(thread->vfs_thread,
                            ctx->sstart, CHIMERA_KV_SESSION_KEY_LEN,
                            NULL, 0, 0,
                            nfs4_drc_hydrate_session_cb,
                            nfs4_drc_hydrate_session_complete, ctx);
    return NFS4_DRC_HYDRATE_INFLIGHT;
} /* nfs4_drc_session_hydrate */
