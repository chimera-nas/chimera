// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "nfs4_drc.h"
#include "nfs4_lease.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_kv_keys.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

#define NFS4_DRC_SESSION_MAGIC   0x3153534Eu /* "NSS1" */
/* Bumped from "NRP1" when the cached bytes changed from the whole on-wire
 * message to the RPC reply alone; a magic mismatch is already read as a miss. */
#define NFS4_DRC_REPLY_MAGIC     0x3250524Eu /* "NRP2" */

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

struct nfs4_drc_rc_ctx {
    struct chimera_vfs_thread *vfs_thread;
    struct nfs4_session       *session;
    uint8_t                    key[CHIMERA_KV_SESSION_KEY_LEN];
    uint32_t                   key_len;
    uint8_t                   *value;
    void                       (*done)(
        bool  ok,
        void *arg);
    void                      *arg;
};

static void
nfs4_drc_rc_finish(
    struct nfs4_drc_rc_ctx *ctx,
    bool                    ok)
{
    ctx->done(ok, ctx->arg);
    free(ctx->value);
    free(ctx);
} /* nfs4_drc_rc_finish */

static void
nfs4_drc_rc_undone(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    nfs4_drc_rc_finish(private_data, true);
} /* nfs4_drc_rc_undone */

static void
nfs4_drc_rc_put_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs4_drc_rc_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_error("NFS4.1 DRC: cannot record RECLAIM_COMPLETE on the "
                          "session record (error %d)", error_code);
    } else if (atomic_load_explicit(&ctx->session->destroyed,
                                    memory_order_acquire)) {
        /* DESTROY_SESSION marks the session before deleting its record, so a
         * write that landed after that delete is seen here: remove it again. */
        chimera_vfs_delete_key(ctx->vfs_thread, ctx->key, ctx->key_len,
                               nfs4_drc_rc_undone, ctx);
        return;
    }
    nfs4_drc_rc_finish(ctx, error_code == CHIMERA_VFS_OK);
} /* nfs4_drc_rc_put_done */

static void
nfs4_drc_rc_get_cb(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs4_drc_rc_ctx         *ctx = private_data;
    struct nfs4_drc_session_record *rec;
    uint32_t                        len;

    /* No record: nothing a node could rebuild, so nothing to mark. */
    if (error_code == CHIMERA_VFS_ENOENT ||
        (error_code == CHIMERA_VFS_OK && !value)) {
        nfs4_drc_rc_finish(ctx, true);
        return;
    }
    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_error("NFS4.1 DRC: cannot read the session record to "
                          "record RECLAIM_COMPLETE (error %d)", error_code);
        nfs4_drc_rc_finish(ctx, false);
        return;
    }

    rec = malloc(sizeof(*rec));
    if (!rec) {
        nfs4_drc_rc_finish(ctx, false);
        return;
    }
    if (nfs4_drc_session_deserialize(value, value_len, rec) != 0) {
        /* A corrupt record cannot be rebuilt on any node either. */
        chimera_nfs_error("NFS4.1 DRC: session record is corrupt; "
                          "RECLAIM_COMPLETE not recorded on it");
        free(rec);
        nfs4_drc_rc_finish(ctx, true);
        return;
    }
    if (rec->flags & NFS4_DRC_SESSION_RECLAIM_COMPLETE) {
        free(rec);
        nfs4_drc_rc_finish(ctx, true);
        return;
    }

    rec->flags |= NFS4_DRC_SESSION_RECLAIM_COMPLETE;
    len         = NFS4_DRC_SESSION_HDR_LEN + rec->mach_len + rec->owner_len;
    ctx->value  = malloc(len);
    if (!ctx->value) {
        free(rec);
        nfs4_drc_rc_finish(ctx, false);
        return;
    }
    len = nfs4_drc_session_serialize(ctx->value, len, rec);
    free(rec);

    chimera_vfs_put_key(ctx->vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, len, nfs4_drc_rc_put_done, ctx);
} /* nfs4_drc_rc_get_cb */

void
nfs4_drc_persist_reclaim_complete(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs4_session       *session,
    void (                    *done )(
        bool  ok,
        void *arg),
    void                      *arg)
{
    struct nfs4_drc_rc_ctx *ctx = calloc(1, sizeof(*ctx));

    if (!ctx) {
        done(false, arg);
        return;
    }
    ctx->vfs_thread = vfs_thread;
    ctx->session    = session;
    ctx->key_len    = nfs_kv_session_key(ctx->key, session->nfs4_session_id);
    ctx->done       = done;
    ctx->arg        = arg;
    chimera_vfs_get_key(vfs_thread, ctx->key, ctx->key_len,
                        nfs4_drc_rc_get_cb, ctx);
} /* nfs4_drc_persist_reclaim_complete */

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

/* Delete a session's metadata record (fire-and-forget). */
static void
nfs4_drc_delete_session_record(
    struct chimera_vfs_thread *vfs_thread,
    const uint8_t             *sessionid)
{
    struct nfs4_drc_kv_ctx *sctx;

    sctx            = malloc(sizeof(*sctx));
    sctx->value     = NULL;
    sctx->value_len = 0;
    sctx->key_len   = nfs_kv_session_key(sctx->key, sessionid);
    chimera_vfs_delete_key(vfs_thread, sctx->key, sctx->key_len,
                           nfs4_drc_kv_done, sctx);
} /* nfs4_drc_delete_session_record */

void
nfs4_drc_forget_session(
    struct chimera_vfs_thread *vfs_thread,
    const uint8_t             *sessionid)
{
    struct nfs4_drc_forget_ctx *ctx;
    uint32_t                    slen;

    nfs4_drc_delete_session_record(vfs_thread, sessionid);

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
 * Runs under the table lock.  Returns the client with a reference for the
 * caller, or NULL when the session must not bring its client back:
 *
 *  - the clientid's record is here but its lease was cancelled.  The fence
 *    removed the session from memory; rebuilding it on the tombstone would let
 *    SEQUENCE succeed while every OPEN answers NFS4ERR_EXPIRED.
 *  - the clientid is unknown but another record holds the owner.  The session
 *    belongs to a record that was superseded or retired, and restoring it would
 *    put a second record under the owner. */
static struct nfs_client *
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
    struct nfs4_client *client, *holder;
    struct nfs_client  *unified = NULL;

    evpl_mutex_lock(&table->nfs4_ct_lock);

    HASH_FIND(nfs4_client_hh_by_id, table->nfs4_ct_clients_by_id,
              &client_id, sizeof(client_id), client);
    if (client) {
        if (client->unified && !client->nfs4_client_fenced_ns &&
            !nfs_client_lease_cancelled(client->unified)) {
            unified = nfs_client_get(client->unified);
        }
        evpl_mutex_unlock(&table->nfs4_ct_lock);
        return unified;
    }

    HASH_FIND(nfs4_client_hh_by_owner, table->nfs4_ct_clients_by_owner,
              owner, owner_len, holder);
    if (holder) {
        evpl_mutex_unlock(&table->nfs4_ct_lock);
        return NULL;
    }

    client                        = calloc(1, sizeof(*client));
    client->nfs4_client_id        = client_id;
    client->nfs4_client_owner_len = owner_len;
    client->nfs4_client_refcnt    = 1;
    client->nfs4_client_verifier  = verifier;
    client->nfs4_client_confirmed = 1;
    memcpy(client->nfs4_client_owner, owner, owner_len);

    /* This client is being restored from its persisted session, not newly
     * established: it will resume that session on its next SEQUENCE without
     * ever seeing NFS4ERR_BADSESSION, so it runs no EXCHANGE_ID and no
     * CREATE_SESSION.  RFC 8881 section 18.51.3 ties the RECLAIM_COMPLETE
     * obligation to establishing a NEW client ID, which this client does not
     * do, and nothing prompts it for one unless the server raises
     * SEQ4_STATUS_RESTART_RECLAIM_NEEDED -- which it does only for a client
     * that failed over from a dead node (nfs4_drc_hydrate_failover_cb, which
     * clears this mark again).  Restore it already marked, or the per-client
     * gate in chimera_nfs4_open() refuses every non-reclaim OPEN with
     * NFS4ERR_GRACE for the life of the mount.  The server-wide reboot window
     * still applies and still expires normally.
     *
     * Two consequences of the mark are accepted deliberately on a same-node
     * restart, or when the client's last node is still alive:
     *  - The same gate answers a CLAIM_PREVIOUS from this client with
     *    NFS4ERR_NO_GRACE.  That forfeits nothing today: no open or lock
     *    state is persisted (nfs_kv_keys.h), so a reclaim could rebuild
     *    nothing a post-grace CLAIM_NULL cannot, and a client that
     *    re-establishes with the same owner and verifier is answered
     *    CONFIRMED_R on this same clientid (nfs4_client_exchange_id case 2),
     *    which tells it no state was lost, so it does not reclaim anyway.
     *    Another client's reclaim of a conflicting share can win a file this
     *    one held; it could once the window closed regardless.  Revisit the
     *    day open or lock state is persisted, or the day hydrated clients
     *    are asked to reclaim.
     *  - The reloaded to_reclaim record for this client is marked returned
     *    when the hydration path re-persists it
     *    (nfs4_drc_hydrate_session_complete), so the grace-end purge keeps
     *    its stable-storage record; its global RECLAIM_COMPLETE is
     *    NFS4ERR_COMPLETE_ALREADY before it reaches the recovery
     *    bookkeeping, so the window cannot end early on the all-reclaimed
     *    exit while that record is outstanding -- the deadline ends it.
     *
     * Set directly rather than through nfs4_client_mark_reclaim_complete():
     * nfs4_ct_lock is held here and is not recursive. */
    client->nfs4_client_reclaim_complete = 1;

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
    client->nfs4_client_in_owner_table = 1;
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

    unified = nfs_client_get(client->unified);

    evpl_mutex_unlock(&table->nfs4_ct_lock);
    return unified;
} /* nfs4_drc_ensure_client */

struct nfs_client *
nfs4_drc_reconstruct_session(
    struct nfs4_client_table             *table,
    const uint8_t                        *sessionid,
    const struct nfs4_drc_session_record *rec,
    uint64_t                              boot_id)
{
    struct nfs4_session *session;
    struct nfs_client   *unified;

    unified = nfs4_drc_ensure_client(table, rec->clientid, rec->verifier,
                                     rec->owner, rec->owner_len,
                                     rec->princ_flavor, rec->princ_uid,
                                     rec->princ_gid,
                                     rec->mach, rec->mach_len, boot_id);
    if (!unified) {
        return NULL;
    }

    session = nfs4_create_session(table, rec->clientid, 0,
                                  rec->replay_max_slots, rec->replay_maxresp_cached,
                                  &rec->fore, &rec->back, sessionid, true);
    if (!session) {
        /* The record went away between the two lookups. */
        nfs_client_put(unified);
        return NULL;
    }

    /* Drop the +1 caller ref; the hash holds the session. */
    nfs4_session_put(session);
    return unified;
} /* nfs4_drc_reconstruct_session */

enum nfs4_drc_repopulate_result
nfs4_drc_repopulate_slot(
    struct nfs4_session *session,
    uint32_t             slotid,
    uint32_t             seqid,
    const void          *data,
    uint32_t             len)
{
    struct nfs4_replay_slot *slot;
    uint64_t expect;

    if (slotid >= session->replay_max_slots ||
        len > session->replay_maxresp_cached) {
        return NFS4_DRC_REPOPULATE_STALE;
    }

    slot = &session->replay_slots[slotid];

    /*
     * A slot leaves RESYNC exactly once, through this CAS or through the one
     * in nfs4_replay_slot_acquire, whichever runs first.  The session is in
     * the table before the reply scan starts, but acquire leaves a RESYNC
     * slot alone until the scan has finished (replay_hydrating), so within a
     * scan this CAS fails only on a slot the scan already filled from another
     * record, which a live request may since have replayed or advanced; the
     * word that beat the CAS says whether the record is that slot's own
     * (kept) or stale (dropped).  Holding IN_PROGRESS while the bytes are
     * copied keeps a concurrent retry off cached_buf (it sees NFS4ERR_DELAY
     * for that instant; an advance to seqid+1 landing then gets
     * NFS4ERR_SEQ_MISORDERED, which the Linux client turns into a backoff).
     *
     * A record that did not install is reported to the caller, which deletes
     * it from the KV store unless it is the live slot's own: the advance path
     * only deletes the record of a slot it finds CACHED, so a dropped record
     * would otherwise survive to win a RESYNC slot after the next restart.
     */
    expect = nfs4_slot_word(0, NFS4_SLOT_RESYNC);
    if (!atomic_compare_exchange_strong_explicit(
            &slot->state_word, &expect,
            nfs4_slot_word(seqid, NFS4_SLOT_IN_PROGRESS),
            memory_order_acq_rel, memory_order_acquire)) {
        /* expect now holds the word that beat us.  CACHED at this seqid is
         * the live slot's own record (the finalize path stores CACHED before
         * it persists).  IN_PROGRESS at this seqid is either that record
         * being replayed (the pin from CACHED) or a new request at this
         * seqid, whose reply replaces the record only if it is captured
         * (cachethis=true, and not demoted over replay_maxresp_cached or the
         * session byte cap); otherwise the slot ends COMPLETED with nothing
         * persisted, and the record is kept all the same.  A resync in flight
         * shows RESYNCING and does not keep the record: the slot ends
         * COMPLETED with no bytes, so the record is stale. */
        if ((expect >> NFS4_SLOT_SEQID_SHIFT) == seqid &&
            ((expect & NFS4_SLOT_STATE_MASK) == NFS4_SLOT_CACHED ||
             (expect & NFS4_SLOT_STATE_MASK) == NFS4_SLOT_IN_PROGRESS)) {
            return NFS4_DRC_REPOPULATE_KEPT;
        }
        return NFS4_DRC_REPOPULATE_STALE;
    }

    slot->cached_buf = malloc(len);
    if (!slot->cached_buf) {
        /* OOM: the seqid is known even though the bytes cannot be kept, so
         * the slot degrades to an uncached retry rather than waiting to
         * resync. */
        atomic_store_explicit(&slot->state_word,
                              nfs4_slot_word(seqid, NFS4_SLOT_COMPLETED),
                              memory_order_release);
        return NFS4_DRC_REPOPULATE_STALE;
    }
    memcpy(slot->cached_buf, data, len);
    slot->cached_len = len;
    atomic_fetch_add_explicit(&session->replay_bytes_in_use, len,
                              memory_order_relaxed);
    atomic_store_explicit(&slot->state_word,
                          nfs4_slot_word(seqid, NFS4_SLOT_CACHED),
                          memory_order_release);
    return NFS4_DRC_REPOPULATE_INSTALLED;
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
    NFS4_DRC_HYDRA_INFLIGHT    = 0, /* KV scan in progress -> reply DELAY        */
    NFS4_DRC_HYDRA_DONE        = 1, /* scan settled; if still not live -> ABSENT */
    NFS4_DRC_HYDRA_RETRY       = 2, /* nothing published yet -> rescan, DELAY    */
    NFS4_DRC_HYDRA_REPLY_RETRY = 3, /* session published, its replies not all
                                     * loaded -> rescan them, DELAY            */
};

struct nfs4_drc_hydra {
    uint8_t        sessionid[NFS4_SESSIONID_SIZE];
    uint8_t        state;
    uint16_t       peer_seen_node;     /* the peer peer_seen_ns came from */
    uint64_t       peer_seen_ns;       /* peer heartbeat seen while pending */
    uint64_t       undecided_since_ns; /* monotonic; 0 = decision not open  */
    uint64_t       replies_failing_ns; /* monotonic; 0 = no reply scan error */
    UT_hash_handle hh;
};

void
nfs4_drc_hydra_init(struct chimera_server_nfs_shared *shared)
{
    evpl_mutex_init(&shared->nfs4_drc_hydra_lock, NULL);
    shared->nfs4_drc_hydra = NULL;
    atomic_store(&shared->nfs4_drc_hydrating, 0);
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
    evpl_mutex_destroy(&shared->nfs4_drc_hydra_lock);
} /* nfs4_drc_hydra_destroy */

struct nfs4_drc_hydrate_ctx {
    struct chimera_server_nfs_thread *thread;
    uint8_t                           sessionid[NFS4_SESSIONID_SIZE];
    uint8_t                           sstart[CHIMERA_KV_SESSION_KEY_LEN];
    uint8_t                           rstart[CHIMERA_KV_REPLY_KEY_LEN];
    bool                              found;
    bool                              restored;
    uint32_t                          nreplies;
    uint32_t                          nstale;   /* records dropped (stale or orphaned) */
    struct nfs_client                *hydrated; /* the client the session rebuilt (a reference) */
    /* Keys of the dropped records, deleted by nfs4_drc_hydrate_reply_complete
     * on the owning thread (bounded by slot count, as in forget_ctx);
     * ndrop_over counts the ones past the bound, left in the store. */
    uint8_t                           drop_keys[NFS4_MAX_REPLY_CACHE_SLOTS][CHIMERA_KV_REPLY_KEY_LEN];
    uint32_t                          drop_key_lens[NFS4_MAX_REPLY_CACHE_SLOTS];
    uint32_t                          ndrop;
    uint32_t                          ndrop_over;
    /* The session record, rebuilt only once the failover decision is made. */
    struct nfs4_drc_session_record    srec;
    uint16_t                          owner_node;
    uint16_t                          peer_seen_node;
    uint64_t                          peer_seen_ns;
    bool                              rc_evidence;
    bool                              failed_over;
};

/*
 * Leave the hydrate for the client's next request to pick up again (it is held
 * off with NFS4ERR_DELAY meanwhile), unless *since shows it has been failing
 * for two leases.  `state` is RETRY while nothing is published, REPLY_RETRY
 * once the session is: that keeps it gated (nfs4_drc_session_hydrating).
 * Returns true when deferred, having freed ctx.
 */
static bool
nfs4_drc_hydrate_defer(
    struct nfs4_drc_hydrate_ctx *ctx,
    uint8_t                      state,
    bool                         pending,
    uint64_t                     observed_ns)
{
    struct chimera_server_nfs_shared *shared = ctx->thread->shared;
    struct nfs4_drc_hydra            *h;
    uint64_t                         *since;
    uint64_t                          now  = nfs_lease_now_ns();
    uint64_t                          wait =
        2ULL * shared->nfs4_recovery.lease_time_s * 1000000000ULL;
    bool                              retry = false;

    evpl_mutex_lock(&shared->nfs4_drc_hydra_lock);
    HASH_FIND(hh, shared->nfs4_drc_hydra, ctx->sessionid,
              NFS4_SESSIONID_SIZE, h);
    if (h) {
        since = (state == NFS4_DRC_HYDRA_REPLY_RETRY) ?
            &h->replies_failing_ns : &h->undecided_since_ns;
        if (!*since) {
            *since = now;
        }
        if (pending && (!h->peer_seen_ns ||
                        h->peer_seen_node != ctx->owner_node)) {
            h->peer_seen_ns   = observed_ns;
            h->peer_seen_node = ctx->owner_node;
        }
        retry = now - *since < wait;
        if (retry) {
            h->state = state;
        }
    }
    evpl_mutex_unlock(&shared->nfs4_drc_hydra_lock);

    if (retry) {
        /* A published session stays counted, so its requests stay gated. */
        if (state == NFS4_DRC_HYDRA_RETRY) {
            atomic_fetch_sub(&shared->nfs4_drc_hydrating, 1);
        }
        free(ctx);
    }
    return retry;
} /* nfs4_drc_hydrate_defer */

static int
nfs4_drc_hydrate_session_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs4_drc_hydrate_ctx *ctx = private_data;

    if (key_len < CHIMERA_KV_SESSION_PREFIX_LEN ||
        memcmp((const uint8_t *) key + CHIMERA_KV_HDR_LEN, ctx->sessionid,
               NFS4_SESSIONID_SIZE) != 0) {
        return 1;  /* past this session's metadata record */
    }
    if (nfs4_drc_session_deserialize(value, value_len, &ctx->srec) != 0) {
        return 0;  /* skip a corrupt record */
    }

    ctx->rc_evidence = (ctx->srec.flags & NFS4_DRC_SESSION_RECLAIM_COMPLETE) != 0;
    ctx->found       = true;
    return 0;
} /* nfs4_drc_hydrate_session_cb */

/* Queue a scanned reply record for deletion, by its scanned key bytes.  The
 * scan callback runs on whatever thread the KV module searches on (a sync
 * delegation thread for a blocking module), which must not allocate VFS
 * requests from the owning thread's free list, so the deletes are issued from
 * nfs4_drc_hydrate_reply_complete instead (as nfs4_drc_forget_complete does). */
static void
nfs4_drc_hydrate_drop(
    struct nfs4_drc_hydrate_ctx *ctx,
    const void                  *key,
    uint32_t                     key_len)
{
    ctx->nstale++;
    if (ctx->ndrop < NFS4_MAX_REPLY_CACHE_SLOTS &&
        key_len <= CHIMERA_KV_REPLY_KEY_LEN) {
        memcpy(ctx->drop_keys[ctx->ndrop], key, key_len);
        ctx->drop_key_lens[ctx->ndrop] = key_len;
        ctx->ndrop++;
    } else {
        ctx->ndrop_over++;
    }
} /* nfs4_drc_hydrate_drop */

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
        nfs4_drc_hydrate_drop(ctx, key, key_len);
        return 0;
    }

    switch (nfs4_drc_repopulate_slot(session, slotid, seqid, cached_data,
                                     cached_len)) {
        case NFS4_DRC_REPOPULATE_INSTALLED:
            ctx->nreplies++;
            break;
        case NFS4_DRC_REPOPULATE_KEPT:
            break;
        case NFS4_DRC_REPOPULATE_STALE:
            chimera_nfs_debug("nfs4 drc dropping stale reply record "
                              "slot %u seqid %u", slotid, seqid);
            nfs4_drc_hydrate_drop(ctx, key, key_len);
            break;
    } /* switch */

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
    struct nfs4_session              *session;
    uint32_t                          i;

    /* A published session whose replies did not all load stays gated and is
     * rescanned; one that was not restored has nothing to protect. */
    if (error_code != CHIMERA_VFS_OK && ctx->restored) {
        if (nfs4_drc_hydrate_defer(ctx, NFS4_DRC_HYDRA_REPLY_RETRY, false, 0)) {
            return;
        }
        chimera_nfs_error("NFS4.1 DRC: reply records of a hydrated session "
                          "still unreadable (error %d) after two leases; "
                          "serving it with %u loaded", error_code,
                          ctx->nreplies);
    }

    /* The reply scan is over, whether it ran to the end, failed part way or
     * never dispatched (chimera_vfs_search_keys then completes inline with the
     * error): let the restored session's RESYNC slots take the client's seqid.
     * Done before the hydra entry flips, so no second hydrate of this
     * sessionid can have rebuilt another session under it.  A failed lookup
     * means the session was destroyed meanwhile. */
    if (ctx->restored) {
        session = nfs4_session_lookup(&shared->nfs4_shared_clients,
                                      ctx->sessionid);
        if (session) {
            nfs4_session_hydrate_done(session);
            nfs4_session_put(session);
        }
    }

    /* Reconstruction settled: flip the hydra entry to DONE so a retry that
     * still cannot find the session in memory resolves to ABSENT (not a
     * perpetual DELAY) and a bad sessionid is not rescanned. */
    evpl_mutex_lock(&shared->nfs4_drc_hydra_lock);
    HASH_FIND(hh, shared->nfs4_drc_hydra, ctx->sessionid, NFS4_SESSIONID_SIZE, h);
    if (h) {
        h->state = NFS4_DRC_HYDRA_DONE;
    }
    evpl_mutex_unlock(&shared->nfs4_drc_hydra_lock);
    atomic_fetch_sub(&shared->nfs4_drc_hydrating, 1);

    /* Delete the records the scan dropped, from this (the owning) thread; see
     * nfs4_drc_hydrate_drop. */
    for (i = 0; i < ctx->ndrop; i++) {
        struct nfs4_drc_kv_ctx *dctx = malloc(sizeof(*dctx));

        dctx->value     = NULL;
        dctx->value_len = 0;
        dctx->key_len   = ctx->drop_key_lens[i];
        memcpy(dctx->key, ctx->drop_keys[i], ctx->drop_key_lens[i]);
        chimera_vfs_delete_key(ctx->thread->vfs_thread, dctx->key,
                               dctx->key_len, nfs4_drc_kv_done, dctx);
    }
    if (ctx->ndrop_over) {
        chimera_nfs_info("NFS4.1 DRC: %u dropped reply record(s) past the "
                         "per-scan delete bound of %u left in the store",
                         ctx->ndrop_over, NFS4_MAX_REPLY_CACHE_SLOTS);
    }

    if (ctx->restored) {
        chimera_nfs_info("NFS4.1 DRC: hydrated session + %u reply record(s), "
                         "%u stale dropped, for a returning client%s",
                         ctx->nreplies, ctx->nstale,
                         ctx->failed_over ? " (failed over; asked to reclaim)" : "");
    } else if (ctx->found) {
        chimera_nfs_info("NFS4.1 DRC: dropped a persisted session whose client "
                         "was fenced or superseded");
    }
    free(ctx);
} /* nfs4_drc_hydrate_reply_complete */

/* Phase 3: re-persist the client's recovery record and repopulate the
 * session's reply slots. */
static void
nfs4_drc_hydrate_replies(struct nfs4_drc_hydrate_ctx *ctx)
{
    struct chimera_server_nfs_thread *thread = ctx->thread;

    /* The client is back without an EXCHANGE_ID, so nothing else re-persists
     * its recovery record: do it here, which also marks the reloaded record as
     * returned so the grace-end purge keeps it, and makes this node its
     * owner. */
    if (ctx->hydrated) {
        nfs_recovery_persist(thread->vfs_thread,
                             &thread->shared->nfs4_recovery, ctx->hydrated);
        nfs_client_put(ctx->hydrated);
        ctx->hydrated = NULL;
    } else if (ctx->found && !ctx->restored) {
        /* The session's client must not come back (nfs4_drc_ensure_client):
        * drop its record so it is not rebuilt after a restart.  The reply
        * scan below finds no session and deletes its replies as orphans. */
        nfs4_drc_delete_session_record(thread->vfs_thread, ctx->sessionid);
    }

    /* Session (and its client) are in place; now repopulate its reply slots.
     * The [hdr] + sessionid prefix selects exactly this session's reply band;
     * the callback stops once a key leaves it. */
    nfs_kv_reply_key(ctx->rstart, ctx->sessionid, 0, 0);
    chimera_vfs_search_keys(thread->vfs_thread,
                            ctx->rstart, CHIMERA_KV_REPLY_KEY_LEN,
                            NULL, 0, 0,
                            nfs4_drc_hydrate_reply_cb,
                            nfs4_drc_hydrate_reply_complete, ctx);
} /* nfs4_drc_hydrate_replies */

static void
nfs4_drc_hydrate_rebuild(
    struct nfs4_drc_hydrate_ctx *ctx,
    bool                         failed_over)
{
    struct chimera_server_nfs_shared *shared = ctx->thread->shared;

    ctx->hydrated = nfs4_drc_reconstruct_session(
        &shared->nfs4_shared_clients, ctx->sessionid, &ctx->srec,
        shared->nfs4_recovery.current_boot_id);
    ctx->restored = ctx->hydrated != NULL;

    if (failed_over && ctx->restored) {
        /* The client's node died with its state, and that state's grace window
         * is now open here: ask it to reclaim (RFC 8881 §8.4.2.1), on every
         * session it has, and let its CLAIM_PREVIOUS through the per-client
         * gate that the rebuild closed (nfs4_drc_ensure_client). */
        nfs4_client_mark_failover(&shared->nfs4_shared_clients,
                                  ctx->srec.clientid, ctx->rc_evidence);
        ctx->failed_over = true;
    }

    nfs4_drc_hydrate_replies(ctx);
} /* nfs4_drc_hydrate_rebuild */

static void
nfs4_drc_hydrate_failover_cb(
    enum nfs_recovery_failover result,
    uint64_t                   observed_ns,
    void                      *arg)
{
    struct nfs4_drc_hydrate_ctx *ctx = arg;

    /* Nothing is rebuilt and no ownership moves while the decision is open.
    * A store that keeps failing, or a live peer whose heartbeat never moves,
    * is given two leases before the session is rebuilt without failover. */
    if ((result == NFS_RECOVERY_STORE_ERROR ||
         result == NFS_RECOVERY_LIVENESS_PENDING) &&
        nfs4_drc_hydrate_defer(ctx, NFS4_DRC_HYDRA_RETRY,
                               result == NFS_RECOVERY_LIVENESS_PENDING,
                               observed_ns)) {
        return;
    }

    if (result == NFS_RECOVERY_STORE_ERROR ||
        result == NFS_RECOVERY_LIVENESS_PENDING) {
        chimera_nfs_error("NFS4.1 DRC: could not decide whether the client's "
                          "previous node is down; rebuilding the session "
                          "without failover");
    }
    nfs4_drc_hydrate_rebuild(ctx, result == NFS_RECOVERY_FAILOVER);
} /* nfs4_drc_hydrate_failover_cb */

static void
nfs4_drc_hydrate_owner_cb(
    enum nfs_recovery_owner result,
    uint16_t                node,
    uint64_t                boot_id,
    void                   *arg)
{
    struct nfs4_drc_hydrate_ctx *ctx = arg;
    struct nfs_recovery         *rec = &ctx->thread->shared->nfs4_recovery;

    /* Last served by another node: if that node is dead, its clients are
     * failing over and this one is owed a reclaim here.  A record that cannot
     * be read leaves that undecided. */
    if (result == NFS_RECOVERY_OWNER_ERROR) {
        nfs4_drc_hydrate_failover_cb(NFS_RECOVERY_STORE_ERROR, 0, ctx);
        return;
    }
    if (result == NFS_RECOVERY_OWNER_FOUND && node != rec->node_id) {
        /* A heartbeat saved from an earlier owner says nothing about this one. */
        ctx->owner_node = node;
        nfs_recovery_import_peer(ctx->thread->vfs_thread, rec, node,
                                 ctx->srec.owner, ctx->srec.owner_len, boot_id,
                                 ctx->peer_seen_node == node ?
                                 ctx->peer_seen_ns : 0,
                                 nfs4_drc_hydrate_failover_cb, ctx);
        return;
    }
    nfs4_drc_hydrate_rebuild(ctx, false);
} /* nfs4_drc_hydrate_owner_cb */

static void
nfs4_drc_hydrate_session_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs4_drc_hydrate_ctx      *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;

    /* A failed scan that had not reached the record proves nothing about
     * whether it exists; one that had read it lost nothing. */
    if (error_code != CHIMERA_VFS_OK && !ctx->found) {
        if (nfs4_drc_hydrate_defer(ctx, NFS4_DRC_HYDRA_RETRY, false, 0)) {
            return;
        }
        chimera_nfs_error("NFS4.1 DRC: session record still unreadable "
                          "(error %d) after two leases; treating the "
                          "session as unknown", error_code);
    }

    /* Phase 2: before the session is rebuilt and its record re-persisted
     * under this node, find which node owned the client -- a failover from a
     * dead one changes how the client is treated (nfs4_drc_hydrate_owner_cb).
     * All of it completes before the hydra entry flips to DONE, so the client,
     * held off with NFS4ERR_DELAY until then, never sees a half-decided
     * state. */
    if (ctx->found) {
        nfs_recovery_lookup_owner(thread->vfs_thread,
                                  &thread->shared->nfs4_recovery,
                                  ctx->srec.owner, ctx->srec.owner_len,
                                  nfs4_drc_hydrate_owner_cb, ctx);
        return;
    }
    nfs4_drc_hydrate_replies(ctx);
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
    uint64_t peer_seen_ns   = 0;
    uint16_t peer_seen_node = 0;

    /* Nothing is persisted unless the 4.1 reply cache is on and the backend is
     * durable -- an unknown session is then simply unknown. */
    if (!rec->nfs4_drc || rec->persistence_disabled) {
        return NFS4_DRC_HYDRATE_ABSENT;
    }

    /* Out of memory: change nothing, and let the client retry on DELAY. */
    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return NFS4_DRC_HYDRATE_INFLIGHT;
    }

    evpl_mutex_lock(&shared->nfs4_drc_hydra_lock);
    HASH_FIND(hh, shared->nfs4_drc_hydra, sessionid, NFS4_SESSIONID_SIZE, h);
    if (h && h->state != NFS4_DRC_HYDRA_RETRY &&
        h->state != NFS4_DRC_HYDRA_REPLY_RETRY) {
        enum nfs4_drc_hydrate_result r =
            (h->state == NFS4_DRC_HYDRA_DONE) ? NFS4_DRC_HYDRATE_ABSENT
                                              : NFS4_DRC_HYDRATE_INFLIGHT;
        evpl_mutex_unlock(&shared->nfs4_drc_hydra_lock);
        free(ctx);
        return r;
    }
    if (h) {
        peer_seen_ns   = h->peer_seen_ns;
        peer_seen_node = h->peer_seen_node;
    } else {
        h = calloc(1, sizeof(*h));
        if (!h) {
            evpl_mutex_unlock(&shared->nfs4_drc_hydra_lock);
            free(ctx);
            return NFS4_DRC_HYDRATE_INFLIGHT;
        }
        memcpy(h->sessionid, sessionid, NFS4_SESSIONID_SIZE);
        HASH_ADD(hh, shared->nfs4_drc_hydra, sessionid, NFS4_SESSIONID_SIZE, h);
    }
    /* A REPLY_RETRY entry is still counted; its session went away meanwhile,
     * so it is hydrated again from the start. */
    if (h->state != NFS4_DRC_HYDRA_REPLY_RETRY) {
        atomic_fetch_add(&shared->nfs4_drc_hydrating, 1);
    }
    h->state = NFS4_DRC_HYDRA_INFLIGHT;
    evpl_mutex_unlock(&shared->nfs4_drc_hydra_lock);

    /* First contact for this sessionid: scan its band out of the KV store.
     * Phase 1 reads the session record; phase 2 decides whether the client
     * failed over and rebuilds the session (+ client); phase 3 repopulates its
     * reply slots; then the hydra entry flips to DONE. */
    ctx->thread         = thread;
    ctx->peer_seen_ns   = peer_seen_ns;
    ctx->peer_seen_node = peer_seen_node;
    memcpy(ctx->sessionid, sessionid, NFS4_SESSIONID_SIZE);
    nfs_kv_session_key(ctx->sstart, sessionid);

    chimera_vfs_search_keys(thread->vfs_thread,
                            ctx->sstart, CHIMERA_KV_SESSION_KEY_LEN,
                            NULL, 0, 0,
                            nfs4_drc_hydrate_session_cb,
                            nfs4_drc_hydrate_session_complete, ctx);
    return NFS4_DRC_HYDRATE_INFLIGHT;
} /* nfs4_drc_session_hydrate */

bool
nfs4_drc_session_hydrating(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *sessionid)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs4_drc_hydra            *h;
    struct nfs4_drc_hydrate_ctx      *ctx = NULL;
    bool                              hydrating;

    if (atomic_load_explicit(&shared->nfs4_drc_hydrating,
                             memory_order_acquire) == 0) {
        return false;
    }

    evpl_mutex_lock(&shared->nfs4_drc_hydra_lock);
    HASH_FIND(hh, shared->nfs4_drc_hydra, sessionid, NFS4_SESSIONID_SIZE, h);
    hydrating = h && h->state != NFS4_DRC_HYDRA_DONE;
    if (h && h->state == NFS4_DRC_HYDRA_REPLY_RETRY) {
        ctx = calloc(1, sizeof(*ctx));
        if (ctx) {
            h->state = NFS4_DRC_HYDRA_INFLIGHT;
        }
    }
    evpl_mutex_unlock(&shared->nfs4_drc_hydra_lock);

    /* The session is published but its reply scan failed: scan again. */
    if (ctx) {
        ctx->thread   = thread;
        ctx->found    = true;
        ctx->restored = true;
        memcpy(ctx->sessionid, sessionid, NFS4_SESSIONID_SIZE);
        nfs4_drc_hydrate_replies(ctx);
    }
    return hydrating;
} /* nfs4_drc_session_hydrating */
