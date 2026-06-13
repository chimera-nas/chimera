// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "nfs4_recovery.h"
#include "nfs4_lease.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "nfs4_drc.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_kv_keys.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

/* Record magics (little-endian first word of each value blob). */
#define NFS_RECOVERY_RECORD_MAGIC    0x3152464Eu /* "NFR1" */
#define NFS_RECOVERY_EPOCH_MAGIC     0x31504645u /* "EPF1" */

/* Recovery value: magic(4) boot_id(8) verifier(8) client_id_hint(8)
 * owner_len(2) + owner bytes. */
#define NFS_RECOVERY_VALUE_HDR_LEN   30u
#define NFS_RECOVERY_VALUE_MAX       (NFS_RECOVERY_VALUE_HDR_LEN + NFS4_OPAQUE_LIMIT)
#define NFS_RECOVERY_EPOCH_VALUE_LEN 20u

/*
 * Generate a fresh, monotonic boot_id from CLOCK_REALTIME.  The cold-start
 * loader bumps strictly past any persisted epoch (see nfs_recovery_epoch_cb).
 */
static uint64_t
nfs_recovery_fresh_boot_id(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
} /* nfs_recovery_fresh_boot_id */

/* ------------------------------------------------------------------ *
*  (de)serialization                                                 *
* ------------------------------------------------------------------ */

uint32_t
nfs_recovery_serialize(
    uint8_t                 *buf,
    uint32_t                 buf_size,
    const struct nfs_client *c)
{
    uint32_t p = 0;

    if (buf_size < NFS_RECOVERY_VALUE_HDR_LEN + c->owner_len) {
        return 0;
    }

    nfs_kv_put_le32(buf, &p, NFS_RECOVERY_RECORD_MAGIC);
    nfs_kv_put_le64(buf, &p, c->boot_id);
    nfs_kv_put_le64(buf, &p, c->verifier);
    nfs_kv_put_le64(buf, &p, c->client_id);
    buf[p]     = c->owner_len & 0xff;
    buf[p + 1] = (c->owner_len >> 8) & 0xff;
    p         += 2;
    memcpy(buf + p, c->owner_string, c->owner_len);
    p += c->owner_len;

    return p;
} /* nfs_recovery_serialize */

int
nfs_recovery_deserialize(
    const uint8_t              *buf,
    uint32_t                    len,
    struct nfs_recovery_record *out)
{
    uint32_t p = 4;
    uint16_t owner_len;

    if (len < NFS_RECOVERY_VALUE_HDR_LEN ||
        nfs_kv_le32(buf) != NFS_RECOVERY_RECORD_MAGIC) {
        return -1;
    }

    out->boot_id        = nfs_kv_le64(buf + p);
    p                  += 8;
    out->verifier       = nfs_kv_le64(buf + p);
    p                  += 8;
    out->client_id_hint = nfs_kv_le64(buf + p);
    p                  += 8;
    owner_len           = (uint16_t) buf[p] | ((uint16_t) buf[p + 1] << 8);
    p                  += 2;

    if (owner_len > NFS4_OPAQUE_LIMIT || p + owner_len > len) {
        return -1;
    }
    out->owner_len = owner_len;
    memcpy(out->owner_string, buf + p, owner_len);
    out->reclaimed = false;

    return 0;
} /* nfs_recovery_deserialize */

uint32_t
nfs_recovery_epoch_serialize(
    uint8_t *buf,
    uint64_t boot_id)
{
    uint32_t p = 0;

    nfs_kv_put_le32(buf, &p, NFS_RECOVERY_EPOCH_MAGIC);
    nfs_kv_put_le64(buf, &p, boot_id);
    nfs_kv_put_le64(buf, &p, nfs_recovery_fresh_boot_id());
    return p;
} /* nfs_recovery_epoch_serialize */

int
nfs_recovery_epoch_deserialize(
    const uint8_t *buf,
    uint32_t       len,
    uint64_t      *out_boot_id)
{
    if (len < NFS_RECOVERY_EPOCH_VALUE_LEN ||
        nfs_kv_le32(buf) != NFS_RECOVERY_EPOCH_MAGIC) {
        return -1;
    }
    *out_boot_id = nfs_kv_le64(buf + 4);
    return 0;
} /* nfs_recovery_epoch_deserialize */

/* ------------------------------------------------------------------ *
*  init / teardown                                                   *
* ------------------------------------------------------------------ */

int
nfs_recovery_load(
    struct nfs_recovery *rec,
    struct chimera_vfs  *vfs,
    uint16_t             node_id,
    uint32_t             grace_time_s,
    bool                 nfs4_drc)
{
    const char *kvname;

    pthread_mutex_init(&rec->lock, NULL);
    rec->to_reclaim      = NULL;
    rec->pending_reclaim = 0;
    rec->current_boot_id = nfs_recovery_fresh_boot_id();
    rec->grace_end_ns    = 0;
    rec->in_grace        = false;
    rec->vfs             = vfs;
    rec->node_id         = node_id;
    rec->grace_time_s    = grace_time_s;
    rec->nfs4_drc        = nfs4_drc;
    atomic_store(&rec->load_state, NFS_REC_LOAD_IDLE);

    kvname                    = (vfs && vfs->kv_module) ? vfs->kv_module->name : "";
    rec->persistence_disabled = (strcmp(kvname, "memkv") == 0);

    if (rec->persistence_disabled) {
        chimera_nfs_info(
            "NFSv4 recovery: KV backend '%s' is non-persistent; client "
            "reclaim%s will not survive a server restart",
            kvname, nfs4_drc ? " and the reply cache" : "");
    }

    return 0;
} /* nfs_recovery_load */

void
nfs_recovery_free(struct nfs_recovery *rec)
{
#ifndef __clang_analyzer__
    struct nfs_recovery_record *r, *tmp;

    pthread_mutex_lock(&rec->lock);
    HASH_ITER(hh, rec->to_reclaim, r, tmp)
    {
        HASH_DEL(rec->to_reclaim, r);
        free(r);
    }
    rec->pending_reclaim = 0;
    pthread_mutex_unlock(&rec->lock);
    pthread_mutex_destroy(&rec->lock);
#endif /* ifndef __clang_analyzer__ */
} /* nfs_recovery_free */

/* ------------------------------------------------------------------ *
*  grace window                                                      *
* ------------------------------------------------------------------ */

void
nfs_recovery_begin_grace(
    struct nfs_recovery *rec,
    uint32_t             grace_time_s)
{
    pthread_mutex_lock(&rec->lock);
    if (rec->to_reclaim == NULL) {
        /* No clients to reclaim -- skip the grace window entirely so
         * normal traffic is accepted immediately. */
        rec->in_grace     = false;
        rec->grace_end_ns = 0;
    } else {
        rec->in_grace     = true;
        rec->grace_end_ns = nfs_lease_now_ns() +
            (uint64_t) grace_time_s * 1000000000ULL;
    }
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_begin_grace */

/* Unconditionally open the grace window for the configured duration.  Used at
 * cold-start kickoff while the to_reclaim set is still loading asynchronously,
 * so a reclaim that arrives before its record loads is not refused. */
static void
nfs_recovery_begin_grace_forced(struct nfs_recovery *rec)
{
    pthread_mutex_lock(&rec->lock);
    rec->in_grace     = true;
    rec->grace_end_ns = nfs_lease_now_ns() +
        (uint64_t) rec->grace_time_s * 1000000000ULL;
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_begin_grace_forced */

void
nfs_recovery_end_grace(struct nfs_recovery *rec)
{
    pthread_mutex_lock(&rec->lock);
    rec->in_grace     = false;
    rec->grace_end_ns = 0;
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_end_grace */

bool
nfs_recovery_in_grace(struct nfs_recovery *rec)
{
    bool in_grace;

    pthread_mutex_lock(&rec->lock);
    in_grace = rec->in_grace;
    pthread_mutex_unlock(&rec->lock);

    return in_grace;
} /* nfs_recovery_in_grace */

/* ------------------------------------------------------------------ *
*  persist / forget (fire-and-forget KV writes)                      *
* ------------------------------------------------------------------ */

struct nfs_recovery_kv_ctx {
    uint8_t  key[CHIMERA_KV_NFS_KEY_MAX];
    uint32_t key_len;
    uint8_t  value[NFS_RECOVERY_VALUE_MAX];
    uint32_t value_len;
};

static void
nfs_recovery_kv_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    (void) error_code;
    free(private_data);
} /* nfs_recovery_kv_done */

void
nfs_recovery_persist(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    const struct nfs_client   *client)
{
    struct nfs_recovery_kv_ctx *ctx;

    if (!client || rec->persistence_disabled) {
        return;
    }

    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_recovery_key(ctx->key, rec->node_id,
                                       client->owner_string, client->owner_len);
    ctx->value_len = nfs_recovery_serialize(ctx->value, sizeof(ctx->value),
                                            client);
    if (ctx->value_len == 0) {
        free(ctx);
        return;
    }

    chimera_vfs_put_key(vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len,
                        nfs_recovery_kv_done, ctx);
} /* nfs_recovery_persist */

void
nfs_recovery_forget(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    const void                *owner,
    uint16_t                   owner_len)
{
    struct nfs_recovery_record *r;
    struct nfs_recovery_kv_ctx *ctx;

    /* Drop any matching in-memory reclaim record so a destroyed client stops
     * being reclaim-eligible (and the grace window can end). */
    pthread_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, owner, owner_len, r);
    if (r) {
        HASH_DEL(rec->to_reclaim, r);
        if (!r->reclaimed && rec->pending_reclaim) {
            rec->pending_reclaim--;
        }
        free(r);
    }
    pthread_mutex_unlock(&rec->lock);

    if (rec->persistence_disabled) {
        return;
    }

    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_recovery_key(ctx->key, rec->node_id, owner, owner_len);
    chimera_vfs_delete_key(vfs_thread, ctx->key, ctx->key_len,
                           nfs_recovery_kv_done, ctx);
} /* nfs_recovery_forget */

/* ------------------------------------------------------------------ *
*  cold-start load (deferred to the first NFSv4 compound)            *
* ------------------------------------------------------------------ */

struct nfs_recovery_load_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs_recovery              *rec;
    /* The async KV layer does NOT copy keys, so every key handed to it must
     * outlive the call -- these live in this heap ctx, not on the stack. */
    uint8_t                           ekey[CHIMERA_KV_PREFIX_LEN];
    uint8_t                           start[CHIMERA_KV_PREFIX_LEN];
};

static void
nfs_recovery_finalize_load(struct nfs_recovery *rec)
{
    uint32_t loaded;
    bool     in_grace;

    pthread_mutex_lock(&rec->lock);
    loaded = HASH_COUNT(rec->to_reclaim);
    if (rec->to_reclaim == NULL) {
        /* Nothing was persisted -- drop the forced grace window so normal
         * traffic is accepted immediately (matches no-clients behavior). */
        rec->in_grace     = false;
        rec->grace_end_ns = 0;
    }
    in_grace = rec->in_grace;
    pthread_mutex_unlock(&rec->lock);

    /* Assertable marker for cross-reboot tests + operational visibility. */
    chimera_nfs_info(
        "NFSv4 recovery: cold-start load complete: %u client record(s) "
        "reloaded, grace %s (boot_id %lu)",
        loaded, in_grace ? "active" : "skipped",
        (unsigned long) rec->current_boot_id);

    atomic_store_explicit(&rec->load_state, NFS_REC_LOAD_READY,
                          memory_order_release);
} /* nfs_recovery_finalize_load */

static int
nfs_recovery_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs_recovery_load_ctx *ctx = private_data;
    struct nfs_recovery          *rec = ctx->rec;
    struct nfs_recovery_record   *r, *existing;

    /* Keys are returned in order; stop once we leave THIS node's recovery band
     * (the 5-byte [type,node] prefix). */
    if (key_len < CHIMERA_KV_PREFIX_LEN ||
        memcmp(key, ctx->start, CHIMERA_KV_PREFIX_LEN) != 0) {
        return 1;
    }

    r = calloc(1, sizeof(*r));
    if (nfs_recovery_deserialize(value, value_len, r) != 0) {
        free(r);
        return 0;
    }

    pthread_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, r->owner_string, r->owner_len, existing);
    if (existing) {
        pthread_mutex_unlock(&rec->lock);
        free(r);
        return 0;
    }
    HASH_ADD_KEYPTR(hh, rec->to_reclaim, r->owner_string, r->owner_len, r);
    rec->pending_reclaim++;
    pthread_mutex_unlock(&rec->lock);

    return 0;
} /* nfs_recovery_scan_cb */

static void
nfs_recovery_drc_done(void *arg)
{
    /* The DRC reload finished reconstructing sessions + clients; only now is
     * the cold-start load truly complete. */
    nfs_recovery_finalize_load((struct nfs_recovery *) arg);
} /* nfs_recovery_drc_done */

static void
nfs_recovery_scan_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_recovery_load_ctx     *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct nfs_recovery              *rec    = ctx->rec;

    (void) error_code;

    free(ctx);

    /* When the reply cache is persistent, repopulate sessions + slots now that
     * the client identity set is loaded, and flip to READY only once that
     * reconstruction completes -- so EXCHANGE_ID/CREATE_SESSION can be held off
     * with NFS4ERR_DELAY until the reconstructed records exist (see
     * nfs_recovery_loading), avoiding a returning client racing in a duplicate
     * record.  Otherwise finalize immediately. */
    if (rec->nfs4_drc) {
        nfs4_drc_reload(thread, nfs_recovery_drc_done, rec);
    } else {
        nfs_recovery_finalize_load(rec);
    }
} /* nfs_recovery_scan_complete */

static void
nfs_recovery_epoch_cb(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs_recovery_load_ctx *ctx  = private_data;
    struct nfs_recovery          *rec  = ctx->rec;
    uint64_t                      prev = 0;
    uint64_t                      now;
    struct nfs_recovery_kv_ctx   *ectx;
    uint32_t                      ekey_len;

    now = nfs_recovery_fresh_boot_id();
    if (error_code == CHIMERA_VFS_OK && value &&
        nfs_recovery_epoch_deserialize(value, value_len, &prev) == 0) {
        /* Bump strictly past the prior boot epoch. */
        rec->current_boot_id = (prev >= now) ? prev + 1 : now;
    } else {
        rec->current_boot_id = now;
    }

    /* Write the new epoch so the next restart bumps again (fire-and-forget). */
    ectx            = malloc(sizeof(*ectx));
    ekey_len        = nfs_kv_epoch_key(ectx->key, rec->node_id);
    ectx->key_len   = ekey_len;
    ectx->value_len = nfs_recovery_epoch_serialize(ectx->value,
                                                   rec->current_boot_id);
    chimera_vfs_put_key(ctx->thread->vfs_thread, ectx->key, ectx->key_len,
                        ectx->value, ectx->value_len,
                        nfs_recovery_kv_done, ectx);

    /* Scan from this node's recovery band start with no end key (flags 0): the
     * KV search returns key-ordered results on every backend, so we stop in
     * nfs_recovery_scan_cb when the 5-byte [type,node] prefix changes.  The
     * node_id in the prefix is what keeps a node from reloading a live peer's
     * clients out of a shared store. */
    nfs_kv_node_prefix(ctx->start, CHIMERA_KV_TYPE_NFS4_RECOVERY, rec->node_id);
    chimera_vfs_search_keys(ctx->thread->vfs_thread,
                            ctx->start, CHIMERA_KV_PREFIX_LEN,
                            NULL, 0, 0,
                            nfs_recovery_scan_cb,
                            nfs_recovery_scan_complete,
                            ctx);
} /* nfs_recovery_epoch_cb */

void
nfs_recovery_kickoff(struct chimera_server_nfs_thread *thread)
{
    struct nfs_recovery          *rec = &thread->shared->nfs4_recovery;
    struct nfs_recovery_load_ctx *ctx;
    int                           expected = NFS_REC_LOAD_IDLE;

    if (rec->persistence_disabled) {
        return;
    }
    if (atomic_load_explicit(&rec->load_state, memory_order_acquire) !=
        NFS_REC_LOAD_IDLE) {
        return;
    }
    if (!atomic_compare_exchange_strong(&rec->load_state, &expected,
                                        NFS_REC_LOAD_RUNNING)) {
        return;
    }

    /* We own the load.  Hold the grace window open until the scan completes. */
    nfs_recovery_begin_grace_forced(rec);

    ctx         = malloc(sizeof(*ctx));
    ctx->thread = thread;
    ctx->rec    = rec;

    /* First read the boot epoch (to bump current_boot_id), then the records.
     * The key lives in the heap ctx -- the async KV layer keeps the pointer and
     * the delegation thread reads it after this function returns. */
    {
        uint32_t ekey_len = nfs_kv_epoch_key(ctx->ekey, rec->node_id);

        chimera_vfs_get_key(thread->vfs_thread, ctx->ekey, ekey_len,
                            nfs_recovery_epoch_cb, ctx);
    }
} /* nfs_recovery_kickoff */

/* ------------------------------------------------------------------ *
*  OPEN gate + reclaim bookkeeping                                   *
* ------------------------------------------------------------------ */

bool
nfs_recovery_loading(struct nfs_recovery *rec)
{
    if (rec->persistence_disabled) {
        return false;
    }
    return atomic_load_explicit(&rec->load_state, memory_order_acquire) !=
           NFS_REC_LOAD_READY;
} /* nfs_recovery_loading */

nfsstat4
nfs_recovery_open_check(
    struct nfs_recovery     *rec,
    const struct nfs_client *client,
    bool                     is_reclaim)
{
    bool in_grace;
    int  ls;

    (void) client;  /* not gated per-client yet; future use. */

    ls = atomic_load_explicit(&rec->load_state, memory_order_acquire);

    pthread_mutex_lock(&rec->lock);
    in_grace = rec->in_grace;
    pthread_mutex_unlock(&rec->lock);

    /* While the cold-start load is still in flight, behave as in-grace so a
     * reclaim that races its record load is not refused. */
    if (!rec->persistence_disabled && ls != NFS_REC_LOAD_READY) {
        in_grace = true;
    }

    if (in_grace && !is_reclaim) {
        return NFS4ERR_GRACE;
    }
    if (!in_grace && is_reclaim) {
        return NFS4ERR_NO_GRACE;
    }
    return NFS4_OK;
} /* nfs_recovery_open_check */

void
nfs_recovery_reclaim_complete(
    struct nfs_recovery     *rec,
    const struct nfs_client *client)
{
    struct nfs_recovery_record *r;

    if (!client) {
        return;
    }

    pthread_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, client->owner_string, client->owner_len, r);
    if (r && !r->reclaimed) {
        r->reclaimed = true;
        if (rec->pending_reclaim) {
            rec->pending_reclaim--;
        }
        if (rec->pending_reclaim == 0) {
            rec->in_grace     = false;
            rec->grace_end_ns = 0;
        }
    }
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_reclaim_complete */

void
nfs_recovery_sweep_once(struct nfs_recovery *rec)
{
    uint64_t now;
    bool     end_now = false;

    pthread_mutex_lock(&rec->lock);
    if (!rec->in_grace) {
        pthread_mutex_unlock(&rec->lock);
        return;
    }
    now = nfs_lease_now_ns();
    if (now >= rec->grace_end_ns || rec->pending_reclaim == 0) {
        end_now = true;
    }
    if (end_now) {
        rec->in_grace     = false;
        rec->grace_end_ns = 0;
    }
    pthread_mutex_unlock(&rec->lock);
} /* nfs_recovery_sweep_once */
