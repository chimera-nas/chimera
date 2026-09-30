// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdatomic.h>
#include "evpl/evpl.h"
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
#define NFS_RECOVERY_RECORD_MAGIC_V1 0x3152464Eu /* "NFR1": no renewal stamp */
#define NFS_RECOVERY_RECORD_MAGIC    0x3252464Eu /* "NFR2" */
#define NFS_RECOVERY_EPOCH_MAGIC     0x31504645u /* "EPF1" */

/* Recovery value (NFR2): magic(4) boot_id(8) verifier(8) client_id_hint(8)
 * last_renew_ns(8) owner_len(2) + owner bytes.  NFR1 has no last_renew_ns. */
#define NFS_RECOVERY_V1_HDR_LEN      30u
#define NFS_RECOVERY_VALUE_HDR_LEN   38u
#define NFS_RECOVERY_VALUE_MAX       (NFS_RECOVERY_VALUE_HDR_LEN + NFS4_OPAQUE_LIMIT)
/* Epoch value: magic(4) boot_id(8) written_at_ns(8, CLOCK_REALTIME). */
#define NFS_RECOVERY_EPOCH_VALUE_LEN 20u

static uint64_t
nfs_recovery_wall_now_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
} /* nfs_recovery_wall_now_ns */

/*
 * Generate a fresh, monotonic boot_id from CLOCK_REALTIME.  The cold-start
 * loader bumps strictly past any persisted epoch (see nfs_recovery_epoch_cb).
 */
static uint64_t
nfs_recovery_fresh_boot_id(void)
{
    return nfs_recovery_wall_now_ns();
} /* nfs_recovery_fresh_boot_id */

/* The client's last_touch_ns is CLOCK_MONOTONIC, meaningless to the next boot;
 * carry it as CLOCK_REALTIME.  Never 0: 0 marks a record with no stamp. */
static uint64_t
nfs_recovery_last_renew_wall_ns(const struct nfs_client *c)
{
    uint64_t now_mono = nfs_lease_now_ns();
    uint64_t now_wall = nfs_recovery_wall_now_ns();
    uint64_t age;

    if (c->last_touch_ns == 0 || c->last_touch_ns >= now_mono) {
        return now_wall;
    }
    age = now_mono - c->last_touch_ns;
    return age < now_wall ? now_wall - age : 1;
} /* nfs_recovery_last_renew_wall_ns */

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
    nfs_kv_put_le64(buf, &p, nfs_recovery_last_renew_wall_ns(c));
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
    uint32_t magic;
    uint32_t hdr_len;
    uint16_t owner_len;

    if (len < 4) {
        return -1;
    }
    magic = nfs_kv_le32(buf);
    if (magic == NFS_RECOVERY_RECORD_MAGIC) {
        hdr_len = NFS_RECOVERY_VALUE_HDR_LEN;
    } else if (magic == NFS_RECOVERY_RECORD_MAGIC_V1) {
        hdr_len = NFS_RECOVERY_V1_HDR_LEN;
    } else {
        return -1;
    }
    if (len < hdr_len) {
        return -1;
    }

    out->boot_id        = nfs_kv_le64(buf + p);
    p                  += 8;
    out->verifier       = nfs_kv_le64(buf + p);
    p                  += 8;
    out->client_id_hint = nfs_kv_le64(buf + p);
    p                  += 8;
    if (magic == NFS_RECOVERY_RECORD_MAGIC) {
        out->last_renew_ns = nfs_kv_le64(buf + p);
        p                 += 8;
    } else {
        out->last_renew_ns = 0;
    }
    owner_len = (uint16_t) buf[p] | ((uint16_t) buf[p + 1] << 8);
    p        += 2;

    if (owner_len > NFS4_OPAQUE_LIMIT || p + owner_len > len) {
        return -1;
    }
    out->owner_len = owner_len;
    memcpy(out->owner_string, buf + p, owner_len);
    out->reclaimed = false;
    out->returned  = false;

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
    uint64_t      *out_boot_id,
    uint64_t      *out_written_ns)
{
    if (len < NFS_RECOVERY_EPOCH_VALUE_LEN ||
        nfs_kv_le32(buf) != NFS_RECOVERY_EPOCH_MAGIC) {
        return -1;
    }
    *out_boot_id = nfs_kv_le64(buf + 4);
    if (out_written_ns) {
        *out_written_ns = nfs_kv_le64(buf + 12);
    }
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
    uint32_t             lease_time_s,
    uint32_t             grace_time_s,
    bool                 nfs4_drc)
{
    const char *kvname;

    evpl_mutex_init(&rec->lock, NULL);
    rec->to_reclaim         = NULL;
    rec->pending_reclaim    = 0;
    rec->current_boot_id    = nfs_recovery_fresh_boot_id();
    rec->grace_end_ns       = 0;
    rec->in_grace           = false;
    rec->vfs                = vfs;
    rec->node_id            = node_id;
    rec->grace_time_s       = grace_time_s;
    rec->lease_time_s       = lease_time_s;
    rec->stale_purged       = 0;
    rec->unreclaimed_purged = 0;
    rec->store_errors       = 0;
    atomic_store(&rec->next_heartbeat_ns, 0);
    rec->nfs4_drc = nfs4_drc;
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

    evpl_mutex_lock(&rec->lock);
    HASH_ITER(hh, rec->to_reclaim, r, tmp)
    {
        HASH_DEL(rec->to_reclaim, r);
        free(r);
    }
    rec->pending_reclaim = 0;
    evpl_mutex_unlock(&rec->lock);
    evpl_mutex_destroy(&rec->lock);
#endif /* ifndef __clang_analyzer__ */
} /* nfs_recovery_free */

/* ------------------------------------------------------------------ *
*  KV write context + grace window                                   *
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

/* Caller holds rec->lock.  Closes the window and retires the to_reclaim set.
 * A record that was neither reclaimed nor re-established belongs to a client
 * that did not come back: RFC 8881 section 8.4.3 lets the server drop it, and
 * dropping it is what keeps the next reboot from waiting on it again.  With
 * vfs_thread NULL (tests, and the all-reclaimed exit where nothing is owed)
 * only the in-memory set is retired. */
static void
nfs_recovery_close_grace_locked(
    struct nfs_recovery       *rec,
    struct chimera_vfs_thread *vfs_thread,
    const char                *reason)
{
    uint32_t                    purged = 0;

    rec->in_grace     = false;
    rec->grace_end_ns = 0;

#ifndef __clang_analyzer__
    struct nfs_recovery_record *r, *tmp;

    HASH_ITER(hh, rec->to_reclaim, r, tmp)
    {
        if (!r->reclaimed && !r->returned) {
            if (vfs_thread) {
                struct nfs_recovery_kv_ctx *ctx = malloc(sizeof(*ctx));

                ctx->key_len = nfs_kv_recovery_key(
                    ctx->key, rec->node_id,
                    (const uint8_t *) r->owner_string, r->owner_len);
                chimera_vfs_delete_key(vfs_thread, ctx->key, ctx->key_len,
                                       nfs_recovery_kv_done, ctx);
            }
            purged++;
        }
        HASH_DEL(rec->to_reclaim, r);
        free(r);
    }
#endif /* ifndef __clang_analyzer__ */

    rec->pending_reclaim     = 0;
    rec->unreclaimed_purged += purged;

    chimera_nfs_info(
        "NFSv4 recovery: grace window closed (%s): %u unreclaimed client "
        "record(s) purged", reason, purged);
} /* nfs_recovery_close_grace_locked */


void
nfs_recovery_begin_grace(
    struct nfs_recovery *rec,
    uint32_t             grace_time_s)
{
    evpl_mutex_lock(&rec->lock);
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
    evpl_mutex_unlock(&rec->lock);
} /* nfs_recovery_begin_grace */

void
nfs_recovery_end_grace(struct nfs_recovery *rec)
{
    evpl_mutex_lock(&rec->lock);
    if (rec->in_grace) {
        nfs_recovery_close_grace_locked(rec, NULL, "forced");
    }
    evpl_mutex_unlock(&rec->lock);
} /* nfs_recovery_end_grace */

bool
nfs_recovery_in_grace(struct nfs_recovery *rec)
{
    bool in_grace;

    evpl_mutex_lock(&rec->lock);
    in_grace = rec->in_grace;
    evpl_mutex_unlock(&rec->lock);

    return in_grace;
} /* nfs_recovery_in_grace */

/* ------------------------------------------------------------------ *
*  persist / forget (fire-and-forget KV writes)                      *
* ------------------------------------------------------------------ */

void
nfs_recovery_persist(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    struct nfs_client         *client)
{
    struct nfs_recovery_kv_ctx *ctx;

    if (!client) {
        return;
    }
    /* Whatever the backend, a client that persists again has come back. */
    nfs_recovery_note_returned(rec, client);
    if (rec->persistence_disabled) {
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

    atomic_store_explicit(&client->recovery_stamp_ns, nfs_lease_now_ns(),
                          memory_order_relaxed);
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
    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, owner, owner_len, r);
    if (r) {
        HASH_DEL(rec->to_reclaim, r);
        if (!r->reclaimed && rec->pending_reclaim) {
            rec->pending_reclaim--;
        }
        free(r);
    }
    evpl_mutex_unlock(&rec->lock);

    if (rec->persistence_disabled) {
        return;
    }

    ctx          = malloc(sizeof(*ctx));
    ctx->key_len = nfs_kv_recovery_key(ctx->key, rec->node_id, owner, owner_len);
    chimera_vfs_delete_key(vfs_thread, ctx->key, ctx->key_len,
                           nfs_recovery_kv_done, ctx);
} /* nfs_recovery_forget */

/* ------------------------------------------------------------------ *
*  refresh cadence                                                    *
* ------------------------------------------------------------------ */

static uint64_t
nfs_recovery_refresh_interval_ns(const struct nfs_recovery *rec)
{
    uint64_t half = (uint64_t) rec->lease_time_s * 1000000000ULL / 2;

    return half ? half : 1000000000ULL;
} /* nfs_recovery_refresh_interval_ns */

bool
nfs_recovery_persist_due(
    const struct nfs_recovery *rec,
    const struct nfs_client   *client,
    uint64_t                   now_ns)
{
    uint64_t stamp;

    if (rec->persistence_disabled || !client->confirmed || client->expired) {
        return false;
    }
    stamp = atomic_load_explicit(&client->recovery_stamp_ns,
                                 memory_order_relaxed);
    return now_ns > stamp &&
           now_ns - stamp >= nfs_recovery_refresh_interval_ns(rec);
} /* nfs_recovery_persist_due */

bool
nfs_recovery_heartbeat_due(
    struct nfs_recovery *rec,
    uint64_t             now_ns)
{
    uint64_t next = atomic_load_explicit(&rec->next_heartbeat_ns,
                                         memory_order_relaxed);

    if (rec->persistence_disabled || now_ns < next) {
        return false;
    }
    return atomic_compare_exchange_strong(
        &rec->next_heartbeat_ns, &next,
        now_ns + nfs_recovery_refresh_interval_ns(rec));
} /* nfs_recovery_heartbeat_due */

void
nfs_recovery_heartbeat(
    struct nfs_recovery       *rec,
    struct chimera_vfs_thread *vfs_thread)
{
    struct nfs_recovery_kv_ctx *ctx;

    if (!nfs_recovery_heartbeat_due(rec, nfs_lease_now_ns())) {
        return;
    }
    if (nfs_recovery_loading(rec)) {
        return; /* current_boot_id is not final until the load is READY */
    }

    ctx            = malloc(sizeof(*ctx));
    ctx->key_len   = nfs_kv_epoch_key(ctx->key, rec->node_id);
    ctx->value_len = nfs_recovery_epoch_serialize(ctx->value,
                                                  rec->current_boot_id);
    chimera_vfs_put_key(vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len,
                        nfs_recovery_kv_done, ctx);
} /* nfs_recovery_heartbeat */

/* ------------------------------------------------------------------ *
*  cold-start load (synchronous, before the NFS listeners start)      *
* ------------------------------------------------------------------ */

struct nfs_recovery_stale_owner {
    uint8_t  owner[NFS4_OPAQUE_LIMIT];
    uint16_t owner_len;
};

struct nfs_recovery_load_ctx {
    struct nfs_recovery             *rec;
    struct chimera_vfs_thread       *vfs_thread;
    /* The async KV layer does NOT copy keys, so every key handed to it must
     * outlive the call -- these live in this heap ctx, not on the stack. */
    uint8_t                          ekey[CHIMERA_KV_PREFIX_LEN];
    uint8_t                          start[CHIMERA_KV_PREFIX_LEN];
    int                              outstanding;   /* KV writes not yet completed */
    bool                             scan_done;
    uint64_t                         last_alive_ns; /* previous instance's last epoch write */
    struct nfs_recovery_stale_owner *stale;         /* records to delete once the scan ends */
    uint32_t                         nstale;
    uint32_t                         stale_cap;
};

void
nfs_recovery_note_store_error(
    struct nfs_recovery *rec,
    const char          *what,
    int                  error)
{
    rec->store_errors++;
    chimera_nfs_error(
        "NFSv4 recovery: cold-start %s failed (error %d); clients owed a "
        "reclaim may be refused", what, error);
} /* nfs_recovery_note_store_error */

bool
nfs_recovery_record_stale(
    const struct nfs_recovery        *rec,
    const struct nfs_recovery_record *r,
    uint64_t                          last_alive_ns)
{
    uint64_t lease_ns = (uint64_t) rec->lease_time_s * 1000000000ULL;
    uint64_t bound_ns = lease_ns + lease_ns / 2;

    if (r->last_renew_ns == 0 || last_alive_ns == 0) {
        return false;
    }
    return last_alive_ns > r->last_renew_ns &&
           last_alive_ns - r->last_renew_ns > bound_ns;
} /* nfs_recovery_record_stale */

/* One KV write issued by the loader.  Unlike nfs_recovery_kv_done it reports
 * back to the load ctx: the private vfs_thread that drives the load must not
 * be torn down while a write it issued is still in flight. */
struct nfs_recovery_load_op {
    struct nfs_recovery_load_ctx *load;
    struct nfs_recovery_kv_ctx    kv;
};

static void
nfs_recovery_load_op_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_recovery_load_op *op = private_data;

    (void) error_code;
    op->load->outstanding--;
    free(op);
} /* nfs_recovery_load_op_done */

static void
nfs_recovery_finalize_load(struct nfs_recovery *rec)
{
    uint32_t loaded;
    bool     in_grace;

    /* Open the window only when somebody is owed a reclaim (the same rule
     * nfs_recovery_begin_grace applies at shared init, when the set is
     * necessarily empty). */
    nfs_recovery_begin_grace(rec, rec->grace_time_s);

    evpl_mutex_lock(&rec->lock);
    loaded   = HASH_COUNT(rec->to_reclaim);
    in_grace = rec->in_grace;
    evpl_mutex_unlock(&rec->lock);

    /* Assertable marker for cross-reboot tests + operational visibility.  The
     * "cold-start load complete: <n>" head is parsed by the KVM reboot test. */
    chimera_nfs_info(
        "NFSv4 recovery: cold-start load complete: %u client record(s) "
        "reloaded, %u stale record(s) purged, grace %s (boot_id %lu)%s",
        loaded, rec->stale_purged, in_grace ? "active" : "skipped",
        (unsigned long) rec->current_boot_id,
        rec->store_errors ? " -- WITH STORE ERRORS, see above" : "");

    /* The epoch was just written; the first heartbeat is due one interval on. */
    atomic_store(&rec->next_heartbeat_ns,
                 nfs_lease_now_ns() + nfs_recovery_refresh_interval_ns(rec));

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

    if (nfs_recovery_record_stale(rec, r, ctx->last_alive_ns)) {
        /* Lease lapsed before the crash: not owed a reclaim.  Deleting during
         * the scan would mutate the band being iterated, so remember the
         * owner and delete once the scan has completed. */
        if (ctx->nstale == ctx->stale_cap) {
            ctx->stale_cap = ctx->stale_cap ? ctx->stale_cap * 2 : 8;
            ctx->stale     = realloc(ctx->stale,
                                     ctx->stale_cap * sizeof(*ctx->stale));
            chimera_nfs_abort_if(ctx->stale == NULL,
                                 "recovery stale-owner list realloc failed");
        }
        ctx->stale[ctx->nstale].owner_len = r->owner_len;
        memcpy(ctx->stale[ctx->nstale].owner, r->owner_string, r->owner_len);
        ctx->nstale++;
        free(r);
        return 0;
    }

    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, r->owner_string, r->owner_len, existing);
    if (existing) {
        evpl_mutex_unlock(&rec->lock);
        free(r);
        return 0;
    }
    HASH_ADD_KEYPTR(hh, rec->to_reclaim, r->owner_string, r->owner_len, r);
    rec->pending_reclaim++;
    evpl_mutex_unlock(&rec->lock);

    return 0;
} /* nfs_recovery_scan_cb */

static void
nfs_recovery_scan_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_recovery_load_ctx *ctx = private_data;
    struct nfs_recovery          *rec = ctx->rec;

    if (error_code != CHIMERA_VFS_OK) {
        /* An empty band completes with OK and no callbacks; anything else
         * means records may be missing from to_reclaim. */
        nfs_recovery_note_store_error(rec, "record scan", error_code);
    }

    /* The scan is over, so the band can be mutated: drop the records of the
     * clients whose leases had lapsed before the crash. */
    for (uint32_t i = 0; i < ctx->nstale; i++) {
        struct nfs_recovery_load_op *op = malloc(sizeof(*op));

        op->load       = ctx;
        op->kv.key_len = nfs_kv_recovery_key(op->kv.key, rec->node_id,
                                             ctx->stale[i].owner,
                                             ctx->stale[i].owner_len);
        ctx->outstanding++;
        chimera_vfs_delete_key(ctx->vfs_thread, op->kv.key, op->kv.key_len,
                               nfs_recovery_load_op_done, op);
    }
    rec->stale_purged = ctx->nstale;

    /* The confirmed-client identity set is loaded, so EXCHANGE_ID can resolve a
     * returning owner to its original clientid.  4.1 sessions + reply slots are
     * NOT bulk-reloaded here -- they are keyed by sessionid (not node) and
     * reconstructed lazily, the first time a client presents an unknown
     * sessionid to this node (nfs4_drc_session_hydrate), which also covers a
     * client that failed over from a peer. */
    ctx->scan_done = true;
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
    struct nfs_recovery_load_op  *op;

    now = nfs_recovery_fresh_boot_id();
    if (error_code == CHIMERA_VFS_OK && value &&
        nfs_recovery_epoch_deserialize(value, value_len, &prev,
                                       &ctx->last_alive_ns) == 0) {
        /* Bump strictly past the prior boot epoch. */
        rec->current_boot_id = (prev >= now) ? prev + 1 : now;
    } else {
        if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_ENOENT) {
            /* A store that has never been written answers ENOENT; anything
             * else is a read that failed, and the boot epoch will not be
             * strictly ordered after the previous one. */
            nfs_recovery_note_store_error(rec, "epoch read", error_code);
        }
        rec->current_boot_id = now;
        ctx->last_alive_ns   = 0; /* no heartbeat on record: keep everything */
    }

    /* Write the new epoch so the next restart bumps again. */
    op               = malloc(sizeof(*op));
    op->load         = ctx;
    op->kv.key_len   = nfs_kv_epoch_key(op->kv.key, rec->node_id);
    op->kv.value_len = nfs_recovery_epoch_serialize(op->kv.value,
                                                    rec->current_boot_id);
    ctx->outstanding++;
    chimera_vfs_put_key(ctx->vfs_thread, op->kv.key, op->kv.key_len,
                        op->kv.value, op->kv.value_len,
                        nfs_recovery_load_op_done, op);

    /* Scan from this node's recovery band start with no end key (flags 0): the
     * KV search returns key-ordered results on every backend, so we stop in
     * nfs_recovery_scan_cb when the 5-byte [type,node] prefix changes.  The
     * node_id in the prefix is what keeps a node from reloading a live peer's
     * clients out of a shared store. */
    nfs_kv_node_prefix(ctx->start, CHIMERA_KV_TYPE_NFS4_RECOVERY, rec->node_id);
    chimera_vfs_search_keys(ctx->vfs_thread,
                            ctx->start, CHIMERA_KV_PREFIX_LEN,
                            NULL, 0, 0,
                            nfs_recovery_scan_cb,
                            nfs_recovery_scan_complete,
                            ctx);
} /* nfs_recovery_epoch_cb */

void
nfs_recovery_cold_start(struct nfs_recovery *rec)
{
    struct evpl                  *evpl;
    struct nfs_recovery_load_ctx *ctx;
    uint32_t                      ekey_len;
    int                           expected = NFS_REC_LOAD_IDLE;

    if (!atomic_compare_exchange_strong(&rec->load_state, &expected,
                                        NFS_REC_LOAD_RUNNING)) {
        return; /* already loaded */
    }

    if (rec->persistence_disabled) {
        /* Nothing survives a restart on this backend; nfs_recovery_load
         * already said so. */
        atomic_store_explicit(&rec->load_state, NFS_REC_LOAD_READY,
                              memory_order_release);
        return;
    }
    if (rec->vfs == NULL) {
        /* Unit-test harness: persistence on, no store to read. */
        nfs_recovery_finalize_load(rec);
        return;
    }

    chimera_nfs_info(
        "NFSv4 recovery: reloading persisted client records from KV backend "
        "'%s' before accepting connections", rec->vfs->kv_module->name);

    evpl            = evpl_create(NULL);
    ctx             = calloc(1, sizeof(*ctx));
    ctx->rec        = rec;
    ctx->vfs_thread = chimera_vfs_thread_init(evpl, rec->vfs);

    ekey_len = nfs_kv_epoch_key(ctx->ekey, rec->node_id);
    chimera_vfs_get_key(ctx->vfs_thread, ctx->ekey, ekey_len,
                        nfs_recovery_epoch_cb, ctx);

    /* Same drive loop as chimera_server_create_share(): the KV operations run
     * on the delegation pool and complete on this private loop.  A backend
     * that never answers holds the server here, exactly as an unanswered
     * mount would -- serving NFSv4 without knowing who may reclaim is worse. */
    while (!ctx->scan_done || ctx->outstanding > 0) {
        evpl_continue(evpl);
    }

    nfs_recovery_finalize_load(rec);

    chimera_vfs_thread_destroy(ctx->vfs_thread);
    evpl_destroy(evpl);
    free(ctx->stale);
    free(ctx);
} /* nfs_recovery_cold_start */

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
    bool load_ready;
    int  ls;

    ls = atomic_load_explicit(&rec->load_state, memory_order_acquire);

    evpl_mutex_lock(&rec->lock);
    in_grace = rec->in_grace;
    evpl_mutex_unlock(&rec->lock);

    /* While the cold-start load is still in flight, behave as in-grace so a
     * reclaim that races its record load is not refused. */
    load_ready = rec->persistence_disabled || ls == NFS_REC_LOAD_READY;
    if (!load_ready) {
        in_grace = true;
    }

    if (in_grace && !is_reclaim) {
        return NFS4ERR_GRACE;
    }
    if (!in_grace && is_reclaim) {
        return NFS4ERR_NO_GRACE;
    }

    /* RFC 7530 §9.6.3.1: a CLAIM_PREVIOUS reclaim is only valid for state this
     * client actually held before the server restart.  The to_reclaim set is
     * the authoritative list of clients with persisted pre-reboot state; a
     * reclaim from a client absent from it is bogus and MUST be rejected with
     * NFS4ERR_RECLAIM_BAD rather than silently fabricating share state.
     *
     * Only enforced once the cold-start load is READY (to_reclaim fully
     * populated) -- while the load is still in flight a reclaim that races its
     * record load must not be refused (handled by the load_ready=in_grace path
     * above, which short-circuits here only when the load has completed).  When
     * persistence is disabled to_reclaim is empty and in_grace is never set, so
     * this leg is unreachable on the default memkv backend. */
    if (in_grace && is_reclaim && load_ready && client) {
        struct nfs_recovery_record *r;
        bool                        reclaimed;

        evpl_mutex_lock(&rec->lock);
        HASH_FIND(hh, rec->to_reclaim, client->owner_string,
                  client->owner_len, r);
        reclaimed = r && r->reclaimed;
        evpl_mutex_unlock(&rec->lock);

        if (!r) {
            return NFS4ERR_RECLAIM_BAD;
        }

        /* RFC 8881 §18.51.3: once this client has declared reclaim complete
         * the server MUST NOT let it reclaim any more state, even though the
         * server-wide grace window is still open because some *other* client
         * has not finished.  Grace is a per-client obligation that this client
         * has already retired. */
        if (reclaimed) {
            return NFS4ERR_NO_GRACE;
        }
    }

    return NFS4_OK;
} /* nfs_recovery_open_check */

nfsstat4
nfs_recovery_io_check(struct nfs_recovery *rec)
{
    /* RFC 7530 §9.6.2: during the grace period the server must reject READ
     * and WRITE operations and non-reclaim locking requests with
     * NFS4ERR_GRACE, and may only service them if it can guarantee that no
     * conflict with an impending reclaim could arise.  The locks that would
     * conflict are precisely the ones the window exists to rebuild, so no
     * such guarantee is available here.  Same window (and same
     * load-still-in-flight treatment) as the OPEN gate; these operations
     * carry no reclaim flag, so is_reclaim is always false and the client is
     * irrelevant. */
    return nfs_recovery_open_check(rec, NULL, false);
} /* nfs_recovery_io_check */

void
nfs_recovery_reclaim_complete(
    struct nfs_recovery     *rec,
    const struct nfs_client *client)
{
    struct nfs_recovery_record *r;
    bool                        loading;

    if (!client) {
        return;
    }

    /* Same guard as in nfs_recovery_sweep_once: while a load is in flight,
     * pending_reclaim counts only the clients streamed in so far, so a zero
     * count does not mean everyone has reclaimed.  The load completes in
     * nfs_server_start before any listener exists, so no request can reach
     * this path mid-load today; the guard defends against a future caller
     * that serves before the load.  Retire the record either way, but honour
     * the zero count only once the load is READY. */
    loading = nfs_recovery_loading(rec);

    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, client->owner_string, client->owner_len, r);
    if (r && !r->reclaimed) {
        r->reclaimed = true;
        if (rec->pending_reclaim) {
            rec->pending_reclaim--;
        }
        if (rec->pending_reclaim == 0 && !loading) {
            nfs_recovery_close_grace_locked(rec, NULL, "all clients reclaimed");
        }
    }
    evpl_mutex_unlock(&rec->lock);
} /* nfs_recovery_reclaim_complete */

void
nfs_recovery_sweep_once(
    struct nfs_recovery       *rec,
    struct chimera_vfs_thread *vfs_thread)
{
    uint64_t now;
    bool     loading;

    /* While a load is in flight, pending_reclaim counts only the clients
     * streamed in so far; a tick reading that zero as "every client has
     * reclaimed" would close a window that had not started, and the
     * CLAIM_PREVIOUS arriving once the records do load would be refused
     * NFS4ERR_NO_GRACE.  The load completes in nfs_server_start before any
     * listener exists, so today no tick can land mid-load; the guard defends
     * against a future caller that serves before the load.  While loading,
     * only the grace_end_ns deadline may end the window.  load_state is read
     * outside rec->lock exactly as nfs_recovery_open_check reads it: it is
     * atomic, and the helper answers false when persistence is disabled
     * (memkv), where in_grace is never set anyway. */
    loading = nfs_recovery_loading(rec);

    evpl_mutex_lock(&rec->lock);
    if (!rec->in_grace) {
        evpl_mutex_unlock(&rec->lock);
        return;
    }
    now = nfs_lease_now_ns();
    if (now >= rec->grace_end_ns) {
        nfs_recovery_close_grace_locked(rec, vfs_thread, "deadline");
    } else if (rec->pending_reclaim == 0 && !loading) {
        nfs_recovery_close_grace_locked(rec, NULL, "all clients reclaimed");
    }
    evpl_mutex_unlock(&rec->lock);
} /* nfs_recovery_sweep_once */

void
nfs_recovery_note_returned(
    struct nfs_recovery     *rec,
    const struct nfs_client *client)
{
    struct nfs_recovery_record *r;

    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, client->owner_string, client->owner_len, r);
    if (r) {
        r->returned = true;
    }
    evpl_mutex_unlock(&rec->lock);
} /* nfs_recovery_note_returned */
