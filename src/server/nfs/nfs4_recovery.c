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
#define NFS_RECOVERY_RECORD_MAGIC_V2 0x3252464Eu /* "NFR2": no owning node */
#define NFS_RECOVERY_RECORD_MAGIC    0x3352464Eu /* "NFR3" */
#define NFS_RECOVERY_EPOCH_MAGIC     0x31504645u /* "EPF1" */

/* Recovery value (NFR3): magic(4) boot_id(8) verifier(8) client_id_hint(8)
 * last_renew_ns(8) owner_node(2) owner_len(2) + owner bytes.  NFR2 has no
 * owner_node; NFR1 has neither owner_node nor last_renew_ns. */
#define NFS_RECOVERY_V1_HDR_LEN      30u
#define NFS_RECOVERY_V2_HDR_LEN      38u
#define NFS_RECOVERY_VALUE_HDR_LEN   40u
#define NFS_RECOVERY_VALUE_MAX       (NFS_RECOVERY_VALUE_HDR_LEN + NFS4_OPAQUE_LIMIT)
/* Epoch value: magic(4) boot_id(8) written_at_ns(8, CLOCK_REALTIME). */
#define NFS_RECOVERY_EPOCH_VALUE_LEN 20u

struct nfs_recovery_imported_peer {
    uint16_t       node;
    uint64_t       boot_id;
    UT_hash_handle hh;
};

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

static uint32_t
nfs_recovery_encode(
    uint8_t    *buf,
    uint32_t    buf_size,
    uint64_t    boot_id,
    uint64_t    verifier,
    uint64_t    client_id,
    uint64_t    last_renew_ns,
    uint16_t    owner_node,
    const void *owner,
    uint16_t    owner_len)
{
    uint32_t p = 0;

    if (buf_size < NFS_RECOVERY_VALUE_HDR_LEN + owner_len) {
        return 0;
    }

    nfs_kv_put_le32(buf, &p, NFS_RECOVERY_RECORD_MAGIC);
    nfs_kv_put_le64(buf, &p, boot_id);
    nfs_kv_put_le64(buf, &p, verifier);
    nfs_kv_put_le64(buf, &p, client_id);
    nfs_kv_put_le64(buf, &p, last_renew_ns);
    buf[p]     = owner_node & 0xff;
    buf[p + 1] = (owner_node >> 8) & 0xff;
    p         += 2;
    buf[p]     = owner_len & 0xff;
    buf[p + 1] = (owner_len >> 8) & 0xff;
    p         += 2;
    memcpy(buf + p, owner, owner_len);
    p += owner_len;

    return p;
} /* nfs_recovery_encode */

uint32_t
nfs_recovery_serialize(
    uint8_t                 *buf,
    uint32_t                 buf_size,
    const struct nfs_client *c,
    uint16_t                 owner_node)
{
    return nfs_recovery_encode(buf, buf_size, c->boot_id, c->verifier,
                               c->client_id, nfs_recovery_last_renew_wall_ns(c),
                               owner_node, c->owner_string, c->owner_len);
} /* nfs_recovery_serialize */

int
nfs_recovery_deserialize(
    const uint8_t              *buf,
    uint32_t                    len,
    struct nfs_recovery_record *out,
    uint16_t                    legacy_owner)
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
    } else if (magic == NFS_RECOVERY_RECORD_MAGIC_V2) {
        hdr_len = NFS_RECOVERY_V2_HDR_LEN;
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
    if (magic != NFS_RECOVERY_RECORD_MAGIC_V1) {
        out->last_renew_ns = nfs_kv_le64(buf + p);
        p                 += 8;
    } else {
        out->last_renew_ns = 0;
    }
    if (magic == NFS_RECOVERY_RECORD_MAGIC) {
        out->owner_node = (uint16_t) buf[p] | ((uint16_t) buf[p + 1] << 8);
        p              += 2;
    } else {
        out->owner_node = legacy_owner;
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
    rec->prev_alive_ns      = 0;
    rec->failover_imported  = 0;
    rec->imports_in_flight  = 0;
    rec->imported_peers     = NULL;
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
    struct nfs_recovery_record        *r, *tmp;
    struct nfs_recovery_imported_peer *p, *ptmp;

    evpl_mutex_lock(&rec->lock);
    HASH_ITER(hh, rec->to_reclaim, r, tmp)
    {
        HASH_DEL(rec->to_reclaim, r);
        free(r);
    }
    HASH_ITER(hh, rec->imported_peers, p, ptmp)
    {
        HASH_DEL(rec->imported_peers, p);
        free(p);
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

/* Delete the fleet-wide record for an owner only while `expected` still owns
 * it, so a node never removes the record of a client a peer has taken over.
 * Read-then-delete is not atomic: should a peer take the client over between
 * the two, its write is lost -- until it re-persists the client, which it does
 * every half lease while the client is live (nfs_recovery_persist_due). */
struct nfs_recovery_cond_delete {
    struct chimera_vfs_thread *vfs_thread;
    uint16_t                   expected;
    uint8_t                    key[CHIMERA_KV_NFS_KEY_MAX];
    uint32_t                   key_len;
    void                       (*done)(
        void *arg);
    void                      *arg;
};

static void
nfs_recovery_cond_delete_finish(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_recovery_cond_delete *c = private_data;

    (void) error_code;
    if (c->done) {
        c->done(c->arg);
    }
    free(c);
} /* nfs_recovery_cond_delete_finish */

static void
nfs_recovery_cond_delete_get_cb(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs_recovery_cond_delete *c = private_data;
    struct nfs_recovery_record       r;

    if (error_code == CHIMERA_VFS_OK && value &&
        nfs_recovery_deserialize(value, value_len, &r, c->expected) == 0 &&
        r.owner_node == c->expected) {
        chimera_vfs_delete_key(c->vfs_thread, c->key, c->key_len,
                               nfs_recovery_cond_delete_finish, c);
        return;
    }
    nfs_recovery_cond_delete_finish(CHIMERA_VFS_OK, c);
} /* nfs_recovery_cond_delete_get_cb */

static void
nfs_recovery_delete_if_owner(
    struct chimera_vfs_thread *vfs_thread,
    const void                *owner,
    uint16_t                   owner_len,
    uint16_t                   expected,
    void (                    *done )(
        void *arg),
    void                      *arg)
{
    struct nfs_recovery_cond_delete *c = malloc(sizeof(*c));

    if (!c) {
        if (done) {
            done(arg);
        }
        return;
    }
    c->vfs_thread = vfs_thread;
    c->expected   = expected;
    c->key_len    = nfs_kv_client_key(c->key, owner, owner_len);
    c->done       = done;
    c->arg        = arg;
    chimera_vfs_get_key(vfs_thread, c->key, c->key_len,
                        nfs_recovery_cond_delete_get_cb, c);
} /* nfs_recovery_delete_if_owner */

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
                /* An imported record is still owned by the dead peer; the
                 * owner check keeps this from deleting a record some other
                 * node has since taken over. */
                nfs_recovery_delete_if_owner(vfs_thread, r->owner_string,
                                             r->owner_len, r->owner_node,
                                             NULL, NULL);
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
    ctx->key_len = nfs_kv_client_key(ctx->key, client->owner_string,
                                     client->owner_len);
    /* Writing the record names this node as the client's owner: a client
     * that failed over here moves to this node's set. */
    ctx->value_len = nfs_recovery_serialize(ctx->value, sizeof(ctx->value),
                                            client, rec->node_id);
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

    nfs_recovery_delete_if_owner(vfs_thread, owner, owner_len, rec->node_id,
                                 NULL, NULL);
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

    /* A courtesy client's stamp is its old last_touch, so a rewrite adds
     * nothing; it would only keep reclaiming the record from a node the client
     * has moved to, for as long as courtesy lasts. */
    if (rec->persistence_disabled || !client->confirmed || client->expired ||
        atomic_load_explicit(&client->courtesy, memory_order_relaxed)) {
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

/* A record the loader acts on once its scan is over (the band being iterated
 * must not be mutated mid-scan). */
struct nfs_recovery_load_item {
    struct nfs_recovery_record rec;
    uint8_t                    action;
};

enum {
    NFS_REC_ITEM_DROP_FLEET  = 0, /* stale fleet record: delete if still ours  */
    NFS_REC_ITEM_DROP_LEGACY = 1, /* legacy record not to be carried: delete   */
    NFS_REC_ITEM_MIGRATE     = 2, /* legacy record: rewrite fleet-wide, delete */
};

struct nfs_recovery_load_ctx {
    struct nfs_recovery           *rec;
    struct chimera_vfs_thread     *vfs_thread;
    /* The async KV layer does NOT copy keys, so every key handed to it must
     * outlive the call -- these live in this heap ctx, not on the stack. */
    uint8_t                        ekey[CHIMERA_KV_PREFIX_LEN];
    uint8_t                        fstart[CHIMERA_KV_HDR_LEN];
    uint8_t                        start[CHIMERA_KV_PREFIX_LEN];
    int                            outstanding;   /* KV writes not yet completed */
    bool                           scan_done;
    uint64_t                       last_alive_ns; /* previous instance's last epoch write */
    struct nfs_recovery_load_item *items;
    uint32_t                       nitems;
    uint32_t                       items_cap;
    uint32_t                       nstale;
    uint32_t                       nmigrated;
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
nfs_recovery_load_cond_done(void *arg)
{
    struct nfs_recovery_load_ctx *ctx = arg;

    ctx->outstanding--;
} /* nfs_recovery_load_cond_done */

/* A legacy record moving to the fleet band.  Its legacy key is deleted only
 * once the fleet record is known to exist, so a failed write or a crash in
 * between leaves it for the next boot to migrate again. */
struct nfs_recovery_migrate_op {
    struct nfs_recovery_load_ctx     *load;
    const struct nfs_recovery_record *r;
    struct nfs_recovery_kv_ctx        kv;
};

static void
nfs_recovery_delete_legacy(
    struct nfs_recovery_load_ctx     *ctx,
    const struct nfs_recovery_record *r)
{
    struct nfs_recovery_load_op *op = malloc(sizeof(*op));

    op->load       = ctx;
    op->kv.key_len = nfs_kv_recovery_key(op->kv.key, ctx->rec->node_id,
                                         (const uint8_t *) r->owner_string,
                                         r->owner_len);
    ctx->outstanding++;
    chimera_vfs_delete_key(ctx->vfs_thread, op->kv.key, op->kv.key_len,
                           nfs_recovery_load_op_done, op);
} /* nfs_recovery_delete_legacy */

static void
nfs_recovery_migrate_put_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_recovery_migrate_op *op  = private_data;
    struct nfs_recovery_load_ctx   *ctx = op->load;
    struct nfs_recovery            *rec = ctx->rec;
    struct nfs_recovery_record     *e;

    if (error_code == CHIMERA_VFS_OK) {
        ctx->nmigrated++;
        nfs_recovery_delete_legacy(ctx, op->r);
    } else if (error_code == CHIMERA_VFS_EEXIST) {
        /* A fleet record this node did not load holds the owner: the client
         * moved to a peer meanwhile, so it is not owed a reclaim here. */
        evpl_mutex_lock(&rec->lock);
        HASH_FIND(hh, rec->to_reclaim, op->r->owner_string, op->r->owner_len, e);
        if (e) {
            HASH_DEL(rec->to_reclaim, e);
            if (!e->reclaimed && rec->pending_reclaim) {
                rec->pending_reclaim--;
            }
            free(e);
        }
        evpl_mutex_unlock(&rec->lock);
        nfs_recovery_delete_legacy(ctx, op->r);
    } else {
        nfs_recovery_note_store_error(rec, "legacy record migration",
                                      error_code);
    }
    ctx->outstanding--;
    free(op);
} /* nfs_recovery_migrate_put_cb */

static void
nfs_recovery_load_defer(
    struct nfs_recovery_load_ctx     *ctx,
    const struct nfs_recovery_record *r,
    uint8_t                           action)
{
    if (ctx->nitems == ctx->items_cap) {
        ctx->items_cap = ctx->items_cap ? ctx->items_cap * 2 : 8;
        ctx->items     = realloc(ctx->items,
                                 ctx->items_cap * sizeof(*ctx->items));
        chimera_nfs_abort_if(ctx->items == NULL,
                             "recovery load item list realloc failed");
    }
    memcpy(&ctx->items[ctx->nitems].rec, r, sizeof(*r));
    ctx->items[ctx->nitems].action = action;
    ctx->nitems++;
} /* nfs_recovery_load_defer */

/* Add a loaded record to to_reclaim unless the owner is already there.  Takes
 * ownership of r; returns false (and frees r) on a duplicate. */
static bool
nfs_recovery_add_loaded(
    struct nfs_recovery        *rec,
    struct nfs_recovery_record *r)
{
    struct nfs_recovery_record *existing;

    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, r->owner_string, r->owner_len, existing);
    if (existing) {
        evpl_mutex_unlock(&rec->lock);
        free(r);
        return false;
    }
    HASH_ADD_KEYPTR(hh, rec->to_reclaim, r->owner_string, r->owner_len, r);
    rec->pending_reclaim++;
    evpl_mutex_unlock(&rec->lock);
    return true;
} /* nfs_recovery_add_loaded */

static void
nfs_recovery_finalize_load(
    struct nfs_recovery *rec,
    uint32_t             migrated)
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
     * "cold-start load complete: <n>" head is parsed by the KVM reboot test,
     * and the text through "grace <state>" by the pynfs cold-restart test. */
    chimera_nfs_info(
        "NFSv4 recovery: cold-start load complete: %u client record(s) "
        "reloaded, %u stale record(s) purged, grace %s, %u legacy record(s) "
        "migrated (boot_id %lu)%s",
        loaded, rec->stale_purged, in_grace ? "active" : "skipped", migrated,
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
    struct nfs_recovery_record   *r;

    /* Keys are returned in order; stop once we leave the fleet-wide client
     * band (the 3-byte type header). */
    if (key_len < CHIMERA_KV_HDR_LEN ||
        memcmp(key, ctx->fstart, CHIMERA_KV_HDR_LEN) != 0) {
        return 1;
    }

    r = calloc(1, sizeof(*r));
    if (nfs_recovery_deserialize(value, value_len, r, rec->node_id) != 0 ||
        r->owner_node != rec->node_id) {
        /* Corrupt, or a peer's client: a live peer serves it, and a dead
         * peer's is imported when it fails over (nfs_recovery_import_peer). */
        free(r);
        return 0;
    }

    if (nfs_recovery_record_stale(rec, r, ctx->last_alive_ns)) {
        /* Lease lapsed before the crash: not owed a reclaim.  Deleting during
         * the scan would mutate the band being iterated, so delete once the
         * scan has completed. */
        nfs_recovery_load_defer(ctx, r, NFS_REC_ITEM_DROP_FLEET);
        ctx->nstale++;
        free(r);
        return 0;
    }

    nfs_recovery_add_loaded(rec, r);
    return 0;
} /* nfs_recovery_scan_cb */

/* The legacy per-node band (CHIMERA_KV_TYPE_NFS4_RECOVERY) written by older
 * builds: load this node's records as before and move them to the fleet-wide
 * band, so a later failover can find them. */
static int
nfs_recovery_legacy_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs_recovery_load_ctx *ctx = private_data;
    struct nfs_recovery          *rec = ctx->rec;
    struct nfs_recovery_record   *r;

    if (key_len < CHIMERA_KV_PREFIX_LEN ||
        memcmp(key, ctx->start, CHIMERA_KV_PREFIX_LEN) != 0) {
        return 1;
    }

    r = calloc(1, sizeof(*r));
    if (nfs_recovery_deserialize(value, value_len, r, rec->node_id) != 0) {
        free(r);
        return 0;
    }
    r->owner_node = rec->node_id;

    if (nfs_recovery_record_stale(rec, r, ctx->last_alive_ns)) {
        nfs_recovery_load_defer(ctx, r, NFS_REC_ITEM_DROP_LEGACY);
        ctx->nstale++;
        free(r);
        return 0;
    }

    /* Defer before handing r to the set (which may free it).  A legacy record
     * whose owner already has a fleet record -- a migration cut short -- only
     * needs its legacy copy deleted. */
    nfs_recovery_load_defer(ctx, r, NFS_REC_ITEM_MIGRATE);
    if (!nfs_recovery_add_loaded(rec, r)) {
        ctx->items[ctx->nitems - 1].action = NFS_REC_ITEM_DROP_LEGACY;
    }
    return 0;
} /* nfs_recovery_legacy_scan_cb */

static void
nfs_recovery_legacy_scan_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_recovery_load_ctx *ctx = private_data;
    struct nfs_recovery          *rec = ctx->rec;

    if (error_code != CHIMERA_VFS_OK) {
        nfs_recovery_note_store_error(rec, "legacy record scan", error_code);
    }

    /* Both scans are over, so the bands can be mutated. */
    for (uint32_t i = 0; i < ctx->nitems; i++) {
        struct nfs_recovery_record     *r = &ctx->items[i].rec;
        struct nfs_recovery_migrate_op *op;

        if (ctx->items[i].action == NFS_REC_ITEM_DROP_FLEET) {
            ctx->outstanding++;
            nfs_recovery_delete_if_owner(ctx->vfs_thread, r->owner_string,
                                         r->owner_len, rec->node_id,
                                         nfs_recovery_load_cond_done, ctx);
            continue;
        }

        if (ctx->items[i].action == NFS_REC_ITEM_MIGRATE) {
            /* Never over a fleet record: the fleet scan skips one owned by a
             * peer, which may have taken the client over while this node
             * was down or still on an older build. */
            op             = malloc(sizeof(*op));
            op->load       = ctx;
            op->r          = r;
            op->kv.key_len = nfs_kv_client_key(op->kv.key, r->owner_string,
                                               r->owner_len);
            op->kv.value_len = nfs_recovery_encode(
                op->kv.value, sizeof(op->kv.value), r->boot_id, r->verifier,
                r->client_id_hint, r->last_renew_ns, rec->node_id,
                r->owner_string, r->owner_len);
            ctx->outstanding++;
            chimera_vfs_put_key_if_absent(ctx->vfs_thread, op->kv.key,
                                          op->kv.key_len, op->kv.value,
                                          op->kv.value_len,
                                          nfs_recovery_migrate_put_cb, op);
            continue;
        }

        nfs_recovery_delete_legacy(ctx, r);
    }
    rec->stale_purged = ctx->nstale;

    /* The confirmed-client identity set is loaded, so EXCHANGE_ID can resolve a
     * returning owner to its original clientid.  4.1 sessions + reply slots are
     * NOT bulk-reloaded here -- they are keyed by sessionid (not node) and
     * reconstructed lazily, the first time a client presents an unknown
     * sessionid to this node (nfs4_drc_session_hydrate), which also covers a
     * client that failed over from a peer. */
    ctx->scan_done = true;
} /* nfs_recovery_legacy_scan_complete */

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

    nfs_kv_node_prefix(ctx->start, CHIMERA_KV_TYPE_NFS4_RECOVERY, rec->node_id);
    chimera_vfs_search_keys(ctx->vfs_thread,
                            ctx->start, CHIMERA_KV_PREFIX_LEN,
                            NULL, 0, 0,
                            nfs_recovery_legacy_scan_cb,
                            nfs_recovery_legacy_scan_complete,
                            ctx);
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
    rec->prev_alive_ns = ctx->last_alive_ns;

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

    /* Scan the fleet-wide client band with no end key (flags 0): the KV search
     * returns key-ordered results on every backend, so nfs_recovery_scan_cb
     * stops when the type header changes.  The owning node in each value is
     * what keeps a node from reloading a live peer's clients.  The legacy
     * per-node band is scanned next (nfs_recovery_scan_complete). */
    nfs_kv_type_prefix(ctx->fstart, CHIMERA_KV_TYPE_NFS4_CLIENT);
    chimera_vfs_search_keys(ctx->vfs_thread,
                            ctx->fstart, CHIMERA_KV_HDR_LEN,
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
        nfs_recovery_finalize_load(rec, 0);
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

    nfs_recovery_finalize_load(rec, ctx->nmigrated);

    chimera_vfs_thread_destroy(ctx->vfs_thread);
    evpl_destroy(evpl);
    free(ctx->items);
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

uint64_t
nfs_recovery_prev_alive_s(const struct nfs_recovery *rec)
{
    return rec->prev_alive_ns / 1000000000ULL;
} /* nfs_recovery_prev_alive_s */

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
        if (rec->pending_reclaim == 0 && !loading && !rec->imports_in_flight) {
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
    if (!rec->in_grace || rec->imports_in_flight) {
        /* An import scan's records must not be purged before it reopens
         * or extends the window. */
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

/* ------------------------------------------------------------------ *
*  failover: owner lookup + import of a dead peer's records           *
* ------------------------------------------------------------------ */

struct nfs_recovery_lookup_ctx {
    uint8_t key[CHIMERA_KV_NFS_KEY_MAX];
    void    (*done)(
        enum nfs_recovery_owner result,
        uint16_t                node,
        void                   *arg);
    void   *arg;
};

static void
nfs_recovery_lookup_cb(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs_recovery_lookup_ctx *ctx = private_data;
    struct nfs_recovery_record      r;

    /* A legacy value cannot be on the fleet band; 0 is never matched below
     * as a real owner for one. */
    if (error_code == CHIMERA_VFS_ENOENT ||
        (error_code == CHIMERA_VFS_OK && !value)) {
        ctx->done(NFS_RECOVERY_OWNER_ABSENT, 0, ctx->arg);
    } else if (error_code == CHIMERA_VFS_OK &&
               nfs_recovery_deserialize(value, value_len, &r, 0) == 0) {
        ctx->done(NFS_RECOVERY_OWNER_FOUND, r.owner_node, ctx->arg);
    } else {
        chimera_nfs_error("NFSv4 recovery: cannot read a client's recovery "
                          "record (error %d)", error_code);
        ctx->done(NFS_RECOVERY_OWNER_ERROR, 0, ctx->arg);
    }
    free(ctx);
} /* nfs_recovery_lookup_cb */

void
nfs_recovery_lookup_owner(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    const void                *owner,
    uint16_t                   owner_len,
    void (                    *done )(
        enum nfs_recovery_owner result,
        uint16_t                node,
        void                   *arg),
    void                      *arg)
{
    struct nfs_recovery_lookup_ctx *ctx;
    uint32_t                        key_len;

    if (rec->persistence_disabled) {
        done(NFS_RECOVERY_OWNER_ABSENT, 0, arg);
        return;
    }

    ctx = malloc(sizeof(*ctx));
    if (!ctx) {
        done(NFS_RECOVERY_OWNER_ERROR, 0, arg);
        return;
    }
    ctx->done = done;
    ctx->arg  = arg;
    key_len   = nfs_kv_client_key(ctx->key, owner, owner_len);
    chimera_vfs_get_key(vfs_thread, ctx->key, key_len,
                        nfs_recovery_lookup_cb, ctx);
} /* nfs_recovery_lookup_owner */

struct nfs_recovery_import_ctx {
    struct nfs_recovery       *rec;
    struct chimera_vfs_thread *vfs_thread;
    uint16_t                   peer;
    uint8_t                    owner[NFS4_OPAQUE_LIMIT];
    uint16_t                   owner_len;
    uint8_t                    ekey[CHIMERA_KV_PREFIX_LEN];
    uint8_t                    fstart[CHIMERA_KV_HDR_LEN];
    uint64_t                   peer_alive_ns;
    uint64_t                   peer_boot_id;
    uint64_t                   seen_ns;
    uint32_t                   imported;
    void                       (*done)(
        enum nfs_recovery_failover result,
        uint64_t                   observed_ns,
        void                      *arg);
    void                      *arg;
};

/* Caller holds rec->lock. */
static bool
nfs_recovery_peer_imported(
    const struct nfs_recovery *rec,
    uint16_t                   peer,
    uint64_t                   boot_id)
{
    struct nfs_recovery_imported_peer *p;

    HASH_FIND(hh, rec->imported_peers, &peer, sizeof(peer), p);
    return p && p->boot_id == boot_id;
} /* nfs_recovery_peer_imported */

/* Caller holds rec->lock. */
static void
nfs_recovery_note_peer_imported(
    struct nfs_recovery *rec,
    uint16_t             peer,
    uint64_t             boot_id)
{
    struct nfs_recovery_imported_peer *p;

    HASH_FIND(hh, rec->imported_peers, &peer, sizeof(peer), p);
    if (!p) {
        p = calloc(1, sizeof(*p));
        if (!p) {
            return; /* uncached: the peer's next failover just rescans */
        }
        p->node = peer;
        HASH_ADD(hh, rec->imported_peers, node, sizeof(p->node), p);
    }
    p->boot_id = boot_id;
} /* nfs_recovery_note_peer_imported */

/* The client is owed a reclaim here: its record is in the open window's set
 * and it has not yet sent RECLAIM_COMPLETE. */
static void
nfs_recovery_import_finish(struct nfs_recovery_import_ctx *ctx)
{
    struct nfs_recovery        *rec = ctx->rec;
    struct nfs_recovery_record *r;
    bool                        owed;

    evpl_mutex_lock(&rec->lock);
    HASH_FIND(hh, rec->to_reclaim, ctx->owner, ctx->owner_len, r);
    owed = rec->in_grace && r && !r->reclaimed;
    evpl_mutex_unlock(&rec->lock);

    ctx->done(owed ? NFS_RECOVERY_FAILOVER : NFS_RECOVERY_NOT_FAILOVER,
              ctx->peer_alive_ns, ctx->arg);
    free(ctx);
} /* nfs_recovery_import_finish */

static int
nfs_recovery_import_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs_recovery_import_ctx *ctx = private_data;
    struct nfs_recovery_record     *r;

    if (key_len < CHIMERA_KV_HDR_LEN ||
        memcmp(key, ctx->fstart, CHIMERA_KV_HDR_LEN) != 0) {
        return 1;
    }

    r = calloc(1, sizeof(*r));
    if (nfs_recovery_deserialize(value, value_len, r, 0) != 0 ||
        r->owner_node != ctx->peer ||
        nfs_recovery_record_stale(ctx->rec, r, ctx->peer_alive_ns)) {
        free(r);
        return 0;
    }

    /* The record keeps owner_node = peer: should the window close before the
    * client returns, the purge deletes it only if the peer still owns it. */
    if (nfs_recovery_add_loaded(ctx->rec, r)) {
        ctx->imported++;
    }
    return 0;
} /* nfs_recovery_import_scan_cb */

static void
nfs_recovery_import_scan_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_recovery_import_ctx *ctx = private_data;
    struct nfs_recovery            *rec = ctx->rec;
    uint64_t                        end;

    evpl_mutex_lock(&rec->lock);
    rec->imports_in_flight--;
    /* A failed scan may have loaded only some of the peer's clients: leave it
     * unmarked so the next of its clients to fail over scans again. */
    if (error_code == CHIMERA_VFS_OK) {
        nfs_recovery_note_peer_imported(rec, ctx->peer, ctx->peer_boot_id);
    }
    rec->failover_imported += ctx->imported;
    if (ctx->imported) {
        /* Hold the window open long enough for the peer's clients, counted
         * from now: the scan may have taken a while. */
        end = nfs_lease_now_ns() + (uint64_t) rec->grace_time_s * 1000000000ULL;
        if (rec->grace_end_ns < end) {
            rec->grace_end_ns = end;
        }
    }
    /* The window was opened before the scan.  With no import left in flight
     * and nobody owed a reclaim -- an empty or failed scan -- it has nothing
     * to protect. */
    if (rec->in_grace && rec->imports_in_flight == 0 &&
        rec->pending_reclaim == 0) {
        nfs_recovery_close_grace_locked(rec, NULL, "failover import settled");
    }
    evpl_mutex_unlock(&rec->lock);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_error("NFSv4 recovery: scan for node %u's client records "
                          "failed (error %d) after importing %u; will retry",
                          ctx->peer, error_code, ctx->imported);
        ctx->done(NFS_RECOVERY_STORE_ERROR, ctx->peer_alive_ns, ctx->arg);
        free(ctx);
        return;
    }
    chimera_nfs_info("NFSv4 recovery: node %u is down; imported %u client "
                     "record(s) for failover", ctx->peer, ctx->imported);

    nfs_recovery_import_finish(ctx);
} /* nfs_recovery_import_scan_complete */

static void
nfs_recovery_import_epoch_cb(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data)
{
    struct nfs_recovery_import_ctx *ctx    = private_data;
    struct nfs_recovery            *rec    = ctx->rec;
    enum nfs_recovery_failover      result = NFS_RECOVERY_NOT_FAILOVER;
    uint64_t                        bound, end;

    /* The peer's heartbeat rewrites its epoch every refresh interval (half a
     * lease); three missed beats mean it is gone.  A peer with no epoch on
     * record never completed a start with persistence on, so nothing defends
     * its records.  A read that fails, or a record that does not decode,
     * proves neither. */
    ctx->peer_alive_ns = 0;
    ctx->peer_boot_id  = 0;
    if (error_code == CHIMERA_VFS_OK && value) {
        if (nfs_recovery_epoch_deserialize(value, value_len, &ctx->peer_boot_id,
                                           &ctx->peer_alive_ns) != 0) {
            result = NFS_RECOVERY_STORE_ERROR;
        } else {
            bound = 3 * nfs_recovery_refresh_interval_ns(rec);
            if (ctx->peer_alive_ns + bound > nfs_recovery_wall_now_ns()) {
                /* Fresh.  Alive only once it has moved since the caller last
                 * looked: a node that has just died looks the same. */
                result = (ctx->seen_ns && ctx->peer_alive_ns != ctx->seen_ns) ?
                    NFS_RECOVERY_NOT_FAILOVER : NFS_RECOVERY_LIVENESS_PENDING;
                ctx->done(result, ctx->peer_alive_ns, ctx->arg);
                free(ctx);
                return;
            }
        }
    } else if (error_code != CHIMERA_VFS_ENOENT) {
        result = NFS_RECOVERY_STORE_ERROR;
    }

    if (result == NFS_RECOVERY_STORE_ERROR) {
        chimera_nfs_error("NFSv4 recovery: cannot read node %u's epoch record "
                          "(error %d); not treating it as down",
                          ctx->peer, error_code);
        ctx->done(result, 0, ctx->arg);
        free(ctx);
        return;
    }

    evpl_mutex_lock(&rec->lock);
    if (nfs_recovery_peer_imported(rec, ctx->peer, ctx->peer_boot_id)) {
        /* Proven dead again, and this boot's records are already in the set. */
        evpl_mutex_unlock(&rec->lock);
        nfs_recovery_import_finish(ctx);
        return;
    }
    /* Hold off every non-reclaim OPEN, lock and I/O before the first of the
     * peer's records is loaded, so nothing is granted that a reclaim would
     * conflict with; the scan's completion settles the window. */
    rec->imports_in_flight++;
    end = nfs_lease_now_ns() + (uint64_t) rec->grace_time_s * 1000000000ULL;
    if (!rec->in_grace || rec->grace_end_ns < end) {
        rec->grace_end_ns = end;
    }
    rec->in_grace = true;
    evpl_mutex_unlock(&rec->lock);

    nfs_kv_type_prefix(ctx->fstart, CHIMERA_KV_TYPE_NFS4_CLIENT);
    chimera_vfs_search_keys(ctx->vfs_thread,
                            ctx->fstart, CHIMERA_KV_HDR_LEN,
                            NULL, 0, 0,
                            nfs_recovery_import_scan_cb,
                            nfs_recovery_import_scan_complete, ctx);
} /* nfs_recovery_import_epoch_cb */

void
nfs_recovery_import_peer(
    struct chimera_vfs_thread *vfs_thread,
    struct nfs_recovery       *rec,
    uint16_t                   peer,
    const void                *owner,
    uint16_t                   owner_len,
    uint64_t                   seen_ns,
    void (                    *done )(
        enum nfs_recovery_failover result,
        uint64_t                   observed_ns,
        void                      *arg),
    void                      *arg)
{
    struct nfs_recovery_import_ctx *ctx;
    uint32_t                        ekey_len;

    if (peer == rec->node_id || rec->persistence_disabled ||
        owner_len > NFS4_OPAQUE_LIMIT) {
        done(NFS_RECOVERY_NOT_FAILOVER, 0, arg);
        return;
    }

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        done(NFS_RECOVERY_STORE_ERROR, 0, arg);
        return;
    }
    ctx->rec        = rec;
    ctx->vfs_thread = vfs_thread;
    ctx->peer       = peer;
    ctx->seen_ns    = seen_ns;
    ctx->owner_len  = owner_len;
    memcpy(ctx->owner, owner, owner_len);
    ctx->done = done;
    ctx->arg  = arg;

    ekey_len = nfs_kv_epoch_key(ctx->ekey, peer);
    chimera_vfs_get_key(vfs_thread, ctx->ekey, ekey_len,
                        nfs_recovery_import_epoch_cb, ctx);
} /* nfs_recovery_import_peer */
