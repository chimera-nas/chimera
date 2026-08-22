// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "vfs_claim.h"
#include "vfs_claim_internal.h"
#include "vfs_internal.h"
#include "common/macros.h"

/*
 * vfs_claim_break.c — the break state machine and the trigger engine
 * (Table 2).  One collect/pin/break loop replaces the six walkers of the
 * old lease core; per-trigger semantics (victim selection, floor,
 * exemptions, style) are the rows of chimera_vfs_claim_trigger_row().
 */

/* -------------------------------------------------------------------- */
/* Cascade helpers                                                      */
/* -------------------------------------------------------------------- */

/* Only an SMB2 RqLs FILE lease cascades one bit per ack; oplocks, dir
* leases, delegations, and the implicit claim break one-shot (R35). */
static inline bool
chimera_vfs_claim_cascades(const struct chimera_vfs_claim *claim)
{
    return claim->construct == CHIMERA_CONSTRUCT_RQLS;
} /* chimera_vfs_claim_cascades */

/* Next single-bit step toward the floor: CW first (flush), then H, then CR
 * (the most shareable, dropped last). */
static inline uint8_t
chimera_vfs_claim_break_step(
    uint8_t used,
    uint8_t floor)
{
    uint8_t excess = used & (uint8_t) ~floor;

    if (excess & CHIMERA_CLAIM_CW) {
        return used & (uint8_t) ~CHIMERA_CLAIM_CW;
    }
    if (excess & CHIMERA_CLAIM_H) {
        return used & (uint8_t) ~CHIMERA_CLAIM_H;
    }
    if (excess & CHIMERA_CLAIM_CR) {
        return used & (uint8_t) ~CHIMERA_CLAIM_CR;
    }
    return used & floor;
} /* chimera_vfs_claim_break_step */

/* -------------------------------------------------------------------- */
/* begin_break / ack / revoke / park                                    */
/* -------------------------------------------------------------------- */

void
chimera_vfs_claim_begin_break_ex(
    struct chimera_vfs_state *state,
    struct chimera_vfs_claim *claim,
    uint8_t                   floor,
    uint32_t                  deadline_ms,
    bool                      one_shot)
{
    struct chimera_vfs_file_state *file = claim->file;
    chimera_vfs_claim_break_cb_t   cb;
    void                          *cb_priv;
    uint8_t                        step = 0;
    bool                           should_invoke;

    if (deadline_ms == 0) {
        deadline_ms = state ? state->default_break_deadline_ms
            : CHIMERA_VFS_CLAIM_DEFAULT_BREAK_DEADLINE_MS;
    }

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    /* A settled-at-0 (ACKED) holder being re-broken re-arms to IDLE. */
    if (claim->break_state == CHIMERA_CLAIM_BREAK_ACKED) {
        claim->break_state = CHIMERA_CLAIM_BREAK_IDLE;
    }

    if (claim->break_state == CHIMERA_CLAIM_BREAK_BREAKING) {
        /* Cascade already running: only deepen the floor. */
        claim->break_floor &= floor;
        should_invoke       = false;
    } else {
        step = (chimera_vfs_claim_cascades(claim) && !one_shot)
            ? chimera_vfs_claim_break_step(claim->used, floor)
            : (uint8_t) (claim->used & floor);
        should_invoke = (step != claim->used) && (claim->break_cb != NULL);
    }

    if (should_invoke) {
        claim->break_state       = CHIMERA_CLAIM_BREAK_BREAKING;
        claim->break_floor       = floor;
        claim->break_needed_mode = step;
        claim->break_notified    = 0;
        claim->break_deadline    = chimera_vfs_now_ticks() +
            chimera_vfs_ns_to_ticks((uint64_t) deadline_ms * 1000000ULL);

        /* Publish {break_state, ack_required, advertised} ATOMICALLY under
         * file->lock (R38: the arm64 store-reorder class).  The advertised
         * drop happens HERE, at break-begin -- not at the notify edge --
         * for AT_BEGIN policies (SMB: a coexisting acquirer settles at
         * LEVEL_II immediately, R29).  NFSv4 delegations advertise NEVER:
         * they keep full mode until DELEGRETURN/revoke (R11).  Lock-bit
         * deny rows read RAW used regardless, so a lock acquirer keeps
         * waiting for the real ack (lock1). */
        if (claim->grant) {
            claim->grant->break_ack_required =
                ((claim->used & (uint8_t) ~step) &
                 (CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H)) != 0;
        }
        if (claim->owner.proto != CHIMERA_CLAIM_PROTO_NFSV4) {
            claim->advertised = step;
        }

        cb      = claim->break_cb;
        cb_priv = claim->cb_private;

        /* One break EVENT bumps the (v2) epoch exactly once (R37). */
        if (claim->grant && claim->grant->is_v2) {
            claim->grant->epoch++;
        }
    } else {
        cb      = NULL;
        cb_priv = NULL;
    }

    if (file) {
        pthread_mutex_unlock(&file->lock);
    }

    if (cb) {
        cb(claim, step, cb_priv);
    }
} /* chimera_vfs_claim_begin_break_ex */

SYMBOL_EXPORT void
chimera_vfs_claim_ack(
    struct chimera_vfs_claim *claim,
    uint8_t                   resulting_used)
{
    struct chimera_vfs_file_state *file    = claim->file;
    bool                           mutated = false;
    chimera_vfs_claim_break_cb_t   cb      = NULL;
    void                          *cb_priv = NULL;
    uint8_t                        step    = 0;

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    if (claim->break_state == CHIMERA_CLAIM_BREAK_BREAKING) {
        claim->used       = resulting_used;
        claim->advertised = resulting_used;

        step = chimera_vfs_claim_cascades(claim)
            ? chimera_vfs_claim_break_step(resulting_used, claim->break_floor)
            : resulting_used;

        if (step != resulting_used && claim->break_cb) {
            uint32_t deadline_ms = (file && file->state)
                ? file->state->default_break_deadline_ms
                : CHIMERA_VFS_CLAIM_DEFAULT_BREAK_DEADLINE_MS;

            claim->break_needed_mode = step;
            claim->advertised        = step;
            claim->break_deadline    = chimera_vfs_now_ticks() +
                chimera_vfs_ns_to_ticks((uint64_t) deadline_ms * 1000000ULL);
            cb      = claim->break_cb;
            cb_priv = claim->cb_private;
        } else {
            claim->break_state = resulting_used ? CHIMERA_CLAIM_BREAK_IDLE
                                                : CHIMERA_CLAIM_BREAK_ACKED;
            mutated = true;
        }
    }

    if (file) {
        pthread_mutex_unlock(&file->lock);
    }

    if (cb) {
        cb(claim, step, cb_priv);
        return;
    }

    if (mutated && file && file->state) {
        chimera_vfs_claim_pump_pending(file->state, file);
        chimera_vfs_claim_pump_io(file->state, file);
    }
} /* chimera_vfs_claim_ack */

SYMBOL_EXPORT void
chimera_vfs_claim_revoke(struct chimera_vfs_claim *claim)
{
    struct chimera_vfs_file_state *file = claim->file;
    chimera_vfs_claim_revoked_cb_t revoked_cb;
    void                          *cb_private;
    bool                           newly_revoked;

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    newly_revoked      = (claim->break_state != CHIMERA_CLAIM_BREAK_REVOKED);
    claim->used        = 0;
    claim->advertised  = 0;
    claim->denied      = 0;
    claim->break_state = CHIMERA_CLAIM_BREAK_REVOKED;
    revoked_cb         = claim->revoked_cb;
    cb_private         = claim->cb_private;

    if (file) {
        pthread_mutex_unlock(&file->lock);
    }

    if (newly_revoked && revoked_cb) {
        revoked_cb(claim, cb_private);
    }

    if (file && file->state) {
        chimera_vfs_claim_pump_pending(file->state, file);
        chimera_vfs_claim_pump_io(file->state, file);
    }
} /* chimera_vfs_claim_revoke */

SYMBOL_EXPORT void
chimera_vfs_claim_park(
    struct chimera_vfs_claim *claim,
    bool                      parked)
{
    struct chimera_vfs_file_state *file = claim->file;

    if (file) {
        pthread_mutex_lock(&file->lock);
    }
    claim->parked = parked ? 1 : 0;
    if (file) {
        pthread_mutex_unlock(&file->lock);
    }
} /* chimera_vfs_claim_park */

/* -------------------------------------------------------------------- */
/* Notified edge / ack-pending / revoke-breaks queries (ported)         */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT bool
chimera_vfs_claim_ack_pending(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    const struct chimera_vfs_claim_grant *except)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim      *cur;
    bool                           breaking = false;

    if (!state || fh_len == 0) {
        return false;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
            cur->grant && cur->grant->break_ack_required &&
            cur->grant != except) {
            breaking = true;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_state_put(state, file);
    return breaking;
} /* chimera_vfs_claim_ack_pending */

SYMBOL_EXPORT bool
chimera_vfs_claim_break_pending_notify(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim      *cur;
    bool                           pending = false;

    if (!state || fh_len == 0) {
        return false;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
            !cur->break_notified) {
            pending = true;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_state_put(state, file);
    return pending;
} /* chimera_vfs_claim_break_pending_notify */

SYMBOL_EXPORT void
chimera_vfs_claim_mark_break_notified(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash,
    const uint8_t            *lease_key)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim      *cur;

    if (!state || fh_len == 0) {
        return;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
            memcmp(cur->owner.key, lease_key, 16) == 0) {
            cur->break_notified = 1;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_claim_mark_break_notified */

SYMBOL_EXPORT void
chimera_vfs_claim_revoke_breaks(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    const struct chimera_vfs_claim_grant *except)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim      *to_revoke[CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH];
    struct chimera_vfs_claim      *cur;
    int                            n = 0;
    int                            i;

    if (!state || fh_len == 0) {
        return;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
            cur->grant != except &&
            n < CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH) {
            to_revoke[n++] = cur;
        }
    }
    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_claim_revoke(to_revoke[i]);
    }

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_claim_revoke_breaks */

/* -------------------------------------------------------------------- */
/* The trigger engine (Table 2)                                         */
/* -------------------------------------------------------------------- */

/* One trigger row, resolved per (trigger, victim, actor). */
struct chimera_claim_trigger_row {
    bool     selects;  /* is this claim a victim?                        */
    uint8_t  floor;
    bool     one_shot;
    uint32_t deadline_ms;
};

/* The trigger table.  `retain` is the caller floor for the rows that take
 * one (OPEN_H / OPEN_H_FORCE / OPEN_W / NS_UNLINK). */
static void
chimera_vfs_claim_trigger_row(
    enum chimera_claim_trigger        trigger,
    const struct chimera_vfs_claim   *victim,
    const struct chimera_claim_actor *actor,
    uint8_t                           retain,
    struct chimera_claim_trigger_row *row)
{
    bool legacy_ii = (victim->construct == CHIMERA_CONSTRUCT_OPLOCK_II);
    bool is_dir    = (victim->construct == CHIMERA_CONSTRUCT_DIR_LEASE);
    bool is_deleg  = (victim->construct == CHIMERA_CONSTRUCT_DELEG_R ||
                      victim->construct == CHIMERA_CONSTRUCT_DELEG_W);
    bool same_owner = actor &&
        chimera_claim_owner_equal(&victim->owner, &actor->owner);
    bool same_key = actor &&
        chimera_claim_owner_same_key(&victim->owner, &actor->owner);
    bool same_handle = actor && actor->op_handle &&
        victim->op_handle == actor->op_handle;

    memset(row, 0, sizeof(*row));
    row->one_shot = true;

    if (!chimera_vfs_claim_revocable(victim)) {
        return;
    }

    switch (trigger) {
        case CHIMERA_TRIGGER_WRITE:
        case CHIMERA_TRIGGER_SMB_LOCK:
            /* A write (or an SMB byte-range lock, MS-FSA 2.1.5.18) stales
             * every read cache to 0.  RqLs and W-coherent holders
             * self-exempt by owner/KEY unconditionally; a legacy LEVEL_II
             * confers no write coherence and NEVER self-exempts (batch6). */
            if (!(victim->used & CHIMERA_CLAIM_CR)) {
                return;
            }
            if (is_dir) {
                return; /* dir caches break via DIR_CONTENT only */
            }
            if ((same_owner || same_key) && !legacy_ii) {
                return;
            }
            row->selects = true;
            row->floor   = 0;
            break;

        case CHIMERA_TRIGGER_RANGE_LOCK:
            /* Core-side lock-vs-cache displacement for the PURE-READ-CACHE
             * residue only: W/H caches carry raw LR|LW deny rows and are
             * handled at admission (the acquirer parks until their break
             * ACKS).  A pure-R cache has no admission row, so the trigger
             * breaks it -- no-ack, floor 0, so the acquirer's synchronous
             * re-probe then grants (brl1/brl3, R51).  retain != 0 flags an
             * EXCLUSIVE lock (documented at chimera_vfs_claim_invalidate):
             * an exclusive lock stales every pure-R cache; a shared lock
             * stales only the locker's OWN pure-R cache (a foreign pure-R
             * cache coexists with a shared lock, R23).  NFSv4 same-client
             * exempt unconditionally (a client's own lock never recalls
             * its own delegation -- delegations are not pure-R anyway). */
        {
            bool excl      = retain != 0;
            bool pure_read = (victim->used & CHIMERA_CLAIM_CR) &&
                !(victim->used & (CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H));

            if (!pure_read || is_deleg || is_dir) {
                return;
            }
            if (victim->owner.proto == CHIMERA_CLAIM_PROTO_NFSV4 && actor &&
                actor->owner.proto == CHIMERA_CLAIM_PROTO_NFSV4 &&
                chimera_claim_owner_same_client(&victim->owner, &actor->owner)) {
                return;
            }
            if (!excl && !(same_owner || same_key || same_handle)) {
                return;
            }
            row->selects = true;
            row->floor   = 0;
            break;
        }

        case CHIMERA_TRIGGER_OPEN_H:
            /* Phase-1 pre-share-check handle break: legacy batch only;
             * RqLs and dir leases keep H on a mere open. */
            if (victim->construct != CHIMERA_CONSTRUCT_OPLOCK_BATCH) {
                return;
            }
            if (!(victim->used & CHIMERA_CLAIM_H & (uint8_t) ~retain)) {
                return;
            }
            if (same_owner || same_key) {
                return;
            }
            row->selects = true;
            row->floor   = retain;
            break;

        case CHIMERA_TRIGGER_OPEN_H_FORCE:
            /* A genuine share conflict strips an RqLs lease's H too
             * (break_twice): any H holder beyond retain. */
            if (!(victim->used & CHIMERA_CLAIM_H & (uint8_t) ~retain)) {
                return;
            }
            if (same_owner || same_key) {
                return;
            }
            row->selects = true;
            row->floor   = victim->used & (uint8_t) ~CHIMERA_CLAIM_H;
            break;

        case CHIMERA_TRIGGER_OPEN_W:
            /* Phase-2 post-grant write-cache break: retain R|H shape (a
             * plain open vs an RWH lease = one RWH->RH break, v2_epoch2). */
            if (!(victim->used & CHIMERA_CLAIM_CW & (uint8_t) ~retain)) {
                return;
            }
            if (same_owner || same_key) {
                return;
            }
            row->selects = true;
            row->floor   = retain;
            break;

        case CHIMERA_TRIGGER_NS_UNLINK:
            /* Unlink/DoC of an open file: strip holders beyond R to R,
             * single-step, awaited (smb2.lease.unlink; R-only peers are
             * untouched). */
            if ((victim->used & (uint8_t) ~retain) == 0) {
                return;
            }
            if (same_owner || same_key || same_handle) {
                return;
            }
            row->selects = true;
            row->floor   = retain;
            break;

        case CHIMERA_TRIGGER_NS_FULL:
            /* Full namespace recall: everyone, including the operating
             * client's own delegation (RFC 7530 10.4.5); only the very
             * handle performing the op is spared. */
            if (victim->used == 0) {
                return;
            }
            if (same_handle) {
                return;
            }
            row->selects     = true;
            row->floor       = 0;
            row->one_shot    = false; /* cascades: RENAME's RH->R and
                                       * UNLINK's ->NONE share the path */
            row->deadline_ms = CHIMERA_VFS_NFS_DELEG_METAOP_MS;
            break;

        case CHIMERA_TRIGGER_FLUSH:
            /* Attr-only setattr: CW holders flush; NFSv4 delegations are
             * always recalled (attribute stability, DELEG20). */
            if (same_handle) {
                return;
            }
            if (!(victim->used & CHIMERA_CLAIM_CW) && !is_deleg) {
                return;
            }
            row->selects     = true;
            row->floor       = 0;
            row->one_shot    = false;
            row->deadline_ms = CHIMERA_VFS_NFS_DELEG_METAOP_MS;
            break;

        case CHIMERA_TRIGGER_DIR_CONTENT:
            /* Directory content change: dir leases to 0 in one ack-required
             * notification; ParentLeaseKey exemption keyed on the lease key
             * alone regardless of client (R44). */
            if (!is_dir || victim->used == 0) {
                return;
            }
            if (same_key) {
                return;
            }
            row->selects = true;
            row->floor   = 0;
            break;

        default:
            break;
    } /* switch */

    if (is_deleg) {
        /* Delegation victims always floor 0 (CB_RECALL is all-or-nothing)
         * and never cascade. */
        row->floor    = 0;
        row->one_shot = true;
        if (row->deadline_ms == 0) {
            row->deadline_ms = CHIMERA_VFS_NFS_DELEG_RECALL_MS;
        }
    }
} /* chimera_vfs_claim_trigger_row */

/* The ONE collect/pin/break loop. */
void
chimera_vfs_claim_trigger_fire(
    struct chimera_vfs_state         *state,
    struct chimera_vfs_file_state    *file,
    enum chimera_claim_trigger        trigger,
    const struct chimera_claim_actor *actor,
    uint8_t                           retain)
{
    struct chimera_vfs_claim        *cur;
    struct chimera_vfs_claim        *to_break[CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH];
    struct chimera_vfs_claim_grant  *break_pin[CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH];
    struct chimera_claim_trigger_row rowv[CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH];
    int                              n = 0;
    int                              i;

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        struct chimera_claim_trigger_row row;

        if (cur->break_state != CHIMERA_CLAIM_BREAK_IDLE &&
            cur->break_state != CHIMERA_CLAIM_BREAK_ACKED) {
            continue;
        }
        chimera_vfs_claim_trigger_row(trigger, cur, actor, retain, &row);
        if (!row.selects) {
            continue;
        }
        if (n < CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH) {
            break_pin[n] = chimera_vfs_claim_pin_grant(cur);
            to_break[n]  = cur;
            rowv[n]      = row;
            n++;
        }
    }
    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_claim_begin_break_ex(state, to_break[i], rowv[i].floor,
                                         rowv[i].deadline_ms, rowv[i].one_shot);
        if (break_pin[i]) {
            chimera_vfs_claim_grant_release(state, break_pin[i], true);
        }
    }
} /* chimera_vfs_claim_trigger_fire */

SYMBOL_EXPORT void
chimera_vfs_claim_invalidate(
    struct chimera_vfs_state         *state,
    const uint8_t                    *fh,
    uint8_t                           fh_len,
    uint64_t                          fh_hash,
    enum chimera_claim_trigger        trigger,
    const struct chimera_claim_actor *actor,
    uint8_t                           retain)
{
    struct chimera_vfs_file_state *file;

    if (!state || fh_len == 0) {
        return;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return;
    }

    chimera_vfs_claim_trigger_fire(state, file, trigger, actor, retain);

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_claim_invalidate */

/* -------------------------------------------------------------------- */
/* NS_FULL / FLUSH / NS_UNLINK blocking engines                         */
/* -------------------------------------------------------------------- */

/* Drive the full/flush recall loop; return true while any holder still
 * blocks.  NS_FULL blocks while any effective mode remains; FLUSH blocks
 * only on effective CW -- except NFSv4 delegations, which always block
 * until returned or revoked (DELEG20). */
bool
chimera_vfs_claim_trigger_ns_full(
    struct chimera_vfs_state             *state,
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_open_handle *skip_handle,
    bool                                  flush_only)
{
    bool had;

    for ( ; ; ) {
        struct chimera_vfs_claim       *cur;
        struct chimera_vfs_claim       *to_break  = NULL;
        struct chimera_vfs_claim       *to_revoke = NULL;
        struct chimera_vfs_claim_grant *break_pin = NULL;
        bool                            is_deleg;

        pthread_mutex_lock(&file->lock);
        for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur;
             cur = cur->next) {
            if (cur->used == 0 || !chimera_vfs_claim_revocable(cur)) {
                continue;
            }
            if (skip_handle && cur->op_handle == skip_handle) {
                continue;
            }
            is_deleg = (cur->construct == CHIMERA_CONSTRUCT_DELEG_R ||
                        cur->construct == CHIMERA_CONSTRUCT_DELEG_W);
            if (flush_only && !(cur->used & CHIMERA_CLAIM_CW) && !is_deleg) {
                continue;
            }
            if (cur->break_state == CHIMERA_CLAIM_BREAK_IDLE ||
                cur->break_state == CHIMERA_CLAIM_BREAK_ACKED) {
                if (cur->break_state == CHIMERA_CLAIM_BREAK_ACKED) {
                    cur->break_state = CHIMERA_CLAIM_BREAK_IDLE;
                }
                to_break = cur;
                break;
            }
            if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
                chimera_vfs_claim_deadline_passed(cur)) {
                to_revoke = cur;
                break;
            }
        }
        if (to_break) {
            break_pin = chimera_vfs_claim_pin_grant(to_break);
        }
        pthread_mutex_unlock(&file->lock);

        if (to_break) {
            /* Cascading (not one-shot): a namespace recall fans out across
             * the retain levels its callers need (RENAME's RH->R vs
             * UNLINK's ->NONE); rename_last depends on the cascade. */
            chimera_vfs_claim_begin_break_ex(state, to_break, 0,
                                             CHIMERA_VFS_NFS_DELEG_METAOP_MS,
                                             false);
            if (break_pin) {
                chimera_vfs_claim_grant_release(state, break_pin, true);
            }
        } else if (to_revoke) {
            chimera_vfs_claim_revoke(to_revoke);
        } else {
            break;
        }
    }

    /* Still blocked iff some holder retains an effective mode: a notified
     * SMB holder counts at its dropped advertised mode (the metadata op
     * proceeds; the ack arrives asynchronously), an NFSv4 delegation at its
     * full mode until DELEGRETURN (advertise NEVER). */
    pthread_mutex_lock(&file->lock);
    had = false;
    {
        struct chimera_vfs_claim *cur;
        uint8_t                   block_mask =
            flush_only ? CHIMERA_CLAIM_CW : 0xFF;

        for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur;
             cur = cur->next) {
            uint8_t mask = block_mask;

            if (skip_handle && cur->op_handle == skip_handle) {
                continue;
            }
            if (cur->construct == CHIMERA_CONSTRUCT_DELEG_R ||
                cur->construct == CHIMERA_CONSTRUCT_DELEG_W) {
                mask = 0xFF;
            }
            if (chimera_vfs_claim_advertised(cur) & mask) {
                had = true;
                break;
            }
        }
    }
    pthread_mutex_unlock(&file->lock);

    return had;
} /* chimera_vfs_claim_trigger_ns_full */

bool
chimera_vfs_claim_trigger_ns_unlink(
    struct chimera_vfs_state             *state,
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_open_handle *skip_handle,
    uint8_t                               retain)
{
    bool had = false;

    for ( ; ; ) {
        struct chimera_vfs_claim       *cur;
        struct chimera_vfs_claim       *to_break  = NULL;
        struct chimera_vfs_claim       *to_revoke = NULL;
        struct chimera_vfs_claim_grant *break_pin = NULL;

        pthread_mutex_lock(&file->lock);
        for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur;
             cur = cur->next) {
            if (!chimera_vfs_claim_revocable(cur)) {
                continue;
            }
            if (skip_handle && cur->op_handle == skip_handle) {
                continue;
            }
            if ((cur->used & (uint8_t) ~retain) == 0) {
                continue;
            }
            if (cur->break_state == CHIMERA_CLAIM_BREAK_IDLE) {
                to_break = cur;
                break;
            }
            if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING &&
                chimera_vfs_claim_deadline_passed(cur)) {
                to_revoke = cur;
                break;
            }
        }
        if (to_break) {
            break_pin = chimera_vfs_claim_pin_grant(to_break);
        }
        pthread_mutex_unlock(&file->lock);

        if (to_break) {
            chimera_vfs_claim_begin_break_ex(state, to_break, retain,
                                             CHIMERA_VFS_NFS_DELEG_METAOP_MS,
                                             true /* single-step */);
            if (break_pin) {
                chimera_vfs_claim_grant_release(state, break_pin, true);
            }
        } else if (to_revoke) {
            chimera_vfs_claim_revoke(to_revoke);
        } else {
            break;
        }
    }

    /* Blocked while any non-spared holder is still BREAKING: the
     * delete-on-close caller must WAIT for the real break ACK before
     * replying (smb2.lease.unlink). */
    pthread_mutex_lock(&file->lock);
    {
        struct chimera_vfs_claim *cur;

        for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur;
             cur = cur->next) {
            if (skip_handle && cur->op_handle == skip_handle) {
                continue;
            }
            if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING) {
                had = true;
                break;
            }
        }
    }
    pthread_mutex_unlock(&file->lock);

    return had;
} /* chimera_vfs_claim_trigger_ns_unlink */

SYMBOL_EXPORT bool
chimera_vfs_claim_break_caching(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash)
{
    struct chimera_vfs_file_state *file;
    bool                           had;

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return false;
    }

    had = chimera_vfs_claim_trigger_ns_full(state, file, NULL, false);

    chimera_vfs_state_put(state, file);
    return had;
} /* chimera_vfs_claim_break_caching */
