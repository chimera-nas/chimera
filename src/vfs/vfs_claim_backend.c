// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "vfs_claim.h"
#include "vfs_claim_internal.h"
#include "vfs_internal.h"
#include "vfs_procs.h"
#include "common/macros.h"

/*
 * vfs_claim_backend.c — the CHIMERA_VFS_CAP_LEASE projection.
 *
 * Two claim shapes cross the boundary (see vfs.h at CHIMERA_VFS_CAP_LEASE):
 *
 *   AGGREGATE: one revocable per-node token per file covering the union of
 *   local holders — rev_used (R|W) + bind_deny (R|W|D).  Held lazily with
 *   escalate-or-reuse; every aggregate dispatch runs on the SERVICE thread
 *   (the VFS close thread) with one in-flight op per file.
 *
 *   RANGE: binding, per-owner, all-or-nothing records for byte-range locks,
 *   confirmed before the local grant's callback fires (optimistic local
 *   insert + rollback on refusal).  Dispatched on the acquirer's own thread
 *   when it has one, else via the service thread (the pending-queue pump).
 *
 * The recall path is fully asynchronous end to end: the backend invokes the
 * recall_cb captured at grant time from ANY context; we marshal to the
 * service thread, drive the trigger engine — which recalls the frontend
 * stacks (NFSv4 CB_RECALL, SMB OPLOCK_BREAK, the implicit claim's drain;
 * FUSE invalidation when it lands) through their existing break callbacks —
 * and acknowledge by releasing the token with the retained mask once the
 * local unions have actually shrunk.
 */

/* ------------------------------------------------------------------ */
/* Union computation                                                  */
/* ------------------------------------------------------------------ */

/* Compute the file's projection target from its local claims.  Caller holds
 * file->lock.  rev_used maps cache use onto data bits (CR->R, CW->W) and
 * folds in the implicit claim and any parked implicit want; bind_deny is
 * the union of binding share-deny bits (R|W|D). */
static void
chimera_vfs_bl_target_locked(
    struct chimera_vfs_file_state *file,
    uint8_t                       *rev_used_out,
    uint8_t                       *bind_deny_out)
{
    struct chimera_vfs_claim *cur;
    uint8_t                   rev  = file->bl_want_used;
    uint8_t                   deny = 0;

    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        uint8_t u = cur->used;

        if (cur->break_state == CHIMERA_CLAIM_BREAK_REVOKED) {
            continue;
        }
        if (u & CHIMERA_CLAIM_CR) {
            rev |= CHIMERA_CLAIM_R;
        }
        if (u & CHIMERA_CLAIM_CW) {
            rev |= CHIMERA_CLAIM_W;
        }
        /* Delegations carry data bits directly. */
        rev |= u & (CHIMERA_CLAIM_R | CHIMERA_CLAIM_W);
    }

    for (cur = file->claims[CHIMERA_CLAIM_CLASS_ACCESS]; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_CLAIM_BREAK_REVOKED) {
            continue;
        }
        if (cur->construct == CHIMERA_CONSTRUCT_IMPLICIT) {
            rev |= cur->used & (CHIMERA_CLAIM_R | CHIMERA_CLAIM_W);
            continue;
        }
        deny |= cur->denied & (CHIMERA_CLAIM_R | CHIMERA_CLAIM_W |
                               CHIMERA_CLAIM_D);
    }

    *rev_used_out  = rev;
    *bind_deny_out = deny;
} /* chimera_vfs_bl_target_locked */

/* ------------------------------------------------------------------ */
/* Service queue                                                      */
/* ------------------------------------------------------------------ */

/* Post `file` onto the service queue (idempotent).  Takes a file reference
 * that the service drain drops. */
static void
chimera_vfs_bl_post(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    bool ring = false;

    pthread_mutex_lock(&state->service_lock);
    if (!file->bl_work_queued) {
        file->bl_work_queued = 1;
        file->bl_work_next   = NULL;
        if (state->service_tail) {
            state->service_tail->bl_work_next = file;
        } else {
            state->service_head = file;
        }
        state->service_tail = file;
        /* Reference for the queue entry. */
        chimera_vfs_state_get(state, file->fh, file->fh_len, file->fh_hash,
                              false);
        ring = true;
    }
    pthread_mutex_unlock(&state->service_lock);

    if (ring && state->service_doorbell) {
        evpl_ring_doorbell(state->service_doorbell);
    }
} /* chimera_vfs_bl_post */

/* Lazy module scan: the close thread attaches before backends register, so
 * the CAP_LEASE probe happens on first use.  The modules array is stable
 * once serving begins. */
static inline bool
chimera_vfs_bl_capable(struct chimera_vfs_state *state)
{
    int i;

    if (state->lease_probed) {
        return state->lease_capable;
    }
    if (!state->vfs) {
        return false;
    }
    for (i = 0; i < CHIMERA_VFS_MAX_MODULES; i++) {
        if (state->vfs->modules[i] &&
            (state->vfs->modules[i]->capabilities & CHIMERA_VFS_CAP_LEASE)) {
            state->lease_capable = 1;
            break;
        }
    }
    state->lease_probed = 1;
    return state->lease_capable;
} /* chimera_vfs_bl_capable */

SYMBOL_EXPORT void
chimera_vfs_claim_backend_reeval(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    uint8_t rev, deny;
    bool    post = false;

    if (!state || !file || file->bl_disabled ||
        !chimera_vfs_bl_capable(state)) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    chimera_vfs_bl_target_locked(file, &rev, &deny);

    switch (file->bl_state) {
        case CHIMERA_VFS_BL_NONE:
            post = (rev | deny) != 0;
            break;
        case CHIMERA_VFS_BL_HELD:
            /* Escalate when the union outgrew the cover; shrink is lazy
             * (the reaper posts an idle file here; the step below drops an
             * empty idle token).  A refused bit is not re-requested until
             * something changes remotely (a recall clears bl_refused). */
            post = ((rev & ~file->bl_held_used) & ~file->bl_refused) != 0 ||
                   (deny & ~file->bl_held_deny) != 0 ||
                   ((rev | deny) == 0 &&
                    chimera_vfs_claim_elapsed_ms(file->bl_last_used,
                                                 chimera_vfs_now_ticks()) >=
                    state->implicit_idle_ms);
            break;
        case CHIMERA_VFS_BL_RECALLING:
            /* Completion check: the drain is done when the union fits the
             * recall's retained mask (binding denials cannot be drained and
             * stall the recall — documented posture). */
            post = ((rev & ~file->bl_recall_retain) == 0 && deny == 0);
            break;
        default:
            break;
    } /* switch */
    pthread_mutex_unlock(&file->lock);

    if (post) {
        chimera_vfs_bl_post(state, file);
    }
} /* chimera_vfs_claim_backend_reeval */

/* ------------------------------------------------------------------ */
/* Aggregate state machine (service thread only)                      */
/* ------------------------------------------------------------------ */

struct chimera_vfs_bl_op {
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    uint8_t                        req_used;
    uint8_t                        req_deny;
};

static void
chimera_vfs_bl_acquire_complete(
    enum chimera_vfs_error error_code,
    uint8_t                granted,
    uint64_t               token,
    void                  *private_data)
{
    struct chimera_vfs_bl_op      *op    = private_data;
    struct chimera_vfs_state      *state = op->state;
    struct chimera_vfs_file_state *file  = op->file;
    bool                           take_ref = false;

    pthread_mutex_lock(&file->lock);
    if (error_code != CHIMERA_VFS_OK) {
        /* Backend refused outright: remember the missing bits so parked
         * implicit I/O fails EACCES instead of spinning; keep any prior
         * cover. */
        file->bl_refused |= op->req_used & ~file->bl_held_used;
        file->bl_state    = file->bl_token ? CHIMERA_VFS_BL_HELD
                                           : CHIMERA_VFS_BL_NONE;
    } else {
        file->bl_refused  |= op->req_used & (uint8_t) ~granted;
        file->bl_held_used = granted;
        file->bl_held_deny = op->req_deny;
        file->bl_token     = token;
        file->bl_state     = CHIMERA_VFS_BL_HELD;
        file->bl_last_used = chimera_vfs_now_ticks();
        if (!file->bl_ref_held) {
            file->bl_ref_held = 1;
            take_ref          = true;
        }
    }
    pthread_mutex_unlock(&file->lock);

    if (take_ref) {
        /* The held token keeps the file anchored (dropped at release). */
        chimera_vfs_state_get(state, file->fh, file->fh_len, file->fh_hash,
                              false);
    }

    /* Wake anything gated on the cover, and re-evaluate (the target may
     * have grown while the op was in flight). */
    chimera_vfs_claim_pump_io(state, file);
    chimera_vfs_claim_pump_pending(state, file);
    chimera_vfs_claim_backend_reeval(state, file);

    chimera_vfs_state_put(state, file); /* op reference */
    free(op);
} /* chimera_vfs_bl_acquire_complete */

static void
chimera_vfs_bl_release_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_bl_op      *op    = private_data;
    struct chimera_vfs_state      *state = op->state;
    struct chimera_vfs_file_state *file  = op->file;
    bool                           drop_ref;

    (void) error_code; /* best-effort: local state advances regardless */

    pthread_mutex_lock(&file->lock);
    drop_ref           = file->bl_ref_held;
    file->bl_ref_held  = 0;
    file->bl_token     = 0;
    file->bl_held_used = 0;
    file->bl_held_deny = 0;
    file->bl_refused   = 0;
    file->bl_state     = CHIMERA_VFS_BL_NONE;
    pthread_mutex_unlock(&file->lock);

    if (drop_ref) {
        chimera_vfs_state_put(state, file);
    }

    chimera_vfs_claim_backend_reeval(state, file); /* re-acquire if needed */

    chimera_vfs_state_put(state, file); /* op reference */
    free(op);
} /* chimera_vfs_bl_release_complete */

/* One service step for `file`; runs on the service thread. */
static void
chimera_vfs_bl_step(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_bl_op *op;
    uint8_t                   rev, deny;
    uint8_t                   action = 0; /* 1=acquire, 2=release(ack) */
    uint64_t                  token  = 0;
    uint8_t                   retained = 0;
    uint64_t                  prev_token = 0;

    /* Resolve the module once: a non-CAP_LEASE file disables projection. */
    if (!file->bl_probed) {
        struct chimera_vfs_module *module =
            chimera_vfs_get_module(state->service_thread, file->fh,
                                   file->fh_len);

        pthread_mutex_lock(&file->lock);
        file->bl_probed = 1;
        if (!module || !(module->capabilities & CHIMERA_VFS_CAP_LEASE)) {
            file->bl_disabled = 1;
        }
        pthread_mutex_unlock(&file->lock);
    }

    if (file->bl_disabled) {
        /* Anything parked on the gate proceeds unprojected. */
        chimera_vfs_claim_pump_io(state, file);
        return;
    }

    pthread_mutex_lock(&file->lock);
    chimera_vfs_bl_target_locked(file, &rev, &deny);

    switch (file->bl_state) {
        case CHIMERA_VFS_BL_NONE:
            if ((rev | deny) != 0) {
                file->bl_state = CHIMERA_VFS_BL_ACQUIRING;
                action         = 1;
            }
            break;
        case CHIMERA_VFS_BL_HELD:
            if (((rev & ~file->bl_held_used) & ~file->bl_refused) != 0 ||
                (deny & ~file->bl_held_deny) != 0) {
                /* Escalate: request the new union, replacing the held
                 * token atomically at the backend (prev_token). */
                file->bl_state = CHIMERA_VFS_BL_ACQUIRING;
                prev_token     = file->bl_token;
                action         = 1;
            } else if ((rev | deny) == 0 &&
                       chimera_vfs_claim_elapsed_ms(
                           file->bl_last_used, chimera_vfs_now_ticks()) >=
                       state->implicit_idle_ms) {
                /* Idle empty cover: drop the token (the reaper posted us). */
                file->bl_state = CHIMERA_VFS_BL_RELEASING;
                token          = file->bl_token;
                retained       = 0;
                action         = 2;
            }
            break;
        case CHIMERA_VFS_BL_RECALLING:
            if ((rev & ~file->bl_recall_retain) == 0 && deny == 0) {
                /* Drained to the recall floor: acknowledge by releasing
                 * down to what the union still needs. */
                file->bl_state = CHIMERA_VFS_BL_RELEASING;
                token          = file->bl_token;
                retained       = rev & file->bl_recall_retain;
                action         = 2;
            }
            break;
        default:
            break;
    } /* switch */
    pthread_mutex_unlock(&file->lock);

    if (!action) {
        return;
    }

    op = calloc(1, sizeof(*op));
    op->state    = state;
    op->file     = file;
    op->req_used = rev;
    op->req_deny = deny;

    /* Reference for the in-flight op. */
    chimera_vfs_state_get(state, file->fh, file->fh_len, file->fh_hash, false);

    if (action == 1) {
        chimera_vfs_lease_acquire_backend(
            state->service_thread, file->fh, file->fh_len, file->fh_hash,
            CHIMERA_VFS_LEASE_AGGREGATE, rev, deny,
            0, 0, 0, &state->node_owner, prev_token,
            chimera_vfs_lease_backend_recall, state,
            chimera_vfs_bl_acquire_complete, op);
    } else {
        chimera_vfs_lease_release_backend(
            state->service_thread, file->fh, file->fh_len, file->fh_hash,
            token, retained,
            chimera_vfs_bl_release_complete, op);
    }
} /* chimera_vfs_bl_step */

/* ------------------------------------------------------------------ */
/* Recall upcall (any thread)                                         */
/* ------------------------------------------------------------------ */

SYMBOL_EXPORT void
chimera_vfs_lease_backend_recall(
    void          *recall_arg,
    const uint8_t *fh,
    uint8_t        fh_len,
    uint64_t       fh_hash,
    uint64_t       token,
    uint8_t        retain)
{
    struct chimera_vfs_state      *state = recall_arg;
    struct chimera_vfs_file_state *file;
    bool                           accept;

    if (!state || fh_len == 0) {
        return;
    }

    /* Ordinary during teardown races: a recall for a file this node no
     * longer tracks is a no-op. */
    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    accept = (file->bl_state == CHIMERA_VFS_BL_HELD &&
              file->bl_token == token);
    if (accept) {
        file->bl_state         = CHIMERA_VFS_BL_RECALLING;
        file->bl_recall_retain = retain;
        file->bl_refused       = 0; /* the world changed; requests may work */
    }
    pthread_mutex_unlock(&file->lock);

    if (accept) {
        /* Drive the frontend recalls on the service thread: the trigger
         * engine reaches NFSv4 (CB_RECALL), SMB (OPLOCK_BREAK), and the
         * implicit claim's drain through their break callbacks; the ack is
         * the eventual release once the unions shrink. */
        chimera_vfs_bl_post(state, file);
    }

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_lease_backend_recall */

/* Drive the local drain for a RECALLING file (service thread). */
static void
chimera_vfs_bl_recall_drive(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    uint8_t retain = file->bl_recall_retain;
    bool    flush_only = (retain & CHIMERA_CLAIM_R) != 0;

    /* Recall the cache-class holders (delegations, oplocks, leases) via the
     * namespace-recall engine: full drain for a to-zero recall, flush-style
     * when the backend lets the node keep its read cover. */
    chimera_vfs_claim_trigger_ns_full(state, file, NULL, flush_only);

    /* Drain the implicit claim when it holds bits beyond the floor. */
    pthread_mutex_lock(&file->lock);
    {
        bool drain = file->implicit_active &&
            (file->implicit_claim.used & (uint8_t) ~retain) != 0;

        pthread_mutex_unlock(&file->lock);
        if (drain) {
            chimera_vfs_claim_begin_break_ex(state, &file->implicit_claim,
                                             0, 0, true);
        }
    }
} /* chimera_vfs_bl_recall_drive */

/* ------------------------------------------------------------------ */
/* Service drain and attach                                           */
/* ------------------------------------------------------------------ */

SYMBOL_EXPORT void
chimera_vfs_claim_backend_service(struct chimera_vfs_state *state)
{
    struct chimera_vfs_file_state      *head, *file, *next;
    struct chimera_vfs_pending_acquire *tickets, *t, *tnext;

    if (!state || !chimera_vfs_bl_capable(state)) {
        return;
    }

    pthread_mutex_lock(&state->service_lock);
    head                = state->service_head;
    state->service_head = NULL;
    state->service_tail = NULL;
    for (file = head; file; file = file->bl_work_next) {
        file->bl_work_queued = 0;
    }
    tickets                = state->service_tickets;
    state->service_tickets = NULL;
    pthread_mutex_unlock(&state->service_lock);

    for (file = head; file; file = next) {
        next = file->bl_work_next;
        file->bl_work_next = NULL;

        if (file->bl_state == CHIMERA_VFS_BL_RECALLING) {
            chimera_vfs_bl_recall_drive(state, file);
        }
        chimera_vfs_bl_step(state, file);

        chimera_vfs_state_put(state, file); /* queue reference */
    }

    /* Deferred RANGE projections handed over by the pending-queue pump. */
    for (t = tickets; t; t = tnext) {
        tnext   = t->next;
        t->next = NULL;
        chimera_vfs_claim_backend_project_range(state->service_thread, state,
                                                t);
    }

    /* Queued fire-and-forget token releases (claim teardown on any thread). */
    {
        struct chimera_vfs_bl_token_release *rels, *rel, *rnext;

        pthread_mutex_lock(&state->service_lock);
        rels                  = state->token_releases;
        state->token_releases = NULL;
        pthread_mutex_unlock(&state->service_lock);

        for (rel = rels; rel; rel = rnext) {
            rnext = rel->next;
            chimera_vfs_lease_release_backend(state->service_thread,
                                              rel->fh, rel->fh_len,
                                              rel->fh_hash, rel->token, 0,
                                              NULL, NULL);
            free(rel);
        }
    }
} /* chimera_vfs_claim_backend_service */

SYMBOL_EXPORT void
chimera_vfs_claim_backend_attach(
    struct chimera_vfs_state  *state,
    struct chimera_vfs        *vfs,
    struct chimera_vfs_thread *service_thread,
    struct evpl_doorbell      *service_doorbell)
{
    state->vfs              = vfs;
    state->service_thread   = service_thread;
    state->service_doorbell = service_doorbell;

    /* Mint this node's wire identity: an INTERNAL-class owner whose
     * client_key distinguishes the node instance (R71's node component).
     * Boot-time ticks XOR pid is sufficient for the in-memory lifetime of
     * the state; a configured cluster node UUID replaces it when
     * projection goes multi-node. */
    memset(&state->node_owner, 0, sizeof(state->node_owner));
    state->node_owner.proto      = CHIMERA_CLAIM_PROTO_INTERNAL;
    state->node_owner.client_key =
        chimera_vfs_now_ticks() ^ ((uint64_t) getpid() << 32);
    state->node_owner.owner_lo = 0;
    state->node_owner.owner_hi = 0;
} /* chimera_vfs_claim_backend_attach */

/* ------------------------------------------------------------------ */
/* RANGE projection                                                   */
/* ------------------------------------------------------------------ */

struct chimera_vfs_bl_range_op {
    struct chimera_vfs_state           *state;
    struct chimera_vfs_file_state      *file;
    struct chimera_vfs_pending_acquire *ticket;
};

static void
chimera_vfs_bl_range_complete(
    enum chimera_vfs_error error_code,
    uint8_t                granted,
    uint64_t               token,
    void                  *private_data)
{
    struct chimera_vfs_bl_range_op     *op     = private_data;
    struct chimera_vfs_state           *state  = op->state;
    struct chimera_vfs_file_state      *file   = op->file;
    struct chimera_vfs_pending_acquire *ticket = op->ticket;
    struct chimera_vfs_claim           *claim  = ticket->claim;

    if (error_code == CHIMERA_VFS_OK && granted) {
        pthread_mutex_lock(&file->lock);
        claim->backend_token = token;
        pthread_mutex_unlock(&file->lock);

        ticket->cb(CHIMERA_CLAIM_GRANTED, claim, NULL,
                   ticket->private_data);
    } else {
        struct chimera_vfs_claim_conflict conflict;

        /* Rollback the optimistic local insert; the conflict is remote and
         * reported empty (whole-file, no local holder to describe). */
        chimera_vfs_claim_release(state, file, claim);

        memset(&conflict, 0, sizeof(conflict));
        conflict.offset = 0;
        conflict.length = UINT64_MAX;
        ticket->cb(CHIMERA_CLAIM_DENIED, NULL, &conflict,
                   ticket->private_data);
    }

    chimera_vfs_state_put(state, file);
    free(op);
} /* chimera_vfs_bl_range_complete */

/* Confirm a locally-granted RANGE claim with the backend before its
 * callback fires.  `thread` must be the calling vfs thread.  Fires the
 * ticket's callback on completion (GRANTED with the token recorded, or
 * DENIED after rollback). */
void
chimera_vfs_claim_backend_project_range(
    struct chimera_vfs_thread          *thread,
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket)
{
    struct chimera_vfs_claim       *claim = ticket->claim;
    struct chimera_vfs_file_state  *file  = ticket->file;
    struct chimera_vfs_bl_range_op *op;

    op = calloc(1, sizeof(*op));
    op->state  = state;
    op->file   = file;
    op->ticket = ticket;

    chimera_vfs_state_get(state, file->fh, file->fh_len, file->fh_hash, false);

    chimera_vfs_lease_acquire_backend(
        thread, file->fh, file->fh_len, file->fh_hash,
        CHIMERA_VFS_LEASE_RANGE, 0, 0,
        (claim->used & CHIMERA_CLAIM_LW) ? 1 : 0,
        claim->offset, claim->length,
        &claim->owner, 0,
        chimera_vfs_lease_backend_recall, state,
        chimera_vfs_bl_range_complete, op);
} /* chimera_vfs_claim_backend_project_range */

/* True when a RANGE claim on `file` must be confirmed with a backend before
 * its grant completes.  Resolves/caches the module probe via the service
 * machinery's flags. */
bool
chimera_vfs_claim_backend_range_projects(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_thread     *thread)
{
    if (!state || !thread || file->bl_disabled ||
        !chimera_vfs_bl_capable(state)) {
        return false;
    }
    if (!file->bl_probed) {
        struct chimera_vfs_module *module =
            chimera_vfs_get_module(thread, file->fh, file->fh_len);

        pthread_mutex_lock(&file->lock);
        file->bl_probed = 1;
        if (!module || !(module->capabilities & CHIMERA_VFS_CAP_LEASE)) {
            file->bl_disabled = 1;
        }
        pthread_mutex_unlock(&file->lock);
    }
    return !file->bl_disabled;
} /* chimera_vfs_claim_backend_range_projects */

/* Fire-and-forget release of a projected RANGE token.  Claim teardown may
 * run on any thread, and request pools are per-thread and unlocked, so the
 * release is queued and issued by the service drain on its own thread. */
void
chimera_vfs_claim_backend_release_token(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    uint64_t                       token)
{
    struct chimera_vfs_bl_token_release *rel;

    if (!state || !token || !chimera_vfs_bl_capable(state)) {
        return;
    }

    rel = calloc(1, sizeof(*rel));
    memcpy(rel->fh, file->fh, file->fh_len);
    rel->fh_len  = file->fh_len;
    rel->fh_hash = file->fh_hash;
    rel->token   = token;

    pthread_mutex_lock(&state->service_lock);
    rel->next             = state->token_releases;
    state->token_releases = rel;
    pthread_mutex_unlock(&state->service_lock);

    if (state->service_doorbell) {
        evpl_ring_doorbell(state->service_doorbell);
    }
} /* chimera_vfs_claim_backend_release_token */

/* Hand a pump-granted projectable ticket to the service thread. */
void
chimera_vfs_claim_backend_defer_ticket(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket)
{
    pthread_mutex_lock(&state->service_lock);
    ticket->next           = state->service_tickets;
    state->service_tickets = ticket;
    pthread_mutex_unlock(&state->service_lock);

    if (state->service_doorbell) {
        evpl_ring_doorbell(state->service_doorbell);
    }
} /* chimera_vfs_claim_backend_defer_ticket */
