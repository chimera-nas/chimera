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
 * the union of binding share-deny bits (R|W|D).  held_rev_out (optional)
 * reports the claim-derived union WITHOUT bl_want_used: a parked want is a
 * bit we are ASKING for, not one we hold, so a recall's drain-completion
 * check must ignore it — otherwise the parked I/O (waiting for the recall
 * to finish) and the recall ack (waiting for rev to shrink) deadlock. */
static void
chimera_vfs_bl_target_locked(
    struct chimera_vfs_file_state *file,
    uint8_t                       *rev_used_out,
    uint8_t                       *bind_deny_out,
    uint8_t                       *held_rev_out)
{
    struct chimera_vfs_claim *cur;
    uint8_t                   rev  = 0;
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

    if (held_rev_out) {
        *held_rev_out = rev;
    }
    *rev_used_out  = (uint8_t) (rev | file->bl_want_used);
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
        /* Ring under service_lock: detach NULLs the doorbell under the same
         * lock, so a late post during teardown cannot ring a doorbell whose
         * eventfd the service thread has already closed. */
        if (state->service_doorbell) {
            evpl_ring_doorbell(state->service_doorbell);
        }
    }
    pthread_mutex_unlock(&state->service_lock);
} /* chimera_vfs_bl_post */

/* Lazy module scan: the close thread attaches before backends register, so
 * the CAP_LEASE probe happens on first use.  The modules array is stable
 * once serving begins. */
SYMBOL_EXPORT bool
chimera_vfs_claim_backend_capable(struct chimera_vfs_state *state)
{
    uint8_t capable = 0;
    int     i;

    if (atomic_load_explicit(&state->lease_probed, memory_order_acquire)) {
        return atomic_load_explicit(&state->lease_capable,
                                    memory_order_relaxed);
    }
    if (!state->vfs) {
        return false;
    }
    for (i = 0; i < CHIMERA_VFS_MAX_MODULES; i++) {
        if (state->vfs->modules[i] &&
            (state->vfs->modules[i]->capabilities & CHIMERA_VFS_CAP_LEASE)) {
            capable = 1;
            break;
        }
    }
    /* Publish the answer before the probed flag, so a reader that sees
     * probed also sees the capability it was resolved to. */
    atomic_store_explicit(&state->lease_capable, capable,
                          memory_order_relaxed);
    atomic_store_explicit(&state->lease_probed, 1, memory_order_release);
    return capable;
} /* chimera_vfs_claim_backend_capable */

SYMBOL_EXPORT void
chimera_vfs_claim_backend_reeval(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    uint8_t rev, deny, held_rev;
    bool    post = false;

    if (!state || !file || file->bl_disabled ||
        !chimera_vfs_claim_backend_capable(state)) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    chimera_vfs_bl_target_locked(file, &rev, &deny, &held_rev);

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
            /* Completion check: the drain is done when the HELD union fits
             * the recall's retained mask (binding denials cannot be drained
             * and stall the recall — documented posture; parked wants are
             * excluded, see bl_target_locked). */
            post = ((held_rev & ~file->bl_recall_retain) == 0 && deny == 0);
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
    struct chimera_vfs_bl_op      *op       = private_data;
    struct chimera_vfs_state      *state    = op->state;
    struct chimera_vfs_file_state *file     = op->file;
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
    uint8_t                   rev, deny, held_rev;
    uint8_t                   action     = 0; /* 1=acquire, 2=release(ack) */
    uint64_t                  token      = 0;
    uint8_t                   retained   = 0;
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
    chimera_vfs_bl_target_locked(file, &rev, &deny, &held_rev);

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
            if ((held_rev & ~file->bl_recall_retain) == 0 && deny == 0) {
                /* Drained to the recall floor: acknowledge by releasing
                 * down to what the HELD union still needs (a parked want
                 * is re-acquired after the release completes -- the
                 * release_complete reeval sees it and posts a fresh
                 * acquire, whose completion pumps the parked I/O). */
                file->bl_state = CHIMERA_VFS_BL_RELEASING;
                token          = file->bl_token;
                retained       = held_rev & file->bl_recall_retain;
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

    op           = calloc(1, sizeof(*op));
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
    uint8_t retain     = file->bl_recall_retain;
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
    struct chimera_vfs_file_state *head, *file, *next;
    struct chimera_vfs_bl_work    *w;
    bool                           is_rel;

    if (!state || !chimera_vfs_claim_backend_capable(state)) {
        return;
    }

    pthread_mutex_lock(&state->service_lock);
    head                = state->service_head;
    state->service_head = NULL;
    state->service_tail = NULL;
    pthread_mutex_unlock(&state->service_lock);

    /* bl_work_queued stays SET across the splice and is cleared per file
     * just before its step: while it is set, bl_post leaves the node —
     * including its bl_work_next link — untouched, so the spliced chain
     * cannot be cross-linked into the live queue by a concurrent post.
     * (Clearing the flags at splice time let a concurrent bl_post re-link
     * a spliced-but-unprocessed file into the NEW queue; this walk then
     * nulled that shared node's bl_work_next, severing the live queue and
     * stranding everything behind it with bl_work_queued=1 — permanently
     * unserviceable parked I/O: the elbencho dirmode hang.) */
    for (file = head; file; file = next) {
        next               = file->bl_work_next;
        file->bl_work_next = NULL;

        pthread_mutex_lock(&state->service_lock);
        file->bl_work_queued = 0;
        pthread_mutex_unlock(&state->service_lock);

        if (file->bl_state == CHIMERA_VFS_BL_RECALLING) {
            chimera_vfs_bl_recall_drive(state, file);
        }
        chimera_vfs_bl_step(state, file);

        chimera_vfs_state_put(state, file); /* queue reference */
    }

    /* The ordered RANGE lane: acquire confirms and token releases execute
     * in exactly the order the local state changed.  Entries are popped ONE
     * AT A TIME under service_lock -- never spliced -- so a concurrent
     * chimera_vfs_claim_cancel can always find (and yank) a still-pending
     * TICKET in the FIFO.
     *
     * The lane is STRICTLY SERIAL across a confirm: dispatching a TICKET
     * sets work_confirming and the drain stops until that confirm
     * completes.  Nothing here may assume the backend answers inline --
     * chimera_vfs_dispatch routes a module's op to a delegation thread
     * whenever async delegation is configured, and a clustered arbiter will
     * never answer inline -- and if the next entry dispatched anyway, an
     * unlock's token release could reach the backend BEFORE the confirm it
     * was queued behind, which is exactly the collision this FIFO exists to
     * prevent.  When the backend does answer inline the completion clears
     * the gate before project_range returns, so this loop keeps draining
     * without a doorbell round trip. */
    for (;;) {
        /* Peek the head's type before committing.  A RELEASE has to be
         * selected AND dispatched under bl_dispatch_lock, so that an inline
         * confirm's drain cannot slip between the two and miss it; a TICKET
         * must NOT hold that lock, because its project_range takes it. */
        pthread_mutex_lock(&state->service_lock);
        if (state->work_confirming) {
            /* A confirm is in flight; its completion resumes the drain. */
            pthread_mutex_unlock(&state->service_lock);
            break;
        }
        w      = state->work_head;
        is_rel = w && w->type == CHIMERA_VFS_BL_WORK_RELEASE;
        pthread_mutex_unlock(&state->service_lock);

        if (!w) {
            break;
        }

        if (is_rel) {
            pthread_mutex_lock(&state->bl_dispatch_lock);
        }

        /* Re-read: the head may have changed while the lock was not held.
         * Only take it if it still matches what we prepared for. */
        pthread_mutex_lock(&state->service_lock);
        w = state->work_head;
        if (w && (w->type == CHIMERA_VFS_BL_WORK_RELEASE) != is_rel) {
            w = NULL;
        }
        if (w) {
            state->work_head = w->next;
            if (!state->work_head) {
                state->work_tail = NULL;
            }
            w->next = NULL;
            if (w->type == CHIMERA_VFS_BL_WORK_TICKET) {
                state->work_confirming = true;
            }
        }
        pthread_mutex_unlock(&state->service_lock);

        if (!w) {
            if (is_rel) {
                pthread_mutex_unlock(&state->bl_dispatch_lock);
            }
            continue;
        }

        switch (w->type) {
            case CHIMERA_VFS_BL_WORK_TICKET:
                chimera_vfs_claim_backend_project_range(state->service_thread,
                                                        state, w->ticket, true);
                break;
            case CHIMERA_VFS_BL_WORK_RELEASE:
                chimera_vfs_lease_release_backend(state->service_thread,
                                                  w->fh, w->fh_len,
                                                  w->fh_hash, w->token, 0,
                                                  NULL, NULL);
                pthread_mutex_unlock(&state->bl_dispatch_lock);
                break;
        } /* switch */
        free(w);
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

/* Called by the service thread before it destroys its doorbell.  Work
 * enqueued after detach is never serviced (teardown traffic on other vfs
 * threads can still release claims); chimera_vfs_state_destroy sweeps the
 * leftover entries. */
SYMBOL_EXPORT void
chimera_vfs_claim_backend_detach(struct chimera_vfs_state *state)
{
    pthread_mutex_lock(&state->service_lock);
    state->service_doorbell = NULL;
    pthread_mutex_unlock(&state->service_lock);
} /* chimera_vfs_claim_backend_detach */

/* ------------------------------------------------------------------ */
/* RANGE projection                                                   */
/* ------------------------------------------------------------------ */

/* One in-flight backend RANGE confirm.  Core-owned, and carrying a SNAPSHOT
 * of what the completion needs out of the ticket: a cancel that loses hands
 * the completion to the callback, and the core reads nothing more from the
 * ticket once the confirm is dispatched. */
struct chimera_vfs_bl_range_op {
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_claim      *claim;
    chimera_vfs_claim_acquire_cb_t cb;
    void                          *cb_private;
    bool                           serial_lane;
};

static void
chimera_vfs_bl_range_complete(
    enum chimera_vfs_error error_code,
    uint8_t                granted,
    uint64_t               token,
    void                  *private_data)
{
    struct chimera_vfs_bl_range_op *op          = private_data;
    struct chimera_vfs_state       *state       = op->state;
    struct chimera_vfs_file_state  *file        = op->file;
    struct chimera_vfs_claim       *claim       = op->claim;
    chimera_vfs_claim_acquire_cb_t  cb          = op->cb;
    void                           *cb_private  = op->cb_private;
    bool                            serial_lane = op->serial_lane;

    free(op);

    /* Resume the serial lane before the callback runs: the callback is
     * protocol code that can take arbitrary locks, and nothing it does
     * needs the lane held. */
    if (serial_lane) {
        pthread_mutex_lock(&state->service_lock);
        state->work_confirming = false;
        if (state->work_head && state->service_doorbell) {
            evpl_ring_doorbell(state->service_doorbell);
        }
        pthread_mutex_unlock(&state->service_lock);
    }

    if (error_code == CHIMERA_VFS_OK && granted) {
        pthread_mutex_lock(&file->lock);
        claim->backend_token = token;
        pthread_mutex_unlock(&file->lock);

        cb(CHIMERA_CLAIM_GRANTED, claim, NULL, cb_private);
    } else {
        struct chimera_vfs_claim_conflict conflict;

        /* Rollback the optimistic local insert; the conflict is remote and
         * reported empty (whole-file, no local holder to describe). */
        chimera_vfs_claim_release(state, file, claim);

        memset(&conflict, 0, sizeof(conflict));
        conflict.offset = 0;
        conflict.length = UINT64_MAX;
        cb(CHIMERA_CLAIM_DENIED, NULL, &conflict, cb_private);
    }

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_bl_range_complete */

/* Confirm a locally-granted RANGE claim with the backend before its
 * callback fires.  `thread` must be the calling vfs thread.  Fires the
 * ticket's callback on completion (GRANTED with the token recorded, or
 * DENIED after rollback), unless a concurrent cancel claims the ticket
 * first.  `serial_lane` is true only for the service drain's dispatch, so
 * the completion knows whether to release the lane gate. */
void
chimera_vfs_claim_backend_project_range(
    struct chimera_vfs_thread          *thread,
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket,
    bool                                serial_lane)
{
    struct chimera_vfs_claim       *claim = ticket->claim;
    struct chimera_vfs_file_state  *file  = ticket->file;
    struct chimera_vfs_bl_range_op *op;

    /* Any queued releases for this file's records must land first. */
    chimera_vfs_claim_backend_drain_releases(thread, state, file);

    op              = calloc(1, sizeof(*op));
    op->state       = state;
    op->file        = file;
    op->serial_lane = serial_lane;
    /* Snapshot the ticket now, while it is unambiguously ours: from the
     * moment a cancel claims this op the ticket is the canceller's to free
     * and the completion must not read it. */
    op->claim      = claim;
    op->cb         = ticket->cb;
    op->cb_private = ticket->private_data;

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
        !chimera_vfs_claim_backend_capable(state)) {
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

/* Drain queued RELEASE entries matching `file` and dispatch them inline on
 * `thread`, IN ORDER, before a confirm for the same file: any admit that
 * observed the range free happens-after the release was enqueued (the
 * enqueue runs under file->lock), so draining here closes every
 * release-then-relock ordering regardless of which path released. */
void
chimera_vfs_claim_backend_drain_releases(
    struct chimera_vfs_thread     *thread,
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_bl_work *w, **pp;
    struct chimera_vfs_bl_work *mine = NULL, **mtail = &mine;

    /* Selection and dispatch together: see bl_dispatch_lock in vfs_claim.h. */
    pthread_mutex_lock(&state->bl_dispatch_lock);

    pthread_mutex_lock(&state->service_lock);
    pp = &state->work_head;
    while ((w = *pp)) {
        if (w->type == CHIMERA_VFS_BL_WORK_RELEASE &&
            w->fh_hash == file->fh_hash &&
            w->fh_len == file->fh_len &&
            memcmp(w->fh, file->fh, w->fh_len) == 0) {
            *pp = w->next;
            if (state->work_tail == w) {
                state->work_tail = NULL; /* fixed below */
            }
            w->next = NULL;
            *mtail  = w;
            mtail   = &w->next;
            continue;
        }
        pp = &w->next;
    }
    /* Recompute tail if we removed it. */
    if (!state->work_tail) {
        for (w = state->work_head; w; w = w->next) {
            state->work_tail = w;
        }
    }
    pthread_mutex_unlock(&state->service_lock);

    while ((w = mine)) {
        mine = w->next;
        chimera_vfs_lease_release_backend(thread, w->fh, w->fh_len,
                                          w->fh_hash, w->token, 0,
                                          NULL, NULL);
        free(w);
    }

    pthread_mutex_unlock(&state->bl_dispatch_lock);
} /* chimera_vfs_claim_backend_drain_releases */

/* Cancel a RANGE acquire confirm that is still QUEUED on the work FIFO --
 * the confirm has not started, so yanking the entry means the callback will
 * never be invoked and the caller owns the ticket.  Returns true then.
 *
 * NEVER blocks.  False means the callback owns the completion: it is
 * running, or about to run, on the thread that dispatched the confirm.  A
 * ticket whose confirm has been dispatched is deliberately NOT reclaimable
 * (see chimera_vfs_claim_cancel in vfs_claim.h for why the caller-owned
 * claim makes that unsafe).
 *
 * This used to spin on sched_yield() until the in-flight confirm's callback
 * returned, so that false could promise the callback had FINISHED.  That
 * turned any caller holding a lock its callback also takes into a deadlock:
 * an NLM client teardown cancels under nlm_state.mutex while the confirm it
 * waits for is blocked taking that same mutex in NLM's acquire callback. */
bool
chimera_vfs_claim_backend_ticket_cancel(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket)
{
    struct chimera_vfs_bl_work *w, **pp;
    struct chimera_vfs_bl_work *yanked = NULL;

    pthread_mutex_lock(&state->service_lock);

    pp = &state->work_head;
    while ((w = *pp)) {
        if (w->type == CHIMERA_VFS_BL_WORK_TICKET && w->ticket == ticket) {
            *pp = w->next;
            if (state->work_tail == w) {
                state->work_tail = NULL;
                if (state->work_head) {
                    struct chimera_vfs_bl_work *t;

                    for (t = state->work_head; t; t = t->next) {
                        state->work_tail = t;
                    }
                }
            }
            yanked = w;
            break;
        }
        pp = &w->next;
    }

    pthread_mutex_unlock(&state->service_lock);

    free(yanked);

    return yanked != NULL;
} /* chimera_vfs_claim_backend_ticket_cancel */

/* Append one entry to the ordered backend-RANGE work FIFO. */
static void
chimera_vfs_bl_work_enqueue(
    struct chimera_vfs_state   *state,
    struct chimera_vfs_bl_work *work)
{
    pthread_mutex_lock(&state->service_lock);
    work->next = NULL;
    if (state->work_tail) {
        state->work_tail->next = work;
    } else {
        state->work_head = work;
    }
    state->work_tail = work;
    /* Ring under service_lock: see chimera_vfs_bl_post. */
    if (state->service_doorbell) {
        evpl_ring_doorbell(state->service_doorbell);
    }
    pthread_mutex_unlock(&state->service_lock);
} /* chimera_vfs_bl_work_enqueue */

/* Fire-and-forget release of a projected RANGE token.  Ordered behind every
 * previously queued backend op: a subsequent acquire of the freed range must
 * not overtake this release at the backend. */
void
chimera_vfs_claim_backend_release_token(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    uint64_t                       token)
{
    struct chimera_vfs_bl_work *work;

    if (!state || !token || !chimera_vfs_claim_backend_capable(state)) {
        return;
    }

    work       = calloc(1, sizeof(*work));
    work->type = CHIMERA_VFS_BL_WORK_RELEASE;
    memcpy(work->fh, file->fh, file->fh_len);
    work->fh_len  = file->fh_len;
    work->fh_hash = file->fh_hash;
    work->token   = token;

    chimera_vfs_bl_work_enqueue(state, work);
} /* chimera_vfs_claim_backend_release_token */

/* Queue a projectable RANGE acquire confirm on the same ordered lane. */
void
chimera_vfs_claim_backend_defer_ticket(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket)
{
    struct chimera_vfs_bl_work *work;

    work         = calloc(1, sizeof(*work));
    work->type   = CHIMERA_VFS_BL_WORK_TICKET;
    work->ticket = ticket;

    chimera_vfs_bl_work_enqueue(state, work);
} /* chimera_vfs_claim_backend_defer_ticket */
