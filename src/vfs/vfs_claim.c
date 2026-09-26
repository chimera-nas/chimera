// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "vfs_claim.h"
#include "vfs_claim_internal.h"
#include "vfs_internal.h"
#include "vfs_procs.h"
#include "common/macros.h"

/*
 * vfs_claim.c — table lifecycle, the admission predicate (Table 1), claim
 * acquisition, tickets/pumps, cache grants, and queries.  The trigger
 * engine (Table 2) and break machinery live in vfs_claim_break.c; the
 * implicit I/O claim, the mandatory-lock I/O predicate, and the reaper in
 * vfs_claim_io.c.
 */

/* -------------------------------------------------------------------- */
/* Lifecycle / sharded table (ported unchanged from the lease core)     */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT bool
chimera_vfs_claim_has_caching(struct chimera_vfs_file_state *file)
{
    bool held = false;

    pthread_mutex_lock(&file->lock);
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_CACHE];
         claim; claim = claim->next) {
        if (chimera_vfs_claim_advertised(claim)) {
            held = true;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);
    return held;
} /* chimera_vfs_claim_has_caching */

SYMBOL_EXPORT struct chimera_vfs_state *
chimera_vfs_state_init(void)
{
    struct chimera_vfs_state *state;
    int                       i;

    state = calloc(1, sizeof(*state));
    if (!state) {
        return NULL;
    }

    for (i = 0; i < CHIMERA_VFS_STATE_NUM_SHARDS; i++) {
        pthread_mutex_init(&state->shards[i].lock, NULL);
    }

    state->default_break_deadline_ms = CHIMERA_VFS_CLAIM_DEFAULT_BREAK_DEADLINE_MS;
    state->implicit_idle_ms          = CHIMERA_VFS_CLAIM_DEFAULT_IMPLICIT_IDLE_MS;

    pthread_mutex_init(&state->service_lock, NULL);
    pthread_mutex_init(&state->bl_dispatch_lock, NULL);

    return state;
} /* chimera_vfs_state_init */

SYMBOL_EXPORT void
chimera_vfs_state_destroy(struct chimera_vfs_state *state)
{
    struct chimera_vfs_file_state *file, *next;
    struct chimera_vfs_bl_work    *work;
    int                            i;

    if (!state) {
        return;
    }

    /* Sweep backend work enqueued after the service thread detached; the
     * backends it targeted are already torn down, so the entries are inert
     * (a deferred TICKET's callback is abandoned with the rest of the
     * shutdown). */
    while ((work = state->work_head) != NULL) {
        state->work_head = work->next;
        free(work);
    }

    for (i = 0; i < CHIMERA_VFS_STATE_NUM_SHARDS; i++) {
        struct chimera_vfs_state_shard *shard = &state->shards[i];
        uint32_t                        s;

        for (s = 0; s < shard->nslots; s++) {
            file = shard->slots[s];
            while (file) {
                next = file->bucket_next;
                pthread_mutex_destroy(&file->lock);
                free(file->smb_pending_delete);
                free(file);
                file = next;
            }
        }
        free(shard->slots);
        pthread_mutex_destroy(&shard->lock);
    }

    free(state);
} /* chimera_vfs_state_destroy */

static inline struct chimera_vfs_state_shard *
chimera_vfs_state_shard_for(
    struct chimera_vfs_state *state,
    uint64_t                  fh_hash)
{
    return &state->shards[(fh_hash >> 32) & (CHIMERA_VFS_STATE_NUM_SHARDS - 1)];
} /* chimera_vfs_state_shard_for */

static inline struct chimera_vfs_file_state **
chimera_vfs_state_slot_for(
    struct chimera_vfs_state_shard *shard,
    uint64_t                        fh_hash)
{
    return &shard->slots[fh_hash & (shard->nslots - 1)];
} /* chimera_vfs_state_slot_for */

static void
chimera_vfs_state_shard_grow(struct chimera_vfs_state_shard *shard)
{
    struct chimera_vfs_file_state **nslots_arr;
    struct chimera_vfs_file_state  *file, *next;
    uint32_t                        nnew;
    uint32_t                        s;

    nnew = shard->nslots ? shard->nslots * 2 : CHIMERA_VFS_STATE_INITIAL_SLOTS;

    nslots_arr = calloc(nnew, sizeof(*nslots_arr));
    if (!nslots_arr) {
        return;
    }

    for (s = 0; s < shard->nslots; s++) {
        file = shard->slots[s];
        while (file) {
            next                                   = file->bucket_next;
            file->bucket_next                      = nslots_arr[file->fh_hash & (nnew - 1)];
            nslots_arr[file->fh_hash & (nnew - 1)] = file;
            file                                   = next;
        }
    }

    free(shard->slots);
    shard->slots  = nslots_arr;
    shard->nslots = nnew;
} /* chimera_vfs_state_shard_grow */

static inline int
chimera_vfs_file_state_match(
    const struct chimera_vfs_file_state *file,
    const uint8_t                       *fh,
    uint8_t                              fh_len,
    uint64_t                             fh_hash)
{
    return file->fh_hash == fh_hash &&
           file->fh_len == fh_len &&
           memcmp(file->fh, fh, fh_len) == 0;
} /* chimera_vfs_file_state_match */

SYMBOL_EXPORT struct chimera_vfs_file_state *
chimera_vfs_state_get(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash,
    bool                      create)
{
    struct chimera_vfs_state_shard *shard;
    struct chimera_vfs_file_state  *file;
    struct chimera_vfs_file_state **slot;

    shard = chimera_vfs_state_shard_for(state, fh_hash);

    pthread_mutex_lock(&shard->lock);

    if (shard->nslots) {
        for (file = *chimera_vfs_state_slot_for(shard, fh_hash);
             file;
             file = file->bucket_next) {
            if (chimera_vfs_file_state_match(file, fh, fh_len, fh_hash)) {
                file->refcount++;
                pthread_mutex_unlock(&shard->lock);
                return file;
            }
        }
    }

    if (!create) {
        pthread_mutex_unlock(&shard->lock);
        return NULL;
    }

    if (shard->count + 1 > shard->nslots) {
        chimera_vfs_state_shard_grow(shard);
        if (!shard->nslots) {
            pthread_mutex_unlock(&shard->lock);
            return NULL;
        }
    }

    file = calloc(1, sizeof(*file));
    if (!file) {
        pthread_mutex_unlock(&shard->lock);
        return NULL;
    }

    memcpy(file->fh, fh, fh_len);
    file->fh_len   = fh_len;
    file->fh_hash  = fh_hash;
    file->refcount = 1;
    file->state    = state;
    pthread_mutex_init(&file->lock, NULL);

    slot              = chimera_vfs_state_slot_for(shard, fh_hash);
    file->bucket_next = *slot;
    *slot = file;
    shard->count++;

    pthread_mutex_unlock(&shard->lock);
    return file;
} /* chimera_vfs_state_get */

SYMBOL_EXPORT void
chimera_vfs_state_put(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_state_shard *shard;
    struct chimera_vfs_file_state **link;
    bool                            empty;
    int                             k;

    if (!file) {
        return;
    }

    shard = chimera_vfs_state_shard_for(state, file->fh_hash);

    pthread_mutex_lock(&shard->lock);

    chimera_vfs_abort_if(file->refcount == 0, "double put on vfs_state file");

    file->refcount--;
    if (file->refcount != 0) {
        pthread_mutex_unlock(&shard->lock);
        return;
    }

    /* Tear down only when no claims remain (a claim is an implicit ref). */
    empty = true;
    for (k = 0; k < CHIMERA_CLAIM_CLASS_COUNT; k++) {
        if (file->claims[k]) {
            empty = false;
            break;
        }
    }

    if (!empty) {
        file->refcount = 1;
        pthread_mutex_unlock(&shard->lock);
        return;
    }

    for (link = chimera_vfs_state_slot_for(shard, file->fh_hash);
         *link;
         link = &(*link)->bucket_next) {
        if (*link == file) {
            *link = file->bucket_next;
            shard->count--;
            break;
        }
    }

    pthread_mutex_unlock(&shard->lock);

    free(file->smb_pending_delete);
    pthread_mutex_destroy(&file->lock);
    free(file);
} /* chimera_vfs_state_put */

SYMBOL_EXPORT void
chimera_vfs_file_state_release(struct chimera_vfs_file_state *file)
{
    if (file) {
        chimera_vfs_state_put(file->state, file);
    }
} /* chimera_vfs_file_state_release */

/* -------------------------------------------------------------------- */
/* SMB annex (ported)                                                   */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT bool
chimera_vfs_state_has_other_share_holder(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *exclude)
{
    struct chimera_vfs_claim *cur;
    bool                      found = false;

    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_ACCESS]; cur; cur = cur->next) {
        if (cur == exclude) {
            continue;
        }
        if (cur->construct == CHIMERA_CONSTRUCT_IMPLICIT) {
            continue;
        }
        found = true;
        break;
    }
    pthread_mutex_unlock(&file->lock);

    return found;
} /* chimera_vfs_state_has_other_share_holder */

SYMBOL_EXPORT void
chimera_vfs_state_set_delete_pending(struct chimera_vfs_file_state *file)
{
    if (file) {
        pthread_mutex_lock(&file->lock);
        file->delete_pending = 1;
        pthread_mutex_unlock(&file->lock);
    }
} /* chimera_vfs_state_set_delete_pending */

SYMBOL_EXPORT void
chimera_vfs_state_clear_delete_pending(struct chimera_vfs_file_state *file)
{
    if (file) {
        pthread_mutex_lock(&file->lock);
        file->delete_pending = 0;
        free(file->smb_pending_delete);
        file->smb_pending_delete = NULL;
        pthread_mutex_unlock(&file->lock);
    }
} /* chimera_vfs_state_clear_delete_pending */

SYMBOL_EXPORT bool
chimera_vfs_state_is_delete_pending(struct chimera_vfs_file_state *file)
{
    bool pending;

    if (!file) {
        return false;
    }
    pthread_mutex_lock(&file->lock);
    pending = file->delete_pending;
    pthread_mutex_unlock(&file->lock);
    return pending;
} /* chimera_vfs_state_is_delete_pending */

SYMBOL_EXPORT void
chimera_vfs_state_stream_holder_inc(struct chimera_vfs_file_state *file)
{
    if (file) {
        pthread_mutex_lock(&file->lock);
        file->stream_holders++;
        pthread_mutex_unlock(&file->lock);
    }
} /* chimera_vfs_state_stream_holder_inc */

SYMBOL_EXPORT void
chimera_vfs_state_stream_holder_dec(struct chimera_vfs_file_state *file)
{
    if (file) {
        pthread_mutex_lock(&file->lock);
        if (file->stream_holders > 0) {
            file->stream_holders--;
        }
        pthread_mutex_unlock(&file->lock);
    }
} /* chimera_vfs_state_stream_holder_dec */

SYMBOL_EXPORT uint32_t
chimera_vfs_state_stream_holders(struct chimera_vfs_file_state *file)
{
    uint32_t n;

    if (!file) {
        return 0;
    }
    pthread_mutex_lock(&file->lock);
    n = file->stream_holders;
    pthread_mutex_unlock(&file->lock);
    return n;
} /* chimera_vfs_state_stream_holders */

/* -------------------------------------------------------------------- */
/* Circles, advertised, overlap                                         */
/* -------------------------------------------------------------------- */

bool
chimera_vfs_claim_same_holder(
    const struct chimera_vfs_claim *a,
    const struct chimera_vfs_claim *b)
{
    if (a->op_handle && a->op_handle == b->op_handle) {
        return true;
    }
    /* An ACCESS claim and its own open's cache grant are one holder
     * (holder-lite: the own_cache link). */
    if (a->grant && (a->grant == b->grant || a->grant == b->own_cache)) {
        return true;
    }
    if (b->grant && b->grant == a->own_cache) {
        return true;
    }
    return false;
} /* chimera_vfs_claim_same_holder */

bool
chimera_vfs_claim_circle_exempt(
    uint8_t                         circle,
    const struct chimera_vfs_claim *holder,
    const struct chimera_vfs_claim *probe)
{
    switch (circle) {
        case CHIMERA_CIRCLE_OWNER:
            return chimera_claim_owner_equal(&holder->owner, &probe->owner) ||
                   chimera_vfs_claim_same_holder(holder, probe);
        case CHIMERA_CIRCLE_KEY:
            return chimera_claim_owner_same_key(&holder->owner, &probe->owner) ||
                   chimera_vfs_claim_same_holder(holder, probe);
        case CHIMERA_CIRCLE_CLIENT:
            return chimera_claim_owner_same_client(&holder->owner, &probe->owner);
        case CHIMERA_CIRCLE_OWNER_OR_KEY:
            return chimera_claim_owner_equal(&holder->owner, &probe->owner) ||
                   chimera_claim_owner_same_key(&holder->owner, &probe->owner) ||
                   chimera_vfs_claim_same_holder(holder, probe);
        default:
            return false;
    } /* switch */
} /* chimera_vfs_claim_circle_exempt */

uint8_t
chimera_vfs_claim_advertised(const struct chimera_vfs_claim *claim)
{
    uint8_t adv;

    if (claim->break_state == CHIMERA_CLAIM_BREAK_REVOKED) {
        return 0;
    }
    adv = claim->advertised;
    if (claim->parked) {
        adv &= (uint8_t) ~CHIMERA_CLAIM_H;
    }
    return adv;
} /* chimera_vfs_claim_advertised */

bool
chimera_vfs_claim_revocable(const struct chimera_vfs_claim *claim)
{
    return claim->break_cb != NULL;
} /* chimera_vfs_claim_revocable */

bool
chimera_vfs_claim_range_overlap(
    uint64_t a_off,
    uint64_t a_len,
    uint64_t b_off,
    uint64_t b_len)
{
    __uint128_t a_end = (a_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) a_off + a_len;
    __uint128_t b_end = (b_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) b_off + b_len;

    return a_off < b_end && b_off < a_end;
} /* chimera_vfs_claim_range_overlap */

static inline bool
chimera_vfs_claim_claims_overlap(
    const struct chimera_vfs_claim *a,
    const struct chimera_vfs_claim *b)
{
    uint64_t a_off = 0, a_len = UINT64_MAX;
    uint64_t b_off = 0, b_len = UINT64_MAX;

    if (a->klass == CHIMERA_CLAIM_CLASS_RANGE) {
        a_off = a->offset;
        a_len = a->length;
    }
    if (b->klass == CHIMERA_CLAIM_CLASS_RANGE) {
        b_off = b->offset;
        b_len = b->length;
    }
    return chimera_vfs_claim_range_overlap(a_off, a_len, b_off, b_len);
} /* chimera_vfs_claim_claims_overlap */

bool
chimera_vfs_claim_deadline_passed(const struct chimera_vfs_claim *claim)
{
    return chimera_vfs_now_ticks() >= claim->break_deadline;
} /* chimera_vfs_claim_deadline_passed */

bool
chimera_vfs_claim_holder_reclaimable(
    const struct chimera_vfs_claim *holder,
    const struct chimera_vfs_claim *probe)
{
    return holder->is_alive_cb &&
           holder->owner.client_key != probe->owner.client_key &&
           !holder->is_alive_cb(holder, holder->cb_private);
} /* chimera_vfs_claim_holder_reclaimable */

/* -------------------------------------------------------------------- */
/* Constructors — the admission mask table, part 1 (stamping)           */
/* -------------------------------------------------------------------- */

static void
chimera_vfs_claim_init_common(
    struct chimera_vfs_claim         *claim,
    enum chimera_claim_construct      construct,
    enum chimera_claim_class          klass,
    uint8_t                           used,
    uint8_t                           denied,
    const struct chimera_claim_owner *owner)
{
    memset(claim, 0, sizeof(*claim));
    claim->construct  = construct;
    claim->klass      = klass;
    claim->used       = used;
    claim->advertised = used;
    claim->denied     = denied;
    claim->owner      = *owner;
    claim->length     = UINT64_MAX;
} /* chimera_vfs_claim_init_common */

SYMBOL_EXPORT void
chimera_vfs_claim_foreach_smb_open(
    struct chimera_vfs_file_state *file,
    chimera_vfs_smb_open_cb_t      cb,
    void                          *private_data)
{
    struct chimera_vfs_claim *cur;

    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_ACCESS]; cur; cur = cur->next) {
        if (cur->construct != CHIMERA_CONSTRUCT_SMB_OPEN &&
            cur->construct != CHIMERA_CONSTRUCT_SMB_OPEN_INERT) {
            continue;
        }
        if (!cur->cb_private) {
            continue;
        }
        cb(cur->cb_private, private_data);
    }
    pthread_mutex_unlock(&file->lock);
} /* chimera_vfs_claim_foreach_smb_open */

SYMBOL_EXPORT void
chimera_vfs_claim_init_smb_open(
    struct chimera_vfs_claim         *claim,
    uint8_t                           access,
    uint8_t                           deny,
    const struct chimera_claim_owner *owner)
{
    chimera_vfs_claim_init_common(claim,
                                  (access | deny) ? CHIMERA_CONSTRUCT_SMB_OPEN
                                                  : CHIMERA_CONSTRUCT_SMB_OPEN_INERT,
                                  CHIMERA_CLAIM_CLASS_ACCESS,
                                  access, deny, owner);
} /* chimera_vfs_claim_init_smb_open */

SYMBOL_EXPORT void
chimera_vfs_claim_init_nfs4_open(
    struct chimera_vfs_claim         *claim,
    uint8_t                           access,
    uint8_t                           deny,
    const struct chimera_claim_owner *owner)
{
    chimera_vfs_claim_init_common(claim, CHIMERA_CONSTRUCT_NFS4_OPEN,
                                  CHIMERA_CLAIM_CLASS_ACCESS,
                                  access, deny, owner);
} /* chimera_vfs_claim_init_nfs4_open */

SYMBOL_EXPORT void
chimera_vfs_claim_init_rqls(
    struct chimera_vfs_claim         *claim,
    uint8_t                           used,
    const struct chimera_claim_owner *owner)
{
    chimera_vfs_claim_init_common(claim, CHIMERA_CONSTRUCT_RQLS,
                                  CHIMERA_CLAIM_CLASS_CACHE,
                                  used, 0, owner);
} /* chimera_vfs_claim_init_rqls */

SYMBOL_EXPORT void
chimera_vfs_claim_init_oplock(
    struct chimera_vfs_claim         *claim,
    uint8_t                           used,
    const struct chimera_claim_owner *owner)
{
    enum chimera_claim_construct c;

    if (used & CHIMERA_CLAIM_H) {
        c = CHIMERA_CONSTRUCT_OPLOCK_BATCH;
    } else if (used & CHIMERA_CLAIM_CW) {
        c = CHIMERA_CONSTRUCT_OPLOCK_EX;
    } else {
        c = CHIMERA_CONSTRUCT_OPLOCK_II;
    }
    chimera_vfs_claim_init_common(claim, c, CHIMERA_CLAIM_CLASS_CACHE,
                                  used, 0, owner);
} /* chimera_vfs_claim_init_oplock */

SYMBOL_EXPORT void
chimera_vfs_claim_init_dir_lease(
    struct chimera_vfs_claim         *claim,
    uint8_t                           used,
    const struct chimera_claim_owner *owner)
{
    chimera_vfs_claim_init_common(claim, CHIMERA_CONSTRUCT_DIR_LEASE,
                                  CHIMERA_CLAIM_CLASS_CACHE,
                                  used & (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_H),
                                  0, owner);
} /* chimera_vfs_claim_init_dir_lease */

SYMBOL_EXPORT void
chimera_vfs_claim_init_delegation(
    struct chimera_vfs_claim         *claim,
    bool                              write,
    const struct chimera_claim_owner *owner)
{
    /* Delegations perform real I/O under their stateids, so they carry the
     * data bits too -- which is what makes a deny-read open conflict with a
     * read delegation (R11/R34). */
    uint8_t used = write
        ? (CHIMERA_CLAIM_R | CHIMERA_CLAIM_W |
           CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW)
        : (CHIMERA_CLAIM_R | CHIMERA_CLAIM_CR);

    chimera_vfs_claim_init_common(claim,
                                  write ? CHIMERA_CONSTRUCT_DELEG_W
                                        : CHIMERA_CONSTRUCT_DELEG_R,
                                  CHIMERA_CLAIM_CLASS_CACHE,
                                  used, 0, owner);
} /* chimera_vfs_claim_init_delegation */

SYMBOL_EXPORT void
chimera_vfs_claim_init_fuse_grant(
    struct chimera_vfs_claim         *claim,
    const struct chimera_claim_owner *owner)
{
    /* A FUSE grant is a revocable read-cache claim held on the kernel's
     * behalf (one per mount+file; owner.client_key = mount identity).  It
     * shares DELEG_R's deny rows and awaited-class break semantics: a
     * conflicting writer waits for the break ACK (coherence=sync), the
     * CLIENT circle self-exempts the mount's own opens and locks, and --
     * unlike an NFSv4 delegation -- the sweep may force-revoke it at the
     * break deadline (the liveness backstop if a mount's notifier wedges). */
    chimera_vfs_claim_init_common(claim, CHIMERA_CONSTRUCT_FUSE_GRANT,
                                  CHIMERA_CLAIM_CLASS_CACHE,
                                  CHIMERA_CLAIM_CR, 0, owner);
} /* chimera_vfs_claim_init_fuse_grant */

SYMBOL_EXPORT void
chimera_vfs_claim_init_range(
    struct chimera_vfs_claim         *claim,
    bool                              exclusive,
    bool                              smb,
    uint64_t                          offset,
    uint64_t                          length,
    const struct chimera_claim_owner *owner)
{
    uint8_t used = exclusive
        ? (CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW)
        : CHIMERA_CLAIM_LR;

    chimera_vfs_claim_init_common(claim,
                                  smb ? CHIMERA_CONSTRUCT_LOCK_SMB
                                      : CHIMERA_CONSTRUCT_LOCK_ADVISORY,
                                  CHIMERA_CLAIM_CLASS_RANGE,
                                  used, 0, owner);
    claim->offset = offset;
    claim->length = length;
} /* chimera_vfs_claim_init_range */

SYMBOL_EXPORT void
chimera_vfs_claim_init_deny_probe(
    struct chimera_vfs_claim         *claim,
    uint8_t                           deny,
    const struct chimera_claim_owner *owner)
{
    chimera_vfs_claim_init_common(claim, CHIMERA_CONSTRUCT_DENY_PROBE,
                                  CHIMERA_CLAIM_CLASS_ACCESS,
                                  0, deny, owner);
} /* chimera_vfs_claim_init_deny_probe */

/* -------------------------------------------------------------------- */
/* The admission mask table, part 2 (deny-row derivation)               */
/* -------------------------------------------------------------------- */

int
chimera_vfs_claim_deny_rows(
    const struct chimera_vfs_claim *claim,
    struct chimera_claim_deny_row   rows[CHIMERA_CLAIM_MAX_DENY_ROWS])
{
    uint8_t adv = chimera_vfs_claim_advertised(claim);
    uint8_t raw = (claim->break_state == CHIMERA_CLAIM_BREAK_REVOKED)
        ? 0 : claim->used;
    int     n = 0;

    switch (claim->construct) {
        case CHIMERA_CONSTRUCT_SMB_OPEN:
        case CHIMERA_CONSTRUCT_NFS4_OPEN:
        case CHIMERA_CONSTRUCT_DENY_PROBE:
            /* Share-deny bits, owner-exempt.  Target ACCESS (open-vs-open,
             * the implicit claim) and CACHE so a deny clash reaches a
             * delegation's data-bit use (R11's deny∩granted arm); cache
             * bits never intersect an R|W|D deny mask, so SMB leases are
             * untouched by design. */
            if (claim->denied) {
                rows[n++] = (struct chimera_claim_deny_row) {
                    .mask    = claim->denied,
                    .circle  = CHIMERA_CIRCLE_OWNER,
                    .targets = CHIMERA_CLAIM_TARGET_ACCESS |
                        CHIMERA_CLAIM_TARGET_CACHE,
                    .admit_only = 0,
                    .raw        = 0,
                };
            }
            break;

        case CHIMERA_CONSTRUCT_RQLS:
            /* W-cache exclusivity across cache holders (R28), KEY-exempt
             * (LeaseKey identity is scoped to the protocol and client). */
            if (adv & CHIMERA_CLAIM_CW) {
                rows[n++] = (struct chimera_claim_deny_row) {
                    .mask = CHIMERA_CLAIM_R | CHIMERA_CLAIM_W |
                        CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                    .circle  = CHIMERA_CIRCLE_KEY,
                    .targets = CHIMERA_CLAIM_TARGET_CACHE,
                };
            }
            /* A W/H-caching lease parks a foreign byte-range lock until its
             * break ACKS: evaluated at RAW used so a mid-break holder still
             * blocks the lock (the acquirer waits for the flush; lock1). */
            if (raw & (CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H)) {
                rows[n++] = (struct chimera_claim_deny_row) {
                    .mask    = CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW,
                    .circle  = CHIMERA_CIRCLE_KEY,
                    .targets = CHIMERA_CLAIM_TARGET_RANGE,
                    .raw     = 1,
                };
            }
            break;

        case CHIMERA_CONSTRUCT_OPLOCK_EX:
        case CHIMERA_CONSTRUCT_OPLOCK_BATCH:
            if (adv & CHIMERA_CLAIM_CW) {
                rows[n++] = (struct chimera_claim_deny_row) {
                    .mask = CHIMERA_CLAIM_R | CHIMERA_CLAIM_W |
                        CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                    .circle  = CHIMERA_CIRCLE_OWNER,
                    .targets = CHIMERA_CLAIM_TARGET_CACHE,
                };
            }
            if (raw & (CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H)) {
                rows[n++] = (struct chimera_claim_deny_row) {
                    .mask    = CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW,
                    .circle  = CHIMERA_CIRCLE_OWNER,
                    .targets = CHIMERA_CLAIM_TARGET_RANGE,
                    .raw     = 1,
                };
            }
            /* Legacy H is sole-across-clients (R28: the H conflict fires
             * exactly when a side is legacy — RqLs claims deny no H). */
            if (claim->construct == CHIMERA_CONSTRUCT_OPLOCK_BATCH &&
                (adv & CHIMERA_CLAIM_H)) {
                rows[n++] = (struct chimera_claim_deny_row) {
                    .mask    = CHIMERA_CLAIM_H,
                    .circle  = CHIMERA_CIRCLE_CLIENT,
                    .targets = CHIMERA_CLAIM_TARGET_CACHE,
                };
            }
            break;

        case CHIMERA_CONSTRUCT_FUSE_GRANT:
            /* DELEG_R's rows -- deny W|CW and LW outside the holder's
             * CLIENT circle, so writers and exclusive lockers park until
             * the kernel invalidation acks (coherence=sync) -- but gated
             * on the EFFECTIVE mode: unlike a delegation (released right
             * after its recall acks), an invalidated grant stays linked at
             * settled-0 for rearm-on-demand and must deny nothing until a
             * fresh acquire re-arms it.  ADVERTISE_NEVER keeps adv nonzero
             * for the whole break, which is what makes the writer wait. */
            if (!(adv & CHIMERA_CLAIM_CR)) {
                break;
            }
            rows[n++] = (struct chimera_claim_deny_row) {
                .mask    = CHIMERA_CLAIM_W | CHIMERA_CLAIM_CW,
                .circle  = CHIMERA_CIRCLE_CLIENT,
                .targets = CHIMERA_CLAIM_TARGET_ACCESS |
                    CHIMERA_CLAIM_TARGET_CACHE,
            };
            rows[n++] = (struct chimera_claim_deny_row) {
                .mask    = CHIMERA_CLAIM_LW,
                .circle  = CHIMERA_CIRCLE_CLIENT,
                .targets = CHIMERA_CLAIM_TARGET_RANGE,
            };
            break;

        case CHIMERA_CONSTRUCT_DELEG_R:
            rows[n++] = (struct chimera_claim_deny_row) {
                .mask    = CHIMERA_CLAIM_W | CHIMERA_CLAIM_CW,
                .circle  = CHIMERA_CIRCLE_CLIENT,
                .targets = CHIMERA_CLAIM_TARGET_ACCESS |
                    CHIMERA_CLAIM_TARGET_CACHE,
            };
            rows[n++] = (struct chimera_claim_deny_row) {
                .mask    = CHIMERA_CLAIM_LW,
                .circle  = CHIMERA_CIRCLE_CLIENT,
                .targets = CHIMERA_CLAIM_TARGET_RANGE,
            };
            break;

        case CHIMERA_CONSTRUCT_DELEG_W:
            rows[n++] = (struct chimera_claim_deny_row) {
                .mask = CHIMERA_CLAIM_R | CHIMERA_CLAIM_W |
                    CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                .circle  = CHIMERA_CIRCLE_CLIENT,
                .targets = CHIMERA_CLAIM_TARGET_ACCESS |
                    CHIMERA_CLAIM_TARGET_CACHE,
            };
            rows[n++] = (struct chimera_claim_deny_row) {
                .mask    = CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW,
                .circle  = CHIMERA_CIRCLE_CLIENT,
                .targets = CHIMERA_CLAIM_TARGET_RANGE,
            };
            break;

        case CHIMERA_CONSTRUCT_LOCK_ADVISORY:
        case CHIMERA_CONSTRUCT_LOCK_SMB:
        {
            uint8_t lock_deny = (claim->used & CHIMERA_CLAIM_LW)
                ? (CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW)
                : CHIMERA_CLAIM_LW;
            uint8_t cache_circle;

            rows[n++] = (struct chimera_claim_deny_row) {
                .mask    = lock_deny,
                .circle  = CHIMERA_CIRCLE_OWNER,
                .targets = CHIMERA_CLAIM_TARGET_RANGE,
            };
            /* Fresh-cache denial (R53a), ADMIT-ONLY: a lock refuses new
             * caches without displacing standing ones (displacement is the
             * RANGE_LOCK trigger's job -- the temporal asymmetry).  Circle:
             * NFSv4 exempts its whole client (R58); SMB exempts the same
             * open or the same grant LeaseKey (brl2); NLM/POSIX the owner. */
            if (claim->construct == CHIMERA_CONSTRUCT_LOCK_SMB) {
                cache_circle = CHIMERA_CIRCLE_OWNER_OR_KEY;
            } else if (claim->owner.proto == CHIMERA_CLAIM_PROTO_NFSV4) {
                cache_circle = CHIMERA_CIRCLE_CLIENT;
            } else {
                cache_circle = CHIMERA_CIRCLE_OWNER;
            }
            rows[n++] = (struct chimera_claim_deny_row) {
                .mask       = CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW,
                .circle     = cache_circle,
                .targets    = CHIMERA_CLAIM_TARGET_CACHE,
                .admit_only = 1,
            };
            break;
        }

        case CHIMERA_CONSTRUCT_OPLOCK_II:
        case CHIMERA_CONSTRUCT_DIR_LEASE:
        case CHIMERA_CONSTRUCT_IMPLICIT:
        case CHIMERA_CONSTRUCT_SMB_OPEN_INERT:
        default:
            break;
    } /* switch */

    return n;
} /* chimera_vfs_claim_deny_rows */

/* -------------------------------------------------------------------- */
/* Admission predicate (Table 1)                                        */
/* -------------------------------------------------------------------- */

/* Effective deny mask claim `hc` presents against counterparty `pc`.
 * `for_displace` excludes admit-only rows (a probe's fresh-cache denial
 * never displaces standing claims). */
static uint8_t
chimera_vfs_claim_deny_against(
    const struct chimera_vfs_claim *hc,
    const struct chimera_vfs_claim *pc,
    bool                            for_displace)
{
    struct chimera_claim_deny_row rows[CHIMERA_CLAIM_MAX_DENY_ROWS];
    int                           n    = chimera_vfs_claim_deny_rows(hc, rows);
    uint8_t                       mask = 0;
    int                           i;

    for (i = 0; i < n; i++) {
        if (for_displace && rows[i].admit_only) {
            continue;
        }
        if (!(rows[i].targets & (1u << pc->klass))) {
            continue;
        }
        if (chimera_vfs_claim_circle_exempt(rows[i].circle, hc, pc)) {
            continue;
        }
        mask |= rows[i].mask;
    }
    return mask;
} /* chimera_vfs_claim_deny_against */

/* Does standing claim `hc` conflict with probe `pc`?  Symmetric two-term
 * evaluation with the delegation displacement immunity (a same-client probe
 * never displaces the client's own delegation, whatever open-owner it
 * arrives under — RFC 8881 §10.2). */
bool
chimera_vfs_claim_conflicts(
    const struct chimera_vfs_claim *hc,
    const struct chimera_vfs_claim *pc)
{
    uint8_t blocked, displace;
    uint8_t hc_adv;

    if (hc == pc) {
        return false;
    }
    for (uint32_t i = 0; i < pc->admit_num_excluded; i++) {
        if (pc->admit_excluded[i] == hc) {
            return false;
        }
    }
    if (!chimera_vfs_claim_claims_overlap(hc, pc)) {
        return false;
    }

    /* blocked: probe uses a bit the holder denies.  Rows evaluated at the
     * holder's advertised mode except raw-scored lock rows (already encoded
     * in the row derivation: rows exist iff adv/raw retains the bits). */
    blocked = pc->used & chimera_vfs_claim_deny_against(hc, pc, false);

    /* displace: the holder's advertised use hits the probe's deny rows. */
    hc_adv   = chimera_vfs_claim_advertised(hc);
    displace = hc_adv & chimera_vfs_claim_deny_against(pc, hc, true);

    if (displace &&
        (hc->construct == CHIMERA_CONSTRUCT_DELEG_R ||
         hc->construct == CHIMERA_CONSTRUCT_DELEG_W) &&
        chimera_claim_owner_same_client(&hc->owner, &pc->owner)) {
        displace = 0;
    }

    return blocked != 0 || displace != 0;
} /* chimera_vfs_claim_conflicts */

/* Batch escape (R8): a hard ACCESS conflict whose blocking open holds a
 * revocable H cache may park on that cache's break instead of denying (the
 * holder may close and dissolve the conflict).  Never escapes onto the
 * PROBE's own coalition (the self-park hang guard).  Also covers the
 * same-client directory-lease arm.  Caller holds file->lock. */
static struct chimera_vfs_claim *
chimera_vfs_claim_batch_escape(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *blocker,
    const struct chimera_vfs_claim *probe)
{
    struct chimera_vfs_claim *cand = NULL;
    struct chimera_vfs_claim *cur;

    if (blocker->klass != CHIMERA_CLAIM_CLASS_ACCESS) {
        return NULL;
    }

    if (blocker->own_cache) {
        cand = &blocker->own_cache->claim;
        if ((cand->used & CHIMERA_CLAIM_H) &&
            chimera_vfs_claim_revocable(cand) &&
            (cand->break_state == CHIMERA_CLAIM_BREAK_IDLE ||
             cand->break_state == CHIMERA_CLAIM_BREAK_BREAKING) &&
            !chimera_claim_owner_same_key(&cand->owner, &probe->owner)) {
            return cand;
        }
    }

    /* Directory-lease arm: any same-client dir lease of the blocking open
     * may relinquish its handle to free the conflict. */
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        if (cur->construct != CHIMERA_CONSTRUCT_DIR_LEASE) {
            continue;
        }
        if (!chimera_claim_owner_same_client(&cur->owner, &blocker->owner)) {
            continue;
        }
        if (!(cur->used & CHIMERA_CLAIM_H) || !chimera_vfs_claim_revocable(cur)) {
            continue;
        }
        if (chimera_claim_owner_same_key(&cur->owner, &probe->owner)) {
            continue;
        }
        if (cur->break_state == CHIMERA_CLAIM_BREAK_IDLE ||
            cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING) {
            return cur;
        }
    }

    return NULL;
} /* chimera_vfs_claim_batch_escape */

/* MS-FSA 2.1.5.18 requires a sole Open for legacy exclusive/batch
 * oplocks, including peers from the same client and metadata-only Opens.
 * For a new granular WRITE cache, 2.1.5.18.1 applies this count in the
 * NO_OPLOCK case; existing granular leases use key-based upgrade rules.
 * The requesting Open itself is exempt. Disconnected courtesy-held Opens
 * retain the existing revocation policy. Caller holds file->lock. */
static struct chimera_vfs_claim *
chimera_vfs_claim_sole_opener_blocker_locked(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *probe,
    const struct chimera_vfs_claim *provisional_cache)
{
    struct chimera_vfs_claim *cur;
    bool                      probe_is_rqls;
    bool                      have_granular_cache = false;
    uint8_t                   sole_mask;

    if (probe->klass != CHIMERA_CLAIM_CLASS_CACHE ||
        probe->owner.proto != CHIMERA_CLAIM_PROTO_SMB2) {
        return NULL;
    }

    switch (probe->construct) {
        case CHIMERA_CONSTRUCT_RQLS:
        case CHIMERA_CONSTRUCT_DIR_LEASE:
            probe_is_rqls = true;
            break;
        case CHIMERA_CONSTRUCT_OPLOCK_II:
        case CHIMERA_CONSTRUCT_OPLOCK_EX:
        case CHIMERA_CONSTRUCT_OPLOCK_BATCH:
            probe_is_rqls = false;
            break;
        default:
            return NULL;
    } /* switch */

    sole_mask = CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H;

    if (!(probe->used & sole_mask)) {
        return NULL;
    }

    if (probe_is_rqls) {
        for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
            if (cur == provisional_cache) {
                continue;
            }
            if ((cur->construct == CHIMERA_CONSTRUCT_RQLS ||
                 cur->construct == CHIMERA_CONSTRUCT_DIR_LEASE) &&
                (chimera_vfs_claim_advertised(cur) &
                 (CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_H))) {
                /* Admission uses the retained break target. A peer already
                 * breaking to NONE still has an Open, but no granular cache
                 * that exempts a fresh WRITE cache from sole-opener rules. */
                have_granular_cache = true;
                break;
            }
        }
    }
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_ACCESS]; cur; cur = cur->next) {
        /* Only this Open is exempt for a legacy oplock. Client identity and
         * metadata-only access do not remove another Open from OpenList. */
        if ((!probe_is_rqls && chimera_claim_owner_equal(&cur->owner, &probe->owner)) ||
            (probe_is_rqls && chimera_claim_owner_same_client(&cur->owner, &probe->owner) &&
             chimera_claim_owner_same_key(&cur->owner, &probe->owner))) {
            continue;
        }
        /* A parked (disconnected durable) holder is courtesy-held: it
         * does not cap a new opener's lease.  If it genuinely conflicts
         * (a write cache) the cache-vs-cache rows already forced a
         * break/deny; otherwise the new open coexists with it. */
        if (cur->parked) {
            continue;
        }
        if (!probe_is_rqls ||
            ((probe->used & CHIMERA_CLAIM_CW) && !have_granular_cache)) {
            return cur;
        }
        /* Existing granular leases use the separate key-based upgrade rules.
         * Retain the H-only interop cap against active, unkeyed data opens. */
        if (!(probe->used & CHIMERA_CLAIM_H) || (!cur->used && !cur->denied)) {
            continue;
        }
        /* An RqLs probe (H only here) coexists with any LEASE-backed
         * open's handle caching -- including its own open and a peer
         * lease.  A non-lease open caps it to CR. */
        if (probe_is_rqls && chimera_claim_owner_has_key(&cur->owner)) {
            continue;
        }
        return cur;
    }

    return NULL;
} /* chimera_vfs_claim_sole_opener_blocker_locked */

enum chimera_vfs_claim_result
chimera_vfs_claim_admit_locked(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *probe,
    struct chimera_vfs_claim      **conflict_claim)
{
    struct chimera_vfs_claim *cur;
    struct chimera_vfs_claim *idle_break    = NULL;
    struct chimera_vfs_claim *expired_break = NULL;
    struct chimera_vfs_claim *waiting_on    = NULL;
    int k;

    if (conflict_claim) {
        *conflict_claim = NULL;
    }

    /* Inert probes are granted with no side effects (R6). */
    if (probe->used == 0 && probe->denied == 0 &&
        probe->construct != CHIMERA_CONSTRUCT_DENY_PROBE) {
        return CHIMERA_CLAIM_GRANTED;
    }

    for (k = 0; k < CHIMERA_CLAIM_CLASS_COUNT; k++) {
        for (cur = file->claims[k]; cur; cur = cur->next) {
            if (!chimera_vfs_claim_conflicts(cur, probe)) {
                continue;
            }

            if (chimera_vfs_claim_revocable(cur)) {
                if (cur->break_state == CHIMERA_CLAIM_BREAK_IDLE ||
                    cur->break_state == CHIMERA_CLAIM_BREAK_ACKED) {
                    if (!idle_break) {
                        idle_break = cur;
                    }
                } else if (cur->break_state == CHIMERA_CLAIM_BREAK_BREAKING) {
                    if (chimera_vfs_claim_deadline_passed(cur)) {
                        if (!expired_break) {
                            expired_break = cur;
                        }
                    } else if (!waiting_on) {
                        waiting_on = cur;
                    }
                }
                /* REVOKED contributes nothing and never conflicts. */
                continue;
            }

            /* Hard (binding) conflict.  ACCESS conflicts may escape onto
             * the blocker's revocable H cache (R8). */
            {
                struct chimera_vfs_claim *escape =
                    chimera_vfs_claim_batch_escape(file, cur, probe);

                if (escape) {
                    if (escape->break_state == CHIMERA_CLAIM_BREAK_IDLE) {
                        if (!idle_break) {
                            idle_break = escape;
                        }
                    } else if (chimera_vfs_claim_deadline_passed(escape)) {
                        if (!expired_break) {
                            expired_break = escape;
                        }
                    } else if (!waiting_on) {
                        waiting_on = escape;
                    }
                    continue;
                }
            }

            if (conflict_claim) {
                *conflict_claim = cur;
            }
            return CHIMERA_CLAIM_DENIED;
        }
    }

    /* The sole-opener denial takes precedence over a breakable cache
     * conflict, exactly as the old arm ran after (and overrode) the
     * caching loop's has_breakable_conflict: a second client's mere OPEN
     * hard-caps the exclusive/batch (or lease-H) grant -- the requester
     * must step its mode down, never wait the opener out. */
    {
        struct chimera_vfs_claim *blocker =
            chimera_vfs_claim_sole_opener_blocker_locked(file, probe, NULL);

        if (blocker) {
            if (conflict_claim) {
                *conflict_claim = blocker;
            }
            return CHIMERA_CLAIM_DENIED;
        }
    }

    if (idle_break || expired_break || waiting_on) {
        if (conflict_claim) {
            *conflict_claim = idle_break ? idle_break
                : (expired_break ? expired_break : waiting_on);
        }
        return CHIMERA_CLAIM_BREAKING;
    }

    return CHIMERA_CLAIM_GRANTED;
} /* chimera_vfs_claim_admit_locked */

void
chimera_vfs_claim_conflict_fill(
    const struct chimera_vfs_claim    *claim,
    struct chimera_vfs_claim_conflict *out)
{
    memset(out, 0, sizeof(*out));
    out->owner     = claim->owner;
    out->construct = claim->construct;
    out->used      = claim->used;
    out->breaking  = (claim->break_state == CHIMERA_CLAIM_BREAK_BREAKING);
    out->revocable = chimera_vfs_claim_revocable(claim);
    if (claim->klass == CHIMERA_CLAIM_CLASS_RANGE) {
        out->offset = claim->offset;
        out->length = claim->length;
    } else {
        out->offset = 0;
        out->length = UINT64_MAX;
    }
    out->policy_tag = claim->policy_tag;
} /* chimera_vfs_claim_conflict_fill */

/* -------------------------------------------------------------------- */
/* Contended floor (the R36 map over split bits, with overrides)        */
/* -------------------------------------------------------------------- */

uint8_t
chimera_vfs_claim_contended_floor(
    const struct chimera_vfs_claim *probe,
    const struct chimera_vfs_claim *holder)
{
    uint8_t a    = probe->used;
    uint8_t keep = holder->used;

    /* Delegation victims always floor at 0: CB_RECALL is all-or-nothing,
     * and any partial floor livelocks the acquirer (verified against the
     * shipped break_retain_for, which computes 0 for every deleg case). */
    if (holder->construct == CHIMERA_CONSTRUCT_DELEG_R ||
        holder->construct == CHIMERA_CONSTRUCT_DELEG_W) {
        return 0;
    }

    /* The implicit INTERNAL claim drains whole (its break is a drain, not a
     * downgrade); a partial floor would leave step == used, begin_break
     * would no-op, and the acquirer would spin on an IDLE conflict. */
    if (holder->construct == CHIMERA_CONSTRUCT_IMPLICIT) {
        return 0;
    }

    /* A byte-range lock is incompatible with any caching on the stream
     * (MS-FSA 2.1.5.18): floor 0. */
    if (probe->klass == CHIMERA_CLAIM_CLASS_RANGE &&
        holder->klass == CHIMERA_CLAIM_CLASS_CACHE) {
        return 0;
    }

    /* An ACCESS conflict resolved through the batch escape / a dir-lease
     * handle conflict strips H and ONLY H (R9): the open never gains access
     * unless the holder closes, so its cached data stays valid. */
    if (probe->klass == CHIMERA_CLAIM_CLASS_ACCESS &&
        holder->klass == CHIMERA_CLAIM_CLASS_CACHE &&
        (holder->construct == CHIMERA_CONSTRUCT_DIR_LEASE ||
         (holder->construct == CHIMERA_CONSTRUCT_RQLS &&
          (holder->used & CHIMERA_CLAIM_H)))) {
        return holder->used & (uint8_t) ~CHIMERA_CLAIM_H;
    }

    if (a & (CHIMERA_CLAIM_W | CHIMERA_CLAIM_CW)) {
        keep &= (uint8_t) ~(CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW);
    }
    if (a & (CHIMERA_CLAIM_R | CHIMERA_CLAIM_CR)) {
        keep &= (uint8_t) ~CHIMERA_CLAIM_CW;
    }
    /* H is sole-across-clients only for a legacy-oplock acquirer; an RqLs
     * acquirer leaves a peer's H intact (derives special-case #21). */
    if ((a & CHIMERA_CLAIM_H) &&
        (probe->construct == CHIMERA_CONSTRUCT_OPLOCK_BATCH) &&
        probe->owner.client_key != holder->owner.client_key) {
        keep &= (uint8_t) ~CHIMERA_CLAIM_H;
    }
    return keep;
} /* chimera_vfs_claim_contended_floor */

/* -------------------------------------------------------------------- */
/* List linkage                                                         */
/* -------------------------------------------------------------------- */

void
chimera_vfs_claim_link_locked(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim)
{
    struct chimera_vfs_claim **head = &file->claims[claim->klass];

    claim->file = file;
    claim->prev = NULL;
    claim->next = *head;
    if (*head) {
        (*head)->prev = claim;
    }
    *head = claim;
} /* chimera_vfs_claim_link_locked */

void
chimera_vfs_claim_unlink_locked(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim)
{
    struct chimera_vfs_claim **head = &file->claims[claim->klass];

    if (claim->prev) {
        claim->prev->next = claim->next;
    } else if (*head == claim) {
        *head = claim->next;
    }
    if (claim->next) {
        claim->next->prev = claim->prev;
    }
    claim->prev = NULL;
    claim->next = NULL;
    claim->file = NULL;
} /* chimera_vfs_claim_unlink_locked */

/* -------------------------------------------------------------------- */
/* try_acquire (the single entrance)                                    */
/* -------------------------------------------------------------------- */

static bool
chimera_vfs_claim_insertion_fenced_locked(const struct chimera_vfs_file_state *file,
                                        const struct chimera_vfs_claim *claim)
{
    return claim->klass == CHIMERA_CLAIM_CLASS_ACCESS && file->access_fence_cookie &&
           claim->admission_cookie != file->access_fence_cookie;
}

static struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_coalesce_locked(
    struct chimera_vfs_file_state *file, const struct chimera_claim_owner *owner,
    uint8_t want, int upgrade_ok, void *member, void *member_next);

static enum chimera_vfs_claim_result
chimera_vfs_claim_try_acquire_internal(
    struct chimera_vfs_state *state, struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim *claim, struct chimera_vfs_claim_conflict *conflict_out,
    struct chimera_vfs_claim_grant *candidate, int upgrade_ok,
    void *member_next, struct chimera_vfs_claim_grant **grant_out)
{
    enum chimera_vfs_claim_result result;
    struct chimera_vfs_claim     *conflict;

    if (conflict_out) {
        memset(conflict_out, 0, sizeof(*conflict_out));
    }

    /* Outer loop: re-probe after courtesy-reclaiming dead holders. */
    for ( ; ; ) {
        bool began_live_break = false;
        bool revoked_any      = false;

        conflict = NULL;

        pthread_mutex_lock(&file->lock);
        if (candidate) {
            struct chimera_vfs_claim_grant *existing = chimera_vfs_claim_grant_coalesce_locked(
                file, &claim->owner, claim->used, upgrade_ok,
                member_next ? candidate->members : NULL, member_next);
            if (existing) {
                *grant_out = existing;
                pthread_mutex_unlock(&file->lock);
                return CHIMERA_CLAIM_GRANTED;
            }
        }
        /* Insertion control, including inert ACCESS registrations. Unlike a
         * share conflict this must not recall/evict a holder or park a fresh
         * request while it holds other claims. Pure test APIs stay unchanged. */
        if (chimera_vfs_claim_insertion_fenced_locked(file, claim)) {
            if (conflict_out) conflict_out->admission_fenced = true;
            pthread_mutex_unlock(&file->lock);
            return CHIMERA_CLAIM_DENIED;
        }
        result = chimera_vfs_claim_admit_locked(file, claim, &conflict);

        if (result == CHIMERA_CLAIM_GRANTED) {
            chimera_vfs_claim_link_locked(file, claim);
            if (candidate) {
                candidate->grant_next = file->grants;
                file->grants = candidate;
                *grant_out = candidate;
            }
            pthread_mutex_unlock(&file->lock);
            /* A byte-range lock displaces pure-read caches through the
             * RANGE_LOCK trigger (2.1.5.18); W/H caches were handled at
             * admission.  Fired only once lock-vs-lock admission passed. */
            if (claim->klass == CHIMERA_CLAIM_CLASS_RANGE) {
                struct chimera_claim_actor actor = {
                    .owner     = claim->owner,
                    .op_handle = claim->op_handle,
                };
                chimera_vfs_claim_trigger_fire(
                    state, file, CHIMERA_TRIGGER_RANGE_LOCK, &actor,
                    (claim->used & CHIMERA_CLAIM_LW) ? 1 : 0);
            }
            /* Single entrance: every admission funnels the backend cover
             * re-evaluation (cheap no-op absent a CAP_LEASE module). */
            chimera_vfs_claim_backend_reeval(state, file);
            return CHIMERA_CLAIM_GRANTED;
        }

        /* Fill the by-value conflict record under the lock -- no caller
         * pin/unref protocol survives the by-value migration. */
        if (conflict && conflict_out) {
            chimera_vfs_claim_conflict_fill(conflict, conflict_out);
        }

        if (result == CHIMERA_CLAIM_DENIED) {
            chimera_vfs_claim_revoked_cb_t revoked_cb = NULL;
            void *cb_private                          = NULL;
            bool reclaimed                            = false;

            /* Reclaim a courtesy holder while the lock is still held.
             * `conflict` is borrowed, not owned: ACCESS and RANGE claims
             * carry no refcount (their grant is NULL), so this lock is the
             * only thing keeping the holder alive.  Unlocking first and
             * revoking afterwards races the holder's owner tearing it down --
             * the NFSv4 lease sweeper frees a lock-owner's range leases
             * outright in lock_state_cleanup -- and the revoke then reads
             * freed memory. */
            if (conflict &&
                chimera_vfs_claim_holder_reclaimable(conflict, claim)) {
                chimera_vfs_claim_revoke_locked(file, conflict,
                                                &revoked_cb, &cb_private);
                reclaimed = true;
            }
            pthread_mutex_unlock(&file->lock);

            if (revoked_cb) {
                /* Past the unlock the claim may already be freed, so it is
                 * not passed on; every revoked callback works from its
                 * cb_private (see chimera_vfs_claim_revoke_locked). */
                revoked_cb(NULL, cb_private);
            }

            if (reclaimed) {
                /* The pumps chimera_vfs_claim_revoke() would have run. */
                if (file->state) {
                    chimera_vfs_claim_pump_pending(file->state, file);
                    chimera_vfs_claim_pump_io(file->state, file);
                    chimera_vfs_claim_backend_reeval(file->state, file);
                }
                continue;
            }
            return CHIMERA_CLAIM_DENIED;
        }
        pthread_mutex_unlock(&file->lock);

        /* BREAKING: kick a break on EVERY breakable conflicting holder
         * (R50), revoking dead/expired ones, then re-probe until no IDLE
         * conflict remains. */
        while (conflict) {
            struct chimera_vfs_claim_grant *iter_pin = NULL;
            chimera_vfs_claim_revoked_cb_t revoked_cb = NULL;
            void *cb_private                          = NULL;
            bool revoked_now                          = false;
            bool do_break                             = false;
            bool do_wait                              = false;
            uint32_t deadline_ms                      = 0;
            uint8_t floor                             = 0;

            /* Classify (and, for a revoke, mutate) with the lock held.
             * `conflict` is borrowed: a courtesy holder may be a grant-less
             * claim -- a byte-range lease -- for which pin_grant returns
             * NULL, and its owner, the NFSv4 lease sweeper, frees it
             * outright in lock_state_cleanup.  Deciding after unlocking
             * races that teardown and reads freed memory.
             *
             * The arm order is load-bearing and matches the original: a
             * never-broken holder sits at break_deadline 0, so the
             * deadline test must stay *after* the IDLE/ACKED arm or every
             * idle holder is revoked instead of broken.
             *
             * begin_break still runs unlocked below.  That is safe for a
             * grant-bearing holder, which iter_pin keeps alive; an NFSv4
             * delegation carries a break_cb with no grant, and closing that
             * window needs a refcount for grant-less claims rather than a
             * lock discipline.  Left alone deliberately: no failure has been
             * observed through it, and a break callback cannot be deferred
             * the way a revoke's can. */
            pthread_mutex_lock(&file->lock);
            iter_pin = chimera_vfs_claim_pin_grant(conflict);

            if (chimera_vfs_claim_holder_reclaimable(conflict, claim)) {
                chimera_vfs_claim_revoke_locked(file, conflict,
                                                &revoked_cb, &cb_private);
                revoked_any = true;
                revoked_now = true;
            } else if (conflict->break_state == CHIMERA_CLAIM_BREAK_IDLE ||
                       conflict->break_state == CHIMERA_CLAIM_BREAK_ACKED) {
                deadline_ms =
                    (conflict->owner.proto == CHIMERA_CLAIM_PROTO_NFSV4)
                    ? CHIMERA_VFS_NFS_DELEG_RECALL_MS : 0;
                floor    = chimera_vfs_claim_contended_floor(claim, conflict);
                do_break = true;
            } else if (chimera_vfs_claim_deadline_passed(conflict)) {
                chimera_vfs_claim_revoke_locked(file, conflict,
                                                &revoked_cb, &cb_private);
                revoked_any = true;
                revoked_now = true;
            } else {
                do_wait = true;
            }
            pthread_mutex_unlock(&file->lock);

            if (revoked_now) {
                if (revoked_cb) {
                    /* The claim may already be freed; see
                     * chimera_vfs_claim_revoke_locked. */
                    revoked_cb(NULL, cb_private);
                }
                if (file->state) {
                    chimera_vfs_claim_pump_pending(file->state, file);
                    chimera_vfs_claim_pump_io(file->state, file);
                    chimera_vfs_claim_backend_reeval(file->state, file);
                }
            } else if (do_break) {
                chimera_vfs_claim_begin_break_ex(
                    state, conflict, floor, deadline_ms,
                    claim->klass == CHIMERA_CLAIM_CLASS_RANGE /* one_shot */);
                began_live_break = true;
            } else if (do_wait) {
                /* Mid-break, deadline pending: the caller waits. */
                if (iter_pin) {
                    chimera_vfs_claim_grant_release(state, iter_pin, false);
                }
                began_live_break = true;
                break;
            }

            if (iter_pin) {
                chimera_vfs_claim_grant_release(state, iter_pin, false);
            }

            conflict = NULL;
            pthread_mutex_lock(&file->lock);
            if (chimera_vfs_claim_admit_locked(file, claim, &conflict) !=
                CHIMERA_CLAIM_BREAKING) {
                conflict = NULL;
            }
            pthread_mutex_unlock(&file->lock);
        }

        if (began_live_break) {
            /* RANGE-only synchronous re-probe (R51): a no-ack read-cache
             * break settles inside begin_break, so a FAIL_IMMEDIATELY lock
             * that broke its own handle's LEVEL_II grants instead of
             * parking (brl1/brl3).  Caching acquirers never shortcut. */
            if (claim->klass == CHIMERA_CLAIM_CLASS_RANGE) {
                pthread_mutex_lock(&file->lock);
                result = chimera_vfs_claim_admit_locked(file, claim, &conflict);
                pthread_mutex_unlock(&file->lock);
                if (result == CHIMERA_CLAIM_GRANTED) {
                    continue;
                }
            }
            return CHIMERA_CLAIM_BREAKING;
        }

        if (revoked_any) {
            continue;
        }

        return CHIMERA_CLAIM_BREAKING;
    }
} /* chimera_vfs_claim_try_acquire */

SYMBOL_EXPORT enum chimera_vfs_claim_result
chimera_vfs_claim_try_acquire(
    struct chimera_vfs_state *state, struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim *claim, struct chimera_vfs_claim_conflict *conflict_out)
{
    return chimera_vfs_claim_try_acquire_internal(state, file, claim, conflict_out,
        NULL, 0, NULL, NULL);
}


/* -------------------------------------------------------------------- */
/* Pending queue + ticketed acquire                                     */
/* -------------------------------------------------------------------- */

static inline void
chimera_vfs_pending_enqueue_locked(
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_pending_acquire *ticket)
{
    ticket->file   = file;
    ticket->queued = true;
    ticket->next   = NULL;
    ticket->prev   = file->pending_tail;

    if (file->pending_tail) {
        file->pending_tail->next = ticket;
    } else {
        file->pending_head = ticket;
    }
    file->pending_tail = ticket;
} /* chimera_vfs_pending_enqueue_locked */

static inline void
chimera_vfs_pending_dequeue_locked(
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_pending_acquire *ticket)
{
    if (ticket->prev) {
        ticket->prev->next = ticket->next;
    } else if (file->pending_head == ticket) {
        file->pending_head = ticket->next;
    }

    if (ticket->next) {
        ticket->next->prev = ticket->prev;
    } else if (file->pending_tail == ticket) {
        file->pending_tail = ticket->prev;
    }

    ticket->prev   = NULL;
    ticket->next   = NULL;
    ticket->queued = false;
} /* chimera_vfs_pending_dequeue_locked */

void
chimera_vfs_claim_pump_pending(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_pending_acquire *head, *t, *next;
    enum chimera_vfs_claim_result       result;
    struct chimera_vfs_claim_conflict   conflict;

    pthread_mutex_lock(&file->lock);
    head               = file->pending_head;
    file->pending_head = NULL;
    file->pending_tail = NULL;
    for (t = head; t; t = t->next) {
        t->queued = false;
    }
    pthread_mutex_unlock(&file->lock);

    for (t = head; t; t = next) {
        next    = t->next;
        t->prev = NULL;
        t->next = NULL;

retry_admission:
        result = chimera_vfs_claim_try_acquire(state, file, t->claim, &conflict);
        if (conflict.admission_fenced) {
            /* Fence release and requeue share this mutex. If release won,
             * retry here rather than losing its only wakeup. */
            pthread_mutex_lock(&file->lock);
            bool fenced = chimera_vfs_claim_insertion_fenced_locked(file, t->claim);
            if (fenced) chimera_vfs_pending_enqueue_locked(file, t);
            pthread_mutex_unlock(&file->lock);
            if (!fenced) goto retry_admission;
            continue;
        }

        if (result == CHIMERA_CLAIM_BREAKING) {
            pthread_mutex_lock(&file->lock);
            chimera_vfs_pending_enqueue_locked(file, t);
            pthread_mutex_unlock(&file->lock);
            continue;
        }

        /* A waiting lock ticket still hard-DENIED stays parked (SMB2
         * blocking lock / NLM block: never bounce DENIED to a waiter). */
        if (result == CHIMERA_CLAIM_DENIED && t->wait_hard) {
            pthread_mutex_lock(&file->lock);
            chimera_vfs_pending_enqueue_locked(file, t);
            pthread_mutex_unlock(&file->lock);
            continue;
        }

        /* A pump-granted projectable lock is confirmed by the service
         * thread (the pump runs on whatever thread released the blocker
         * and has no dispatch context of its own); the callback fires from
         * there after confirmation.
         *
         * The test is the RANGE capability and the RANGE per-file flag --
         * not the aggregate ones, which say nothing about whether this
         * backend arbitrates byte ranges.  The per-file flag may still be
         * unresolved here (resolving it needs a thread to identify the
         * module), in which case the service thread discovers the answer
         * and a backend that does not arbitrate ranges reports ENOTSUP,
         * which chimera_vfs_bl_range_complete turns back into the local
         * grant rather than a denial. */
        if (result == CHIMERA_CLAIM_GRANTED &&
            t->claim->klass == CHIMERA_CLAIM_CLASS_RANGE &&
            !t->claim->local_only &&
            !file->bl_range_disabled &&
            t->claim->owner.proto == CHIMERA_CLAIM_PROTO_POSIX &&
            chimera_vfs_claim_backend_range_capable(state)) {
            chimera_vfs_claim_backend_defer_ticket(state, t);
            continue;
        }

        t->cb(result,
              result == CHIMERA_CLAIM_GRANTED ? t->claim : NULL,
              result == CHIMERA_CLAIM_GRANTED ? NULL : &conflict,
              t->private_data);
    }
} /* chimera_vfs_claim_pump_pending */

SYMBOL_EXPORT void
chimera_vfs_claim_acquire(
    struct chimera_vfs_thread          *thread,
    struct chimera_vfs_state           *state,
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_claim           *claim,
    struct chimera_vfs_pending_acquire *ticket,
    bool                                wait,
    bool                                wait_hard,
    chimera_vfs_claim_acquire_cb_t      cb,
    chimera_vfs_claim_blocked_cb_t      blocked_cb,
    void                               *private_data)
{
    enum chimera_vfs_claim_result     result;
    struct chimera_vfs_claim_conflict conflict;

    ticket->claim        = claim;
    ticket->cb           = cb;
    ticket->private_data = private_data;
    ticket->file         = file;
    ticket->queued       = false;
    ticket->wait         = wait;
    ticket->wait_hard    = wait_hard;
    ticket->prev         = NULL;
    ticket->next         = NULL;

    result = chimera_vfs_claim_try_acquire(state, file, claim, &conflict);

    if (!conflict.admission_fenced &&
        ((result == CHIMERA_CLAIM_BREAKING && wait) ||
         (result == CHIMERA_CLAIM_DENIED && wait_hard))) {
        pthread_mutex_lock(&file->lock);
        chimera_vfs_pending_enqueue_locked(file, ticket);
        pthread_mutex_unlock(&file->lock);
        /* Fired after the enqueue but before return, so the deferred
         * result callback can never overtake it (NLM4_BLOCKED). */
        if (blocked_cb) {
            blocked_cb(private_data);
        }
        return;
    }

    /* A locally-granted byte-range lock on a CAP_LEASE file is confirmed
     * with the backend BEFORE the callback fires: optimistic local insert,
     * rollback + DENIED on refusal (binding claims are all-or-nothing at
     * the arbiter, never recallable).  Dispatched on the CALLER's thread,
     * so the callback fires synchronously for FAIL_IMMEDIATELY-style
     * callers whenever the backend answers inline -- which is the common
     * case but NOT guaranteed (async delegation, and eventually a
     * clustered arbiter, both answer later), so this is not something a
     * caller may depend on.  Unlock-then-relock ordering is preserved by
     * program order because the release paths with a thread dispatch their
     * backend release synchronously too (chimera_vfs_claim_release_ranged);
     * only threadless teardown releases ride the queued lane. */
    if (result == CHIMERA_CLAIM_GRANTED &&
        claim->klass == CHIMERA_CLAIM_CLASS_RANGE &&
        chimera_vfs_claim_backend_range_projects(state, file, thread, claim)) {
        chimera_vfs_claim_backend_project_range(thread, state, ticket, false);
        return;
    }

    cb(result,
       result == CHIMERA_CLAIM_GRANTED ? claim : NULL,
       result == CHIMERA_CLAIM_GRANTED ? NULL : &conflict,
       private_data);
} /* chimera_vfs_claim_acquire */

/* Historical alias: ordering is now structural (releases enqueue under
 * file->lock; confirms drain matching releases first), so the thread is
 * unused and this is plain release. */
SYMBOL_EXPORT void
chimera_vfs_claim_release_ranged(
    struct chimera_vfs_thread     *thread,
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim)
{
    (void) thread;
    chimera_vfs_claim_release(state, file, claim);
} /* chimera_vfs_claim_release_ranged */

SYMBOL_EXPORT void
chimera_vfs_claim_release(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim)
{
    uint64_t token;

    pthread_mutex_lock(&file->lock);
    if (claim->file == file) {
        chimera_vfs_claim_unlink_locked(file, claim);
    }
    token                = claim->backend_token;
    claim->backend_token = 0;
    if (token) {
        /* Enqueued UNDER file->lock: any admit that sees these bytes free
         * happens-after this entry exists, so the inline-confirm drain
         * always finds it (release-then-relock ordering, all paths). */
        chimera_vfs_claim_backend_release_token(state, file, token);
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_claim_pump_pending(state, file);
    chimera_vfs_claim_pump_io(state, file);
    chimera_vfs_claim_backend_reeval(state, file);
} /* chimera_vfs_claim_release */

static bool
chimera_vfs_claim_replace_internal(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *destination,
    struct chimera_vfs_claim      *source,
    bool                           allow_narrow)
{
    bool narrowed = false;

    pthread_mutex_lock(&file->lock);
    chimera_vfs_abort_if(source->file != file ||
                         source->klass != CHIMERA_CLAIM_CLASS_ACCESS ||
                         source->break_cb || source->own_cache || source->grant ||
                         source->backend_token || source->parked,
                         "claim_move_replace: source is not a standing plain share reservation");
    if (source != destination) {
        chimera_vfs_abort_if(destination->file &&
                             (destination->file != file ||
                              destination->klass != CHIMERA_CLAIM_CLASS_ACCESS ||
                              destination->construct != source->construct ||
                              destination->break_cb || destination->own_cache ||
                              destination->grant || destination->backend_token ||
                              destination->parked ||
                              !chimera_claim_owner_equal(&destination->owner, &source->owner)),
                             "claim replacement: incompatible destination");
        if (destination->file) {
            narrowed = (destination->used & ~source->used) ||
                (destination->advertised & ~source->advertised) ||
                (destination->denied & ~source->denied);
            chimera_vfs_abort_if(narrowed && !allow_narrow,
                                 "claim_move_replace: replacement does not subsume destination");
            chimera_vfs_claim_unlink_locked(file, destination);
        }

        /* Read linkage after unlinking destination: the nodes may have been
         * adjacent. Redirect both neighbours before making source reusable. */
        *destination = *source;
        if (destination->prev) {
            destination->prev->next = destination;
        } else {
            file->claims[destination->klass] = destination;
        }
        if (destination->next) {
            destination->next->prev = destination;
        }
        memset(source, 0, sizeof(*source));
    }
    destination->admit_excluded     = NULL;
    destination->admit_num_excluded = 0;
    pthread_mutex_unlock(&file->lock);
    return narrowed;
} /* chimera_vfs_claim_replace_internal */

SYMBOL_EXPORT void
chimera_vfs_claim_move_replace(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *destination,
    struct chimera_vfs_claim      *source)
{
    chimera_vfs_claim_replace_internal(file, destination, source, false);
} /* chimera_vfs_claim_move_replace */

SYMBOL_EXPORT bool
chimera_vfs_claim_replace_deferred(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *destination,
    struct chimera_vfs_claim      *source)
{
    return chimera_vfs_claim_replace_internal(file, destination, source, true);
} /* chimera_vfs_claim_replace_deferred */

SYMBOL_EXPORT void
chimera_vfs_claim_replacement_complete(struct chimera_vfs_file_state *file)
{
    chimera_vfs_claim_pump_pending(file->state, file);
    chimera_vfs_claim_pump_io(file->state, file);
    chimera_vfs_claim_backend_reeval(file->state, file);
} /* chimera_vfs_claim_replacement_complete */

static __uint128_t
chimera_vfs_claim_range_end(const struct chimera_vfs_claim *claim)
{
    return claim->length == UINT64_MAX ? ((__uint128_t) 1 << 64) :
           (__uint128_t) claim->offset + claim->length;
} /* chimera_vfs_claim_range_end */

SYMBOL_EXPORT void
chimera_vfs_claim_range_publish(
    struct chimera_vfs_file_state   *file,
    struct chimera_vfs_claim *const *previous,
    uint32_t                         num_previous,
    struct chimera_vfs_claim *const *replacement,
    uint32_t                         num_replacement)
{
    pthread_mutex_lock(&file->lock);
    for (uint32_t i = 0; i < num_previous; i++) {
        const struct chimera_vfs_claim *claim = previous[i];
        chimera_vfs_abort_if(!claim || claim->file != file ||
                             claim->klass != CHIMERA_CLAIM_CLASS_RANGE ||
                             claim->break_cb || claim->own_cache || claim->grant ||
                             claim->backend_token || claim->parked ||
                             claim->construct != previous[0]->construct ||
                             !chimera_claim_owner_equal(&claim->owner, &previous[0]->owner),
                             "range publication: incompatible previous claim");
        for (uint32_t j = 0; j < i; j++) {
            chimera_vfs_abort_if(previous[j] == claim, "range publication: duplicate previous claim");
        }
    }
    for (uint32_t i = 0; i < num_replacement; i++) {
        struct chimera_vfs_claim *claim = replacement[i];
        chimera_vfs_abort_if(!num_previous || !claim || claim->file || claim->prev || claim->next ||
                             claim->klass != CHIMERA_CLAIM_CLASS_RANGE || !claim->length ||
                             claim->break_cb || claim->own_cache || claim->grant ||
                             claim->backend_token || claim->parked ||
                             claim->construct != previous[0]->construct ||
                             !chimera_claim_owner_equal(&claim->owner, &previous[0]->owner),
                             "range publication: incompatible replacement claim");
        for (uint32_t j = 0; j < i; j++) {
            chimera_vfs_abort_if(replacement[j] == claim, "range publication: duplicate replacement claim");
        }
        /* A final interval may span several adjacent admitted fragments.
         * Validate the complete union before detaching any of its protection. */
        __uint128_t cursor = claim->offset;
        __uint128_t end    = chimera_vfs_claim_range_end(claim);
        while (cursor < end) {
            __uint128_t covered = cursor;
            for (uint32_t j = 0; j < num_previous; j++) {
                const struct chimera_vfs_claim *old = previous[j];
                if (old->offset <= cursor && !(claim->used & ~old->used) &&
                    !(claim->advertised & ~old->advertised) && !(claim->denied & ~old->denied)) {
                    __uint128_t old_end = chimera_vfs_claim_range_end(old);
                    if (old_end > covered) {
                        covered = old_end;
                    }
                }
            }
            chimera_vfs_abort_if(covered == cursor, "range publication: unreserved coverage");
            cursor = covered;
        }
    }
    for (uint32_t i = 0; i < num_previous; i++) {
        chimera_vfs_claim_unlink_locked(file, previous[i]);
        previous[i]->admit_excluded     = NULL;
        previous[i]->admit_num_excluded = 0;
    }
    for (uint32_t i = 0; i < num_replacement; i++) {
        replacement[i]->admit_excluded     = NULL;
        replacement[i]->admit_num_excluded = 0;
        chimera_vfs_claim_link_locked(file, replacement[i]);
    }
    pthread_mutex_unlock(&file->lock);
} /* chimera_vfs_claim_range_publish */

SYMBOL_EXPORT void
chimera_vfs_claim_range_retire(
    struct chimera_vfs_file_state   *file,
    struct chimera_vfs_claim *const *claims,
    uint32_t                         num_claims)
{
    pthread_mutex_lock(&file->lock);
    for (uint32_t i = 0; i < num_claims; i++) {
        const struct chimera_vfs_claim *claim = claims[i];
        chimera_vfs_abort_if(!claim || claim->file != file ||
                             claim->klass != CHIMERA_CLAIM_CLASS_RANGE ||
                             claim->break_cb || claim->own_cache || claim->grant ||
                             claim->backend_token || claim->parked,
                             "range retirement: incompatible claim");
        for (uint32_t j = 0; j < i; j++) {
            chimera_vfs_abort_if(claims[j] == claim, "range retirement: duplicate claim");
        }
    }
    for (uint32_t i = 0; i < num_claims; i++) {
        chimera_vfs_claim_unlink_locked(file, claims[i]);
        claims[i]->admit_excluded     = NULL;
        claims[i]->admit_num_excluded = 0;
    }
    pthread_mutex_unlock(&file->lock);
} /* chimera_vfs_claim_range_retire */

SYMBOL_EXPORT void
chimera_vfs_claim_shrink(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_claim      *claim,
    uint8_t                        new_used,
    uint8_t                        new_denied)
{
    pthread_mutex_lock(&file->lock);
    claim->used       = new_used;
    claim->advertised = new_used;
    claim->denied     = new_denied;
    pthread_mutex_unlock(&file->lock);

    if (file->state) {
        chimera_vfs_claim_pump_pending(file->state, file);
        chimera_vfs_claim_pump_io(file->state, file);
        chimera_vfs_claim_backend_reeval(file->state, file);
    }
} /* chimera_vfs_claim_shrink */

SYMBOL_EXPORT enum chimera_vfs_claim_result
chimera_vfs_claim_test(
    struct chimera_vfs_file_state     *file,
    const struct chimera_vfs_claim    *probe,
    struct chimera_vfs_claim_conflict *conflict_out)
{
    enum chimera_vfs_claim_result result;
    struct chimera_vfs_claim     *conflict = NULL;

    pthread_mutex_lock(&file->lock);
    result = chimera_vfs_claim_admit_locked(file, probe, &conflict);
    if (conflict && conflict_out) {
        chimera_vfs_claim_conflict_fill(conflict, conflict_out);
    }
    pthread_mutex_unlock(&file->lock);
    return result;
} /* chimera_vfs_claim_test */

SYMBOL_EXPORT enum chimera_vfs_claim_result
chimera_vfs_claim_test_locks(
    struct chimera_vfs_file_state     *file,
    const struct chimera_vfs_claim    *probe,
    struct chimera_vfs_claim_conflict *conflict_out)
{
    enum chimera_vfs_claim_result result = CHIMERA_CLAIM_GRANTED;

    if (conflict_out) {
        memset(conflict_out, 0, sizeof(*conflict_out));
    }
    if (!file) {
        return result;
    }
    pthread_mutex_lock(&file->lock);
    for (struct chimera_vfs_claim *claim = file->claims[CHIMERA_CLAIM_CLASS_RANGE];
         claim; claim = claim->next) {
        if (claim->provisional ||
            (claim->construct != CHIMERA_CONSTRUCT_LOCK_ADVISORY &&
             claim->construct != CHIMERA_CONSTRUCT_LOCK_SMB)) {
            continue;
        }
        if (chimera_vfs_claim_conflicts(claim, probe)) {
            if (conflict_out) {
                chimera_vfs_claim_conflict_fill(claim, conflict_out);
            }
            result = CHIMERA_CLAIM_DENIED;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);
    return result;
} /* chimera_vfs_claim_test_locks */

SYMBOL_EXPORT enum chimera_vfs_claim_result
chimera_vfs_claim_test_range_view(
    struct chimera_vfs_file_state         *file,
    const struct chimera_vfs_claim        *probe,
    const struct chimera_vfs_claim *const *ranges,
    uint32_t                               num_ranges,
    struct chimera_vfs_claim_conflict     *conflict_out)
{
    if (conflict_out) {
        memset(conflict_out, 0, sizeof(*conflict_out));
    }
    for (uint32_t i = 0; i < num_ranges; i++) {
        const struct chimera_vfs_claim *claim = ranges[i];
        chimera_vfs_abort_if(!claim || claim->klass != CHIMERA_CLAIM_CLASS_RANGE ||
                             claim->break_cb || claim->grant || claim->own_cache ||
                             claim->backend_token || claim->parked,
                             "range view: invalid private claim");
        if (chimera_vfs_claim_conflicts(claim, probe)) {
            if (conflict_out) {
                chimera_vfs_claim_conflict_fill(claim, conflict_out);
            }
            return CHIMERA_CLAIM_DENIED;
        }
    }
    return file ? chimera_vfs_claim_test(file, probe, conflict_out) : CHIMERA_CLAIM_GRANTED;
} /* chimera_vfs_claim_test_range_view */

SYMBOL_EXPORT bool
chimera_vfs_claim_range_is_local(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_claim       *claim)
{
    if (!thread || !handle || !claim || claim->backend_token) {
        return false;
    }
    /* Match backend_range_projects without probing or modifying file state. */
    if (claim->local_only || claim->owner.proto != CHIMERA_CLAIM_PROTO_POSIX) {
        return true;
    }
    struct chimera_vfs_module *module = chimera_vfs_get_module(thread, handle->fh, handle->fh_len);
    return module && !(module->capabilities & CHIMERA_VFS_CAP_CLAIM_RANGE);
} /* chimera_vfs_claim_range_is_local */

SYMBOL_EXPORT bool
chimera_vfs_claim_range_owner_holds(
    struct chimera_vfs_file_state    *file,
    const struct chimera_claim_owner *owner,
    const struct chimera_vfs_claim   *except,
    uint64_t                          offset,
    uint64_t                          length)
{
    struct chimera_vfs_claim *cur;
    bool                      held = false;

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_RANGE]; cur; cur = cur->next) {
        if (cur == except ||
            !chimera_claim_owner_equal(&cur->owner, owner) ||
            !chimera_vfs_claim_range_overlap(cur->offset, cur->length,
                                             offset, length)) {
            continue;
        }
        held = true;
        break;
    }
    pthread_mutex_unlock(&file->lock);

    return held;
} /* chimera_vfs_claim_range_owner_holds */

SYMBOL_EXPORT bool
chimera_vfs_claim_cancel(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket)
{
    struct chimera_vfs_file_state *file = ticket->file;
    bool                           was_queued;

    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    was_queued = ticket->queued;
    if (was_queued) {
        chimera_vfs_pending_dequeue_locked(file, ticket);
    }
    pthread_mutex_unlock(&file->lock);

    if (was_queued) {
        return true;
    }

    /* Not on the pending queue.  A projectable lock granted locally is NOT
     * complete yet: its ticket is either queued for the backend confirm
     * (chimera_vfs_claim_backend_defer_ticket) or has one in flight, with
     * the callback unfired either way.  Claim it so nothing ever completes
     * a ticket whose owner is tearing it down, and roll back the optimistic
     * local insert, releasing the range for other waiters.  ticket_cancel
     * never blocks: when it cannot claim the ticket the callback has
     * already been invoked, and false hands ownership to that callback. */
    if (state && chimera_vfs_claim_backend_ticket_cancel(state, ticket)) {
        chimera_vfs_claim_release(state, file, ticket->claim);
        return true;
    }

    return false;
} /* chimera_vfs_claim_cancel */

/* -------------------------------------------------------------------- */
/* REPLACE-geometry carve                                               */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT void
chimera_vfs_claim_range_replace(
    struct chimera_vfs_state         *state,
    struct chimera_vfs_file_state    *file,
    const struct chimera_claim_owner *owner,
    const struct chimera_vfs_claim   *except,
    uint64_t                          offset,
    uint64_t                          length,
    uint8_t                           new_mask,
    struct chimera_vfs_claim         *spare[2],
    int                              *spare_used,
    void (                           *released_cb )(
        struct chimera_vfs_claim *claim,
        void                     *arg),
    void                             *released_arg)
{
    struct chimera_vfs_claim *cur, *next;
    struct chimera_vfs_claim *released[CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH];
    int                       n_released = 0;
    int                       n_spare    = 0;
    int                       i;

    /* v1 supports the carve (unlock/split) only; inserting the replacement
     * extent is the caller's subsequent acquire.  new_mask is reserved. */
    chimera_vfs_abort_if(new_mask != 0,
                         "range_replace: new_mask insertion not yet supported");

    pthread_mutex_lock(&file->lock);

    for (cur = file->claims[CHIMERA_CLAIM_CLASS_RANGE]; cur; cur = next) {
        next = cur->next;

        if (cur == except) {
            continue;
        }
        if (!chimera_claim_owner_equal(&cur->owner, owner)) {
            continue;
        }
        if (!chimera_vfs_claim_range_overlap(cur->offset, cur->length,
                                             offset, length)) {
            continue;
        }

        /* Compute left/right remainders of `cur` outside [offset, end). */
        {
            uint64_t cur_off          = cur->offset;
            uint64_t cur_len          = cur->length;
            bool     has_left         = cur_off < offset;
            uint64_t carve_end_is_eof = (length == UINT64_MAX);
            uint64_t carve_end        = carve_end_is_eof ? UINT64_MAX
                : offset + length;
            bool     has_right;

            if (carve_end_is_eof) {
                has_right = false;
            } else if (cur_len == UINT64_MAX) {
                has_right = true;
            } else {
                has_right = (cur_off + cur_len) > carve_end;
            }

            /* A remainder is a NEW claim with the carved geometry, not the
             * record the backend knows: the parent's token is released
             * below, so a remainder that inherited it would name a freed
             * record and re-release it later -- onto whatever record has
             * since reused the id.  Clear it.  CLAIMTODO: the remainder is
             * consequently held locally but unprojected until it is
             * re-locked; projecting it needs a vfs thread here, which the
             * carve path does not have. */
            if (has_left && n_spare < 2 && spare && spare[n_spare]) {
                struct chimera_vfs_claim *left = spare[n_spare++];

                *left               = *cur;
                left->offset        = cur_off;
                left->length        = offset - cur_off;
                left->backend_token = 0;
                left->prev          = NULL;
                left->next          = NULL;
                chimera_vfs_claim_link_locked(file, left);
            }
            if (has_right && n_spare < 2 && spare && spare[n_spare]) {
                struct chimera_vfs_claim *right = spare[n_spare++];

                *right        = *cur;
                right->offset = carve_end;
                right->length = (cur_len == UINT64_MAX) ? UINT64_MAX
                    : (cur_off + cur_len) - carve_end;
                right->backend_token = 0;
                right->prev          = NULL;
                right->next          = NULL;
                chimera_vfs_claim_link_locked(file, right);
            }
        }

        chimera_vfs_claim_unlink_locked(file, cur);
        if (n_released < CHIMERA_VFS_CLAIM_MAX_BREAK_BATCH) {
            released[n_released++] = cur;
        }
    }

    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n_released; i++) {
        if (released[i]->backend_token) {
            chimera_vfs_claim_backend_release_token(
                state, file, released[i]->backend_token);
            released[i]->backend_token = 0;
        }
    }

    if (spare_used) {
        *spare_used = n_spare;
    }

    for (i = 0; i < n_released; i++) {
        if (released_cb) {
            released_cb(released[i], released_arg);
        }
    }

    chimera_vfs_claim_pump_pending(state, file);
    chimera_vfs_claim_pump_io(state, file);
    chimera_vfs_claim_backend_reeval(state, file);
} /* chimera_vfs_claim_range_replace */

/* -------------------------------------------------------------------- */
/* Cache grants                                                         */
/* -------------------------------------------------------------------- */

static struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_find_locked(
    struct chimera_vfs_file_state    *file,
    const struct chimera_claim_owner *owner)
{
    struct chimera_vfs_claim_grant *g;

    for (g = file->grants; g; g = g->grant_next) {
        if (chimera_claim_owner_equal(&g->claim.owner, owner)) {
            return g;
        }
        /* Same client-scoped LeaseKey joins the grant;
         * restricted to RqLs/dir constructs (a legacy oplock's owner is a
         * per-open file id, never a shareable key). */
        if (g->claim.construct != CHIMERA_CONSTRUCT_OPLOCK_II &&
            g->claim.construct != CHIMERA_CONSTRUCT_OPLOCK_EX &&
            g->claim.construct != CHIMERA_CONSTRUCT_OPLOCK_BATCH &&
            chimera_claim_owner_same_key(&g->claim.owner, owner)) {
            return g;
        }
    }
    return NULL;
} /* chimera_vfs_claim_grant_find_locked */

static struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_coalesce_locked(
    struct chimera_vfs_file_state    *file,
    const struct chimera_claim_owner *owner,
    uint8_t                           want,
    int                               upgrade_ok,
    void                             *member,
    void                             *member_next)
{
    struct chimera_vfs_claim_grant *grant;

    grant = chimera_vfs_claim_grant_find_locked(file, owner);
    if (grant) {
        grant->refcount++;
        if (member) {
            memcpy(member_next, &grant->members, sizeof(grant->members));
            grant->members = member;
        }

        /* Dir-lease re-arm after a content break settled at 0: re-request
         * under the same key re-establishes the lease and advances the
         * epoch (R31/R44). */
        if (grant->claim.construct == CHIMERA_CONSTRUCT_DIR_LEASE &&
            grant->claim.break_state == CHIMERA_CLAIM_BREAK_ACKED &&
            grant->claim.used == 0 &&
            want != 0) {
            struct chimera_vfs_claim  probe    = grant->claim;
            struct chimera_vfs_claim *conflict = NULL;

            probe.used        = want;
            probe.advertised  = want;
            probe.break_state = CHIMERA_CLAIM_BREAK_IDLE;
            if (chimera_vfs_claim_admit_locked(file, &probe, &conflict) ==
                CHIMERA_CLAIM_GRANTED) {
                grant->claim.used              = want;
                grant->claim.advertised        = want;
                grant->claim.break_state       = CHIMERA_CLAIM_BREAK_IDLE;
                grant->claim.break_needed_mode = 0;
                grant->claim.break_floor       = 0;
                grant->epoch++;
            }
            return grant;
        }

        /* R31: upgrade only to a STRICT SUPERSET of the current mode, only
         * when IDLE or settled-at-0 (ACKED re-arms to IDLE), and only if
         * grantable without breaking another owner.  Never mid-break: the
         * re-open succeeds at the current downgrading state and reports
         * BREAK_IN_PROGRESS (breaking3). */
        if (upgrade_ok &&
            (grant->claim.break_state == CHIMERA_CLAIM_BREAK_IDLE ||
             grant->claim.break_state == CHIMERA_CLAIM_BREAK_ACKED)) {
            uint8_t cur = grant->claim.used;

            if (want != cur && (want & cur) == cur) {
                struct chimera_vfs_claim  probe    = grant->claim;
                struct chimera_vfs_claim *conflict = NULL;

                probe.used        = want;
                probe.advertised  = want;
                probe.break_state = CHIMERA_CLAIM_BREAK_IDLE;
                if (chimera_vfs_claim_admit_locked(file, &probe, &conflict) ==
                    CHIMERA_CLAIM_GRANTED) {
                    grant->claim.used        = want;
                    grant->claim.advertised  = want;
                    grant->claim.break_state = CHIMERA_CLAIM_BREAK_IDLE;
                    grant->epoch++;
                }
            }
        }
    }
    return grant;
} /* chimera_vfs_claim_grant_coalesce */

static struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_coalesce_internal(
    struct chimera_vfs_file_state *file, const struct chimera_claim_owner *owner,
    uint8_t want, int upgrade_ok, void *member, void *member_next)
{
    pthread_mutex_lock(&file->lock);
    struct chimera_vfs_claim_grant *grant = chimera_vfs_claim_grant_coalesce_locked(
        file, owner, want, upgrade_ok, member, member_next);
    pthread_mutex_unlock(&file->lock);
    return grant;
}

SYMBOL_EXPORT struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_coalesce(
    struct chimera_vfs_file_state *file, const struct chimera_claim_owner *owner,
    uint8_t want, int upgrade_ok)
{
    return chimera_vfs_claim_grant_coalesce_internal(file, owner, want, upgrade_ok, NULL, NULL);
}

SYMBOL_EXPORT struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_coalesce_member(
    struct chimera_vfs_file_state *file, const struct chimera_claim_owner *owner,
    uint8_t want, int upgrade_ok, void *member, void *member_next)
{
    chimera_vfs_abort_if(!member || !member_next, "grant member attachment requires a member and next link");
    return chimera_vfs_claim_grant_coalesce_internal(file, owner, want, upgrade_ok, member, member_next);
}

SYMBOL_EXPORT uint8_t
chimera_vfs_claim_grant_try_upgrade(
    struct chimera_vfs_file_state  *file,
    struct chimera_vfs_claim_grant *grant,
    uint8_t                         want_used)
{
    uint8_t granted;

    pthread_mutex_lock(&file->lock);

    granted = grant->claim.used;

    /* Deferred-open rescue (R31): refcount 1, IDLE, strict superset, and
     * the grant is the SOLE cache claim on the file. */
    if (grant->refcount == 1 &&
        grant->claim.break_state == CHIMERA_CLAIM_BREAK_IDLE &&
        want_used != granted &&
        (want_used & granted) == granted &&
        file->claims[CHIMERA_CLAIM_CLASS_CACHE] == &grant->claim &&
        grant->claim.next == NULL) {
        struct chimera_vfs_claim  probe    = grant->claim;
        struct chimera_vfs_claim *conflict = NULL;

        probe.used       = want_used;
        probe.advertised = want_used;
        /* This grant belongs to a CREATE whose reply is still deferred. Its
         * provisional RH mode must not itself turn a fresh-key admission into
         * an established granular lease upgrade: a peer that acknowledged its
         * cache break but kept its Open still prevents a new WRITE cache.
         * Actual same-key reopens use grant_coalesce and retain their separate
         * upgrade rules. */
        if (!chimera_vfs_claim_sole_opener_blocker_locked(file, &probe,
                                                          &grant->claim) &&
            chimera_vfs_claim_admit_locked(file, &probe, &conflict) ==
            CHIMERA_CLAIM_GRANTED) {
            grant->claim.used       = want_used;
            grant->claim.advertised = want_used;
            granted                 = want_used;
        }
    }

    pthread_mutex_unlock(&file->lock);

    return granted;
} /* chimera_vfs_claim_grant_try_upgrade */

SYMBOL_EXPORT uint8_t
chimera_vfs_claim_grant_cap_mode(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_claim *template_claim,
    bool                            strict)
{
    struct chimera_vfs_claim  probe    = *template_claim;
    struct chimera_vfs_claim *conflict = NULL;
    uint8_t                   mode     = template_claim->used;

    pthread_mutex_lock(&file->lock);
    for ( ; ; ) {
        probe.used       = mode;
        probe.advertised = mode;
        if (chimera_vfs_claim_admit_locked(file, &probe, &conflict) ==
            CHIMERA_CLAIM_GRANTED) {
            break;
        }
        if (mode & CHIMERA_CLAIM_CW) {
            mode &= (uint8_t) ~CHIMERA_CLAIM_CW;
        } else if (mode & CHIMERA_CLAIM_H) {
            mode &= (uint8_t) ~CHIMERA_CLAIM_H;
        } else {
            if (strict) {
                mode = 0;
            }
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);

    return mode;
} /* chimera_vfs_claim_grant_cap_mode */

static enum chimera_vfs_claim_result
chimera_vfs_claim_grant_acquire_internal(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_file_state      *file,
    const struct chimera_vfs_claim     *template_claim,
    int                                 upgrade_ok,
    uint8_t                             is_v2,
    uint16_t                            initial_epoch,
    enum chimera_vfs_claim_grant_flavor flavor,
    void                               *member_seed,
    bool                               *member_seeded,
    void                               *member_next,
    struct chimera_vfs_claim_grant    **grant_out,
    struct chimera_vfs_claim_conflict  *conflict_out)
{
    struct chimera_vfs_claim_grant *grant;
    enum chimera_vfs_claim_result   result;

    (void) flavor;

    *grant_out = NULL;
    if (member_seeded) {
        *member_seeded = false;
    }
    if (conflict_out) {
        memset(conflict_out, 0, sizeof(*conflict_out));
    }

    grant = chimera_vfs_claim_grant_coalesce_internal(file, &template_claim->owner,
        template_claim->used, upgrade_ok, member_next ? member_seed : NULL, member_next);
    if (grant) {
        *grant_out = grant;
        return CHIMERA_CLAIM_GRANTED;
    }

    grant = calloc(1, sizeof(*grant));
    if (!grant) {
        return CHIMERA_CLAIM_DENIED;
    }
    grant->claim       = *template_claim;
    grant->claim.grant = grant;
    grant->file        = file;
    grant->refcount    = 1;
    grant->epoch       = initial_epoch;
    grant->is_v2       = is_v2;
    /* Seed the member head BEFORE the claim becomes visible to the conflict
     * matrix, so a break callback fired against the mid-insert grant finds a
     * live member instead of revoking a memberless grant (the old
     * pre-registered-member discipline). */
    grant->members = member_seed;

    result = chimera_vfs_claim_try_acquire_internal(state, file, &grant->claim,
        conflict_out, grant, upgrade_ok, member_next, grant_out);
    if (result != CHIMERA_CLAIM_GRANTED) {
        free(grant);
        return result;
    }
    /* A competing first-acquire may have won before our locked admission.
     * Our candidate was never visible in that case, so no breaker can pin it. */
    if (*grant_out != grant) { free(grant); }
    else if (member_seeded) { *member_seeded = (member_seed != NULL); }

    return CHIMERA_CLAIM_GRANTED;
} /* chimera_vfs_claim_grant_acquire */

SYMBOL_EXPORT enum chimera_vfs_claim_result
chimera_vfs_claim_grant_acquire(
    struct chimera_vfs_state *state, struct chimera_vfs_file_state *file,
    const struct chimera_vfs_claim *template_claim, int upgrade_ok, uint8_t is_v2,
    enum chimera_vfs_claim_grant_flavor flavor, void *member_seed, bool *member_seeded,
    struct chimera_vfs_claim_grant **grant_out, struct chimera_vfs_claim_conflict *conflict_out)
{
    return chimera_vfs_claim_grant_acquire_internal(state, file, template_claim, upgrade_ok,
        is_v2, 1, flavor, member_seed, member_seeded, NULL, grant_out, conflict_out);
}

SYMBOL_EXPORT enum chimera_vfs_claim_result
chimera_vfs_claim_grant_acquire_member(
    struct chimera_vfs_state *state, struct chimera_vfs_file_state *file,
    const struct chimera_vfs_claim *template_claim, int upgrade_ok, uint8_t is_v2,
    uint16_t initial_epoch, enum chimera_vfs_claim_grant_flavor flavor, void *member, void *member_next,
    struct chimera_vfs_claim_grant **grant_out, struct chimera_vfs_claim_conflict *conflict_out)
{
    void *next = NULL;
    chimera_vfs_abort_if(!member || !member_next, "grant member attachment requires a member and next link");
    memcpy(member_next, &next, sizeof(next));
    return chimera_vfs_claim_grant_acquire_internal(state, file, template_claim, upgrade_ok,
        is_v2, initial_epoch, flavor, member, NULL, member_next, grant_out, conflict_out);
}

SYMBOL_EXPORT bool
chimera_vfs_claim_grant_publish_oplock_locked(
    struct chimera_vfs_file_state *file, struct chimera_vfs_claim_grant *candidate,
    const struct chimera_vfs_claim *template_claim, void *member)
{
    struct chimera_vfs_claim probe = *template_claim;
    uint8_t mode = probe.used;
    chimera_vfs_abort_if(!candidate || candidate->refcount || !member ||
        probe.klass != CHIMERA_CLAIM_CLASS_CACHE ||
        (probe.construct != CHIMERA_CONSTRUCT_OPLOCK_II &&
         probe.construct != CHIMERA_CONSTRUCT_OPLOCK_EX &&
         probe.construct != CHIMERA_CONSTRUCT_OPLOCK_BATCH), "invalid prepared oplock grant");
    if (chimera_vfs_claim_grant_find_locked(file, &probe.owner)) return false;
    while (mode) {
        probe.used = probe.advertised = mode;
        probe.construct = (mode & CHIMERA_CLAIM_H) ? CHIMERA_CONSTRUCT_OPLOCK_BATCH :
            (mode & CHIMERA_CLAIM_CW) ? CHIMERA_CONSTRUCT_OPLOCK_EX : CHIMERA_CONSTRUCT_OPLOCK_II;
        if (chimera_vfs_claim_admit_locked(file, &probe, NULL) == CHIMERA_CLAIM_GRANTED) {
            candidate->claim = probe;
            candidate->claim.grant = candidate;
            candidate->file = file;
            candidate->refcount = 1;
            candidate->epoch = 1;
            candidate->members = member;
            chimera_vfs_claim_link_locked(file, &candidate->claim);
            candidate->grant_next = file->grants;
            file->grants = candidate;
            return true;
        }
        /* Legacy wire grants have no RH-only level. A failed EX/BATCH
         * request may retain READ only; never hide an unadvertised H right. */
        mode = (mode != CHIMERA_CLAIM_CR) ? (mode & CHIMERA_CLAIM_CR) : 0;
    }
    return false;
}

/* Caller holds file->lock through protocol publication. Coalescing shares the
 * existing grant's version/epoch and never resets either to request values. */
SYMBOL_EXPORT struct chimera_vfs_claim_grant *
chimera_vfs_claim_grant_publish_rqls_locked(
    struct chimera_vfs_file_state *file, struct chimera_vfs_claim_grant *candidate,
    const struct chimera_vfs_claim *template_claim, uint8_t is_v2, uint16_t initial_epoch,
    void *member, void *member_next)
{
    chimera_vfs_abort_if(!candidate || candidate->refcount || !member || !member_next ||
        (template_claim->construct != CHIMERA_CONSTRUCT_RQLS &&
         template_claim->construct != CHIMERA_CONSTRUCT_DIR_LEASE),
        "invalid prepared RqLs grant");
    struct chimera_vfs_claim_grant *grant = chimera_vfs_claim_grant_coalesce_locked(
        file, &template_claim->owner, template_claim->used, 1, member, member_next);
    if (grant) { return grant; }
    struct chimera_vfs_claim probe = *template_claim;
    while (probe.used) {
        if (chimera_vfs_claim_admit_locked(file, &probe, NULL) == CHIMERA_CLAIM_GRANTED) {
            candidate->claim = probe;
            candidate->claim.grant = candidate;
            candidate->file = file;
            candidate->refcount = 1;
            candidate->epoch = initial_epoch;
            candidate->is_v2 = is_v2;
            candidate->members = member;
            void *next = NULL;
            memcpy(member_next, &next, sizeof(next));
            chimera_vfs_claim_link_locked(file, &candidate->claim);
            candidate->grant_next = file->grants;
            file->grants = candidate;
            return candidate;
        }
        if (probe.used & CHIMERA_CLAIM_CW) { probe.used &= ~CHIMERA_CLAIM_CW; }
        else if (probe.used & CHIMERA_CLAIM_H) { probe.used &= ~CHIMERA_CLAIM_H; }
        else { probe.used = 0; }
        probe.advertised = probe.used;
    }
    return NULL;
}

SYMBOL_EXPORT void
chimera_vfs_claim_grant_release(
    struct chimera_vfs_state       *state,
    struct chimera_vfs_claim_grant *grant,
    bool                            pump)
{
    struct chimera_vfs_file_state   *file = grant->file;
    struct chimera_vfs_claim_grant **pp;
    bool                             last;

    pthread_mutex_lock(&file->lock);

    chimera_vfs_abort_if(grant->refcount == 0,
                         "double release of claim grant");
    grant->refcount--;
    last = (grant->refcount == 0);

    if (last) {
        for (pp = &file->grants; *pp; pp = &(*pp)->grant_next) {
            if (*pp == grant) {
                *pp = grant->grant_next;
                break;
            }
        }
        if (grant->claim.file == file) {
            chimera_vfs_claim_unlink_locked(file, &grant->claim);
        }
    }

    pthread_mutex_unlock(&file->lock);

    if (last) {
        if (pump) {
            chimera_vfs_claim_pump_pending(state, file);
            chimera_vfs_claim_pump_io(state, file);
        }
        chimera_vfs_claim_backend_reeval(state, file);
        free(grant);
    }
} /* chimera_vfs_claim_grant_release */

SYMBOL_EXPORT void
chimera_vfs_claim_grant_revoke_empty(struct chimera_vfs_claim_grant *grant)
{
    struct chimera_vfs_file_state *file = grant->file;
    chimera_vfs_claim_revoked_cb_t revoked = NULL;
    void *private_data = NULL;
    pthread_mutex_lock(&file->lock);
    bool empty = !grant->members;
    if (empty) chimera_vfs_claim_revoke_locked(file, &grant->claim, &revoked, &private_data);
    pthread_mutex_unlock(&file->lock);
    if (!empty) return;
    if (revoked) revoked(&grant->claim, private_data);
    if (file->state) {
        chimera_vfs_claim_pump_pending(file->state, file);
        chimera_vfs_claim_pump_io(file->state, file);
        chimera_vfs_claim_backend_reeval(file->state, file);
    }
}

SYMBOL_EXPORT struct chimera_vfs_claim_grant *
chimera_vfs_claim_pin_grant(struct chimera_vfs_claim *claim)
{
    struct chimera_vfs_claim_grant *grant = NULL;

    if (claim->grant) {
        grant = claim->grant;
        grant->refcount++;
    }

    return grant;
} /* chimera_vfs_claim_pin_grant */

SYMBOL_EXPORT bool
chimera_vfs_claim_client_holds_handle_cache(
    struct chimera_vfs_file_state *file,
    uint64_t                       client_key)
{
    const struct chimera_vfs_claim *cur;
    bool                            held = false;

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        if (cur->owner.client_key == client_key &&
            cur->construct == CHIMERA_CONSTRUCT_RQLS &&
            (cur->used & CHIMERA_CLAIM_H) &&
            cur->break_state != CHIMERA_CLAIM_BREAK_REVOKED) {
            held = true;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);
    return held;
} /* chimera_vfs_claim_client_holds_handle_cache */

SYMBOL_EXPORT bool
chimera_vfs_claim_client_holds_cache(
    struct chimera_vfs_file_state *file,
    uint64_t                       client_key)
{
    const struct chimera_vfs_claim *cur;
    bool                            held = false;

    pthread_mutex_lock(&file->lock);
    for (cur = file->claims[CHIMERA_CLAIM_CLASS_CACHE]; cur; cur = cur->next) {
        if (cur->owner.client_key == client_key &&
            cur->construct == CHIMERA_CONSTRUCT_RQLS &&
            cur->break_state != CHIMERA_CLAIM_BREAK_REVOKED) {
            held = true;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);
    return held;
} /* chimera_vfs_claim_client_holds_cache */

/* -------------------------------------------------------------------- */
/* Scan                                                                 */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT void
chimera_vfs_claim_scan(
    struct chimera_vfs_file_state         *file,
    const struct chimera_vfs_claim_filter *filter,
    chimera_vfs_claim_visit_cb_t           visit_cb,
    void                                  *arg)
{
    const struct chimera_vfs_claim *cur;
    int                             k;

    pthread_mutex_lock(&file->lock);
    for (k = 0; k < CHIMERA_CLAIM_CLASS_COUNT; k++) {
        if (filter->klass_mask && !(filter->klass_mask & (1u << k))) {
            continue;
        }
        for (cur = file->claims[k]; cur; cur = cur->next) {
            if (filter->used_any && !(cur->used & filter->used_any)) {
                continue;
            }
            if (filter->proto && cur->owner.proto != filter->proto) {
                continue;
            }
            if (filter->exclude_client &&
                cur->owner.client_key == filter->exclude_client) {
                continue;
            }
            if (filter->breaking_only &&
                cur->break_state != CHIMERA_CLAIM_BREAK_BREAKING) {
                continue;
            }
            if (!filter->include_inert &&
                cur->construct == CHIMERA_CONSTRUCT_SMB_OPEN_INERT) {
                continue;
            }
            visit_cb(cur, arg);
        }
    }
    pthread_mutex_unlock(&file->lock);
} /* chimera_vfs_claim_scan */
