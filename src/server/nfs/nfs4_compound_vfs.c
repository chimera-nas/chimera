// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Encode a supported NFSv4 wire run as one VFS compound. Protocol decisions
 * execute through per-operation callbacks; replies and shared open state are
 * published only after the VFS finish phase accepts the attempt.
 *
 * OPEN and OPEN_DOWNGRADE reserve their owners and maintain
 * a private journal of coalesced states and rights. Share claims are acquired
 * through VFS operations before conditional truncation or following I/O.
 * CLOSE updates the same private view. LOCK and LOCKU use reserved lock owners
 * and private interval journals; VFS range reservations protect admitted
 * coverage until accepted atomic publication. Handles and stateids remain
 * private until finish. CLOSE retires child locks through the same journals;
 * shared lock owners reserve their connected parent set before execution.
 * v4.0 owner sequence numbers, confirmation and typed replies use private
 * journals too. Explicit coordination operations memoize delegation queries,
 * namespace recalls and pNFS truncate recalls outside retryable pure checks.
 * Delegation grants run after accepted finish. Cross-export credentials and
 * pNFS REMOVE's data-server cleanup still require dispatcher boundaries.
 *
 * Finish-time rejection takes precedence over ordinary per-operation errors:
 * EAGAIN restarts the attempt only after the finish adapter has aborted backend
 * effects. An accepted operation error preserves the successful prefix.
 */

#include <stdlib.h>
#include <string.h>
#include <xxhash.h>
#include "vfs/vfs_claim.h"
#include "vfs/sdk/vfs_access.h"
#include "common/evpl_iovec_cursor.h"

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_access.h"
#include "nfs4_named_attr.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_cb.h"
#include "nfs4_callback.h"
#include "nfs4_op_matrix.h"
#include "server/server.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include <xxhash.h>
#include "vfs/vfs_claim.h"
#include "vfs/vfs_compound.h"

/*
 * Metadata operations explicitly open the current object. Stateid-bearing
 * operations bind their authorized handle at execution time; anonymous I/O
 * lets the executor infer an appropriate open against the execution cursor.
 *
 * A sequence's ops act on an OPEN handle, and something has to open it.  The
 * executor used to decide, from a table keyed on op type; now the encoder says
 * so, because the encoder is what knows why it is opening -- and the table is
 * what handed an O_PATH descriptor to fgetxattr and made a data open of a FIFO
 * block.
 *
 * `cur_flags` tracks what the sequence's current open handle was opened with as
 * the sequence is BUILT, so consecutive ops on one object share a single OPEN:
 * a LOOKUP and the GETATTR behind it, a READLINK and its type gate.  It is
 * cleared wherever the current filehandle moves, because an open of the old
 * object says nothing about the new one.
 *
 * The serve test mirrors chimera_vfs_compound_handle_serves exactly: a handle
 * serves an op that asks for no flag it lacks and that wants the same side of
 * CHIMERA_VFS_OPEN_PATH -- a path open and a data open are different handles
 * from different caches, never interchangeable.
 *
 * Returns the OPEN's index, 0 when the open already in hand serves and no op
 * was added, or -1 if the sequence is full.  Callers test only for -1; index 0
 * is the seed PUTFH's and can never be an OPEN's.
 */
static int
nfs4_vfs_open_for(
    struct chimera_vfs_compound *compound,
    unsigned int                *cur_flags,
    unsigned int                 want)
{
    if (*cur_flags &&
        !(want & ~*cur_flags) &&
        (((*cur_flags ^ want) & CHIMERA_VFS_OPEN_PATH) == 0)) {
        return 0;
    }

    *cur_flags = want;

    return chimera_vfs_compound_add_open_current(compound, want, 0);
} /* nfs4_vfs_open_for */

/* A CLAIM_NULL OPEN names a file in the current directory; every other claim
 * re-opens an object the client already has, and needs no directory. */
static int
nfs4_vfs_open_is_named(const struct nfs_argop4 *argop)
{
    return argop->opopen.claim.claim == CLAIM_NULL;
} /* nfs4_vfs_open_is_named */

/* The three intents an NFSv4 op has for the object it addresses. */
#define NFS4_VFS_OPEN_DIR                                       \
        (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |        \
         CHIMERA_VFS_OPEN_DIRECTORY)
#define NFS4_VFS_OPEN_META                                      \
        (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH)
/* REGULAR_ONLY is what carries the type rule when the caller opens
 * explicitly: the open refuses a directory with EISDIR (NFS4ERR_ISDIR)
 * atomically, rather than the data operation failing afterwards with whatever
 * errno the backend gives a directory. */
#define NFS4_VFS_OPEN_DATA                                      \
        (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_REGULAR_ONLY)

/* One NFSv4 op encodes to several VFS ops -- the operation, the open the
 * encoder puts in front of it, and for the ops with a type gate a stat and a
 * second open.  The NFSv4 op count is still bounded by the VFS compound's own
 * limit, because every NFSv4 op costs at least one VFS op; the running total
 * is what vfs_ops bounds, op by op. */
#define NFS4_VFS_COMPOUND_MAX_OPS  128

/*
 * The least reply-buffer space one READDIR entry can consume: the attribute
 * value buffer alone is 256 bytes (see chimera_nfs4_readdir_entry_fill), before
 * the entry struct, its name and its attrmask array.  Used to bound how many
 * entries a given maxcount could possibly admit.
 */
#define NFS4_VFS_READDIR_MIN_ENTRY 256

/* How many iovecs one READ's answer may arrive in; the per-op path reserves the
 * same number. */
#define NFS4_VFS_READ_MAX_IOV      256

/*
 * How many entries a READDIR's reply could possibly hold.
 *
 * Two things bound the page in the per-op path: maxcount, charged 16 bytes of
 * fixed READDIR4resok overhead before any entry, and the reply buffer's own
 * 8192-byte floor.  Whichever is smaller, divided by the least an entry can
 * cost, is the most entries that reply can carry -- so a VFS sequence asked for
 * that many will never be the thing that ends the page first.
 */
static uint32_t
nfs4_vfs_readdir_max_entries(
    uint32_t maxcount,
    uint64_t avail)
{
    uint64_t budget = maxcount - 16;
    uint64_t floor  = avail > 8192 ? avail - 8192 : 0;

    if (floor < budget) {
        budget = floor;
    }

    return (uint32_t) (budget / NFS4_VFS_READDIR_MIN_ENTRY);
} /* nfs4_vfs_readdir_max_entries */

struct nfs4_vfs_compound_ctx;
struct nfs4_vfs_coordination {
    struct nfs4_vfs_coordination          *next;
    struct nfs4_vfs_compound_ctx          *ctx;
    struct chimera_vfs_compound           *compound;
    uint64_t                               token;
    uint32_t                               index, fh_len;
    uint8_t                                fh[CHIMERA_VFS_FH_SIZE];
    bool                                   layout_barrier;
    struct chimera_vfs_file_state         *namespace_file;
    struct nfs_client                     *holder_client;
    struct nfs_delegation_combine_journal *combine;
    bool                                   queried, got_change, got_size;
    int                                    query_status;
    uint64_t                               change, size;
    struct timespec                        query_time;
};
struct nfs4_vfs_state {
    struct nfs_open_state              shadow;
    struct nfs_open_state             *target;
    struct nfs_open_owner_reservation *group;
    struct nfs4_vfs_op                *last_open;
    int                                initially_public, active, closed, version_dirty;
};

struct nfs4_vfs_lock {
    struct nfs_lock_state             *target;
    struct nfs_lock_owner_reservation *group;
    struct nfs4_vfs_state             *parent;
    struct nfs_lock_range_journal     *ranges;
    uint32_t                           seqid;
    int                                initially_public, active, dirty;
};

struct nfs4_vfs_op {
    struct nfs4_vfs_compound_ctx      *ctx;
    struct nfs_lock_owner_reservation *lock_group;
    struct nfs_lock_state             *lock_candidate;
    struct nfs4_vfs_lock              *lock_state;
    struct chimera_vfs_claim           lock_claim;
    struct nfs4_owner_replay_journal  *open_replay, *lock_replay;
    uint32_t                           open_seqid, lock_seqid;
    struct nfs4_replay_cache          *open_response, *close_response;
    struct nfs_client                 *lockt_client;
    nfsstat4                           lockt_client_status;
    int                                lockt_checked;
    int                                open_replay_restore;
    uint32_t                           open_rflags;
    int                                replay_hit, replay_recorded;
    int                                lock_reservation;
    int                                open_reservation;
    struct chimera_vfs_claim           open_claim;
    struct nfs_open_state             *reserved_open;
    struct nfs_open_state             *downgrade_scratch;
    nfsstat4                          *test_stateid_statuses;
    nfsstat4                           stateid_reservation_status;
    struct stateid4                    reserved_stateid;
    struct nfs_open_owner_reservation *open_group;
    struct nfs4_vfs_state             *open_state;
    int                                open_committed;
    struct stateid4                    close_stateid;
    int                                close_passed;
    int                                read_plus_data;
    int                                prepare_index;
    int                                io_authorize;
    int                                have_io_owner;
    struct chimera_claim_actor         io_owner;
    struct chimera_vfs_open_handle    *range_src_handle;
    struct chimera_claim_actor         range_src_owner;
    int                                range_have_src_owner;
    int                                range_src_open, range_dst_open;
    int                                range_src_attr, range_restore;
    uint64_t                           range_length;
    /* Immutable decoded ACL input; VFS clones it for each execution attempt. */
    struct chimera_acl                *input_acl;
    /* ACL GETATTR results are staged before a successor can mutate anything. */
    struct GETATTR4res                 getattr_result;
    int                                getattr_staged;
    int                                getattr_coordinate;
    int                                namespace_parent;
    struct {
        int  lookup, coordinate;
        bool found;
    } namespace_targets[2];
    uint32_t                           namespace_count;

    /* Protocol-specific status decided during execution, not encoded by errno. */
    nfsstat4                           verify_status;

    /* LOCKT snapshots only values: neither claims nor lock owners are pinned
     * until reply construction, and a retry must probe them again. */
    struct chimera_vfs_claim_conflict  lockt_conflict;
    uint8_t                            lockt_owner[NFS4_OPAQUE_LIMIT];
    uint32_t                           lockt_owner_len;

    /* READDIR, which marshals its page entry by entry as the sequence produces
     * it: the entry list being built, and the reply-buffer mark it started
     * from so a retried sequence can put the buffer back. */
    struct nfs_nfs4_readdir_cursor     readdir_cursor;
    uint32_t                           readdir_mark;
    int                                readdir_have_mark;

    uint32_t                           res_index; /* index into the COMPOUND's arg/res arrays        */
    int                                vfs_lo; /* first VFS op belonging to this NFSv4 op         */
    int                                vfs_hi; /* last VFS op belonging to this NFSv4 op          */
    int                                vfs_aux; /* injected helper getattr, or -1                  */
    int                                vfs_res; /* the VFS op this NFSv4 op's result comes from    */
    /* OPEN: an UNCHECKED4 create asked for size 0, which truncates an object
     * that already existed.  Recorded here because the create attributes it is
     * read from are blanked by the executor once the name resolves to something
     * (CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY), which is the same
     * blanking the per-op path does and for the same reason. */
    int                                open_trunc_if_existed;
    /* OPEN: whether the open named a child (CLAIM_NULL) rather than re-opening
     * the current filehandle (CLAIM_FH).  The two differ in what the open
     * reports back -- an open-by-handle produces no attributes and no directory
     * change info -- and so in what may be passed on from it. */
    int                                open_by_name;
    /* READ, WRITE, SETATTR: the handle this op resolved from its stateid, and
     * holds a reference to for as long as the sequence runs.  Borrowed by the
     * VFS op; released here when the sequence is over, whatever the outcome. */
    struct chimera_vfs_open_handle    *io_handle;
};

struct nfs4_vfs_compound_ctx {
    struct nfs_request                   *req;
    struct nfs4_vfs_coordination         *coordination;
    struct nfs_delegation_combine_journal combines[NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_combines;
    bool                                  accepted;
    struct nfs_client                    *client;
    uint32_t                              num_reserved;
    struct nfs_open_state                *closed_states[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    const struct chimera_vfs_claim       *closed_claims[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_closed_states, num_closed_claims;
    struct nfs_open_owner_reservation    *groups[NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_groups;
    struct nfs_lock_owner_reservation    *lock_groups[NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_lock_groups;
    struct nfs4_vfs_lock                  locks[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_locks;
    const struct chimera_vfs_claim      **range_previous, **range_current;
    uint32_t                              range_capacity, num_range_previous, num_range_current;
    struct nfs4_vfs_state                 states[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_states;
    struct stateid4                       initial_stateid, initial_saved_stateid;
    struct stateid4                       current_stateid, saved_stateid;
    int                                   initial_stateid_valid, initial_saved_stateid_valid;
    int                                   current_stateid_valid, saved_stateid_valid;
    uint32_t                              attempt_mark;
    uint64_t                              reply_bound;
    uint64_t                              getattr_staged_bytes;
    uint64_t                              getattr_staged_bound;
    uint32_t                              retries;
    uint32_t                              num_ops;
    struct chimera_vfs_compound          *accepted_compound;
    uint32_t                              grant_next;
    int                                   accepted_index, accepted_fhlen;
    uint8_t                               accepted_fh[CHIMERA_VFS_FH_SIZE];
    nfsstat4                              accepted_status;
    struct nfs4_vfs_op                    ops[NFS4_VFS_COMPOUND_MAX_OPS];

    /* A SECINFO that succeeded has taken the current filehandle away. */
    int                                   fh_consumed;
    int                                   initial_fh_consumed;

    /* Only a legacy OPEN requires a separate completion tail. */
    int                                   open_present;
    uint32_t                              open_res_index;

    /* An OPEN that filled successfully still owes the part of itself that can
     * suspend -- the delegation grant, the deferred truncate -- and that part
     * runs after the sequence has been freed, so what it needs is copied out
     * here rather than left pointing into the compound. */
    int                                   open_filled;
    int                                   open_has_attr;
    struct chimera_vfs_attrs              open_attr;
};

static void nfs4_vfs_attempt_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data);

static void
nfs4_vfs_dispose(
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx)
{
    nfs4_change_finish(ctx->req->thread->shared->nfs4_state_table.change_table,
                       &ctx->req->change_observations, ctx->accepted);
    chimera_vfs_compound_free(compound);
    for (uint32_t i = 0; i < ctx->num_combines; i++) {
        nfs_delegation_combine_finish(&ctx->combines[i], ctx->accepted,
                                      &ctx->req->thread->shared->nfs4_state_table,
                                      ctx->req->thread->vfs_thread);
    }
    while (ctx->coordination) {
        struct nfs4_vfs_coordination *entry = ctx->coordination;
        ctx->coordination = entry->next;
        if (entry->layout_barrier) {
            nfs_layout_table_barrier_release(&ctx->req->thread->shared->nfs4_layout_table,
                                             entry->fh, entry->fh_len);
        }
        if (entry->namespace_file) {
            chimera_vfs_state_put(ctx->req->thread->vfs->vfs_state, entry->namespace_file);
        }
        if (entry->holder_client) {
            nfs_client_finish_compound(entry->holder_client,
                                       &ctx->req->thread->shared->nfs4_state_table,
                                       ctx->req->thread->vfs_thread);
        }
        free(entry);
    }
    /* Refused builds may own inputs beyond num_ops, which is assigned only
     * once construction finishes. */
    for (uint32_t i = 0; i < NFS4_VFS_COMPOUND_MAX_OPS; i++) {
        struct nfs4_vfs_op *map = &ctx->ops[i];
        free(map->input_acl);
        free(map->downgrade_scratch);
        free(map->open_response);
        free(map->close_response);
        if (map->lockt_client) {
            nfs_client_finish_compound(map->lockt_client, &ctx->req->thread->shared->nfs4_state_table,
                                       ctx->req->thread->vfs_thread);
        }
    }
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        nfs_lock_range_journal_free(ctx->locks[i].ranges);
    }
    for (uint32_t i = 0; i < ctx->num_lock_groups; i++) {
        nfs_lock_owner_finish_compound(ctx->lock_groups[i],
                                       &ctx->req->thread->shared->nfs4_state_table, ctx->req->thread->vfs_thread);
    }
    free(ctx->range_previous);
    free(ctx->range_current);
    for (uint32_t i = 0; i < ctx->num_groups; i++) {
        nfs_open_owner_finish_compound(ctx->groups[i],
                                       &ctx->req->thread->shared->nfs4_state_table, ctx->req->thread->vfs_thread);
    }
    if (ctx->client) {
        nfs_client_finish_compound(ctx->client, &ctx->req->thread->shared->nfs4_state_table,
                                   ctx->req->thread->vfs_thread);
    }
    free(ctx);
} /* nfs4_vfs_dispose */

static void
nfs4_vfs_state_copy(
    struct nfs_open_state       *dst,
    const struct nfs_open_state *src)
{
    dst->owner                     = src->owner;
    dst->principal_flavor          = src->principal_flavor;
    dst->principal_machinename_len = src->principal_machinename_len;
    memcpy(dst->principal_machinename, src->principal_machinename, src->principal_machinename_len);
    memcpy(dst->fh, src->fh, src->fh_len);
    dst->fh_len       = src->fh_len;
    dst->share_access = src->share_access;
    dst->share_deny   = src->share_deny;
    dst->share_combos = src->share_combos;
    dst->seqid        = src->seqid;
    dst->handle       = src->handle;
} /* nfs4_vfs_state_copy */

static void
nfs4_vfs_stateid(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs4_vfs_state        *entry,
    uint32_t                      seqid,
    struct stateid4              *sid)
{
    struct nfs_open_state *target = entry->target;

    nfs4_stateid_encode(sid, seqid, NFS4_STATEID_TYPE_OPEN, target->shard,
                        target->slot_idx, target->generation, ctx->req->thread->shared->nfs4_state_table.epoch);
} /* nfs4_vfs_stateid */

static struct nfs4_vfs_state *
nfs4_vfs_private_open(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid)
{
    for (uint32_t i = 0; i < ctx->num_states; i++) {
        struct nfs4_vfs_state *entry = &ctx->states[i];
        struct stateid4        identity;
        nfs4_vfs_stateid(ctx, entry, 0, &identity);
        if (entry->active && !memcmp(identity.other, sid->other, sizeof(sid->other))) {
            return entry;
        }
    }
    return NULL;
} /* nfs4_vfs_private_open */

static struct nfs4_vfs_state *
nfs4_vfs_open_target(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs_open_state        *target)
{
    for (uint32_t i = 0; i < ctx->num_states; i++) {
        if (ctx->states[i].target == target && ctx->states[i].active) {
            return &ctx->states[i];
        }
    }
    return NULL;
} /* nfs4_vfs_open_target */

static void
nfs4_vfs_lock_stateid(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs4_vfs_lock         *entry,
    struct stateid4              *sid)
{
    struct nfs_lock_state *target = entry->target;

    nfs4_stateid_encode(sid, entry->seqid, NFS4_STATEID_TYPE_LOCK, target->shard,
                        target->slot_idx, target->generation, ctx->req->thread->shared->nfs4_state_table.epoch);
} /* nfs4_vfs_lock_stateid */

static struct nfs4_vfs_lock *
nfs4_vfs_private_lock(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid)
{
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock *entry = &ctx->locks[i];
        struct stateid4       identity;
        nfs4_vfs_lock_stateid(ctx, entry, &identity);
        if (entry->active && !memcmp(identity.other, sid->other, sizeof(sid->other))) {
            return entry;
        }
    }
    return NULL;
} /* nfs4_vfs_private_lock */

/* Borrowed arrays remain valid through the immediately following admission or
 * probe. Only this attempt can change its frozen owners' private geometry. */
static void
nfs4_vfs_range_view(
    struct nfs4_vfs_compound_ctx *ctx,
    const uint8_t                *fh,
    uint32_t                      fhlen)
{
    ctx->num_range_previous = ctx->num_range_current = 0;
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock                  *entry = &ctx->locks[i];
        uint32_t                               count;
        const struct chimera_vfs_claim *const *claims;
        if (!entry->active || !entry->parent ||
            entry->parent->shadow.fh_len != fhlen || memcmp(entry->parent->shadow.fh, fh, fhlen)) {
            continue;
        }
        claims = nfs_lock_range_journal_previous(entry->ranges, &count);
        chimera_nfs_abort_if(ctx->num_range_previous + count > ctx->range_capacity,
                             "compound range exclusion capacity");
        memcpy(ctx->range_previous + ctx->num_range_previous, claims, count * sizeof(*claims));
        ctx->num_range_previous += count;
        if (entry->parent->closed) {
            continue;
        }
        claims = nfs_lock_range_journal_current(entry->ranges, &count);
        chimera_nfs_abort_if(ctx->num_range_current + count > ctx->range_capacity,
                             "compound private range capacity");
        memcpy(ctx->range_current + ctx->num_range_current, claims, count * sizeof(*claims));
        ctx->num_range_current += count;
    }
} /* nfs4_vfs_range_view */

static void
nfs4_vfs_publish_states(
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx)
{
    struct nfs_state_table        *table  = &ctx->req->thread->shared->nfs4_state_table;
    struct chimera_vfs_thread     *thread = ctx->req->thread->vfs_thread;
    struct chimera_vfs_file_state *retired[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                       num_retired = 0;

    if (ctx->req->minorversion == 0) {
        for (uint32_t i = 0; i < ctx->num_groups; i++) {
            nfs_open_owner_publish_replay(ctx->groups[i]);
        }
        for (uint32_t i = 0; i < ctx->num_lock_groups; i++) {
            nfs_lock_owner_publish_replay(ctx->lock_groups[i]);
        }
    }

    /* A CLOSE retires even unchanged existing children. Their slots and lease
     * nodes remain pinned until parent teardown, but physical range coverage
     * must leave before a replacement parent/child identity is published. */
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock *entry = &ctx->locks[i];
        if (entry->active && entry->parent && entry->parent->closed) {
            struct chimera_vfs_file_state *file = nfs_lock_range_journal_retire(entry->ranges, thread);
            if (file) {
                retired[num_retired++] = file;
            }
        }
    }

    for (uint32_t i = 0; i < ctx->num_states; i++) {
        struct nfs4_vfs_state *entry = &ctx->states[i];
        struct nfs_open_state *state = &entry->shadow;
        if (!entry->active) {
            continue;
        }
        if (entry->closed) {
            if (entry->initially_public) {
                nfs_open_owner_close_compound(entry->group, entry->target, state->seqid, table, thread);
            }
        } else if (entry->last_open) {
            struct nfs4_vfs_op                   *map    = entry->last_open;
            struct nfs_open_state_compound_update update = {
                .fh             = state->fh,                                      .fh_len
                                = state->fh_len,
                .share_access   = state->share_access,                            .share_deny
                                = state->share_deny,
                .share_combos   = state->share_combos,                            .seqid
                                = state->seqid,
                .handle         = state->handle,
                .claim_file     = chimera_vfs_compound_take_reservation(compound, map->open_reservation),
                .admitted_claim = &map->open_claim,
            };
            chimera_nfs_abort_if(!update.claim_file, "accepted OPEN has no share reservation");
            chimera_vfs_dup_handle(thread, state->handle);
            nfs_open_owner_apply_compound(entry->group, entry->target, &update, table, thread);
        } else if (entry->version_dirty) {
            nfs_open_owner_confirm_compound(entry->group, entry->target, state->seqid);
        }
    }
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock           *entry = &ctx->locks[i];
        if (!entry->active || !entry->dirty || (entry->parent && entry->parent->closed)) {
            continue;
        }
        chimera_nfs_abort_if(!entry->parent || entry->parent->closed,
                             "accepted lock has no live parent");
        struct chimera_vfs_open_handle *handle = entry->parent->shadow.handle;
        chimera_vfs_dup_handle(thread, handle);
        nfs_lock_state_apply_compound(entry->group, entry->target, entry->parent->target,
                                      handle, entry->seqid, entry->ranges, table, thread);
    }
    for (uint32_t i = 0; i < num_retired; i++) {
        chimera_vfs_claim_replacement_complete(retired[i]);
        chimera_vfs_state_put(thread->vfs->vfs_state, retired[i]);
    }
} /* nfs4_vfs_publish_states */

static nfsstat4
nfs4_vfs_decode_attrs(
    struct nfs4_vfs_op       *map,
    const struct fattr4      *wire,
    struct chimera_vfs_attrs *attr)
{
    if (wire->num_attrmask && (wire->attrmask[0] & (1U << FATTR4_ACL))) {
        map->input_acl = malloc(chimera_acl_size(CHIMERA_ACL_MAX_ACES));
        if (!map->input_acl) {
            return NFS4ERR_RESOURCE;
        }
    }
    return chimera_nfs4_unmarshall_attrs(attr, wire->num_attrmask, wire->attrmask,
                                         wire->attr_vals.data, wire->attr_vals.len,
                                         map->input_acl, CHIMERA_ACL_MAX_ACES);
} /* nfs4_vfs_decode_attrs */

static struct nfs_lock_state *
nfs4_vfs_frozen_lock(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid)
{
    for (uint32_t i = 0; i < ctx->num_groups; i++) {
        struct nfs_open_owner_reservation *group = ctx->groups[i];
        for (uint32_t j = 0; j < group->num_locks; j++) {
            struct nfs_lock_state *state = group->locks[j];
            struct stateid4        identity;
            nfs4_stateid_encode(&identity, 0, NFS4_STATEID_TYPE_LOCK, state->shard,
                                state->slot_idx, state->generation, ctx->req->thread->shared->nfs4_state_table.epoch);
            if (!memcmp(identity.other, sid->other, sizeof(sid->other))) {
                return state;
            }
        }
    }
    return NULL;
} /* nfs4_vfs_frozen_lock */

static nfsstat4
nfs4_vfs_freeze_lock_parent(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid)
{
    struct nfs_request                *req   = ctx->req;
    struct nfs_state_table            *table = &req->thread->shared->nfs4_state_table;
    struct nfs_open_state             *parent;
    struct nfs_open_owner_reservation *group;
    void                              *state;
    uint8_t                            type, owner[NFS4_OPAQUE_LIMIT];
    uint16_t                           owner_len;
    nfsstat4                           status;

    if (nfs4_vfs_private_open(ctx, sid) || nfs4_vfs_frozen_lock(ctx, sid)) {
        return NFS4_OK;
    }
    if (ctx->num_groups == NFS4_VFS_COMPOUND_MAX_OPS) {
        return NFS4ERR_RESOURCE;
    }
    status = nfs_state_table_acquire_no_renew(table, sid, 0, &state, &type);
    if (status != NFS4_OK) {
        return status;
    }
    status = nfs_state_check_client(state, type, ctx->client);
    if (req->minorversion == 0 && status != NFS4_OK) {
        status = NFS4ERR_DELAY; /* use the state-derived legacy client path */
    }
    if (status == NFS4_OK && type != NFS4_SLOT_TYPE_OPEN && type != NFS4_SLOT_TYPE_LOCK) {
        status = NFS4ERR_BAD_STATEID;
    }
    if (status == NFS4_OK) {
        parent    = type == NFS4_SLOT_TYPE_OPEN ? state : ((struct nfs_lock_state *) state)->open_state;
        owner_len = parent->owner->owner_len;
        memcpy(owner, parent->owner->owner, owner_len);
    }
    nfs_state_table_release(table, state, type, req->thread->vfs_thread);
    if (status != NFS4_OK) {
        return status;
    }
    status = nfs_open_owner_reserve_compound_locks(ctx->client, owner, owner_len,
                                                   req->principal_flavor, req->principal_machinename, req->
                                                   principal_machinename_len,
                                                   ctx, 0, table, req->thread->vfs_thread, &group);
    if (status == NFS4_OK) {
        ctx->groups[ctx->num_groups++] = group;
    }
    return status;
} /* nfs4_vfs_freeze_lock_parent */

/* Reserve the connected parent set, not every state belonging to this client.
 * The caller also visits children of any newly appended OPEN-owner group. */
static int
nfs4_vfs_expand_lock_parents(
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx,
    const uint8_t                *owner,
    uint32_t                      owner_len)
{
    struct nfs_request *req = ctx->req;
    struct stateid4     parents[NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t            count;

    if (owner_len > NFS4_OPAQUE_LIMIT ||
        nfs_lock_owner_compound_parents(ctx->client, owner, owner_len,
                                        ctx, &req->thread->shared->nfs4_state_table, parents,
                                        NFS4_VFS_COMPOUND_MAX_OPS, &count) != NFS4_OK) {
        return 0;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (nfs4_vfs_freeze_lock_parent(ctx, &parents[i]) != NFS4_OK) {
            return 0;
        }
        uint32_t total = 0;
        for (uint32_t j = 0; j < ctx->num_groups; j++) {
            total += ctx->groups[j]->num_existing;
        }
        if (total > NFS4_VFS_COMPOUND_MAX_OPS) {
            return 0;
        }
        nfs4_vfs_attempt_reset(compound, ctx);
    }
    return 1;
} /* nfs4_vfs_expand_lock_parents */

static nfsstat4
nfs4_vfs_reserve_lock_owner(
    struct nfs4_vfs_compound_ctx       *ctx,
    const uint8_t                      *owner,
    uint32_t                            owner_len,
    uint32_t                            first,
    uint32_t                            end,
    struct nfs_lock_owner_reservation **out)
{
    struct nfs_request *req        = ctx->req;
    uint32_t            candidates = 0;

    for (uint32_t i = 0; i < ctx->num_lock_groups; i++) {
        struct nfs_lock_owner *lo = ctx->lock_groups[i]->owner;
        if (lo->owner_len == owner_len && !memcmp(lo->owner, owner, owner_len)) {
            *out = ctx->lock_groups[i];
            return NFS4_OK;
        }
    }
    for (uint32_t i = first; i < end; i++) {
        const struct nfs_argop4 *op = &req->args_compound->argarray[i];
        if (op->argop == OP_LOCK && op->oplock.locker.new_lock_owner &&
            op->oplock.locker.open_owner.lock_owner.owner.len == owner_len &&
            !memcmp(op->oplock.locker.open_owner.lock_owner.owner.data, owner, owner_len)) {
            candidates++;
        }
    }
    if (ctx->num_lock_groups >= NFS4_VFS_COMPOUND_MAX_OPS || owner_len > NFS4_OPAQUE_LIMIT) {
        return NFS4ERR_RESOURCE;
    }
    nfsstat4 status = nfs_lock_owner_reserve_compound(ctx->client,
                                                      owner, owner_len, ctx, candidates, &req->thread->shared->
                                                      nfs4_state_table,
                                                      req->thread->vfs_thread, out);
    if (status == NFS4_OK) {
        ctx->lock_groups[ctx->num_lock_groups++] = *out;
    }
    return status;
} /* nfs4_vfs_reserve_lock_owner */

static int
nfs4_vfs_op_encodable(uint32_t argop)
{
    switch (argop) {
        case OP_PUTFH:
        case OP_LOOKUP:
        case OP_LOOKUPP:
        case OP_GETATTR:
        case OP_ACCESS:
        case OP_GETFH:
        case OP_READLINK:
        case OP_SAVEFH:
        case OP_RESTOREFH:
        case OP_COMMIT:
        case OP_READDIR:
        case OP_GETXATTR:
        case OP_SETXATTR:
        case OP_LISTXATTRS:
        case OP_REMOVEXATTR:
        case OP_OPEN:
        case OP_CLOSE:
        case OP_OPEN_CONFIRM:
        case OP_OPEN_DOWNGRADE:
        case OP_TEST_STATEID:
        case OP_IO_ADVISE:
        case OP_SECINFO_NO_NAME:
        case OP_CREATE:
        case OP_REMOVE:
        case OP_RENAME:
        case OP_LINK:
        case OP_SETATTR:
        case OP_READ:
        case OP_READ_PLUS:
        case OP_WRITE:
        case OP_LOCKT:
        case OP_LOCK:
        case OP_LOCKU:
        case OP_SECINFO:
        case OP_ALLOCATE:
        case OP_DEALLOCATE:
        case OP_SEEK:
        case OP_WRITE_SAME:
        case OP_COPY:
        case OP_CLONE:
        case OP_VERIFY:
        case OP_NVERIFY:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_encodable */

/*
 * Does this op touch the reply buffer at a moment the two paths do not share?
 *
 * Results are marshalled in op order at the end, so the buffer fills the same
 * way it would have op by op -- including READDIR's, whose page is bounded by
 * the buffer's floor at exactly the point the per-op path would have met it.
 * The four xattr ops are the exception: GETXATTR, SETXATTR and REMOVEXATTR
 * stage their "user."-qualified name when the sequence is *built*, and GETXATTR
 * and LISTXATTRS decide how large an answer to ask for from the headroom at
 * that same moment -- before any result has been marshalled.  Each therefore
 * sees a different amount of free buffer than the op-at-a-time path would have
 * left it, and shifts the buffer under everything that follows.  The headroom
 * test in chimera_nfs4_compound_try_vfs is what stops either from changing an
 * answer.
 */


/* Legacy OPEN still ends a run. Reserved-owner paths perform their fallible
 * admission, verifier checks and truncate inside the VFS compound. */
static int
nfs4_vfs_op_ends_run(uint32_t argop)
{
    return argop == OP_OPEN;
} /* nfs4_vfs_op_ends_run */

/*
 * Is this an xattr name the sequence can carry?
 *
 * The per-op path stages the "user."-qualified name and reports NFS4ERR_INVAL
 * for an empty key and NFS4ERR_NAMETOOLONG for one that will not fit -- both
 * decided before any VFS call, so they belong to the per-op path.  Testing the
 * length here rather than staging the name and refusing afterwards matters: a
 * staged name cannot be given back to the reply buffer, and a refusal that had
 * consumed some of it would leave the per-op path with less to work with.
 */
static int
nfs4_vfs_xattr_name_ok(uint32_t wire_len)
{
    return wire_len > 0 &&
           wire_len <= CHIMERA_VFS_XATTR_NAME_MAX -
           CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
} /* nfs4_vfs_xattr_name_ok */

/*
 * An upper bound on the reply-buffer space one op can consume, staged or
 * filled.  Deliberately generous: it exists only to decide whether the buffer
 * is roomy enough that no size decision anywhere in the sequence can be
 * affected by how the two paths interleave their allocations.
 */
static uint64_t
nfs4_vfs_op_reply_bound(const struct nfs_argop4 *argop)
{
    /* Slack for the small fixed allocations around each result -- attrmask
     * arrays, opaque headers, and xdr_dbuf_alloc_space's own rounding. */
    const uint64_t slack = 512;

    switch (argop->argop) {
        case OP_GETATTR:
            /* Fixed portion only. ACL GETATTR reserves its additional bytes
             * from the actual snapshot during execution, before successors. */
            return 4096 + slack;
        case OP_TEST_STATEID:
            return (uint64_t) argop->optest_stateid.num_ts_stateids * sizeof(nfsstat4) + slack;
        case OP_LOCK:
        case OP_LOCKT:
            return NFS4_OPAQUE_LIMIT + slack;
        case OP_READLINK:
            return 4096 + slack;
        case OP_GETFH:
            return CHIMERA_NFS_FH_MAX + slack;
        case OP_READ:
            return sizeof(struct evpl_iovec) * NFS4_VFS_READ_MAX_IOV + slack;
        case OP_READ_PLUS:
            /* DATA uses a copying opaque, unlike the ordinary READ iovecs. */
            return (uint64_t) argop->opread_plus.rpa_count +
                   sizeof(struct evpl_iovec) * NFS4_VFS_READ_MAX_IOV +
                   sizeof(struct read_plus_content) + slack;
        case OP_READDIR:
            /* Entries are charged against maxcount, plus one entry's worth for
             * the candidate that is allocated and rolled back when it does not
             * fit -- that allocation must succeed, or the entry would be
             * refused for want of buffer rather than for want of maxcount. */
            return (uint64_t) argop->opreaddir.maxcount + 4096 + slack;
        case OP_GETXATTR:
            return (uint64_t) CHIMERA_NFS4_GETXATTR_MAX +
                   CHIMERA_VFS_XATTR_NAME_MAX + slack;
        case OP_LISTXATTRS:
            /* The staged name buffer, plus the result array that points into
            * it: at most one entry per two bytes of names, 16 bytes each. */
            return 9 * (uint64_t) argop->oplistxattrs.lxa_maxcount + slack;
        case OP_SETXATTR:
        case OP_REMOVEXATTR:
            return CHIMERA_VFS_XATTR_NAME_MAX + slack;
        default:
            return slack;
    } /* switch */
} /* nfs4_vfs_op_reply_bound */

/* The NFSv4 op a given VFS op carries the result of. */
static struct nfs4_vfs_op *
nfs4_vfs_op_by_vfs_res(
    struct nfs4_vfs_compound_ctx *ctx,
    uint32_t                      index)
{
    uint32_t k;

    for (k = 0; k < ctx->num_ops; k++) {
        if (ctx->ops[k].vfs_res >= 0 &&
            (uint32_t) ctx->ops[k].vfs_res == index) {
            return &ctx->ops[k];
        }
    }

    return NULL;
} /* nfs4_vfs_op_by_vfs_res */

/*
 * Discard whatever a previous attempt at this READDIR marshalled.
 *
 * Rewinding the reply buffer to the mark, rather than merely stopping at it,
 * is safe because during a sequence the entry fill is the ONLY thing that
 * allocates from that buffer: every other result is marshalled after the
 * sequence has finished, and what the build staged (the xattr names) it staged
 * before the sequence started.  So everything past the mark belongs to this op.
 */
static void
nfs4_vfs_readdir_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;
    struct nfs4_vfs_op           *map = nfs4_vfs_op_by_vfs_res(ctx, index);

    if (!map) {
        return;
    }

    if (map->readdir_have_mark) {
        ctx->req->encoding->dbuf->used = map->readdir_mark;
    } else {
        map->readdir_mark      = ctx->req->encoding->dbuf->used;
        map->readdir_have_mark = 1;
    }

    /* The fixed READDIR4resok overhead is charged before any entry:
     * cookieverf (8) plus the dirlist4 booleans (4 each). */
    map->readdir_cursor.count         = 16;
    map->readdir_cursor.entries       = NULL;
    map->readdir_cursor.last          = NULL;
    map->readdir_cursor.change_status = NFS4_OK;
} /* nfs4_vfs_readdir_reset */

/*
 * Marshal one entry, by the same code and at the same moment the per-op path
 * runs it from: inside the enumeration, while the backend still owns the
 * attributes it is handing over -- which is what lets a per-entry ACL be
 * encoded here at all.  maxcount is applied entry by entry through the cursor,
 * so the page ends on the entry that does not fit rather than at a count
 * guessed before anything was marshalled.
 */
static int
nfs4_vfs_readdir_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data)
{
    struct nfs4_vfs_compound_ctx         *ctx = private_data;
    struct nfs4_vfs_op                   *map = nfs4_vfs_op_by_vfs_res(ctx, index);
    const struct chimera_vfs_compound_op *vop;

    (void) inum;

    if (!map) {
        return -1;
    }

    vop = chimera_vfs_compound_op(compound, index);

    return chimera_nfs4_readdir_entry_fill(
        ctx->req,
        &ctx->req->args_compound->argarray[map->res_index].opreaddir,
        &map->readdir_cursor,
        vop->fh, (int) vop->fh_len,
        cookie, name, namelen, attrs);
} /* nfs4_vfs_readdir_append */

/*
 * Finish a READDIR whose page is already marshalled.  All that is left is what
 * the per-op path does in its own completion: the too-small judgement, the
 * cookie verifier, and pointing the result at the list.
 */
static void
nfs4_vfs_readdir_fill(
    struct READDIR4res                   *res,
    const struct chimera_vfs_compound_op *vop,
    const struct nfs4_vfs_op             *map)
{
    uint64_t cv;

    /* The too-small judgement (RFC 7530 16.24.4) is the gate's: a READDIR that
     * reaches here passed it. */
    cv = vop->r_verifier ? vop->r_verifier : vop->r_cookie;
    memcpy(res->resok4.cookieverf, &cv, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof     = vop->eof;
    res->resok4.reply.entries = map->readdir_cursor.entries;
} /* nfs4_vfs_readdir_fill */

/*
 * Does the object an exclusive create collided with carry this OPEN's own
 * verifier?  If so the create is a retry of one that already succeeded and the
 * OPEN succeeds against the existing object; if not, somebody else's file is in
 * the way (RFC 7530 §16.16.4).
 */
static int
nfs4_vfs_open_verifier_matches(
    const struct OPEN4args         *args,
    const struct chimera_vfs_attrs *attr)
{
    const uint8_t *verf;
    uint32_t       verf_atime, verf_mtime;

    verf = (args->openhow.how.mode == EXCLUSIVE4) ?
        args->openhow.how.createverf :
        args->openhow.how.ch_createboth.cva_verf;

    memcpy(&verf_atime, verf, sizeof(verf_atime));
    memcpy(&verf_mtime, verf + sizeof(verf_atime), sizeof(verf_mtime));

    return (attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) &&
           (attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) &&
           attr->va_atime.tv_sec == verf_atime &&
           attr->va_mtime.tv_sec == verf_mtime;
} /* nfs4_vfs_open_verifier_matches */

static int
nfs4_vfs_open_is_exclusive(const struct OPEN4args *args)
{
    return args->openhow.opentype == OPEN4_CREATE &&
           (args->openhow.how.mode == EXCLUSIVE4 ||
            args->openhow.how.mode == EXCLUSIVE4_1);
} /* nfs4_vfs_open_is_exclusive */

/*
 * Map a failed VFS op onto the status its NFSv4 operation reports.  Two
 * operations do not use the generic mapping: PUTFH turns a missing object into
 * NFS4ERR_STALE, and LISTXATTRS turns a too-small buffer into NFS4ERR_TOOSMALL
 * rather than the xattr-size error.
 */
static nfsstat4
nfs4_vfs_op_errno(
    const struct nfs_argop4              *ap,
    const struct chimera_vfs_compound_op *vop,
    struct nfs_request                   *req)
{
    enum chimera_vfs_error err   = vop->status;
    uint32_t               argop = ap->argop;

    if (argop == OP_PUTFH) {
        return chimera_nfs4_putfh_errno(err);
    }
    if (vop->type == CHIMERA_VFS_COMPOUND_OP_COORDINATE) {
        if (err == CHIMERA_VFS_EAGAIN) {
            return NFS4ERR_DELAY;
        }
        if (err == CHIMERA_VFS_ENOSPC) {
            return NFS4ERR_RESOURCE;
        }
    }

    /* LISTXATTRS and READDIR both turn a too-small buffer into
     * NFS4ERR_TOOSMALL rather than the generic size error -- READDIR's is set
     * by the gate, which is where its page is judged. */
    if ((argop == OP_LISTXATTRS || argop == OP_READDIR) &&
        err == CHIMERA_VFS_ERANGE) {
        return NFS4ERR_TOOSMALL;
    }

    /* An exclusive create whose re-open of the colliding object failed.  No
     * object that is not a regular file can be carrying the verifier an
     * exclusive create stamped, so whatever is in the way and however the
     * backend reported it, the answer the protocol wants is simply "something
     * else is already there" (RFC 7530 §16.16.4). */
    if (argop == OP_OPEN && vop->existed && nfs4_vfs_open_is_exclusive(&ap->opopen)) {
        return NFS4ERR_EXIST;
    }

    /* An OPEN refused by the type gate: the VFS reports the nearest POSIX
     * answer, but NFSv4 distinguishes a directory from a symlink from any other
     * special file, and differently in each minor version, so the mode the
     * refusal carried is what decides it.  A mode is recorded only by the
     * resolve step, which runs only when the type gate was asked for, so a
     * non-regular one here means the gate is what refused. */
    if (argop == OP_OPEN && vop->existed && vop->existing_mode &&
        !S_ISREG(vop->existing_mode)) {
        return chimera_nfs4_open_nonreg_status(req->minorversion,
                                               vop->existing_mode);
    }

    /* A checked filehandle OPEN can reject a special inode before it returns
     * attributes. Match the ordinary OPEN path's special-file status. */
    if (argop == OP_OPEN && err == CHIMERA_VFS_ENXIO) {
        return req->minorversion ? NFS4ERR_WRONG_TYPE : NFS4ERR_INVAL;
    }

    /* I/O the executor refused on the object's type, before it opened it for
     * data.  It reports the nearest POSIX answer; NFSv4 has its own. */
    if ((argop == OP_READ || argop == OP_READ_PLUS || argop == OP_WRITE) && vop->existing_mode &&
        !S_ISREG(vop->existing_mode)) {
        return chimera_nfs4_data_nonreg_status(vop->existing_mode);
    }

    return chimera_nfs4_errno_to_nfsstat4(err);
} /* nfs4_vfs_op_errno */

static nfsstat4
nfs4_vfs_lockt_fill(
    struct nfs_request       *req,
    const struct nfs4_vfs_op *map,
    struct LOCKT4res         *res)
{
    const struct chimera_vfs_claim_conflict *conflict = &map->lockt_conflict;

    res->status = map->verify_status;
    if (res->status == NFS4ERR_DENIED) {
        res->denied.offset   = conflict->offset;
        res->denied.length   = conflict->length;
        res->denied.locktype =
            (conflict->used & (CHIMERA_CLAIM_W | CHIMERA_CLAIM_CW |
                               CHIMERA_CLAIM_LW)) ? WRITE_LT : READ_LT;
        res->denied.owner.clientid   = conflict->owner.client_key;
        res->denied.owner.owner.len  = map->lockt_owner_len;
        res->denied.owner.owner.data = NULL;
        if (map->lockt_owner_len) {
            res->denied.owner.owner.data =
                xdr_dbuf_alloc_space(map->lockt_owner_len, req->encoding->dbuf);
            chimera_nfs_abort_if(res->denied.owner.owner.data == NULL,
                                 "Failed to allocate lock owner");
            memcpy(res->denied.owner.owner.data, map->lockt_owner,
                   map->lockt_owner_len);
        }
    }
    return res->status;
} /* nfs4_vfs_lockt_fill */

/* Fill one NFSv4 result from the VFS ops that produced it. */
static nfsstat4
nfs4_vfs_op_fill(
    struct nfs_request           *req,
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs4_vfs_op           *map,
    struct nfs_argop4            *argop,
    struct nfs_resop4            *resop)
{
    const struct chimera_vfs_compound_op *vop =
        chimera_vfs_compound_op(compound, (uint32_t) map->vfs_res);
    nfsstat4                              status;
    uint32_t                              requested;
    void                                 *names;

    if (map->open_response && argop->argop == OP_OPEN) {
        uint32_t *attributes = xdr_dbuf_alloc_space(3 * sizeof(*attributes), req->encoding->dbuf);
        uint8_t  *who        = map->open_response->open.delegation_who_len ?
            xdr_dbuf_alloc_space(map->open_response->open.delegation_who_len, req->encoding->dbuf) : NULL;
        uint32_t  length;
        chimera_nfs_abort_if(!attributes || (map->open_response->open.delegation_who_len && !who) ||
                             !nfs4_replay_fill_open(map->open_response, &resop->opopen,
                                                    attributes, req->fh, &length, who),
                             "accepted OPEN replay lost its snapshot");
        req->fhlen = length;
        return resop->opopen.status;
    }

    switch (argop->argop) {
        case OP_OPEN:
        {
            struct OPEN4args               *oargs = &argop->opopen;
            struct OPEN4res                *ores  = &resop->opopen;
            struct chimera_vfs_open_handle *handle;
            uint32_t                        install_rflags = 0;
            int                             rc;

            if (vop->existed && nfs4_vfs_open_is_exclusive(oargs)) {
                /* An exclusive create that collided.  The object was opened so
                 * that this one question could be asked of it, and the answer
                 * settles the OPEN by itself: carrying this verifier is what
                 * makes the object ours, and nothing else about it matters.
                 *
                 * In particular its TYPE does not: no object that is not a
                 * regular file can be carrying a verifier an exclusive create
                 * stamped, so a directory or a symlink here is not a type
                 * error, it is just somebody else's name (RFC 7530 §16.16.4).
                 */
                if (!nfs4_vfs_open_verifier_matches(oargs, &vop->attr)) {
                    return NFS4ERR_EXIST;
                }
            } else if ((vop->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
                       !S_ISREG(vop->attr.va_mode)) {
                /* RFC 7530 §16.16.6 / RFC 8881 §18.16.4: OPEN targets a regular
                 * file.  The type gate before the open catches this for the
                 * modes that ask for it; a GUARDED4 create does not, so the
                 * object it opened is classified here, exactly as the per-op
                 * path does on its own open completion. */
                return chimera_nfs4_open_nonreg_status(req->minorversion,
                                                       vop->attr.va_mode);
            }

            handle = map->reserved_open ? vop->out_handle :
                chimera_vfs_compound_take_handle(compound, (uint32_t) map->vfs_res);

            if (!handle) {
                return NFS4ERR_SERVERFAULT;
            }

            /* install_state and everything after it -- the delegation offer,
             * the completion, the 4.0 seqid advance -- read the OPEN's
             * arguments and result through req->index.  The fill loop has not
             * moved it yet, so move it here. Later fills use their own mapped
             * arguments; the legacy OPEN tail still ends its sequence. */
            req->index = (int) map->res_index;

            /* Capture the file handle before install_state, which may release
             * the handle when it coalesces onto an existing open state. */
            memcpy(req->fh, handle->fh, handle->fh_len);
            req->fhlen = handle->fh_len;

            /* From here the two paths are the same code: the object is open
             * and its attributes are in hand, which is all install_state ever
             * needed.  It owns the handle now, including releasing it on every
             * failure.
             *
             * An open-by-handle reports no attributes, and install_state reads
             * that as "access was established when this filehandle was
             * resolved" and skips the check -- which is what it must not be
             * told by an empty attribute set that looks like a real one. */
            if (map->reserved_open) {
                ores->resok4.stateid = map->reserved_stateid;
                chimera_nfs4_set_current_stateid(req, &map->reserved_stateid);
                status = NFS4_OK;
            } else {
                status = chimera_nfs4_open_install_state(req, handle,
                                                         map->open_by_name ?
                                                         &vop->attr : NULL,
                                                         vop->created,
                                                         NULL, 0,
                                                         &ores->resok4.stateid,
                                                         &install_rflags);

            }

            if (status != NFS4_OK) {
                return status;
            }

            ores->status        = NFS4_OK;
            ores->resok4.rflags = install_rflags |
                OPEN4_RESULT_LOCKTYPE_POSIX;
            ores->resok4.num_attrset = 0;

            /* Which of the requested attributes the create actually applied.
             * set_attr is the executor's copy, which it blanks when the name
             * resolved to something that already existed -- so an open that
             * created nothing reports nothing set, which is what the per-op
             * path reports for the same reason.
             *
             * The requested set lives in a different arm of openhow.how for
             * each create mode, and EXCLUSIVE4 has none at all: its verifier
             * occupies that slot, so reading it as an attribute request would
             * be reading the verifier's bytes as an attribute mask. */
            if (oargs->openhow.opentype == OPEN4_CREATE &&
                oargs->openhow.how.mode != EXCLUSIVE4) {
                struct chimera_vfs_attrs applied = vop->applied_attr;
                uint32_t                 n_mask;
                uint32_t                *mask;

                if (oargs->openhow.how.mode == EXCLUSIVE4_1) {
                    n_mask = oargs->openhow.how.ch_createboth.cva_attrs.num_attrmask;
                    mask   = oargs->openhow.how.ch_createboth.cva_attrs.attrmask;
                } else {
                    n_mask = oargs->openhow.how.createattrs.num_attrmask;
                    mask   = oargs->openhow.how.createattrs.attrmask;
                }

                rc = xdr_dbuf_alloc_array(&ores->resok4, attrset, 4,
                                          req->encoding->dbuf);
                chimera_nfs_abort_if(rc, "Failed to allocate array");

                ores->resok4.num_attrset = chimera_nfs4_mask2attr(
                    &applied, n_mask, mask, ores->resok4.attrset);
            }

            if (map->open_by_name) {
                struct chimera_vfs_attrs pre  = vop->dir_pre_attr;
                struct chimera_vfs_attrs post = vop->dir_post_attr;

                chimera_nfs4_set_changeinfo(&ores->resok4.cinfo, &pre, &post);
            } else {
                /* An open-by-handle changed no directory. */
                ores->resok4.cinfo.atomic = 0;
                ores->resok4.cinfo.before = 0;
                ores->resok4.cinfo.after  = 0;
            }

            /* An UNCHECKED4 size-0 create of a name that was already there.
             * Applied by chimera_nfs4_open_complete, after the share
             * reservation is held, so an OPEN that fails does not empty the
             * file on its way to failing. */
            req->open_trunc_pending = !map->reserved_open &&
                map->open_trunc_if_existed && vop->existed;

            /* The rest of the OPEN -- the delegation offer and that truncate --
             * can suspend, so it runs once the sequence has been freed.  Copy
             * out what it needs; vop does not outlive the compound. */
            ctx->open_filled   = !map->reserved_open;
            ctx->open_has_attr = map->open_by_name;
            ctx->open_attr     = vop->attr;

            return NFS4_OK;
        }


        case OP_OPEN_CONFIRM:
            resop->opopen_confirm.status              = NFS4_OK;
            resop->opopen_confirm.resok4.open_stateid = map->reserved_stateid;
            return NFS4_OK;

        case OP_OPEN_DOWNGRADE:
            resop->opopen_downgrade.status              = NFS4_OK;
            resop->opopen_downgrade.resok4.open_stateid = map->reserved_stateid;
            return NFS4_OK;

        case OP_TEST_STATEID:
            resop->optest_stateid.tsr_status                      = NFS4_OK;
            resop->optest_stateid.tsr_resok4.num_tsr_status_codes = argop->optest_stateid.num_ts_stateids;
            resop->optest_stateid.tsr_resok4.tsr_status_codes     = map->test_stateid_statuses;
            return NFS4_OK;

        case OP_LOCK:
            resop->oplock.status              = NFS4_OK;
            resop->oplock.resok4.lock_stateid = map->reserved_stateid;
            return NFS4_OK;
        case OP_LOCKU:
            resop->oplocku.status       = NFS4_OK;
            resop->oplocku.lock_stateid = map->reserved_stateid;
            return NFS4_OK;

        case OP_IO_ADVISE:
            resop->opio_advise.ior_status           = NFS4_OK;
            resop->opio_advise.resok4.num_ior_hints = 0;
            resop->opio_advise.resok4.ior_hints     = NULL;
            return NFS4_OK;

        case OP_CLOSE:
            resop->opclose.status       = NFS4_OK;
            resop->opclose.open_stateid = map->close_stateid;
            chimera_nfs4_clear_current_stateid(req);
            return NFS4_OK;

        case OP_CREATE:
        {
            struct CREATE4args      *cargs   = &argop->opcreate;
            struct CREATE4res       *cres    = &resop->opcreate;
            struct chimera_vfs_attrs applied = vop->applied_attr;
            struct chimera_vfs_attrs pre     = vop->dir_pre_attr;
            struct chimera_vfs_attrs post    = vop->dir_post_attr;

            cres->status         = NFS4_OK;
            cres->resok4.attrset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t),
                                                        req->encoding->dbuf);
            chimera_nfs_abort_if(cres->resok4.attrset == NULL,
                                 "Failed to allocate space");
            cres->resok4.num_attrset = chimera_nfs4_mask2attr(
                &applied,
                cargs->createattrs.num_attrmask,
                cargs->createattrs.attrmask,
                cres->resok4.attrset);

            chimera_nfs4_set_changeinfo(&cres->resok4.cinfo, &pre, &post);
            return NFS4_OK;
        }

        case OP_READ:
        {
            struct READ4res   *rdres = &resop->opread;
            struct evpl_iovec *riov;
            int                rniov;

            chimera_vfs_compound_take_iov(compound, (uint32_t) map->vfs_res,
                                          &riov, &rniov);

            rdres->status             = NFS4_OK;
            rdres->resok4.eof         = vop->eof_read;
            rdres->resok4.data.length = vop->read_len;
            rdres->resok4.data.niov   = rniov;
            rdres->resok4.data.iov    = riov;
            return NFS4_OK;
        }

        case OP_READ_PLUS:
        {
            struct READ_PLUS4res                 *res  = &resop->opread_plus;
            const struct chimera_vfs_compound_op *data =
                chimera_vfs_compound_op(compound, map->read_plus_data);
            struct read_plus_content             *content;
            uint32_t                              length = vop->is_data ? data->read_len : vop->read_len;

            res->rp_status         = NFS4_OK;
            res->rp_resok4.rpr_eof = vop->is_data && vop->read_len ?
                data->eof_read : vop->eof_read;
            res->rp_resok4.num_rpr_contents = 0;
            res->rp_resok4.rpr_contents     = NULL;
            if (!length) {
                return NFS4_OK;
            }
            content = xdr_dbuf_alloc_space(sizeof(*content), req->encoding->dbuf);
            chimera_nfs_abort_if(content == NULL, "Failed to allocate space");
            memset(content, 0, sizeof(*content));
            if (vop->is_data) {
                struct evpl_iovec_cursor cursor;
                content->rpc_content       = NFS4_CONTENT_DATA;
                content->rpc_data.d_offset = argop->opread_plus.rpa_offset;
                chimera_nfs_abort_if(
                    xdr_dbuf_alloc_opaque(&content->rpc_data.d_data, length,
                                          req->encoding->dbuf) != 0,
                    "Failed to allocate space");
                content->rpc_data.d_data.len = length;
                evpl_iovec_cursor_init(&cursor, data->iov, data->niov);
                evpl_iovec_cursor_copy(&cursor, content->rpc_data.d_data.data, length);
            } else {
                content->rpc_content        = NFS4_CONTENT_HOLE;
                content->rpc_hole.di_offset = argop->opread_plus.rpa_offset;
                content->rpc_hole.di_length = length;
            }
            res->rp_resok4.num_rpr_contents = 1;
            res->rp_resok4.rpr_contents     = content;
            return NFS4_OK;
        }

        case OP_WRITE:
        {
            struct WRITE4res *wrres = &resop->opwrite;

            wrres->status       = NFS4_OK;
            wrres->resok4.count = vop->written;
            /* Achieved durability, which the backend may report as more than
             * was asked for. */
            wrres->resok4.committed = vop->committed;
            memcpy(wrres->resok4.writeverf, &req->thread->shared->nfs_verifier,
                   sizeof(wrres->resok4.writeverf));
            return NFS4_OK;
        }

        case OP_SECINFO:
        case OP_SECINFO_NO_NAME:
        {
            struct SECINFO4res              *sires = argop->argop == OP_SECINFO ?
                &resop->opsecinfo : &resop->opsecinfo_no_name;
            const struct chimera_nfs_export *export;

            /* Named lookup and same-export current/parent queries share the
             * current export's security policy. */
            export = chimera_nfs_get_export_by_id(req->thread->shared,
                                                  req->export_id);

            sires->resok4 = xdr_dbuf_alloc_space(4 * sizeof(struct secinfo4),
                                                 req->encoding->dbuf);
            chimera_nfs_abort_if(sires->resok4 == NULL,
                                 "Failed to allocate space");

            sires->num_resok4 = chimera_nfs_fill_secinfo(
                sires->resok4,
                export ? export->sec_allowed : 0,
                req->thread->shared->gss_enabled);

            sires->status = NFS4_OK;
            return NFS4_OK;
        }

        case OP_LOCKT:
            return nfs4_vfs_lockt_fill(req, map, &resop->oplockt);

        case OP_SETATTR:
        {
            struct SETATTR4args     *sargs   = &argop->opsetattr;
            struct SETATTR4res      *sres    = &resop->opsetattr;
            struct chimera_vfs_attrs applied = vop->applied_attr;

            sres->status   = NFS4_OK;
            sres->attrsset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t),
                                                  req->encoding->dbuf);
            chimera_nfs_abort_if(sres->attrsset == NULL,
                                 "Failed to allocate space");
            sres->num_attrsset = chimera_nfs4_mask2attr(
                &applied,
                sargs->obj_attributes.num_attrmask,
                sargs->obj_attributes.attrmask,
                sres->attrsset);
            return NFS4_OK;
        }

        case OP_REMOVE:
        {
            struct REMOVE4res       *rres = &resop->opremove;
            struct chimera_vfs_attrs pre  = vop->dir_pre_attr;
            struct chimera_vfs_attrs post = vop->dir_post_attr;

            rres->status = NFS4_OK;
            /* change_info4 for the parent directory (RFC 7530 §16.25.5). */
            chimera_nfs4_set_changeinfo(&rres->resok4.cinfo, &pre, &post);
            return NFS4_OK;
        }

        case OP_RENAME:
        {
            struct RENAME4res       *rres      = &resop->oprename;
            struct chimera_vfs_attrs from_pre  = vop->from_dir_pre_attr;
            struct chimera_vfs_attrs from_post = vop->from_dir_post_attr;
            struct chimera_vfs_attrs to_pre    = vop->dir_pre_attr;
            struct chimera_vfs_attrs to_post   = vop->dir_post_attr;

            rres->status = NFS4_OK;
            /* Two change_info4s, because a rename changes two directories
             * (RFC 7530 SS16.27.5).  The VFS op reports both for the same
             * reason, so each half comes straight across. */
            chimera_nfs4_set_changeinfo(&rres->resok4.source_cinfo,
                                        &from_pre, &from_post);
            chimera_nfs4_set_changeinfo(&rres->resok4.target_cinfo,
                                        &to_pre, &to_post);
            return NFS4_OK;
        }

        case OP_LINK:
        {
            struct LINK4res         *lres = &resop->oplink;
            struct chimera_vfs_attrs pre  = vop->dir_pre_attr;
            struct chimera_vfs_attrs post = vop->dir_post_attr;

            lres->status = NFS4_OK;
            /* One directory changed: the one the new name went into
             * (RFC 7530 SS16.9.5). */
            chimera_nfs4_set_changeinfo(&lres->resok4.cinfo, &pre, &post);
            return NFS4_OK;
        }

        case OP_PUTFH:
            /* The staleness rule already ran as the precheck; nothing else in
             * a PUTFH4res but its status. */
            resop->opputfh.status = NFS4_OK;
            return NFS4_OK;

        case OP_LOOKUP:
            /* LOOKUP4res carries only a status; the object it resolved is the
             * sequence's current filehandle. */
            resop->oplookup.status = NFS4_OK;
            return NFS4_OK;

        case OP_LOOKUPP:
            /* As LOOKUP: the parent it resolved is the current filehandle. */
            resop->oplookupp.status = NFS4_OK;
            return NFS4_OK;

        case OP_SAVEFH:
            /* Save the object the sequence had current when the SAVEFH ran, so
             * a RESTOREFH -- here or, in principle, later -- finds it. */
            chimera_nfs4_savefh_apply(req, vop->fh, (int) vop->fh_len);
            resop->opsavefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_RESTOREFH:
            /* The executor has already made the saved object current; this
             * restores the request-side bookkeeping that rides with it.
             * Both inherited and locally saved slots are restricted to the
             * export and squash policy already in force for this run. */
            chimera_nfs4_restorefh_apply(req);
            resop->oprestorefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_COMMIT:
            /* The regular-file rule already ran as the precheck; this re-tests
             * it against the attributes the flush itself reported, as the
             * per-op completion does, and stamps the write verifier. */
            status = chimera_nfs4_commit_fill(req, &resop->opcommit,
                                              &vop->attr);
            resop->opcommit.status = status;
            return status;

        case OP_VERIFY:
        case OP_NVERIFY:
            /* A mismatch failed the op in the gate; reaching here is a match. */
            resop->opverify.status = NFS4_OK;
            return NFS4_OK;

        case OP_WRITE_SAME:
            resop->opwrite_same.resok4.num_wr_callback_id = 0;
            resop->opwrite_same.resok4.wr_callback_id     = NULL;
            resop->opwrite_same.resok4.wr_count           = vop->written;
            resop->opwrite_same.resok4.wr_committed       = vop->committed;
            memcpy(resop->opwrite_same.resok4.wr_writeverf,
                   &req->thread->shared->nfs_verifier,
                   sizeof(resop->opwrite_same.resok4.wr_writeverf));
            resop->opwrite_same.wsr_status = NFS4_OK;
            return NFS4_OK;

        case OP_COPY:
            resop->opcopy.cr_status                                = NFS4_OK;
            resop->opcopy.cr_resok4.cr_response.num_wr_callback_id = 0;
            resop->opcopy.cr_resok4.cr_response.wr_callback_id     = NULL;
            resop->opcopy.cr_resok4.cr_response.wr_count           = vop->written;
            resop->opcopy.cr_resok4.cr_response.wr_committed       = FILE_SYNC4;
            memcpy(resop->opcopy.cr_resok4.cr_response.wr_writeverf,
                   &req->thread->shared->nfs_verifier,
                   sizeof(resop->opcopy.cr_resok4.cr_response.wr_writeverf));
            resop->opcopy.cr_resok4.cr_requirements.cr_consecutive = false;
            resop->opcopy.cr_resok4.cr_requirements.cr_synchronous = true;
            return NFS4_OK;

        case OP_CLONE:
            resop->opclone.cl_status = NFS4_OK;
            return NFS4_OK;

        case OP_ALLOCATE:
            resop->opallocate.ar_status = NFS4_OK;
            return NFS4_OK;

        case OP_DEALLOCATE:
            resop->opdeallocate.dr_status = NFS4_OK;
            return NFS4_OK;

        case OP_SEEK:
            resop->opseek.resok4.sr_eof    = vop->seek_eof;
            resop->opseek.resok4.sr_offset = vop->seek_offset;
            resop->opseek.sa_status        = NFS4_OK;
            return NFS4_OK;

        case OP_READDIR:
            nfs4_vfs_readdir_fill(&resop->opreaddir, vop, map);
            status                  = NFS4_OK;
            resop->opreaddir.status = status;
            return status;

        case OP_GETXATTR:
            status = chimera_nfs4_getxattr_fill(req, &resop->opgetxattr,
                                                vop->buffer, vop->buffer_len);
            resop->opgetxattr.gxr_status = status;
            return status;

        case OP_SETXATTR:
            chimera_nfs4_setxattr_fill(&resop->opsetxattr, &vop->pre_ctime,
                                       &vop->post_ctime);
            resop->opsetxattr.sxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_LISTXATTRS:
            /* The result entries point into the name buffer instead of copying
             * each name out of it, so it has to outlive the reply -- and the
             * sequence's buffer is freed the moment this callback returns.
             * Restage the names where the per-op path keeps them, in the reply
             * buffer, before pointing anything at them. */
            names = xdr_dbuf_alloc_space(vop->buffer_len ? vop->buffer_len : 1,
                                         req->encoding->dbuf);

            if (!names) {
                resop->oplistxattrs.lxr_status = NFS4ERR_RESOURCE;
                return NFS4ERR_RESOURCE;
            }

            memcpy(names, vop->buffer, vop->buffer_len);

            status = chimera_nfs4_listxattrs_fill(req, &resop->oplistxattrs,
                                                  names,
                                                  vop->buffer_count,
                                                  vop->eof, vop->r_cookie);
            resop->oplistxattrs.lxr_status = status;
            return status;

        case OP_REMOVEXATTR:
            chimera_nfs4_removexattr_fill(&resop->opremovexattr,
                                          &vop->pre_ctime, &vop->post_ctime);
            resop->opremovexattr.rxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_GETATTR:
            if (map->getattr_staged) {
                resop->opgetattr = map->getattr_result;
                return resop->opgetattr.status;
            }
            status = chimera_nfs4_getattr_fill(req, &argop->opgetattr,
                                               &resop->opgetattr, &vop->attr,
                                               vop->fh, (int) vop->fh_len, false);
            resop->opgetattr.status = status;
            return status;

        case OP_ACCESS:
            requested = chimera_nfs4_access_requested(req, &argop->opaccess,
                                                      &vop->attr, vop->fh,
                                                      (int) vop->fh_len);
            /* The executor evaluated the client's whole request against the
             * object's ACL (or mode) while it held it; the mapping back is
             * limited to the bits this server says it evaluated. */
            chimera_nfs4_access_fill(req, &resop->opaccess, requested,
                                     vop->granted, &vop->attr);
            return NFS4_OK;

        case OP_GETFH:
            status = chimera_nfs4_getfh_fill(req, &resop->opgetfh,
                                             vop->fh, (int) vop->fh_len);
            resop->opgetfh.status = status;
            return status;

        case OP_READLINK:
            status = chimera_nfs4_readlink_fill(req, &resop->opreadlink,
                                                vop->target, vop->target_len);
            resop->opreadlink.status = status;
            return status;

        default:
            return NFS4ERR_SERVERFAULT;
    } /* switch */
} /* nfs4_vfs_op_fill */

/*
 * Give back everything the sequence borrowed on the caller's behalf: the
 * handles its I/O ops resolved from stateids, and the payload of any WRITE it
 * carried.
 *
 * A WRITE's payload is released here rather than in its fill, because it has to
 * be released whether or not the fill ran -- the op may have failed, or the
 * sequence may have stopped in front of it.  Zeroing niov is what stops the
 * dispatcher's own sweep of undispatched WRITEs from releasing it a second
 * time.
 */
static void
nfs4_vfs_compound_give_back(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_vfs_compound_ctx     *ctx)
{
    struct nfs_request *req = ctx->req;
    uint32_t            k;

    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs_argop4 *argop =
            &req->args_compound->argarray[ctx->ops[k].res_index];

        if (ctx->ops[k].io_handle) {
            chimera_vfs_release(thread->vfs_thread, ctx->ops[k].io_handle);
            ctx->ops[k].io_handle = NULL;
        }
        if (ctx->ops[k].range_src_handle) {
            chimera_vfs_release(thread->vfs_thread, ctx->ops[k].range_src_handle);
            ctx->ops[k].range_src_handle = NULL;
        }

        if (argop->argop == OP_WRITE && argop->opwrite.data.niov) {
            evpl_iovecs_release(thread->evpl, argop->opwrite.data.iov,
                                argop->opwrite.data.niov);
            argop->opwrite.data.niov = 0;
        }
    }
} /* nfs4_vfs_compound_give_back */

/*
 * The NFSv4-side rules that decide whether an operation may proceed, applied as
 * each op finishes rather than when its result is filled.
 *
 * PUTFH's staleness rule is the one that has to be here.  A file handle is
 * stale when the object has no names left and no client holds it open -- and
 * only the server knows the second half, so the VFS cannot answer it.  Applied
 * at fill time it came too late: everything behind the PUTFH had already run,
 * which was harmless while the sequence only read and stopped being harmless as
 * soon as it could rename.
 *
 * It answers from the client table, which is memory the server already holds:
 * no I/O, no waiting, and nothing remembered, so asking it twice asks the same
 * question rather than taking a step twice.
 */
static void
nfs4_vfs_exclude_state(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs_open_state        *state)
{
    for (uint32_t i = 0; i < ctx->num_closed_states; i++) {
        if (ctx->closed_states[i] == state) {
            return;
        }
    }
    chimera_nfs_abort_if(ctx->num_closed_states >= 2 * NFS4_VFS_COMPOUND_MAX_OPS,
                         "compound state exclusion overflow");
    ctx->closed_states[ctx->num_closed_states++] = state;
} /* nfs4_vfs_exclude_state */

static void
nfs4_vfs_exclude_claim(
    struct nfs4_vfs_compound_ctx   *ctx,
    const struct chimera_vfs_claim *claim)
{
    for (uint32_t i = 0; i < ctx->num_closed_claims; i++) {
        if (ctx->closed_claims[i] == claim) {
            return;
        }
    }
    chimera_nfs_abort_if(ctx->num_closed_claims >= 2 * NFS4_VFS_COMPOUND_MAX_OPS,
                         "compound claim exclusion overflow");
    ctx->closed_claims[ctx->num_closed_claims++] = claim;
} /* nfs4_vfs_exclude_claim */

/* Rebuild, rather than append, after each state transition. Public states and
 * prior attempt claims remain visible to peers; this attempt sees only its
 * current journal grant. A tentative downgrade also excludes its predecessor
 * while acquiring the replacement claim, before committing the journal. */
static void
nfs4_vfs_rebuild_state_view(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs4_vfs_state        *replacing)
{
    ctx->num_closed_states = ctx->num_closed_claims = 0;
    for (uint32_t i = 0; i < ctx->num_states; i++) {
        struct nfs4_vfs_state *entry = &ctx->states[i];
        if (!entry->active || (!entry->closed && !entry->last_open && entry != replacing)) {
            continue;
        }
        if (entry->initially_public) {
            nfs4_vfs_exclude_state(ctx, entry->target);
            if (entry->target->share_claim_held) {
                nfs4_vfs_exclude_claim(ctx, &entry->target->share_claim);
            }
        }
    }
    for (uint32_t i = 0; i < ctx->num_ops; i++) {
        struct nfs4_vfs_op *map = &ctx->ops[i];
        if (map->open_committed && map->open_state &&
            (map->open_state->closed || map->open_state == replacing || map->open_state->last_open != map)) {
            nfs4_vfs_exclude_claim(ctx, &map->open_claim);
        }
    }
} /* nfs4_vfs_rebuild_state_view */

static nfsstat4
nfs4_vfs_share_authorize(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs_open_owner        *owner,
    const uint8_t                *fh,
    uint16_t                      fh_len,
    uint32_t                      access,
    uint32_t                      deny)
{
    nfsstat4 status = nfs_client_check_share_conflict_except(owner->client, owner, fh, fh_len,
                                                             access, deny, ctx->closed_states, ctx->num_closed_states);

    if (status != NFS4_OK) {
        return status;
    }
    for (uint32_t i = 0; i < ctx->num_states; i++) {
        struct nfs4_vfs_state *entry = &ctx->states[i];
        struct nfs_open_state *peer  = &entry->shadow;
        if (entry->active && !entry->closed && peer->owner != owner && peer->fh_len == fh_len &&
            !memcmp(peer->fh, fh, fh_len) && ((peer->share_access & deny) || (peer->share_deny & access))) {
            return NFS4ERR_SHARE_DENIED;
        }
    }
    return NFS4_OK;
} /* nfs4_vfs_share_authorize */

static void
nfs4_vfs_open_checked(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        return;
    }

    struct nfs4_vfs_op                   *map   = private_data;
    struct nfs4_vfs_compound_ctx         *ctx   = map->ctx;
    const struct chimera_vfs_compound_op *op    = chimera_vfs_compound_op(compound, index);
    struct nfs_open_state                *state = map->reserved_open;
    struct nfs_request                   *req   = ctx->req;
    nfsstat4                              error = NFS4_OK;
    const struct OPEN4args               *oa    = &req->args_compound->argarray[map->res_index].opopen;
    struct nfs4_vfs_state                *entry = NULL;

    state->share_access = oa->share_access & OPEN4_SHARE_ACCESS_BOTH;
    state->share_deny   = oa->share_deny;
    state->share_combos = nfs_open_combo_bit(state->share_access, state->share_deny);

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (op->existed && nfs4_vfs_open_is_exclusive(oa)) {
        if (!nfs4_vfs_open_verifier_matches(oa, &op->attr)) {
            error = NFS4ERR_EXIST;
        }
    } else if ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
               !S_ISREG(op->attr.va_mode)) {
        error = chimera_nfs4_open_nonreg_status(req->minorversion, op->attr.va_mode);
    }
    if (error == NFS4_OK && map->open_by_name && !op->created) {
        uint32_t required = 0;
        if (state->share_access & OPEN4_SHARE_ACCESS_READ) {
            required |= CHIMERA_ACE_READ_DATA;
        }
        if (state->share_access & OPEN4_SHARE_ACCESS_WRITE) {
            required |= CHIMERA_ACE_WRITE_DATA;
        }
        if (!chimera_vfs_access_allowed(&op->attr, &req->cred, required)) {
            error = NFS4ERR_ACCESS;
        }
    }
    if (error == NFS4_OK) {
        error = nfs4_vfs_share_authorize(ctx, state->owner, op->fh, op->fh_len,
                                         state->share_access, state->share_deny);
    }
    if (error != NFS4_OK) {
        map->verify_status = error;
        *status            = CHIMERA_VFS_EINVAL;
        return;
    }
    for (uint32_t i = 0; i < ctx->num_states; i++) {
        struct nfs4_vfs_state *prior = &ctx->states[i];
        if (prior->active && !prior->closed && prior->group == map->open_group &&
            prior->shadow.fh_len == op->fh_len && !memcmp(prior->shadow.fh, op->fh, op->fh_len)) {
            entry = prior;
            break;
        }
    }
    if (entry) {
        struct nfs_open_state *prior = &entry->shadow;
        /* Preserve the legacy same-owner self-conflict rule. */
        if ((prior->share_access & state->share_deny) || (state->share_access & prior->share_deny)) {
            map->verify_status = NFS4ERR_SHARE_DENIED;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        state->share_access |= prior->share_access;
        state->share_deny   |= prior->share_deny;
        state->share_combos |= prior->share_combos;
        state->seqid         = prior->seqid + 1;
    } else {
        chimera_nfs_abort_if(ctx->num_states >= 2 * NFS4_VFS_COMPOUND_MAX_OPS,
                             "compound state journal overflow");
        entry = &ctx->states[ctx->num_states++];
        memset(entry, 0, sizeof(*entry));
        entry->target = state;
        entry->group  = map->open_group;
        state->seqid  = 1;
    }
    /* Tentative values belong to this OPEN until admission and truncate pass.
     * A later failed OPEN must not change the successful prefix's journal. */
    map->open_state = entry;
    state->handle   = op->out_handle;
    memcpy(state->fh, op->fh, op->fh_len);
    state->fh_len = op->fh_len;
    nfs4_vfs_stateid(ctx, entry, state->seqid, &map->reserved_stateid);
} /* nfs4_vfs_open_checked */

static void
nfs4_vfs_open_reserve_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }

    struct nfs4_vfs_op        *map   = private_data;
    struct nfs_open_state     *state = map->reserved_open;
    struct chimera_claim_owner owner = { 0 };
    uint8_t                    access = 0, deny = 0;

    (void) compound;
    (void) index;
    (void) status;
    if (state->share_access & OPEN4_SHARE_ACCESS_READ) {
        access |= CHIMERA_CLAIM_R;
    }
    if (state->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        access |= CHIMERA_CLAIM_W;
    }
    if (state->share_deny & OPEN4_SHARE_DENY_READ) {
        deny |= CHIMERA_CLAIM_R;
    }
    if (state->share_deny & OPEN4_SHARE_DENY_WRITE) {
        deny |= CHIMERA_CLAIM_W;
    }
    owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    owner.client_key = state->owner->client->client_id;
    owner.owner_lo   = XXH3_64bits(state->owner->owner, state->owner->owner_len);
    chimera_vfs_claim_init_nfs4_open(&map->open_claim, access, deny, &owner);
    map->open_claim.is_alive_cb        = nfs_client_lease_alive;
    map->open_claim.revoked_cb         = nfs_client_lease_revoked_cb;
    map->open_claim.cb_private         = state->owner->client;
    map->open_claim.admit_excluded     = map->ctx->closed_claims;
    map->open_claim.admit_num_excluded = map->ctx->num_closed_claims;
} /* nfs4_vfs_open_reserve_prepare */

static void
nfs4_vfs_open_reserve_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        return;
    }

    struct nfs4_vfs_op                   *map = private_data;
    const struct chimera_vfs_compound_op *op  = chimera_vfs_compound_op(compound, index);

    if (*status != CHIMERA_VFS_OK) {
        map->verify_status = (op->claim_result == CHIMERA_CLAIM_BREAKING ||
                              op->claim_conflict.admission_fenced) ?
            NFS4ERR_DELAY : NFS4ERR_SHARE_DENIED;
    }
} /* nfs4_vfs_open_reserve_complete */

/* The VFS opens a handle able to carry both grants. Inherited grants come
 * only from the frozen or attempt-private state already authorized for this FH. */
static void
nfs4_vfs_open_union_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }

    struct nfs4_vfs_op             *map       = private_data;
    struct nfs_open_state          *state     = map->reserved_open;
    struct nfs4_vfs_state          *entry     = map->open_state;
    const struct OPEN4args         *oa        = &map->ctx->req->args_compound->argarray[map->res_index].opopen;
    struct chimera_vfs_compound_op *args      = chimera_vfs_compound_op_args(compound, index);
    uint32_t                        requested = oa->share_access & OPEN4_SHARE_ACCESS_BOTH;

    (void) status;
    if (requested == state->share_access) {
        chimera_vfs_compound_op_skip(compound, index);
    } else if (entry->active && entry->shadow.share_access == state->share_access) {
        state->handle = entry->shadow.handle;
        chimera_vfs_compound_op_skip(compound, index);
    } else {
        args->open_flags              = CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_WRITE_ONLY;
        args->inherited_grant_handle  = entry->shadow.handle;
        args->inherited_grant_handle2 = state->handle;
    }
} /* nfs4_vfs_open_union_prepare */

static void
nfs4_vfs_open_union_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        return;
    }

    struct nfs4_vfs_op                   *map = private_data;
    const struct chimera_vfs_compound_op *op  = chimera_vfs_compound_op(compound, index);

    if (*status == CHIMERA_VFS_OK && op->out_handle) {
        map->reserved_open->handle = op->out_handle;
    }
} /* nfs4_vfs_open_union_complete */

static void
nfs4_vfs_open_commit(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        return;
    }

    struct nfs4_vfs_op    *map   = private_data;
    struct nfs4_vfs_state *entry = map->open_state;

    (void) compound;
    (void) index;
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    nfs4_vfs_state_copy(&entry->shadow, map->reserved_open);
    entry->active                   = 1;
    entry->last_open                = map;
    map->open_committed             = 1;
    map->ctx->current_stateid       = map->reserved_stateid;
    map->ctx->current_stateid_valid = 1;
    nfs4_vfs_rebuild_state_view(map->ctx, NULL);
} /* nfs4_vfs_open_commit */

static void
nfs4_vfs_open_truncate_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }

    struct nfs4_vfs_op                   *map  = private_data;
    const struct chimera_vfs_compound_op *open = chimera_vfs_compound_op(compound, map->vfs_res);

    (void) status;
    chimera_vfs_compound_op_args(compound, index)->in_handle = map->reserved_open->handle;
    if (!map->open_trunc_if_existed || !open->existed) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_open_truncate_prepare */

/* LOCKT tests claims at its execution boundary, before later mutations.
 * claim_test only reads admission state and copies its conflict while holding
 * the file lock: it neither acquires claims nor sends breaks.  Resolve owner
 * bytes under the client-table lock after the file lock has been released. */
static void
nfs4_vfs_lock_conflict_owner(struct nfs4_vfs_op *map)
{
    struct nfs4_vfs_compound_ctx     *ctx   = map->ctx;
    const struct chimera_claim_owner *owner = &map->lockt_conflict.owner;

    if (owner->proto != CHIMERA_CLAIM_PROTO_NFSV4) {
        return;
    }
    for (uint32_t i = 0; i < ctx->num_lock_groups; i++) {
        struct nfs_lock_owner *lo = ctx->lock_groups[i]->owner;
        if (lo->client->client_id == owner->client_key &&
            XXH3_64bits(lo->owner, lo->owner_len) == owner->owner_lo) {
            map->lockt_owner_len = lo->owner_len;
            memcpy(map->lockt_owner, lo->owner, lo->owner_len);
            return;
        }
    }
    nfs4_client_lookup_lock_owner(&ctx->req->thread->shared->nfs4_shared_clients,
                                  owner->client_key, owner->owner_lo, map->lockt_owner,
                                  sizeof(map->lockt_owner), &map->lockt_owner_len);
} /* nfs4_vfs_lock_conflict_owner */

static void
nfs4_vfs_lockt_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                   *map  = private_data;
    struct nfs4_vfs_compound_ctx         *ctx  = map->ctx;
    struct nfs_request                   *req  = map->ctx->req;
    const struct LOCKT4args              *args =
        &req->args_compound->argarray[map->res_index].oplockt;
    const struct chimera_vfs_compound_op *op =
        chimera_vfs_compound_op(compound, index);
    struct chimera_vfs_state             *vfs_state = req->thread->vfs->vfs_state;
    struct chimera_vfs_file_state        *file;
    struct chimera_vfs_claim              probe;
    struct chimera_claim_owner            owner = { 0 };
    enum chimera_vfs_claim_result         result;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if ((op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(op->attr.va_mode)) {
        map->verify_status = chimera_nfs4_data_nonreg_status(op->attr.va_mode);
        *status            = CHIMERA_VFS_EINVAL;
        return;
    }

    file = chimera_vfs_state_get(vfs_state, op->fh, (int) op->fh_len,
                                 chimera_vfs_hash(op->fh, op->fh_len), false);
    if (!file) {
        return;
    }
    owner.proto = CHIMERA_CLAIM_PROTO_NFSV4;
    /* v4.0 uses its operation-specific pin; later versions use the session. */
    owner.client_key = req->minorversion ? ctx->client->client_id : map->lockt_client->client_id;
    owner.owner_lo   = XXH3_64bits(args->owner.owner.data, args->owner.owner.len);
    chimera_vfs_claim_init_range(&probe,
                                 !(args->locktype == READ_LT ||
                                   args->locktype == READW_LT), false,
                                 args->offset, args->length, &owner);
    map->lockt_conflict.length = UINT64_MAX;
    nfs4_vfs_range_view(map->ctx, op->fh, op->fh_len);
    probe.admit_excluded     = map->ctx->range_previous;
    probe.admit_num_excluded = map->ctx->num_range_previous;
    result                   = chimera_vfs_claim_test_range_view(file, &probe, map->ctx->range_current,
                                                                 map->ctx->num_range_current, &map->lockt_conflict);
    chimera_vfs_state_put(vfs_state, file);
    if (result != CHIMERA_CLAIM_GRANTED) {
        nfs4_vfs_lock_conflict_owner(map);
        map->verify_status = NFS4ERR_DENIED;
        *status            = CHIMERA_VFS_EACCES;
    }
} /* nfs4_vfs_lockt_complete */

static void
nfs4_vfs_getattr_queried(
    void    *private_data,
    int      status,
    bool     got_change,
    uint64_t change,
    bool     got_size,
    uint64_t size)
{
    struct nfs4_vfs_coordination *entry = private_data;

    entry->query_status = status;
    entry->got_change   = got_change;
    entry->got_size     = got_size;
    entry->change       = change;
    entry->size         = size;
    clock_gettime(CLOCK_REALTIME, &entry->query_time);
    chimera_vfs_compound_coordinate_done(entry->compound, entry->token, CHIMERA_VFS_OK);
} /* nfs4_vfs_getattr_queried */

static void
nfs4_vfs_getattr_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct nfs4_vfs_op               *map    = private_data;
    struct nfs4_vfs_compound_ctx     *ctx    = map->ctx;
    struct chimera_server_nfs_thread *thread = ctx->req->thread;
    struct nfs4_vfs_coordination     *entry  = calloc(1, sizeof(*entry));

    if (!entry) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    entry->ctx      = ctx;
    entry->compound = compound;
    entry->index    = index;
    entry->token    = token;
    entry->fh_len   = fh_len;
    memcpy(entry->fh, fh, fh_len);
    entry->next       = ctx->coordination;
    ctx->coordination = entry;
    struct nfs_delegation *deleg = nfs4_find_conflicting_write_deleg_pinned(
        thread, fh, fh_len, ctx->client->client_id, &entry->holder_client);
    if (!deleg) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
        return;
    }
    for (uint32_t i = 0; i < ctx->num_combines; i++) {
        if (ctx->combines[i].deleg == deleg) {
            entry->combine = &ctx->combines[i];
            break;
        }
    }
    if (!entry->combine && ctx->num_combines < NFS4_VFS_COMPOUND_MAX_OPS) {
        struct nfs_delegation_combine_journal *journal = &ctx->combines[ctx->num_combines];
        if (nfs_delegation_combine_reserve(deleg, ctx, journal) == NFS4_OK) {
            ctx->num_combines++;
            entry->combine = journal;
        }
    }
    if (!entry->combine) {
        nfs_state_table_release(&thread->shared->nfs4_state_table, deleg, NFS4_SLOT_TYPE_DELEG, thread->vfs_thread);
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EAGAIN);
        return;
    }
    entry->queried = true;
    nfs4_cb_getattr(thread, deleg, entry, nfs4_vfs_getattr_queried);
} /* nfs4_vfs_getattr_coordinate */

/* Memoized query inputs are usable only while the same delegation is current.
 * Revalidation takes no delegation reference and cannot trigger its cleanup. */
static nfsstat4
nfs4_vfs_getattr_combine(
    struct nfs4_vfs_op                     *map,
    const struct chimera_vfs_compound_op   *op,
    struct chimera_vfs_attrs               *attr,
    struct nfs_delegation_combine_journal **target,
    struct nfs_delegation_combine_journal  *pending)
{
    struct nfs4_vfs_compound_ctx *ctx = map->ctx;
    struct nfs4_vfs_coordination *entry;

    for (entry = ctx->coordination; entry; entry = entry->next) {
        if (entry->index == (uint32_t) map->getattr_coordinate &&
            entry->fh_len == op->fh_len && !memcmp(entry->fh, op->fh, op->fh_len)) {
            break;
        }
    }
    if (!entry) {
        return NFS4ERR_SERVERFAULT;
    }
    int match = nfs4_write_delegation_matches(ctx->req->thread, op->fh, op->fh_len,
                                              ctx->client->client_id,
                                              entry->combine ? entry->combine->deleg : NULL);
    if (!match) {
        /* Backend attrs were captured before coordination. A holder that
         * returned during the query may have flushed newer data; retry with a
         * fresh GETATTR instead of serving the earlier snapshot. */
        return entry->combine ? NFS4ERR_DELAY : NFS4_OK;
    }
    if (match < 0) {
        return NFS4ERR_DELAY;
    }
    if (entry->query_status || !entry->got_change || !entry->got_size) {
        /* The live holder can have newer data than local metadata. */
        return NFS4ERR_DELAY;
    }
    if (!entry->query_status && entry->got_change) {
        *target  = entry->combine;
        *pending = *entry->combine;
        return nfs_delegation_combine_attrs(pending, entry->change, entry->got_size,
                                            entry->size, &entry->query_time, attr);
    }
    return NFS4_OK;
} /* nfs4_vfs_getattr_combine */

/* Marshal variable-size ACL output while failure can still stop successors.
 * Only the private reply arena changes here; accepted completion publishes it. */
static void
nfs4_vfs_getattr_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                   *map   = private_data;
    struct nfs4_vfs_compound_ctx         *ctx   = map->ctx;
    struct nfs_request                   *req   = ctx->req;
    struct nfs_argop4                    *argop =
        &req->args_compound->argarray[map->res_index];
    const struct chimera_vfs_compound_op *op =
        chimera_vfs_compound_op(compound, map->vfs_res);
    uint64_t                              fixed, extra, consumed, reserved;
    uint32_t                              mark = req->encoding->dbuf->used;

    if (*status != CHIMERA_VFS_OK) {
        /* Coordination memo allocation failures are frontend reply-resource
         * failures. Reapply this mapping when a memoized failure is retried. */
        if (map->getattr_coordinate && index == (uint32_t) map->getattr_coordinate &&
            *status == CHIMERA_VFS_ENOSPC) {
            map->verify_status = NFS4ERR_RESOURCE;
        }
        return;
    }
    struct chimera_vfs_attrs               attr = op->attr;
    struct nfs_delegation_combine_journal *combine_target = NULL, pending;
    struct nfs4_change_observation        *observation = NULL;
    map->verify_status = nfs4_change_project(req->thread->shared->nfs4_state_table.change_table,
                                             op->fh, op->fh_len, &attr, &req->change_observations, &observation);
    if (map->verify_status != NFS4_OK) {
        *status = CHIMERA_VFS_EAGAIN;
        return;
    }
    if (map->getattr_coordinate) {
        map->verify_status = nfs4_vfs_getattr_combine(map, op, &attr, &combine_target, &pending);
        if (map->verify_status != NFS4_OK) {
            *status = CHIMERA_VFS_EAGAIN;
            return;
        }
    }
    fixed = nfs4_vfs_op_reply_bound(argop);
    extra = 0;
    if (argop->opgetattr.num_attr_request &&
        (argop->opgetattr.attr_request[0] & (1U << FATTR4_ACL))) {
        if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_ACL) {
            extra = chimera_nfs4_acl_wire_size(op->attr.va_acl);
        } else {
            extra = chimera_nfs4_acl_wire_size(NULL) +
                8 * (4 * sizeof(uint32_t) + ((CHIMERA_IDMAP_WHO_MAX + 3) & ~3u));
        }
    }

    /* READDIR already consumed part of its static allowance. Earlier staged
     * GETATTRs consumed both their fixed allowance and their dynamic addition. */
    consumed = (uint64_t) mark - ctx->attempt_mark - ctx->getattr_staged_bytes;
    reserved = ctx->getattr_staged_bound + fixed + consumed;
    reserved = reserved < ctx->reply_bound ? ctx->reply_bound - reserved : 0;
    if ((uint64_t) req->encoding->dbuf->size - mark <
        fixed + extra + reserved + 8192) {
        map->verify_status = NFS4ERR_RESOURCE;
        *status            = CHIMERA_VFS_ENOSPC;
        return;
    }

    map->getattr_result.status = chimera_nfs4_getattr_fill(
        req, &argop->opgetattr, &map->getattr_result, &attr,
        op->fh, (int) op->fh_len, true);
    if (map->getattr_result.status != NFS4_OK) {
        req->encoding->dbuf->used = mark;
        map->verify_status        = map->getattr_result.status;
        *status                   = CHIMERA_VFS_ENOSPC;
        return;
    }
    if (combine_target) {
        *combine_target = pending;
    }
    nfs4_change_observe(observation, &attr);
    map->getattr_staged        = 1;
    ctx->getattr_staged_bound += fixed;
    ctx->getattr_staged_bytes += req->encoding->dbuf->used - mark;
} /* nfs4_vfs_getattr_complete */

static void
nfs4_vfs_attempt_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;

    (void) compound;
    nfs4_change_finish(ctx->req->thread->shared->nfs4_state_table.change_table,
                       &ctx->req->change_observations, false);
    for (uint32_t i = 0; i < ctx->num_combines; i++) {
        nfs_delegation_combine_reset(&ctx->combines[i]);
    }
    ctx->req->encoding->dbuf->used = ctx->attempt_mark;
    ctx->getattr_staged_bytes      = 0;
    ctx->getattr_staged_bound      = 0;
    ctx->fh_consumed               = ctx->initial_fh_consumed;
    ctx->current_stateid           = ctx->initial_stateid;
    ctx->saved_stateid             = ctx->initial_saved_stateid;
    ctx->current_stateid_valid     = ctx->initial_stateid_valid;
    ctx->saved_stateid_valid       = ctx->initial_saved_stateid_valid;
    for (uint32_t i = 0; i < ctx->num_ops; i++) {
        if (ctx->ops[i].range_src_handle) {
            chimera_vfs_release(ctx->req->thread->vfs_thread, ctx->ops[i].range_src_handle);
            ctx->ops[i].range_src_handle = NULL;
        }
        ctx->ops[i].range_have_src_owner = 0;
        ctx->ops[i].range_length         = 0;
        if (ctx->ops[i].io_handle) {
            chimera_vfs_release(ctx->req->thread->vfs_thread, ctx->ops[i].io_handle);
            ctx->ops[i].io_handle = NULL;
        }
        struct nfs4_vfs_op *map = &ctx->ops[i];
        map->open_replay   = map->lock_replay = NULL;
        map->replay_hit    = map->replay_recorded = 0;
        map->close_passed  = 0;
        map->lockt_checked = 0;
        for (uint32_t n = 0; n < map->namespace_count; n++) {
            map->namespace_targets[n].found = false;
        }
        map->lock_state     = NULL;
        map->open_state     = NULL;
        map->open_committed = 0;
        if (map->reserved_open) {
            map->reserved_open->handle = NULL;
            map->reserved_open->fh_len = 0;
        }
        ctx->ops[i].have_io_owner  = 0;
        ctx->ops[i].verify_status  = 0;
        ctx->ops[i].getattr_staged = 0;
        memset(&ctx->ops[i].getattr_result, 0,
               sizeof(ctx->ops[i].getattr_result));
        ctx->ops[i].readdir_have_mark = 0;
        memset(&ctx->ops[i].lockt_conflict, 0,
               sizeof(ctx->ops[i].lockt_conflict));
        ctx->ops[i].lockt_owner_len = 0;
    }
    ctx->num_closed_states = ctx->num_closed_claims = 0;
    ctx->num_states        = 0;
    for (uint32_t i = 0; i < ctx->num_groups; i++) {
        struct nfs_open_owner_reservation *group = ctx->groups[i];
        nfs4_owner_replay_reset(&group->owner_replay);
        for (uint32_t j = 0; j < group->num_existing; j++) {
            struct nfs4_vfs_state *entry = &ctx->states[ctx->num_states++];
            memset(entry, 0, sizeof(*entry));
            entry->target           = group->existing[j];
            entry->group            = group;
            entry->initially_public = entry->active = 1;
            nfs4_vfs_state_copy(&entry->shadow, entry->target);
        }
    }
    for (uint32_t i = 0; i < ctx->num_lock_groups; i++) {
        nfs4_owner_replay_reset(&ctx->lock_groups[i]->owner_replay);
    }
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock *entry = &ctx->locks[i];
        entry->active = entry->initially_public;
        entry->dirty  = 0;
        entry->seqid  = entry->initially_public ? entry->target->seqid : 0;
        entry->parent = entry->initially_public ? nfs4_vfs_open_target(ctx, entry->target->open_state) : NULL;
        nfs_lock_range_journal_reset(entry->ranges);
    }
} /* nfs4_vfs_attempt_reset */

static void
nfs4_vfs_compound_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;
    struct nfs_request           *req = ctx->req;
    uint32_t                      k;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }

    /* READDIR judges its own page, and now can: the page is marshalled inside
     * the enumeration, so by the time the op finishes the answer exists.  It
     * used to be settled after the sequence, which is what forced the rule
     * that nothing mutating may follow a READDIR. */
    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op                   *map = &ctx->ops[k];
        const struct chimera_vfs_compound_op *vop;

        if (map->vfs_res < 0 || (uint32_t) map->vfs_res != index) {
            continue;
        }

        if (req->args_compound->argarray[map->res_index].argop != OP_READDIR) {
            break;
        }

        vop = chimera_vfs_compound_op(compound, index);

        if (map->readdir_cursor.change_status != NFS4_OK) {
            map->verify_status = map->readdir_cursor.change_status;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }

        /* RFC 7530 16.24.4, applied here so it stops the sequence rather than
         * being discovered once the ops behind it have already run. */
        if (!vop->eof && map->readdir_cursor.entries == NULL) {
            *status = CHIMERA_VFS_ERANGE;
            return;
        }

        break;
    }

    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op                   *map = &ctx->ops[k];
        const struct chimera_vfs_compound_op *aux;

        if (map->vfs_aux < 0 || (uint32_t) map->vfs_aux != index) {
            continue;
        }

        aux = chimera_vfs_compound_op(compound, index);

        switch (req->args_compound->argarray[map->res_index].argop) {
            case OP_PUTFH:
                if ((aux->attr.va_set_mask & CHIMERA_VFS_ATTR_NLINK) &&
                    aux->attr.va_nlink == 0) {
                    bool live = nfs4_clients_have_open_state_except(
                        &req->thread->shared->nfs4_shared_clients, aux->fh, aux->fh_len,
                        ctx->closed_states, ctx->num_closed_states);
                    for (uint32_t i = 0; !live && i < ctx->num_states; i++) {
                        struct nfs4_vfs_state *entry = &ctx->states[i];
                        struct nfs_open_state *state = &entry->shadow;
                        live = entry->active && !entry->closed && state->fh_len == aux->fh_len &&
                            !memcmp(state->fh, aux->fh, aux->fh_len);
                    }
                    if (!live) {
                        *status = CHIMERA_VFS_ESTALE;
                    }
                }
                break;

            case OP_READLINK:
                /* RFC 7530 §16.25: READLINK applies to a symbolic link. */
                if (!(aux->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
                    *status = CHIMERA_VFS_EFAULT;
                } else if (!S_ISLNK(aux->attr.va_mode)) {
                    *status = CHIMERA_VFS_EINVAL;
                }
                break;

            case OP_VERIFY:
            case OP_NVERIFY:
            {
                nfsstat4 vs = chimera_nfs4_verify_status(req, map->res_index,
                                                         &aux->attr,
                                                         aux->fh,
                                                         (int) aux->fh_len);

                if (vs != NFS4_OK) {
                    /* Carried out of band: the answer is an NFSv4 status with
                     * no errno that means it, and the fill reads it back. */
                    map->verify_status = vs;
                    *status            = CHIMERA_VFS_EINVAL;
                }
                break;
            }

            case OP_COMMIT:
            /* COMMIT and these v4.2 operations require a regular file. */
            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
            case OP_WRITE_SAME:
            case OP_READ_PLUS:
                if ((aux->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
                    !S_ISREG(aux->attr.va_mode)) {
                    *status = chimera_vfs_nonreg_error(aux->attr.va_mode);
                }
                break;

            default:
                break;
        } /* switch */

        return;
    }
} /* nfs4_vfs_compound_gate */

/* Owner sequence numbers are attempt-local protocol state. Record both
 * successful operations and consuming errors before the next wire operation;
 * only accepted finish copies these journals to their reserved owners. */
static void
nfs4_vfs_replay_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;

    if (ctx->req->minorversion != 0) {
        return;
    }
    for (uint32_t i = 0; i < ctx->num_ops; i++) {
        struct nfs4_vfs_op            *map = &ctx->ops[i];
        if (index < (uint32_t) map->prepare_index || index > (uint32_t) map->vfs_hi ||
            map->replay_hit || map->replay_recorded || (!map->open_replay && !map->lock_replay) ||
            (*status == CHIMERA_VFS_OK && index != (uint32_t) map->vfs_hi)) {
            continue;
        }
        const struct nfs_argop4       *argop  = &ctx->req->args_compound->argarray[map->res_index];
        struct chimera_vfs_compound_op result = *chimera_vfs_compound_op(compound, index);
        result.status = *status;
        nfsstat4                       error = map->verify_status ? map->verify_status :
            nfs4_vfs_op_errno(argop, &result, ctx->req);
        if (argop->argop == OP_OPEN) {
            const struct OPEN4args               *oa       = &argop->opopen;
            struct OPEN4res                       response = { .status = error };
            const struct chimera_vfs_compound_op *opened   = chimera_vfs_compound_op(compound, map->vfs_res);
            uint32_t                              attributes[3];
            if (error == NFS4_OK) {
                response.resok4.stateid                    = map->reserved_stateid;
                response.resok4.rflags                     = map->open_rflags;
                response.resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;
                if (map->open_by_name) {
                    struct chimera_vfs_attrs pre = opened->dir_pre_attr, post = opened->dir_post_attr;
                    chimera_nfs4_set_changeinfo(&response.resok4.cinfo, &pre, &post);
                }
                if (oa->openhow.opentype == OPEN4_CREATE && oa->openhow.how.mode != EXCLUSIVE4) {
                    const struct fattr4     *requested = oa->openhow.how.mode == EXCLUSIVE4_1 ?
                        &oa->openhow.how.ch_createboth.cva_attrs : &oa->openhow.how.createattrs;
                    struct chimera_vfs_attrs applied = opened->applied_attr;
                    response.resok4.attrset     = attributes;
                    response.resok4.num_attrset = chimera_nfs4_mask2attr(&applied, requested->num_attrmask,
                                                                         requested->attrmask, attributes);
                }
            }
            chimera_nfs_abort_if(!nfs4_owner_replay_record_open(map->open_replay, map->open_seqid,
                                                                &response, opened->fh, opened->fh_len),
                                 "unsupported OPEN journal reply");
            *map->open_response = map->open_replay->replay;
        } else if (argop->argop == OP_LOCK) {
            struct LOCK4res lock = { .status = error };
            if (error == NFS4_OK) {
                lock.resok4.lock_stateid = map->reserved_stateid;
            } else if (error == NFS4ERR_DENIED) {
                lock.denied.offset   = map->lockt_conflict.offset;
                lock.denied.length   = map->lockt_conflict.length;
                lock.denied.locktype = (map->lockt_conflict.used &
                                        (CHIMERA_CLAIM_W | CHIMERA_CLAIM_CW | CHIMERA_CLAIM_LW)) ? WRITE_LT : READ_LT;
                lock.denied.owner.clientid   = map->lockt_conflict.owner.client_key;
                lock.denied.owner.owner.len  = map->lockt_owner_len;
                lock.denied.owner.owner.data = map->lockt_owner;
            }
            if (map->open_replay) {
                nfs4_owner_replay_record_lock(map->open_replay, map->open_seqid, &lock);
            }
            if (map->lock_replay) {
                nfs4_owner_replay_record_lock(map->lock_replay, map->lock_seqid, &lock);
            }
        } else {
            const struct stateid4 *sid = argop->argop == OP_CLOSE ?
                &map->close_stateid : &map->reserved_stateid;
            if (map->open_replay) {
                nfs4_owner_replay_record(map->open_replay, map->open_seqid, argop->argop, error,
                                         error == NFS4_OK ? sid : NULL);
            }
            if (map->lock_replay) {
                nfs4_owner_replay_record(map->lock_replay, map->lock_seqid, argop->argop, error,
                                         error == NFS4_OK ? sid : NULL);
            }
        }
        map->replay_recorded = 1;
    }
} /* nfs4_vfs_replay_complete */

/* Explicit external coordination is memoized by the VFS across attempts.
 * Its barriers remain owned until the logical compound is disposed. */
static void
nfs4_vfs_layout_recalled(void *private_data)
{
    struct nfs4_vfs_coordination *entry = private_data;

    chimera_vfs_compound_coordinate_done(entry->compound, entry->token, CHIMERA_VFS_OK);
} /* nfs4_vfs_layout_recalled */

/* Namespace recall is an explicit request-lifetime effect. Keep the file
 * state pinned even when empty so retry checks observe newly granted caches
 * without allocating or releasing references from a pure operation callback. */
static void
nfs4_vfs_namespace_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct nfs4_vfs_op           *map   = private_data;
    struct nfs4_vfs_compound_ctx *ctx   = map->ctx;
    struct nfs4_vfs_coordination *entry = calloc(1, sizeof(*entry));

    if (!entry) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    entry->compound = compound;
    entry->index    = index;
    entry->token    = token;
    entry->fh_len   = fh_len;
    memcpy(entry->fh, fh, fh_len);
    entry->next       = ctx->coordination;
    ctx->coordination = entry;
    uint64_t hash = chimera_vfs_hash(fh, fh_len);
    entry->namespace_file = chimera_vfs_state_get(ctx->req->thread->vfs->vfs_state,
                                                  fh, fh_len, hash, true);
    if (!entry->namespace_file) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    bool     blocked = chimera_vfs_claim_break_caching(ctx->req->thread->vfs->vfs_state,
                                                       fh, fh_len, hash);
    chimera_vfs_compound_coordinate_done(compound, token,
                                         blocked ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* nfs4_vfs_namespace_coordinate */

static void
nfs4_vfs_namespace_lookup_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op *map = private_data;

    (void) compound;
    for (uint32_t i = 0; i < map->namespace_count; i++) {
        if ((uint32_t) map->namespace_targets[i].lookup == index) {
            map->namespace_targets[i].found = *status == CHIMERA_VFS_OK;
            break;
        }
    }
    /* Legacy recall-only lookups ignore failure; REMOVE/RENAME itself decides
     * the final error after both recall checks, in its original order. */
    *status = CHIMERA_VFS_OK;
} /* nfs4_vfs_namespace_lookup_complete */

static void
nfs4_vfs_namespace_coordinate_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op *map = private_data;

    (void) status;
    for (uint32_t i = 0; i < map->namespace_count; i++) {
        if ((uint32_t) map->namespace_targets[i].coordinate == index && !map->namespace_targets[i].found) {
            chimera_vfs_compound_op_skip(compound, index);
            return;
        }
    }
} /* nfs4_vfs_namespace_coordinate_prepare */

static void
nfs4_vfs_namespace_restore(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                   *map    = private_data;
    const struct chimera_vfs_compound_op *parent = chimera_vfs_compound_op(compound, map->namespace_parent);
    struct chimera_vfs_compound_op       *op     = chimera_vfs_compound_op_args(compound, index);

    if (!parent || !parent->fh_len) {
        *status = CHIMERA_VFS_EINVAL;
        return;
    }
    op->arg_fh_len = parent->fh_len;
    memcpy(op->arg_fh, parent->fh, parent->fh_len);
} /* nfs4_vfs_namespace_restore */

static void
nfs4_vfs_namespace_ready(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op *map = private_data;

    (void) index;
    for (uint32_t i = 0; i < map->namespace_count; i++) {
        if (!map->namespace_targets[i].found) {
            continue;
        }
        const struct chimera_vfs_compound_op *lookup = chimera_vfs_compound_op(compound,
                                                                               map->namespace_targets[i].lookup);
        struct nfs4_vfs_coordination         *entry;
        for (entry = map->ctx->coordination; entry; entry = entry->next) {
            if (entry->index == (uint32_t) map->namespace_targets[i].coordinate &&
                entry->fh_len == lookup->fh_len && !memcmp(entry->fh, lookup->fh, lookup->fh_len)) {
                break;
            }
        }
        if (!entry || !entry->namespace_file || chimera_vfs_claim_has_caching(entry->namespace_file)) {
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }
    }
} /* nfs4_vfs_namespace_ready */

static int
nfs4_vfs_namespace_add_target(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    const char                  *name,
    uint32_t                     name_len)
{
    uint32_t      which  = map->namespace_count++;
    int           lookup = chimera_vfs_compound_add_lookup(compound, name, name_len, CHIMERA_VFS_ATTR_FH, 0);

    if (lookup < 0) {
        return -1;
    }
    map->namespace_targets[which].lookup = lookup;
    chimera_vfs_compound_set_op_callbacks(compound, lookup, NULL, nfs4_vfs_namespace_lookup_complete, map);
    int           coordinate = chimera_vfs_compound_add_coordinate(compound, nfs4_vfs_namespace_coordinate, map);
    if (coordinate < 0) {
        return -1;
    }
    map->namespace_targets[which].coordinate = coordinate;
    chimera_vfs_compound_set_op_prepare(compound, coordinate, nfs4_vfs_namespace_coordinate_prepare, map);
    /* Restore current parent without disturbing the wire SAVEFH slot. */
    const uint8_t placeholder = 0;
    int           restore     = chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
    if (restore >= 0) {
        chimera_vfs_compound_set_op_prepare(compound, restore, nfs4_vfs_namespace_restore, map);
    }
    return restore;
} /* nfs4_vfs_namespace_add_target */

static void
nfs4_vfs_layout_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct nfs4_vfs_op           *map   = private_data;
    struct nfs4_vfs_compound_ctx *ctx   = map->ctx;
    struct nfs4_vfs_coordination *entry = calloc(1, sizeof(*entry));

    if (!entry) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    entry->ctx      = ctx;
    entry->compound = compound;
    entry->token    = token;
    entry->index    = index;
    entry->fh_len   = fh_len;
    memcpy(entry->fh, fh, fh_len);
    entry->next           = ctx->coordination;
    ctx->coordination     = entry;
    entry->layout_barrier = nfs_layout_table_barrier_acquire(
        &ctx->req->thread->shared->nfs4_layout_table, fh, fh_len);
    if (!entry->layout_barrier) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EAGAIN);
        return;
    }
    chimera_nfs4_cb_recall_and_wait(ctx->req->thread, fh, fh_len,
                                    nfs4_vfs_layout_recalled, entry);
} /* nfs4_vfs_layout_coordinate */

/* Delegation grants are protocol publication. They run only after accepted
* finish, while owner/client reservations and the reply remain retained. */
static void
nfs4_vfs_publish_delegations(struct nfs_request *req)
{
    struct nfs4_vfs_compound_ctx *ctx      = req->compound_probe_private;
    struct chimera_vfs_compound  *compound = ctx->accepted_compound;

    while (ctx->grant_next < ctx->num_ops) {
        struct nfs4_vfs_op *map = &ctx->ops[ctx->grant_next];
        struct nfs_argop4  *arg = &req->args_compound->argarray[map->res_index];
        struct OPEN4res    *res = &req->res_compound.resarray[map->res_index].opopen;
        if ((int) map->res_index > ctx->accepted_index) {
            break;
        }
        if (arg->argop == OP_OPEN && map->reserved_open &&
            res->status == NFS4_OK && !map->replay_hit &&
            map->open_state && !map->open_state->closed) {
            const struct chimera_vfs_compound_op *op =
                chimera_vfs_compound_op(compound, map->vfs_res);
            req->index = map->res_index;
            req->fhlen = op->fh_len;
            memcpy(req->fh, op->fh, op->fh_len);
            if (chimera_nfs4_open_grant_delegation(req, res,
                                                   map->open_by_name ? &op->attr : NULL)) {
                return;
            }
        }
        ctx->grant_next++;
    }
    req->index = ctx->accepted_index;
    req->fhlen = ctx->accepted_fhlen;
    memcpy(req->fh, ctx->accepted_fh, req->fhlen);
    nfsstat4 status = ctx->accepted_status;
    req->compound_probe_resume  = NULL;
    req->compound_probe_private = NULL;
    nfs4_vfs_dispose(compound, ctx);
    chimera_nfs4_compound_complete(req, status);
} /* nfs4_vfs_publish_delegations */

/*
 * The sequence is over.  Fill the results of every NFSv4 op that ran, stopping
 * at the first that failed, and hand the request back to the reply path exactly
 * as a failing per-op handler would.
 */
static void
nfs4_vfs_compound_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx         *ctx    = private_data;
    struct nfs_request                   *req    = ctx->req;
    struct chimera_server_nfs_thread     *thread = req->thread;
    const struct chimera_vfs_compound_op *vop;
    uint32_t                              completed;
    uint32_t                              k;
    int                                   j;
    nfsstat4                              status   = NFS4_OK;
    uint32_t                              fail_res = 0;
    int                                   failed   = 0;

    completed = chimera_vfs_compound_num_completed(compound);
    enum chimera_vfs_error                aggregate            = chimera_vfs_compound_finish_status(compound);
    bool                                  preexecution_failure = completed == 0;
    if (aggregate != CHIMERA_VFS_OK || preexecution_failure) {
        /* Backend finish rejected the attempt. No result has been filled or
         * ownership transferred yet. EAGAIN is retryable only after backend
         * rollback; the future transaction finish hook owns that guarantee. */
        if (!preexecution_failure && aggregate == CHIMERA_VFS_EAGAIN && ctx->retries++ < 8) {
            if (chimera_vfs_compound_retry(compound)) {
                return;
            }
        }
        /* Submission can fail before the first operation (for example while
         * allocating its retry snapshot). There is no per-op status to fill,
         * and potentially no snapshot to retry. Report the execution failure
         * on the first wire op without entering the successful-prefix loop. */
        if (aggregate == CHIMERA_VFS_OK) {
            aggregate = chimera_vfs_compound_execution_status(compound);
            if (aggregate == CHIMERA_VFS_OK) {
                aggregate = CHIMERA_VFS_EIO;
            }
        }
        status = aggregate == CHIMERA_VFS_EAGAIN ? NFS4ERR_DELAY :
            chimera_nfs4_errno_to_nfsstat4(aggregate);
        req->index = (int) ctx->ops[0].res_index;
        nfs4_fail_undispatched_op(thread,
                                  &req->args_compound->argarray[req->index],
                                  &req->res_compound.resarray[req->index], status);
        nfs4_vfs_compound_give_back(thread, ctx);
        if (preexecution_failure &&
            req->args_compound->argarray[req->index].argop == OP_OPEN) {
            /* Final OPEN errors still use its seqid/replay completion. No
             * state or handle was installed, and disposal drops any private
             * reservation before the protocol error is published. */
            nfs4_vfs_dispose(compound, ctx);
            chimera_nfs4_open_complete(req, status);
            return;
        }
        if (req->open_4_0_owner) {
            nfs_open_owner_put(req->open_4_0_owner);
            req->open_4_0_owner = NULL;
        }
        nfs4_vfs_dispose(compound, ctx);
        chimera_nfs4_compound_complete(req, status);
        return;
    }


    ctx->accepted = true;
    nfs4_vfs_publish_states(compound, ctx);

    if (req->session && ctx->client) {
        nfs_client_touch(ctx->client);
    }

    for (k = 0; k < ctx->num_ops; k++) {
        if (ctx->ops[k].lockt_checked) {
            nfs_client_touch(ctx->ops[k].lockt_client);
        }
    }

    for (k = 0; k < ctx->num_ops && !failed; k++) {
        struct nfs4_vfs_op *map   = &ctx->ops[k];
        struct nfs_argop4  *argop = &req->args_compound->argarray[map->res_index];
        struct nfs_resop4  *resop = &req->res_compound.resarray[map->res_index];

        /* Result slots come from a bump allocator that does not zero, and some
         * result types marshal fields that sit OUTSIDE their status union --
         * SETATTR4res carries num_attrsset and an attrsset pointer.  A fill
         * that never runs, because the sequence failed in front of it, would
         * otherwise leave the marshaller dereferencing whatever was there.
         * Each per-op handler initializes its own result on entry; this is the
         * same guarantee for every op the sequence carries. */
        memset(resop, 0, sizeof(*resop));
        resop->resop = argop->argop;

        /* The same reply-buffer headroom gate the per-op dispatcher applies
         * before it runs an operation. */
        if (req->encoding->dbuf->size - req->encoding->dbuf->used < 8192) {
            nfs4_fail_undispatched_op(thread, argop, resop, NFS4ERR_RESOURCE);
            status   = NFS4ERR_RESOURCE;
            fail_res = map->res_index;
            failed   = 1;
            break;
        }

        for (j = map->vfs_lo; j <= map->vfs_hi; j++) {
            vop = chimera_vfs_compound_op(compound, (uint32_t) j);

            chimera_nfs_abort_if(vop == NULL || vop->status == CHIMERA_VFS_UNSET,
                                 "NFSv4 compound: VFS op %d never ran", j);

            if (vop->status != CHIMERA_VFS_OK) {
                /* VERIFY and NVERIFY answer with NFS4ERR_NOT_SAME or
                 * NFS4ERR_SAME, which no errno encodes; the gate recorded the
                 * real answer when it failed the op. */
                status = map->verify_status ? map->verify_status :
                    nfs4_vfs_op_errno(argop, vop, req);
                resop->opillegal.status = status;
                if (argop->argop == OP_LOCKT && status == NFS4ERR_DENIED) {
                    nfs4_vfs_lockt_fill(req, map, &resop->oplockt);
                }
                if (argop->argop == OP_LOCK && status == NFS4ERR_DENIED) {
                    struct LOCKT4res denied = { 0 };
                    nfs4_vfs_lockt_fill(req, map, &denied);
                    resop->oplock.denied = denied.denied;
                }
                fail_res = map->res_index;
                failed   = 1;
                break;
            }
        }

        if (failed) {
            break;
        }

        status = nfs4_vfs_op_fill(req, compound, ctx, map, argop, resop);

        if (status != NFS4_OK) {
            fail_res = map->res_index;
            failed   = 1;
        }
    }

    /* The current filehandle the COMPOUND is left with is whatever the last op
     * that ran was addressing (a LOOKUP that failed did not move it; a
     * RESTOREFH moved it back to the saved object).  Applied after the fills,
     * not before them, because a RESTOREFH's fill re-points req->fh at the
     * saved handle as its own bookkeeping -- for a RESTOREFH in the middle of a
     * sequence that is not where the COMPOUND ends up. */
    if (completed > 0) {
        vop = chimera_vfs_compound_op(compound, completed - 1);
        if (failed) {
            for (k = 0; k < ctx->num_ops; k++) {
                const struct nfs4_vfs_op *map    = &ctx->ops[k];
                uint32_t                  opcode = req->args_compound->argarray[map->res_index].argop;
                if (map->res_index == fail_res && map->namespace_count) {
                    const struct chimera_vfs_compound_op *parent =
                        chimera_vfs_compound_op(compound, map->namespace_parent);
                    /* Recall failure stops before our internal PUTFH restore.
                     * Namespace ops leave the protocol cursor at the original
                     * parent/destination, not at the recalled victim. */
                    if (parent && parent->status == CHIMERA_VFS_OK && parent->fh_len) {
                        vop = parent;
                    }
                    break;
                }
                if (map->res_index == fail_res && (opcode == OP_COPY || opcode == OP_CLONE)) {
                    const struct chimera_vfs_compound_op *dest =
                        chimera_vfs_compound_op(compound, map->prepare_index);
                    /* Source inspection temporarily moves the VFS cursor, but
                     * COPY/CLONE leave the protocol's destination unchanged. */
                    if (dest->status == CHIMERA_VFS_OK) {
                        vop = dest;
                    }
                    break;
                }
            }
        }
        if (vop && vop->fh_len) {
            memcpy(req->fh, vop->fh, vop->fh_len);
            req->fhlen = (int) vop->fh_len;
        }
    }

    req->current_stateid             = ctx->current_stateid;
    req->current_stateid_valid       = ctx->current_stateid_valid;
    req->saved_current_stateid       = ctx->saved_stateid;
    req->saved_current_stateid_valid = ctx->saved_stateid_valid;

    /* A SECINFO took the current filehandle away.  Applied here, after the
     * sequence has said where it ended up, because what it resolved on the way
     * -- the name it looked up -- is not what the COMPOUND is left holding. */
    if (ctx->fh_consumed) {
        req->fhlen = 0;
    }

    /* Point req->index at the operation whose status the compound carries, so
     * chimera_nfs4_compound_complete truncates (or completes) exactly as it
     * does for a per-op handler.  When nothing failed that is the last op the
     * sequence carried, which for a sequence ending in an OPEN is short of the
     * COMPOUND's end -- the dispatcher picks the remainder up from there. */
    req->index = failed ? (int) fail_res :
        (int) ctx->ops[ctx->num_ops - 1].res_index;

    /*
     * Every way out of an OPEN goes through its own completion, not the generic
     * one.  chimera_nfs4_open_finish is what advances a 4.0 open_owner's seqid
     * and drops the reference the encoder pinned on it -- and it has to run for
     * the outcomes that FAIL too, because most OPEN errors are in the advance
     * set (RFC 7530 §9.1.7).  Skipping it on the failure path leaves the owner
     * one seqid behind, and the client's next OPEN is answered NFS4ERR_BAD_SEQID
     * for a request that was perfectly good.
     */
    nfs4_vfs_compound_give_back(thread, ctx);

    if (failed && ctx->open_present && fail_res == ctx->open_res_index) {
        req->index = (int) fail_res;

        nfs4_vfs_dispose(compound, ctx);

        chimera_nfs4_open_complete(req, status);
        return;
    }

    if (ctx->open_present && !ctx->open_filled && req->open_4_0_owner) {
        /* The sequence stopped before the OPEN ran at all, so there is no
         * seqid to advance -- but the encoder's pin is still outstanding. */
        nfs_open_owner_put(req->open_4_0_owner);
        req->open_4_0_owner = NULL;
    }

    if (!failed && ctx->open_filled) {
        /* The OPEN's own tail.  It can park on a CB_NULL probe and it can issue
         * a truncate, so it owns the completion from here; nothing of the
         * sequence may still be needed, which is why the attributes it takes
         * were copied out of the compound before this. */
        struct chimera_vfs_attrs fattr        = ctx->open_attr;
        int                      ctx_has_attr = ctx->open_has_attr;
        struct OPEN4res         *ores         =
            &req->res_compound.resarray[ctx->open_res_index].opopen;

        nfs4_vfs_dispose(compound, ctx);

        if (chimera_nfs4_open_grant_delegation(req, ores,
                                               ctx_has_attr ? &fattr : NULL)) {
            return; /* parked; resumes through nfs4_cb_null_complete */
        }

        chimera_nfs4_open_complete(req, NFS4_OK);
        return;
    }

    if (req->minorversion > 0 &&
        chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
        ctx->accepted_compound = compound;
        ctx->accepted_index    = req->index;
        ctx->accepted_fhlen    = req->fhlen;
        memcpy(ctx->accepted_fh, req->fh, req->fhlen);
        ctx->accepted_status        = failed ? status : NFS4_OK;
        req->compound_probe_private = ctx;
        req->compound_probe_resume  = nfs4_vfs_publish_delegations;
        nfs4_vfs_publish_delegations(req);
        return;
    }
    nfs4_vfs_dispose(compound, ctx);

    chimera_nfs4_compound_complete(req, failed ? status : NFS4_OK);
} /* nfs4_vfs_compound_complete */

/* READDIR carries its verifier as an opaque; the VFS takes it as a value. */
static uint64_t
nfs4_vfs_readdir_verifier(const struct READDIR4args *args)
{
    uint64_t verifier;

    memcpy(&verifier, args->cookieverf, sizeof(verifier));

    return verifier;
} /* nfs4_vfs_readdir_verifier */

/*
 * Append one xattr op, staging its name the way the per-op handler does.
 * Returns the VFS op index, or -1 (which refuses the whole sequence) if the
 * name will not fit in the reply buffer -- the length itself was already
 * accepted by nfs4_vfs_xattr_name_ok during the scan.
 */
static int
nfs4_vfs_add_xattr_op(
    struct nfs_request          *req,
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop)
{
    const xdr_opaque *wire;
    char             *name;
    int               namelen;

    switch (argop->argop) {
        case OP_GETXATTR:
            wire = &argop->opgetxattr.gxa_name;
            break;
        case OP_SETXATTR:
            wire = &argop->opsetxattr.sxa_key;
            break;
        default:
            wire = &argop->opremovexattr.rxa_name;
            break;
    } /* switch */

    if (chimera_nfs4_xattr_stage_name(req, wire->data, wire->len,
                                      &name, &namelen) != NFS4_OK) {
        return -1;
    }

    switch (argop->argop) {
        case OP_GETXATTR:
            return chimera_vfs_compound_add_getxattr(
                compound, name, namelen,
                chimera_nfs4_xattr_stage_max(req, CHIMERA_NFS4_GETXATTR_MAX));
        case OP_SETXATTR:
            return chimera_vfs_compound_add_setxattr(
                compound, argop->opsetxattr.sxa_option, name, namelen,
                argop->opsetxattr.sxa_value.data,
                argop->opsetxattr.sxa_value.len);
        default:
            return chimera_vfs_compound_add_removexattr(compound, name,
                                                        namelen);
    } /* switch */
} /* nfs4_vfs_add_xattr_op */

/*
 * The create attributes an exclusive OPEN carries.
 *
 * EXCLUSIVE4 carries none at all -- the verifier occupies the attribute slot --
 * so the object's mode is undefined until the client's follow-up SETATTR (RFC
 * 7530 §16.16.5) and it is created owner-only, the same safe default the per-op
 * path, Linux nfsd and NFS-Ganesha all use.  EXCLUSIVE4_1 does carry
 * attributes, and takes the same default when they leave the mode out.
 *
 * Either way the verifier is stamped into atime and mtime, overwriting anything
 * the client asked for there -- which is why an EXCLUSIVE4_1 that sets
 * time_access_set or time_modify_set is refused rather than silently clobbered.
 */
static nfsstat4
nfs4_vfs_open_exclusive_attrs(
    const struct OPEN4args   *args,
    struct chimera_vfs_attrs *attr,
    struct nfs4_vfs_op       *map)
{
    const uint8_t *verf;
    uint32_t       part;

    if (args->openhow.how.mode == EXCLUSIVE4_1) {
        nfsstat4 status = nfs4_vfs_decode_attrs(map,
                                                &args->openhow.how.ch_createboth.cva_attrs, attr);
        if (status != NFS4_OK) {
            return status;
        }
        verf = args->openhow.how.ch_createboth.cva_verf;
    } else {
        verf = args->openhow.how.createverf;
    }

    attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;

    memcpy(&part, verf, 4);
    attr->va_atime.tv_sec  = part;
    attr->va_atime.tv_nsec = 0;
    memcpy(&part, verf + 4, 4);
    attr->va_mtime.tv_sec  = part;
    attr->va_mtime.tv_nsec = 0;

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        attr->va_mode      = 0600;
    }
    return NFS4_OK;
} /* nfs4_vfs_open_exclusive_attrs */

/*
 * Append an OPEN, marshalling its arguments the way the per-op path does before
 * it makes any VFS call.
 *
 * All of this is a pure function of the OPEN's own arguments -- the create mode
 * selects flags and unmarshals the create attributes, share_access selects the
 * data-access intent -- which is why it can happen here, when the sequence is
 * built, rather than from inside it.  The two things that are NOT arguments,
 * because they depend on what the name resolves to, are the options: refusing a
 * non-regular object by type, and applying the create attributes only to an
 * object this open actually creates.
 */
static int
nfs4_vfs_add_open_op(
    struct nfs_request          *req,
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop,
    struct nfs4_vfs_op          *map)
{
    const struct OPEN4args  *args = &argop->opopen;
    struct chimera_vfs_attrs attr;
    unsigned int             flags   = 0;
    uint32_t                 opts    = 0;
    const char              *name    = NULL;
    int                      namelen = 0;
    uint64_t                 attr_mask;

    memset(&attr, 0, sizeof(attr));

    if (args->claim.claim == CLAIM_NULL) {
        name              = (const char *) args->claim.file.data;
        namelen           = (int) args->claim.file.len;
        map->open_by_name = 1;
    }

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        if (args->openhow.how.mode != UNCHECKED4) {
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
        }

        if (args->openhow.how.mode == UNCHECKED4 ||
            args->openhow.how.mode == GUARDED4) {
            /* Resolve an existing name's type rather than opening it, so a
             * socket or a directory is answered for by type.  An exclusive
             * create does NOT ask for this: it wants the plain collision, so
             * that whatever is in the way it can go and look at it.  (The
             * per-op path draws the line in the same place.) */
            flags |= CHIMERA_VFS_OPEN_CREATE_REGULAR;

            if (nfs4_vfs_decode_attrs(map, &args->openhow.how.createattrs,
                                      &attr) != NFS4_OK) {
                return -1;
            }
        } else {
            /* EXCLUSIVE4 and EXCLUSIVE4_1 stamp the client's verifier into the
             * object's atime and mtime, which is how a repeat of the same
             * create recognises its own earlier one.  (Linux nfsd does the
             * same; a server-private xattr would be better and is a TODO on the
             * per-op path.)  A collision therefore has to be looked at rather
             * than refused, which is what EXCLUSIVE_RETRY is for. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY;

            if (nfs4_vfs_open_exclusive_attrs(args, &attr, map) != NFS4_OK) {
                return -1;
            }
        }

        if (args->openhow.how.mode == UNCHECKED4) {
            /* An UNCHECKED4 create of a name that is already there opens it
             * without restyling it, except that size 0 truncates -- and the
             * truncate is deliberately not part of the open, so an OPEN that
             * fails afterwards leaves the file's contents alone. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY |
                CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY;

            map->open_trunc_if_existed =
                (attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) && attr.va_size == 0;
        }
    } else if (namelen) {
        /* A plain open must classify a non-regular object before a backend
         * tries to open it. */
        opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY;
    }

    /* The share access the client asked for is the data-access intent the
     * engine's open gate authorizes and stamps on the handle for every later
     * stateful READ/WRITE through this open. */
    if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        flags |= CHIMERA_VFS_OPEN_WRITE_ONLY;
    }

    (void) req;

    /* The same attributes the per-op path's open asks for -- no more, so an
     * object is not stat'd more thoroughly on one path than the other.  An
     * exclusive create adds the two the verifier lives in. */
    attr_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
        CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID | CHIMERA_VFS_ATTR_ACL |
        CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME;

    if (opts & CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY) {
        attr_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
    }

    return chimera_vfs_compound_add_open(compound, name, namelen, flags, opts, &attr, attr_mask, 0, 0);
} /* nfs4_vfs_add_open_op */

static int
nfs4_vfs_stateid_closed(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid)
{
    for (uint32_t i = 0; i < ctx->num_ops; i++) {
        struct nfs4_vfs_op *map = &ctx->ops[i];
        if (map->close_passed &&
            !memcmp(map->close_stateid.other, sid->other, sizeof(sid->other))) {
            return 1;
        }
    }
    return 0;
} /* nfs4_vfs_stateid_closed */

/* Every reserved OPEN, including CLOSE-only parents, has one journal view. */
static struct nfs_open_state *
nfs4_vfs_reserved_state(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid)
{
    struct nfs4_vfs_state *opened = nfs4_vfs_private_open(ctx, sid);

    return opened ? &opened->shadow : NULL;
} /* nfs4_vfs_reserved_state */

static nfsstat4
nfs4_vfs_close_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    const struct stateid4       *sid)
{
    struct nfs4_vfs_compound_ctx *ctx = map->ctx;
    struct nfs_open_state        *state;
    struct nfs4_vfs_state        *opened;
    uint32_t                      fhlen;
    const uint8_t                *fh = chimera_vfs_compound_current_fh(compound, &fhlen);
    nfsstat4                      status;

    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (chimera_nfs4_stateid_is_current(sid)) {
        if (!ctx->current_stateid_valid) {
            return NFS4ERR_BAD_STATEID;
        }
        sid = &ctx->current_stateid;
    }
    if (nfs4_vfs_stateid_closed(ctx, sid)) {
        return NFS4ERR_BAD_STATEID;
    }
    opened = nfs4_vfs_private_open(ctx, sid);
    if (!opened || opened->closed) {
        return map->stateid_reservation_status ? map->stateid_reservation_status : NFS4ERR_BAD_STATEID;
    }
    state = &opened->shadow;
    if (state->fh_len != fhlen || memcmp(state->fh, fh, fhlen)) {
        return NFS4ERR_BAD_STATEID;
    }
    status = nfs4_stateid_check_seqid(state->seqid, sid->seqid);
    if (status != NFS4_OK) {
        return status;
    }
    opened->closed = 1;
    state->seqid++;
    nfs4_vfs_stateid(ctx, opened, state->seqid, &map->close_stateid);
    map->close_passed = 1;
    nfs4_vfs_rebuild_state_view(ctx, NULL);
    ctx->current_stateid_valid = 0;
    return NFS4_OK;
} /* nfs4_vfs_close_prepare */

static nfsstat4
nfs4_vfs_confirm_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx *ctx = map->ctx;
    const struct stateid4        *sid = &ctx->req->args_compound->argarray[map->res_index].opopen_confirm.open_stateid
    ;
    struct nfs4_vfs_state        *entry = nfs4_vfs_private_open(ctx, sid);
    uint32_t                      length;
    const uint8_t                *fh = chimera_vfs_compound_current_fh(compound, &length);

    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (!entry || entry->closed || entry->group->owner_replay.confirmed ||
        entry->shadow.fh_len != length || memcmp(entry->shadow.fh, fh, length)) {
        return NFS4ERR_BAD_STATEID;
    }
    nfsstat4 status = nfs4_stateid_check_seqid(entry->shadow.seqid, sid->seqid);
    if (status != NFS4_OK) {
        return status;
    }
    entry->shadow.seqid++;
    entry->version_dirty                 = 1;
    entry->group->owner_replay.confirmed = true;
    nfs4_vfs_stateid(ctx, entry, entry->shadow.seqid, &map->reserved_stateid);
    return NFS4_OK;
} /* nfs4_vfs_confirm_prepare */

static nfsstat4
nfs4_vfs_downgrade_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx     *ctx  = map->ctx;
    struct nfs_request               *req  = ctx->req;
    const struct OPEN_DOWNGRADE4args *args = &req->args_compound->argarray[map->res_index].opopen_downgrade;
    const struct stateid4            *sid  = &args->open_stateid;
    struct nfs4_vfs_state            *entry;
    struct nfs_open_state            *state;
    uint32_t                          fh_len;
    const uint8_t                    *fh     = chimera_vfs_compound_current_fh(compound, &fh_len);
    uint32_t                          access = args->share_access & ~OPEN4_SHARE_ACCESS_WANT_DELEG_MASK;
    nfsstat4                          status;

    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (chimera_nfs4_stateid_is_current(sid)) {
        if (!ctx->current_stateid_valid) {
            return NFS4ERR_BAD_STATEID;
        }
        sid = &ctx->current_stateid;
    }
    entry = nfs4_vfs_private_open(ctx, sid);
    if (!entry || entry->closed || nfs4_vfs_stateid_closed(ctx, sid)) {
        if (!entry && map->stateid_reservation_status) {
            return map->stateid_reservation_status;
        }
        return NFS4ERR_BAD_STATEID;
    }
    state = &entry->shadow;
    if (state->owner->client != ctx->client || state->fh_len != fh_len ||
        memcmp(state->fh, fh, fh_len)) {
        return NFS4ERR_BAD_STATEID;
    }
    if (!nfs_open_state_check_principal(state, req->principal_flavor, req->principal_machinename,
                                        req->principal_machinename_len)) {
        return NFS4ERR_ACCESS;
    }
    status = nfs4_stateid_check_seqid(state->seqid, sid->seqid);
    if (status != NFS4_OK) {
        return status;
    }
    if (!access || (access & ~state->share_access) || (args->share_deny & ~state->share_deny) ||
        !nfs_open_downgrade_expressible(state->share_combos, access, args->share_deny)) {
        return NFS4ERR_INVAL;
    }
    map->open_state = entry;
    nfs4_vfs_state_copy(map->reserved_open, state);
    map->reserved_open->share_access = access;
    map->reserved_open->share_deny   = args->share_deny;
    map->reserved_open->share_combos = nfs_open_combo_bit(access, args->share_deny);
    map->reserved_open->seqid++;
    nfs4_vfs_stateid(ctx, entry, map->reserved_open->seqid, &map->reserved_stateid);
    nfs4_vfs_rebuild_state_view(ctx, entry);
    return NFS4_OK;
} /* nfs4_vfs_downgrade_prepare */

static void
nfs4_vfs_downgrade_reserve_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }

    struct nfs4_vfs_op *map = private_data;

    nfs4_vfs_open_reserve_prepare(compound, index, status, private_data);
    chimera_vfs_compound_op_args(compound, index)->in_handle = map->reserved_open->handle;
} /* nfs4_vfs_downgrade_reserve_prepare */

static nfsstat4
nfs4_vfs_lock_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx *ctx     = map->ctx;
    struct nfs_request           *req     = ctx->req;
    const struct nfs_argop4      *op      = &req->args_compound->argarray[map->res_index];
    int                           locking = op->argop == OP_LOCK;
    const struct LOCK4args       *args    = &op->oplock;
    const struct stateid4        *sid     = locking ? (args->locker.new_lock_owner ?
                                                       &args->locker.open_owner.open_stateid : &args->locker.lock_owner.
                                                       lock_stateid) :
        &op->oplocku.lock_stateid;
    struct stateid4               current;
    struct nfs4_vfs_lock         *entry = NULL;
    struct nfs4_vfs_state        *parent;
    uint32_t                      fhlen, seqid;
    const uint8_t                *fh     = chimera_vfs_compound_current_fh(compound, &fhlen);
    uint32_t                      mode   = locking ? args->locktype : op->oplocku.locktype;
    uint64_t                      offset = locking ? args->offset : op->oplocku.offset;
    uint64_t                      length = locking ? args->length : op->oplocku.length;
    nfsstat4                      status;

    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (locking) {
        status = nfs_recovery_open_check(&req->thread->shared->nfs4_recovery,
                                         ctx->client, args->reclaim != 0);
        if (status != NFS4_OK) {
            return status;
        }
    }
    if (chimera_nfs4_stateid_is_current(sid)) {
        if (!ctx->current_stateid_valid) {
            return NFS4ERR_BAD_STATEID;
        }
        current       = ctx->current_stateid;
        current.seqid = 0;
        sid           = &current;
    }
    if (locking && args->locker.new_lock_owner) {
        parent = nfs4_vfs_private_open(ctx, sid);
        if (!parent || parent->closed || !map->lock_group) {
            return map->stateid_reservation_status ? map->stateid_reservation_status : NFS4ERR_BAD_STATEID;
        }
        seqid = parent->shadow.seqid;
        for (uint32_t i = 0; i < ctx->num_locks; i++) {
            struct nfs4_vfs_lock *candidate = &ctx->locks[i];
            if (candidate->active && candidate->group == map->lock_group && candidate->parent &&
                !candidate->parent->closed &&
                candidate->parent->shadow.fh_len == fhlen &&
                !memcmp(candidate->parent->shadow.fh, fh, fhlen)) {
                entry = candidate;
                break;
            }
        }
        if (!entry) {
            for (uint32_t i = 0; i < ctx->num_locks; i++) {
                if (ctx->locks[i].target == map->lock_candidate) {
                    entry         = &ctx->locks[i];
                    entry->parent = parent;
                    break;
                }
            }
        }
    } else {
        entry = nfs4_vfs_private_lock(ctx, sid);
        if (!entry) {
            return map->stateid_reservation_status ? map->stateid_reservation_status : NFS4ERR_BAD_STATEID;
        }
        parent = entry->parent;
        seqid  = entry->seqid;
    }
    if (!entry || !parent || parent->closed ||
        parent->shadow.owner->client != ctx->client ||
        parent->shadow.fh_len != fhlen || memcmp(parent->shadow.fh, fh, fhlen)) {
        return NFS4ERR_BAD_STATEID;
    }
    if (!nfs_open_state_check_principal(&parent->shadow, req->principal_flavor,
                                        req->principal_machinename, req->principal_machinename_len)) {
        return NFS4ERR_ACCESS;
    }
    status = nfs4_stateid_check_seqid(seqid, sid->seqid);
    if (status != NFS4_OK) {
        return status;
    }
    if (mode < READ_LT || mode > WRITEW_LT || !length ||
        (length != UINT64_MAX && offset > UINT64_MAX - length)) {
        return NFS4ERR_INVAL;
    }
    bool write = mode == WRITE_LT || mode == WRITEW_LT;
    /* Preserve the legacy server's permissive LOCK admission. RFC 8881
     * 18.10.4 leaves lock-mode versus OPEN-mode checks to filesystem policy;
     * a range reservation does not grant additional READ/WRITE rights. */
    map->lock_state = entry;
    if (locking) {
        struct chimera_claim_owner owner = {
            .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
            .client_key = entry->group->owner->client->client_id,
            .owner_lo   = XXH3_64bits(entry->group->owner->owner, entry->group->owner->owner_len),
        };
        chimera_vfs_claim_init_range(&map->lock_claim, write, false, offset, length, &owner);
        /* Admission protects tentative coverage; GETLK sees the accepted
         * journal ranges installed at compound completion. */
        map->lock_claim.provisional = 1;
        map->lock_claim.is_alive_cb = nfs_client_lease_alive;
        map->lock_claim.revoked_cb  = nfs_client_lease_revoked_cb;
        map->lock_claim.cb_private  = ctx->client;
    } else {
        if (!nfs_lock_range_journal_unlock(entry->ranges, offset, length)) {
            return NFS4ERR_RESOURCE;
        }
        entry->seqid++;
        entry->dirty = 1;
        nfs4_vfs_lock_stateid(ctx, entry, &map->reserved_stateid);
        ctx->current_stateid       = map->reserved_stateid;
        ctx->current_stateid_valid = 1;
    }
    return NFS4_OK;
} /* nfs4_vfs_lock_prepare */

static void
nfs4_vfs_lock_reserve_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }

    struct nfs4_vfs_op             *map    = private_data;
    struct nfs4_vfs_compound_ctx   *ctx    = map->ctx;
    struct nfs_open_state          *parent = &map->lock_state->parent->shadow;
    struct chimera_vfs_compound_op *op     = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    nfs4_vfs_range_view(ctx, parent->fh, parent->fh_len);
    map->lock_claim.admit_excluded     = ctx->range_previous;
    map->lock_claim.admit_num_excluded = ctx->num_range_previous;
    op->in_handle                      = parent->handle;
    op->claim_ranges                   = ctx->range_current;
    op->num_claim_ranges               = ctx->num_range_current;
} /* nfs4_vfs_lock_reserve_prepare */

static void
nfs4_vfs_lock_reserve_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        return;
    }

    struct nfs4_vfs_op                   *map = private_data;

    if (*status == CHIMERA_VFS_OK) {
        return;
    }
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    map->lockt_conflict = op->claim_conflict;
    if (*status == CHIMERA_VFS_EACCES || *status == CHIMERA_VFS_EAGAIN) {
        map->verify_status = NFS4ERR_DENIED;
        nfs4_vfs_lock_conflict_owner(map);
    }
} /* nfs4_vfs_lock_reserve_complete */

static void
nfs4_vfs_lock_commit(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    if (((struct nfs4_vfs_op *) private_data)->replay_hit) {
        return;
    }

    struct nfs4_vfs_op     *map   = private_data;
    struct nfs4_vfs_lock   *entry = map->lock_state;
    const struct LOCK4args *args  = &map->ctx->req->args_compound->argarray[map->res_index].oplock;

    (void) compound;
    (void) index;
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (!nfs_lock_range_journal_lock(entry->ranges, args->offset, args->length,
                                     args->locktype == WRITE_LT || args->locktype == WRITEW_LT, &map->lock_claim)) {
        map->verify_status = NFS4ERR_RESOURCE;
        *status            = CHIMERA_VFS_ENOSPC;
        return;
    }
    entry->seqid++;
    entry->active = entry->dirty = 1;
    nfs4_vfs_lock_stateid(map->ctx, entry, &map->reserved_stateid);
    map->ctx->current_stateid       = map->reserved_stateid;
    map->ctx->current_stateid_valid = 1;
} /* nfs4_vfs_lock_commit */

static nfsstat4
nfs4_vfs_io_advise_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx *ctx   = map->ctx;
    struct nfs_request           *req   = ctx->req;
    struct nfs_state_table       *table = &req->thread->shared->nfs4_state_table;
    const struct stateid4        *sid   = &req->args_compound->argarray[map->res_index].opio_advise.iaa_stateid;
    struct stateid4               current;
    struct nfs_open_state        *open_state;
    struct nfs_open_state        *reserved;
    uint32_t                      fh_len, seqid;
    const uint8_t                *fh = chimera_vfs_compound_current_fh(compound, &fh_len);
    nfsstat4                      status;

    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (chimera_nfs4_stateid_is_current(sid)) {
        if (!ctx->current_stateid_valid) {
            return NFS4ERR_BAD_STATEID;
        }
        current       = ctx->current_stateid;
        current.seqid = 0;
        sid           = &current;
    }
    if (nfs4_stateid_is_special(sid)) {
        return NFS4_OK;
    }
    if (nfs4_vfs_stateid_closed(ctx, sid)) {
        return NFS4ERR_BAD_STATEID;
    }
    reserved = nfs4_vfs_reserved_state(ctx, sid);
    struct nfs4_vfs_lock *lock_entry = nfs4_vfs_private_lock(ctx, sid);
    if (lock_entry) {
        if (!lock_entry->parent || lock_entry->parent->closed) {
            return NFS4ERR_BAD_STATEID;
        }
        open_state = &lock_entry->parent->shadow;
        seqid      = lock_entry->seqid;
    } else if (reserved) {
        open_state = reserved;
        seqid      = open_state->seqid;
    } else {
        return nfs_state_table_advise(table, sid, ctx->client, fh, fh_len,
                                      req->principal_flavor, req->principal_machinename,
                                      req->principal_machinename_len);
    }
    if (!open_state || open_state->owner->client != ctx->client ||
        open_state->fh_len != fh_len || memcmp(open_state->fh, fh, fh_len)) {
        status = NFS4ERR_BAD_STATEID;
    } else if (!nfs_open_state_check_principal(open_state, req->principal_flavor,
                                               req->principal_machinename, req->principal_machinename_len)) {
        status = NFS4ERR_ACCESS;
    } else {
        status = nfs4_stateid_check_seqid(seqid, sid->seqid);
    }
    return status;
} /* nfs4_vfs_io_advise_prepare */

static void
nfs4_vfs_test_stateids(struct nfs4_vfs_op *map)
{
    struct nfs4_vfs_compound_ctx   *ctx  = map->ctx;
    const struct TEST_STATEID4args *args = &ctx->req->args_compound->argarray[map->res_index].optest_stateid;

    for (uint32_t i = 0; i < args->num_ts_stateids; i++) {
        const struct stateid4 *sid        = &args->ts_stateids[i];
        struct nfs4_vfs_state *entry      = nfs4_vfs_private_open(ctx, sid);
        struct nfs4_vfs_lock  *lock_entry = nfs4_vfs_private_lock(ctx, sid);
        nfsstat4               status;
        if (nfs4_stateid_is_special(sid) || chimera_nfs4_stateid_is_current(sid) ||
            nfs4_vfs_stateid_closed(ctx, sid) || (entry && entry->closed)) {
            status = NFS4ERR_BAD_STATEID;
        } else if (entry) {
            status = nfs4_stateid_check_seqid(entry->shadow.seqid, sid->seqid);
        } else if (lock_entry) {
            status = !lock_entry->parent || lock_entry->parent->closed ? NFS4ERR_BAD_STATEID :
                nfs4_stateid_check_seqid(lock_entry->seqid, sid->seqid);
        } else {
            status = nfs_state_table_test_stateid(&ctx->req->thread->shared->nfs4_state_table,
                                                  sid, ctx->client);
        }
        map->test_stateid_statuses[i] = status;
    }
} /* nfs4_vfs_test_stateids */

/* Anonymous I/O must include unpublished OPEN denies as well as the public
 * state table. The VFS independently enforces all nonexcluded protocol claims. */
static nfsstat4
nfs4_vfs_anonymous_authorize(
    struct nfs4_vfs_compound_ctx *ctx,
    const uint8_t                *fh,
    uint16_t                      fhlen,
    uint32_t                      access)
{
    nfsstat4 status = nfs4_clients_check_io_denied_except(
        &ctx->req->thread->shared->nfs4_shared_clients, fh, fhlen, access,
        ctx->closed_states, ctx->num_closed_states);

    if (status != NFS4_OK) {
        return status;
    }
    for (uint32_t i = 0; i < ctx->num_states; i++) {
        struct nfs4_vfs_state *entry = &ctx->states[i];
        struct nfs_open_state *state = &entry->shadow;
        if (entry->active && !entry->closed && state->fh_len == fhlen &&
            !memcmp(state->fh, fh, fhlen) && (state->share_deny & access)) {
            return NFS4ERR_LOCKED;
        }
    }
    return NFS4_OK;
} /* nfs4_vfs_anonymous_authorize */

static nfsstat4
nfs4_vfs_owner_io_denied(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs_open_state        *state,
    uint32_t                      access)
{
    nfsstat4 status = nfs_client_check_io_denied_except(state->owner->client, state->owner,
                                                        state->fh, state->fh_len, access,
                                                        ctx->closed_states, ctx->num_closed_states);

    if (status != NFS4_OK) {
        return status;
    }
    for (uint32_t i = 0; i < ctx->num_states; i++) {
        struct nfs4_vfs_state *entry = &ctx->states[i];
        struct nfs_open_state *peer  = &entry->shadow;
        if (entry->active && !entry->closed && peer->owner != state->owner &&
            peer->fh_len == state->fh_len && !memcmp(peer->fh, state->fh, state->fh_len) &&
            (peer->share_deny & access)) {
            return NFS4ERR_LOCKED;
        }
    }
    return NFS4_OK;
} /* nfs4_vfs_owner_io_denied */

/*
 * Resolve the handle a READ or WRITE runs against, and authorize it.
 *
 * A special stateid is anonymous: there is no open to consult, so the I/O runs
 * against the current object and the deny-share reservations held by any owner
 * of any client are what authorize it.  A real one names an open (or a lock,
 * which has one), and then that open's own handle and its granted access mode
 * are what authorize it -- the OPENMODE rule.
 *
 * Called at the operation's execution checkpoint. A non-OK result is retained
 * privately and stops the compound before I/O. Delegation stateids use a pure
 * authorization snapshot and carry their holder actor into VFS admission.
 */
static nfsstat4
nfs4_vfs_io_authorize(
    struct nfs4_vfs_compound_ctx     *ctx,
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct stateid4            *sid,
    uint32_t                          share_access,
    const uint8_t                    *fh,
    int                               fhlen,
    struct chimera_vfs_open_handle  **out_handle,
    struct chimera_claim_actor       *out_owner,
    int                              *have_owner)
{
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    struct nfs_open_state          *open_state;
    struct nfs_lock_state          *lock_state;
    struct chimera_vfs_open_handle *state_handle;
    uint32_t                        current_seqid;
    void                           *state_void;
    uint8_t                         state_type;
    nfsstat4                        status;

    /* SEEK inspects allocation metadata through either kind of data open.
     * Zero requests no new data-access mode, while anonymous/share checks
     * still treat the metadata query as a read. */
    uint32_t                        check_access = share_access ? share_access : OPEN4_SHARE_ACCESS_READ;

    *out_handle = NULL;
    *have_owner = 0;

    if (nfs4_vfs_stateid_closed(ctx, sid)) {
        return NFS4ERR_BAD_STATEID;
    }
    struct nfs_open_state          *reserved   = nfs4_vfs_reserved_state(ctx, sid);
    struct nfs4_vfs_lock           *lock_entry = nfs4_vfs_private_lock(ctx, sid);
    if (lock_entry) {
        if (!lock_entry->parent || lock_entry->parent->closed) {
            return NFS4ERR_BAD_STATEID;
        }
        reserved = &lock_entry->parent->shadow;
    }
    if (reserved) {
        open_state   = reserved;
        state_handle = nfs_state_io_handle(open_state, NFS4_SLOT_TYPE_OPEN, check_access);
        if (state_handle->fh_len != fhlen || memcmp(state_handle->fh, fh, fhlen)) {
            return NFS4ERR_BAD_STATEID;
        }
        status = nfs4_stateid_check_seqid(lock_entry ? lock_entry->seqid : open_state->seqid, sid->seqid);
        if (status != NFS4_OK) {
            return status;
        }
        if (share_access && !(open_state->share_access & share_access)) {
            return NFS4ERR_OPENMODE;
        }
        if (!nfs_open_state_check_principal(open_state, req->principal_flavor,
                                            req->principal_machinename, req->principal_machinename_len)) {
            return NFS4ERR_ACCESS;
        }
        status = nfs4_vfs_owner_io_denied(ctx, open_state, check_access);
        if (status != NFS4_OK) {
            return status;
        }
        memset(out_owner, 0, sizeof(*out_owner));
        out_owner->owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
        out_owner->owner.client_key = open_state->owner->client->client_id;
        out_owner->owner.owner_lo   = state_handle->fh_hash;
        *have_owner                 = 1;
        chimera_vfs_dup_handle(thread->vfs_thread, state_handle);
        *out_handle = state_handle;
        return NFS4_OK;
    }

    if (nfs4_stateid_is_special(sid)) {
        return nfs4_vfs_anonymous_authorize(ctx, fh, fhlen, check_access);
    }

    struct nfs4_stateid_view view;
    nfs4_stateid_decode(&view, sid);
    if (view.type == NFS4_STATEID_TYPE_DELEG) {
        status = nfs_state_table_delegation_io(table, sid, ctx->client,
                                               fh, fhlen, check_access, out_owner);
        *have_owner = status == NFS4_OK;
        return status;
    }
    if (view.type != NFS4_STATEID_TYPE_OPEN && view.type != NFS4_STATEID_TYPE_LOCK) {
        return NFS4ERR_BAD_STATEID;
    }

    status = (req->session ? nfs_state_table_acquire_no_renew : nfs_state_table_acquire)(table, sid, 0, &state_void, &
                                                                                         state_type);

    if (status != NFS4_OK) {
        return status;
    }

    status = nfs_state_check_client(state_void, state_type,
                                    req->session ?
                                    ctx->client : NULL);

    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return status;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        open_state    = state_void;
        state_handle  = nfs_state_io_handle(state_void, state_type, check_access);
        current_seqid = open_state->seqid;
    } else {
        lock_state    = state_void;
        open_state    = lock_state->open_state;
        state_handle  = nfs_state_io_handle(state_void, state_type, check_access);
        current_seqid = lock_state->seqid;
    }

    /* Session stateids still validate explicit versions; seqid zero alone
     * asks for the current version (RFC 8881 section 8.2.4). */
    status = nfs4_stateid_check_seqid(current_seqid, sid->seqid);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return status;
    }

    /* RFC 7530 §9.1.4 / RFC 8881 §9.1.2: I/O through an open (or lock) stateid
     * is limited to the associated open's granted access mode. */
    if (share_access && (open_state->share_access & share_access) == 0) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return NFS4ERR_OPENMODE;
    }

    status = nfs4_vfs_owner_io_denied(ctx, open_state, check_access);

    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return status;
    }

    if (!nfs_open_state_check_principal(open_state,
                                        req->principal_flavor,
                                        req->principal_machinename,
                                        req->principal_machinename_len)) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return NFS4ERR_ACCESS;
    }

    if (!state_handle) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return NFS4ERR_NOTSUPP;
    }

    if (state_handle->fh_len != fhlen || memcmp(state_handle->fh, fh, fhlen)) {
        nfs_state_table_release(table, state_void, state_type, thread->vfs_thread);
        return NFS4ERR_BAD_STATEID;
    }

    /* Whose I/O this is.  Without it the claim layer arbitrates the client's
     * own I/O against the client's own share reservation -- denying it, and
     * recalling the very delegation it is being done under. */
    memset(out_owner, 0, sizeof(*out_owner));
    out_owner->owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    out_owner->owner.client_key = open_state->owner->client->client_id;
    out_owner->owner.owner_lo   = state_handle->fh_hash;
    out_owner->owner.owner_hi   = 0;
    *have_owner                 = 1;

    /* A reference of our own, so the handle outlives the state slot. */
    chimera_vfs_dup_handle(thread->vfs_thread, state_handle);
    *out_handle = state_handle;

    nfs_state_table_release(table, state_void, state_type, thread->vfs_thread);

    return NFS4_OK;
} /* nfs4_vfs_io_authorize */

/*
 * Authorize a size-changing SETATTR, and resolve the handle it applies
 * through.
 *
 * Apply the per-op path's rule against the execution-time current filehandle:
 * a size change is a write (RFC 7530 §9.1.4.3), so through a special stateid it
 * must honour deny-WRITE reservations held by any owner of any client, and
 * through a real one the stateid must name an open OF THIS OBJECT that was
 * opened for writing.  The open's own handle then carries that grant, the way a
 * descriptor carries ftruncate(2)'s -- which a fresh open by name would not.
 *
 * Returns NFS4_OK with *out_handle either a reference the caller must release,
 * or NULL meaning "apply against the current object".
 */
static nfsstat4
nfs4_vfs_setattr_authorize(
    struct nfs4_vfs_compound_ctx     *ctx,
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct SETATTR4args        *args,
    const uint8_t                    *fh,
    int                               fhlen,
    struct chimera_vfs_open_handle  **out_handle,
    struct chimera_claim_actor       *out_owner,
    int                              *have_owner)
{
    /* A size change uses the write context of an OPEN or its LOCK stateid.
     * Share the execution-time owner, principal, version and access checks
     * with WRITE, including handles broadened by a later OPEN. */
    if (!nfs4_stateid_is_special(&args->stateid)) {
        struct nfs4_stateid_view view;
        nfs4_stateid_decode(&view, &args->stateid);
        if (view.type != NFS4_STATEID_TYPE_OPEN && view.type != NFS4_STATEID_TYPE_LOCK) {
            return NFS4ERR_BAD_STATEID;
        }
    }
    return nfs4_vfs_io_authorize(ctx, thread, req, &args->stateid,
                                 OPEN4_SHARE_ACCESS_WRITE, fh, fhlen,
                                 out_handle, out_owner, have_owner);
} /* nfs4_vfs_setattr_authorize */

/* Stateid wire inputs stay immutable. Resolve the current-stateid placeholder
 * from attempt-private protocol state, rather than editing RPC arguments. */
static const struct stateid4 *
nfs4_vfs_arg_stateid(const struct nfs_argop4 *argop)
{
    switch (argop->argop) {
        case OP_LOCK: return argop->oplock.locker.new_lock_owner ?
                   &argop->oplock.locker.open_owner.open_stateid : &argop->oplock.locker.lock_owner.lock_stateid;
        case OP_LOCKU: return &argop->oplocku.lock_stateid;
        case OP_CLOSE: return &argop->opclose.open_stateid;
        case OP_OPEN_CONFIRM: return &argop->opopen_confirm.open_stateid;
        case OP_OPEN_DOWNGRADE: return &argop->opopen_downgrade.open_stateid;
        case OP_IO_ADVISE: return &argop->opio_advise.iaa_stateid;
        case OP_READ: return &argop->opread.stateid;
        case OP_READ_PLUS: return &argop->opread_plus.rpa_stateid;
        case OP_WRITE: return &argop->opwrite.stateid;
        case OP_SETATTR: return &argop->opsetattr.stateid;
        case OP_ALLOCATE: return &argop->opallocate.aa_stateid;
        case OP_DEALLOCATE: return &argop->opdeallocate.da_stateid;
        case OP_SEEK: return &argop->opseek.sa_stateid;
        case OP_WRITE_SAME: return &argop->opwrite_same.wsa_stateid;
        default: return NULL;
    } /* switch */
} /* nfs4_vfs_arg_stateid */

/* Delegation/layout stateids still need the legacy coordination path. This
 * only classifies immutable input, not whether that stateid is authorized. */
static int
nfs4_vfs_stateid_encodable(
    struct nfs_request    *req,
    const struct stateid4 *sid,
    int                    after_open)
{
    struct nfs4_stateid_view view;

    if (!sid) {
        return 1;
    }
    if (chimera_nfs4_stateid_is_current(sid)) {
        if (after_open || !req->current_stateid_valid) {
            return 1;
        }
        sid = &req->current_stateid;
    }
    if (nfs4_stateid_is_special(sid)) {
        return 1;
    }
    nfs4_stateid_decode(&view, sid);
    return view.type == NFS4_STATEID_TYPE_OPEN || view.type == NFS4_STATEID_TYPE_LOCK;
} /* nfs4_vfs_stateid_encodable */

/* Data-server READ/WRITE is authorized by the MDS layout plus this server's
 * filehandle/export credentials, not by an entry in its local state table.
 * Do not extend that exception to other stateid-bearing operations. */
static bool
nfs4_vfs_data_server_io(
    struct nfs_request *req,
    uint32_t            opcode)
{
    return (opcode == OP_READ || opcode == OP_READ_PLUS || opcode == OP_WRITE) &&
           chimera_server_config_get_nfs_data_server(req->thread->shared->config);
} /* nfs4_vfs_data_server_io */

/* v4.1+ SECINFO consumes the protocol cursor. v4.0 restores its directory
 * after the internal name lookup and retains it for subsequent operations. */
static void
nfs4_vfs_secinfo_checked(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op *map = private_data;

    (void) compound;
    (void) index;
    if (*status == CHIMERA_VFS_OK && map->ctx->req->minorversion >= 1) {
        map->ctx->fh_consumed = 1;
    }
} /* nfs4_vfs_secinfo_checked */

/* Classify v4.0 owner replay before validating versions or changing private
 * state. A replay skips only this wire operation, preserving its prefix and
 * suffix within the same VFS attempt. */
static nfsstat4
nfs4_vfs_replay_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx     *ctx     = map->ctx;
    const struct nfs_argop4          *op      = &ctx->req->args_compound->argarray[map->res_index];
    const struct stateid4            *sid     = nfs4_vfs_arg_stateid(op);
    struct nfs4_owner_replay_journal *journal = NULL;
    struct nfs4_vfs_state            *parent  = NULL;
    struct nfs4_vfs_lock             *entry   = NULL;
    uint32_t                          incoming;
    bool                              new_lock = op->argop == OP_LOCK && op->oplock.locker.new_lock_owner;

    if (ctx->req->minorversion != 0 ||
        (op->argop != OP_CLOSE && op->argop != OP_OPEN_CONFIRM && op->argop != OP_OPEN_DOWNGRADE &&
         op->argop != OP_LOCK && op->argop != OP_LOCKU &&
         (op->argop != OP_OPEN || !map->open_response))) {
        return NFS4_OK;
    }
    if (sid && (nfs4_stateid_is_special(sid) || chimera_nfs4_stateid_is_current(sid))) {
        return NFS4ERR_BAD_STATEID;
    }
    if (op->argop == OP_OPEN) {
        journal  = &map->open_group->owner_replay;
        incoming = op->opopen.seqid;
    } else if (new_lock || op->argop == OP_CLOSE || op->argop == OP_OPEN_CONFIRM || op->argop == OP_OPEN_DOWNGRADE) {
        parent = nfs4_vfs_private_open(ctx, sid);
        if (!parent) {
            if (op->argop == OP_CLOSE && map->close_response) {
                map->replay_hit    = 1;
                map->close_stateid = map->close_response->stateid;
                map->verify_status = map->close_response->status;
                chimera_vfs_compound_op_skip(compound, map->prepare_index);
                return map->verify_status;
            }
            return map->stateid_reservation_status ? map->stateid_reservation_status : NFS4ERR_BAD_STATEID;
        }
        journal  = &parent->group->owner_replay;
        incoming = new_lock ? op->oplock.locker.open_owner.open_seqid :
            (op->argop == OP_CLOSE ? op->opclose.seqid :
             (op->argop == OP_OPEN_CONFIRM ? op->opopen_confirm.seqid : op->opopen_downgrade.seqid));
    } else {
        entry = nfs4_vfs_private_lock(ctx, sid);
        if (!entry) {
            return map->stateid_reservation_status ? map->stateid_reservation_status : NFS4ERR_BAD_STATEID;
        }
        journal  = &entry->group->owner_replay;
        incoming = op->argop == OP_LOCK ? op->oplock.locker.lock_owner.lock_seqid : op->oplocku.seqid;
    }
    int classification = nfs4_owner_replay_classify(journal, incoming, op->argop);
    if (classification == NFS4_SEQID_BAD) {
        return NFS4ERR_BAD_SEQID;
    }
    if (classification == NFS4_SEQID_NEW && new_lock) {
        if (!map->lock_group || op->oplock.locker.open_owner.lock_owner.clientid !=
            parent->shadow.owner->client->client_id) {
            return NFS4ERR_BAD_STATEID;
        }
        /* v4.0 re-establishment may reuse an empty child. A held child can
         * only answer the original establishing LOCK as a replay. */
        for (uint32_t i = 0; i < ctx->num_locks; i++) {
            struct nfs4_vfs_lock *candidate = &ctx->locks[i];
            if (candidate->active && candidate->group == map->lock_group &&
                candidate->parent && !candidate->parent->closed &&
                candidate->parent->shadow.fh_len == parent->shadow.fh_len &&
                !memcmp(candidate->parent->shadow.fh, parent->shadow.fh, parent->shadow.fh_len) &&
                !nfs_lock_range_journal_empty(candidate->ranges)) {
                journal        = &map->lock_group->owner_replay;
                classification = nfs4_owner_replay_classify(journal,
                                                            op->oplock.locker.open_owner.lock_seqid, OP_LOCK);
                if (classification != NFS4_SEQID_REPLAY) {
                    return NFS4ERR_BAD_SEQID;
                }
                break;
            }
        }
    }
    if (classification == NFS4_SEQID_REPLAY) {
        const struct nfs4_replay_cache *replay = &journal->replay;
        if (op->argop == OP_OPEN) {
            if (replay->status == NFS4_OK && !replay->open_valid) {
                return NFS4ERR_DELAY;
            }
            *map->open_response = *replay;
        }
        map->replay_hit       = 1;
        map->reserved_stateid = map->close_stateid = replay->stateid;
        map->verify_status    = replay->status;
        if (op->argop == OP_LOCK && replay->status == NFS4ERR_DENIED) {
            struct LOCK4res result;
            if (!nfs4_replay_fill_lock(replay, &result, map->lockt_owner)) {
                return NFS4ERR_BAD_SEQID;
            }
            map->lockt_conflict.offset = result.denied.offset;
            map->lockt_conflict.length = result.denied.length;
            map->lockt_conflict.used   = result.denied.locktype == WRITE_LT ? CHIMERA_CLAIM_W :
                CHIMERA_CLAIM_R;
            map->lockt_conflict.owner.client_key = result.denied.owner.clientid;
            map->lockt_owner_len                 = result.denied.owner.owner.len;
        }
        chimera_vfs_compound_op_skip(compound, map->prepare_index);
        return replay->status;
    }
    if (op->argop == OP_OPEN) {
        map->open_replay = journal;
        map->open_seqid  = incoming;
        map->open_rflags = OPEN4_RESULT_LOCKTYPE_POSIX |
            (journal->confirmed ? 0 : OPEN4_RESULT_CONFIRM);
    } else if (parent) {
        map->open_replay = &parent->group->owner_replay;
        map->open_seqid  = incoming;
        if (new_lock) {
            map->lock_replay = &map->lock_group->owner_replay;
            map->lock_seqid  = op->oplock.locker.open_owner.lock_seqid;
        }
    } else {
        map->lock_replay = journal;
        map->lock_seqid  = incoming;
    }
    return NFS4_OK;
} /* nfs4_vfs_replay_prepare */

static void
nfs4_vfs_replay_skip_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op *map = private_data;

    (void) status;
    if (map->replay_hit) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_replay_skip_prepare */

static void
nfs4_vfs_open_replay_restore(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op             *map = private_data;

    if (!map->replay_hit) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    struct OPEN4res                 response;
    uint32_t                        attributes[3], length;
    uint8_t                         fh[NFS4_FHSIZE];
    uint8_t                         who[NFS4_OPAQUE_LIMIT];
    if (!nfs4_replay_fill_open(map->open_response, &response, attributes, fh, &length, who)) {
        map->verify_status = NFS4ERR_DELAY;
        *status            = CHIMERA_VFS_EINVAL;
        return;
    }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
    memcpy(op->arg_fh, fh, length);
    op->arg_fh_len = length;
} /* nfs4_vfs_open_replay_restore */

/* The backend cannot resolve a protocol export junction. Restrict fallback
 * to component names that could name one; ordinary names are safe from every
 * cursor position, without resolving a future cursor during construction. */
static bool
nfs4_vfs_possible_junction(
    struct chimera_server_nfs_thread *thread,
    const xdr_opaque                 *name)
{
    struct chimera_nfs_export root, sibling;

    /* REST changes exports on another thread; use a locked snapshot rather
     * than the older namespace path's plain lockless root-id fast gate. */
    return chimera_nfs_get_export_copy(thread->shared, "/", &root) == 0 &&
           chimera_nfs_get_export_by_component(thread->shared, name->data,
                                               name->len, &sibling) == 0;
} /* nfs4_vfs_possible_junction */

/* The first VFS operation of each wire operation is its protocol checkpoint.
 * Checks use the resolved cursor, and are repeated on every backend attempt.
 * References acquired here are private and released on reset or completion. */
static void
nfs4_vfs_operation_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op             *map   = private_data;
    struct nfs4_vfs_compound_ctx   *ctx   = map->ctx;
    struct nfs_request             *req   = ctx->req;
    const struct nfs_argop4        *argop = &req->args_compound->argarray[map->res_index];
    struct chimera_vfs_compound_op *args  = chimera_vfs_compound_op_args(compound, index);
    nfsstat4                        error = NFS4_OK;

    /* Export configuration can change after construction or between rejected
     * attempts. Recheck at the actual name operation as well as its first
     * checkpoint, before it can expose the underlying shadowed entry. A new
     * possible junction is left to the normal namespace path on client retry. */
    if (!ctx->fh_consumed &&
        ((argop->argop == OP_LOOKUP &&
          nfs4_vfs_possible_junction(req->thread, &argop->oplookup.objname)) ||
         (argop->argop == OP_SECINFO &&
          nfs4_vfs_possible_junction(req->thread, &argop->opsecinfo.name)))) {
        map->verify_status = NFS4ERR_DELAY;
        *status            = CHIMERA_VFS_EINVAL;
        return;
    }

    if (index == (uint32_t) map->prepare_index) {
        if (ctx->fh_consumed && argop->argop != OP_PUTFH && argop->argop != OP_RESTOREFH &&
            argop->argop != OP_TEST_STATEID) {
            map->verify_status = NFS4ERR_NOFILEHANDLE;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        if (argop->argop == OP_PUTFH || argop->argop == OP_RESTOREFH) {
            ctx->fh_consumed = 0;
        }

        error = nfs4_vfs_replay_prepare(compound, map);
        if (error != NFS4_OK || map->replay_hit) {
            map->verify_status = error;
            if (error != NFS4_OK) {
                *status = CHIMERA_VFS_EINVAL;
            }
            return;
        }

        switch (argop->argop) {
            case OP_OPEN:
            {
                bool reclaim = argop->opopen.claim.claim == CLAIM_PREVIOUS;
                error = nfs_recovery_open_check(&req->thread->shared->nfs4_recovery,
                                                req->session ? ctx->client : NULL, reclaim);
                if (error == NFS4_OK && req->minorversion > 0 && req->session) {
                    bool done = nfs4_client_reclaim_complete(&req->thread->shared->nfs4_shared_clients,
                                                             req->session->nfs4_session_clientid);
                    if (reclaim && done) {
                        error = NFS4ERR_NO_GRACE;
                    } else if (!reclaim && !done) {
                        error = NFS4ERR_GRACE;
                    }
                }
                break;
            }
            case OP_LOCKT:
            {
                const struct LOCKT4args *lockt = &argop->oplockt;
                error = nfs_recovery_io_check(&req->thread->shared->nfs4_recovery);
                if (error == NFS4_OK && (!lockt->length ||
                                         (lockt->length != UINT64_MAX && lockt->offset > UINT64_MAX - lockt->length))) {
                    error = NFS4ERR_INVAL;
                }
                if (error == NFS4_OK && !req->minorversion) {
                    error = map->lockt_client_status;
                    if (error == NFS4_OK) {
                        map->lockt_checked = 1;
                    }
                }
                break;
            }
            case OP_LOCK:
            case OP_LOCKU:
                error = nfs4_vfs_lock_prepare(compound, map);
                break;
            case OP_OPEN_CONFIRM:
                error = nfs4_vfs_confirm_prepare(compound, map);
                break;
            case OP_OPEN_DOWNGRADE:
                error = nfs4_vfs_downgrade_prepare(compound, map);
                break;
            case OP_TEST_STATEID:
                nfs4_vfs_test_stateids(map);
                break;
            case OP_IO_ADVISE:
                error = nfs4_vfs_io_advise_prepare(compound, map);
                break;
            case OP_SECINFO_NO_NAME:
                if (argop->opsecinfo_no_name != SECINFO_STYLE4_CURRENT_FH &&
                    argop->opsecinfo_no_name != SECINFO_STYLE4_PARENT) {
                    error = NFS4ERR_INVAL;
                }
                break;
            case OP_CLOSE:
                error = nfs4_vfs_close_prepare(compound, map, &argop->opclose.open_stateid);
                break;
            case OP_PUTFH:
            case OP_LOOKUP:
            case OP_LOOKUPP:
            case OP_CREATE:
                ctx->current_stateid_valid = 0;
                break;
            case OP_SAVEFH:
                ctx->saved_stateid       = ctx->current_stateid;
                ctx->saved_stateid_valid = ctx->current_stateid_valid;
                break;
            case OP_RESTOREFH:
                ctx->current_stateid       = ctx->saved_stateid;
                ctx->current_stateid_valid = ctx->saved_stateid_valid;
                break;
            default:
                break;
        } /* switch */
        if (map->io_authorize) {
            const struct stateid4 *sid = nfs4_vfs_arg_stateid(argop);
            struct stateid4        current;
            bool                   data_server_io = nfs4_vfs_data_server_io(req, argop->argop);
            uint32_t               fhlen;
            const uint8_t         *fh   = chimera_vfs_compound_current_fh(compound, &fhlen);
            uint32_t               want = argop->argop == OP_SEEK ? 0 :
                (argop->argop == OP_READ || argop->argop == OP_READ_PLUS ?
                 OPEN4_SHARE_ACCESS_READ : OPEN4_SHARE_ACCESS_WRITE);
            if (!fh) {
                error = NFS4ERR_NOFILEHANDLE;
            }
            if (error == NFS4_OK && (argop->argop == OP_READ || argop->argop == OP_READ_PLUS || argop->argop == OP_WRITE
                                     ||
                                     argop->argop == OP_SETATTR)) {
                error = nfs_recovery_io_check(&req->thread->shared->nfs4_recovery);
            }
            if (error == NFS4_OK && !data_server_io && chimera_nfs4_stateid_is_current(sid)) {
                if (!ctx->current_stateid_valid) {
                    error = NFS4ERR_BAD_STATEID;
                } else {
                    current       = ctx->current_stateid;
                    current.seqid = 0;
                    sid           = &current;
                }
            }
            if (error == NFS4_OK && !data_server_io && nfs4_vfs_stateid_closed(ctx, sid)) {
                error = NFS4ERR_BAD_STATEID;
            }
            if (error == NFS4_OK && data_server_io) {
                /* Preserve the legacy DS path: infer an open against the
                 * execution cursor using request credentials, with no MDS
                 * stateid handle, share check, or delegated claim owner. */
                map->io_handle     = NULL;
                map->have_io_owner = 0;
            } else if (error == NFS4_OK && (nfs4_vfs_reserved_state(ctx, sid) || nfs4_vfs_private_lock(ctx, sid))) {
                error = nfs4_vfs_io_authorize(ctx, req->thread, req, sid, want,
                                              fh, fhlen, &map->io_handle, &map->io_owner, &map->have_io_owner);
            } else if (error == NFS4_OK && argop->argop == OP_SETATTR) {
                struct SETATTR4args local = argop->opsetattr;
                local.stateid = *sid;
                error         = nfs4_vfs_setattr_authorize(ctx, req->thread, req, &local,
                                                           fh, fhlen, &map->io_handle, &map->io_owner, &map->
                                                           have_io_owner);
            } else if (error == NFS4_OK) {
                error = nfs4_vfs_io_authorize(ctx, req->thread, req, sid, want,
                                              fh, fhlen, &map->io_handle, &map->io_owner, &map->have_io_owner);
            }
        }
    }
    if (error != NFS4_OK) {
        map->verify_status = error;
        *status            = CHIMERA_VFS_EINVAL;
        return;
    }
    if (map->io_authorize && (index == (uint32_t) map->vfs_res ||
                              (argop->argop == OP_READ_PLUS &&
                               index == (uint32_t) map->read_plus_data))) {
        args->in_handle            = map->io_handle;
        args->have_io_owner        = map->have_io_owner;
        args->io_owner             = map->io_owner;
        args->io_view.excluded     = ctx->closed_claims;
        args->io_view.num_excluded = ctx->num_closed_claims;
    }
    if (argop->argop == OP_READ_PLUS && index == (uint32_t) map->read_plus_data) {
        const struct chimera_vfs_compound_op *extent =
            chimera_vfs_compound_op(compound, map->vfs_res);
        if (!extent->is_data || !extent->read_len) {
            chimera_vfs_compound_op_skip(compound, index);
        } else {
            args->count = extent->read_len;
        }
    }
} /* nfs4_vfs_operation_prepare */

/* COPY/CLONE preserve the wire saved/current slots while temporarily selecting
 * the source for its open and size query. Endpoint handles are either private
 * stateid references or owned results of this attempt's OPEN operations. */
static void
nfs4_vfs_range_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op             *map        = private_data;
    struct nfs4_vfs_compound_ctx   *ctx        = map->ctx;
    struct nfs_request             *req        = ctx->req;
    const struct nfs_argop4        *argop      = &req->args_compound->argarray[map->res_index];
    struct chimera_vfs_compound_op *op         = chimera_vfs_compound_op_args(compound, index);
    int                             copy       = argop->argop == OP_COPY;
    const struct stateid4          *src_sid    = copy ? &argop->opcopy.ca_src_stateid : &argop->opclone.cl_src_stateid;
    const struct stateid4          *dst_sid    = copy ? &argop->opcopy.ca_dst_stateid : &argop->opclone.cl_dst_stateid;
    struct stateid4                 current    = ctx->current_stateid;
    uint64_t                        src_offset = copy ? argop->opcopy.ca_src_offset : argop->opclone.cl_src_offset;
    uint64_t                        dst_offset = copy ? argop->opcopy.ca_dst_offset : argop->opclone.cl_dst_offset;
    uint64_t                        count      = copy ? argop->opcopy.ca_count : argop->opclone.cl_count;
    nfsstat4                        error      = NFS4_OK;

    current.seqid = 0;

    if (ctx->fh_consumed) {
        error = NFS4ERR_NOFILEHANDLE;
        goto denied;
    }

    if (chimera_nfs4_stateid_is_current(src_sid)) {
        if (!ctx->current_stateid_valid) {
            error = NFS4ERR_BAD_STATEID;
        }
        src_sid = &current;
    }
    if (chimera_nfs4_stateid_is_current(dst_sid)) {
        if (!ctx->current_stateid_valid) {
            error = NFS4ERR_BAD_STATEID;
        }
        dst_sid = &current;
    }
    if (error != NFS4_OK) {
        goto denied;
    }

    if (index == (uint32_t) map->prepare_index) {
        uint32_t       src_len, dst_len;
        const uint8_t *src_fh = chimera_vfs_compound_saved_fh(compound, &src_len);
        const uint8_t *dst_fh = chimera_vfs_compound_current_fh(compound, &dst_len);
        if (!src_fh || !dst_fh) {
            error = NFS4ERR_NOFILEHANDLE;
        } else if (copy && src_len == dst_len && !memcmp(src_fh, dst_fh, src_len)) {
            error = NFS4ERR_INVAL;
        } else if (count && (src_offset > UINT64_MAX - count || dst_offset > UINT64_MAX - count)) {
            error = NFS4ERR_INVAL;
        }
        if (error != NFS4_OK) {
            goto denied;
        }

        /* Anonymous COPY share checks follow source-range validation, matching
         * the legacy operation's error precedence. */
        if (!nfs4_stateid_is_special(src_sid)) {
            error = nfs4_vfs_io_authorize(ctx, req->thread, req, src_sid, OPEN4_SHARE_ACCESS_READ,
                                          src_fh, src_len, &map->range_src_handle, &map->range_src_owner, &map->
                                          range_have_src_owner);
        }
        if (error == NFS4_OK && !nfs4_stateid_is_special(dst_sid)) {
            error = nfs4_vfs_io_authorize(ctx, req->thread, req, dst_sid, OPEN4_SHARE_ACCESS_WRITE,
                                          dst_fh, dst_len, &map->io_handle, &map->io_owner, &map->have_io_owner);
        }
    } else if (index == (uint32_t) map->range_src_open) {
        if (map->range_src_handle) {
            chimera_vfs_compound_op_skip(compound, index);
        }
    } else if (index == (uint32_t) map->range_src_attr) {
        op->in_handle = map->range_src_handle ? map->range_src_handle :
            chimera_vfs_compound_op(compound, map->range_src_open)->out_handle;
    } else if (index == (uint32_t) map->range_restore) {
        const struct chimera_vfs_compound_op *dest = chimera_vfs_compound_op(compound, map->prepare_index);
        const struct chimera_vfs_attrs       *attr = &chimera_vfs_compound_op(compound, map->range_src_attr)->attr;
        uint64_t                              size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;
        memcpy(op->arg_fh, dest->fh, dest->fh_len);
        op->arg_fh_len = dest->fh_len;
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISREG(attr->va_mode)) {
            error = chimera_nfs4_errno_to_nfsstat4(chimera_vfs_nonreg_error(attr->va_mode));
        } else if (copy && (src_offset > size || (count && count > size - src_offset))) {
            error = NFS4ERR_INVAL;
        } else {
            map->range_length = count ? count : (src_offset < size ? size - src_offset : 0);
            if (dst_offset > UINT64_MAX - map->range_length) {
                error = NFS4ERR_INVAL;
            }
        }
        if (error != NFS4_OK) {
            goto denied;
        }
        if (nfs4_stateid_is_special(src_sid)) {
            uint32_t       len;
            const uint8_t *fh = chimera_vfs_compound_saved_fh(compound, &len);
            error = nfs4_vfs_anonymous_authorize(ctx, fh, len, OPEN4_SHARE_ACCESS_READ);
        }
        if (error == NFS4_OK && nfs4_stateid_is_special(dst_sid)) {
            error = nfs4_vfs_anonymous_authorize(ctx, dest->fh, dest->fh_len, OPEN4_SHARE_ACCESS_WRITE);
        }
    } else if (index == (uint32_t) map->range_dst_open) {
        if (map->io_handle) {
            chimera_vfs_compound_op_skip(compound, index);
        }
    } else if (index == (uint32_t) map->vfs_res) {
        op->src_handle = map->range_src_handle ? map->range_src_handle :
            chimera_vfs_compound_op(compound, map->range_src_open)->out_handle;
        op->in_handle = map->io_handle ? map->io_handle :
            chimera_vfs_compound_op(compound, map->range_dst_open)->out_handle;
        op->length                   = map->range_length;
        op->io_owner                 = map->io_owner;
        op->have_io_owner            = map->have_io_owner;
        op->src_io_owner             = map->range_src_owner;
        op->have_src_io_owner        = map->range_have_src_owner;
        op->io_view.excluded         = ctx->closed_claims;
        op->io_view.num_excluded     = ctx->num_closed_claims;
        op->src_io_view.excluded     = ctx->closed_claims;
        op->src_io_view.num_excluded = ctx->num_closed_claims;
        if (!op->src_handle || !op->in_handle) {
            error = NFS4ERR_BAD_STATEID;
        } else if (!op->length) {
            chimera_vfs_compound_op_skip(compound, index);
        } else if (!copy && (op->src_handle->vfs_module != op->in_handle->vfs_module ||
                             !(op->in_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE))) {
            error = NFS4ERR_NOTSUPP;
        }
    }
 denied:
    if (error != NFS4_OK) {
        map->verify_status = error;
        *status            = CHIMERA_VFS_EINVAL;
    }
} /* nfs4_vfs_range_prepare */

/*
 * Append a CREATE, translating the object type NFSv4 names into the three
 * shapes the VFS makes.  A device's numbers and a special file's type travel in
 * the attributes, which is where mknod wants them anyway, so the translation is
 * all here and there is none on the other side.
 */
static int
nfs4_vfs_add_create_op(
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop,
    struct nfs4_vfs_op          *map)
{
    const struct CREATE4args *args = &argop->opcreate;
    struct chimera_vfs_attrs  attr;
    uint8_t                   type;
    const char               *target    = NULL;
    int                       targetlen = 0;

    memset(&attr, 0, sizeof(attr));

    if (nfs4_vfs_decode_attrs(map, &args->createattrs, &attr) != NFS4_OK) {
        return -1;
    }

    switch (args->objtype.type) {
        case NF4DIR:
            type = CHIMERA_VFS_COMPOUND_CREATE_DIR;
            break;
        case NF4LNK:
            type      = CHIMERA_VFS_COMPOUND_CREATE_SYMLINK;
            target    = (const char *) args->objtype.linkdata.data;
            targetlen = (int) args->objtype.linkdata.len;
            break;
        default:
            /* NF4BLK, NF4CHR, NF4SOCK or NF4FIFO -- the scan admits no other
             * type here. */
            type              = CHIMERA_VFS_COMPOUND_CREATE_NODE;
            attr.va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            attr.va_rdev      = 0;

            switch (args->objtype.type) {
                case NF4BLK:
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFBLK;
                    attr.va_rdev =
                        ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                        (uint64_t) args->objtype.devdata.specdata2;
                    break;
                case NF4CHR:
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFCHR;
                    attr.va_rdev =
                        ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                        (uint64_t) args->objtype.devdata.specdata2;
                    break;
                case NF4SOCK:
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFSOCK;
                    break;
                default: /* NF4FIFO */
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFIFO;
                    break;
            } /* switch */
            break;
    } /* switch */

    return chimera_vfs_compound_add_create(compound, type, (const char *) args->objname.data, (int) args->objname.len,
                                           target, targetlen, &attr, CHIMERA_VFS_ATTR_FH, 0, 0);
} /* nfs4_vfs_add_create_op */

static bool
nfs4_vfs_v40_open_journalable(
    struct nfs_request     *req,
    const struct OPEN4args *args)
{
    struct nfs_client     *client    = NULL;
    struct nfs_open_owner *owner     = NULL;
    bool                   supported = true;

    if (!nfs4_session_is_live(req->session) || req->session->nfs4_session_clientid != args->owner.clientid ||
        args->owner.owner.len > NFS4_OPAQUE_LIMIT ||
        nfs4_client_reserve_compound(&req->thread->shared->nfs4_shared_clients,
                                     args->owner.clientid, &client) != NFS4_OK) {
        return false;
    }
    evpl_mutex_lock(&client->lock);
    HASH_FIND(hh, client->open_owners_by_str, args->owner.owner.data, args->owner.owner.len, owner);
    if (owner) {
        evpl_mutex_lock(&owner->lock);
        supported = !owner->replay.valid || owner->replay.op != OP_OPEN ||
            owner->replay.status != NFS4_OK || owner->replay.open_valid;
        evpl_mutex_unlock(&owner->lock);
    }
    evpl_mutex_unlock(&client->lock);
    nfs_client_finish_compound(client, &req->thread->shared->nfs4_state_table, req->thread->vfs_thread);
    return supported;
} /* nfs4_vfs_v40_open_journalable */

int
chimera_nfs4_compound_try_vfs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct chimera_vfs_compound  *compound;
    struct nfs4_vfs_compound_ctx *ctx;
    struct nfs_argop4            *argop;
    uint32_t                      first, num, nenc, i, k, credential_putfh;
    uint8_t                       cur_fh[NFS4_FHSIZE];
    int                           cur_fhlen = 0;
    int                           lead_putfh;
    int                           have_lookupp = 0, have_saved = 0;
    int                           seed_saved = 0;
    int                           cur_moved  = 0;
    /* What the sequence's current open handle carries, as it is built.  Zero
     * means nothing is open on the current object -- see nfs4_vfs_open_for. */
    unsigned int                  cur_open_flags = 0;
    /* OPEN and OPEN_DOWNGRADE share frozen-owner journals; legacy OPEN ends a run. */
    const struct stateid4        *downgrade_public_sids[NFS4_VFS_COMPOUND_MAX_OPS] = { 0 };
    const struct stateid4        *lock_public_sids[NFS4_VFS_COMPOUND_MAX_OPS]      = { 0 };
    int                           have_locks                                       = 0;
    const struct stateid4        *scan_current                                     = req->current_stateid_valid ? &req->
        current_stateid : NULL;
    const struct stateid4        *scan_saved = req->saved_current_stateid_valid ? &req
        ->saved_current_stateid :
        NULL;
    int                           saw_close                               = 0;
    bool                          journal_open[NFS4_VFS_COMPOUND_MAX_OPS] = { false };
    int                           open_at                                 = -1;
    int                           open_can_continue                       = 0;
    /* The export the sequence runs under, established by the op that seeds the
     * current object.  A later PUTFH is admitted only back into this one -- see
     * the PUTFH case.  Read during the scan, which is before the seed handle is
     * decoded, so it cannot come from req->export_id. */
    uint16_t                      seq_export = req->export_id;
    /* Set when the scan meets an op the sequence cannot carry: the run ends in
     * front of it, and the dispatcher picks up from there. */
    int                           stop = 0;
    /* The initial cursor seed, or a checkpoint for a handle-free prefix. */
    uint32_t                      vfs_ops = 1;
    uint64_t                      reply_bound = 0, avail;
    uint32_t                      build_mark;
    int                           idx, next;
    int                           open_4_0_pinned = 0;

    first = (uint32_t) req->index;
    num   = req->res_compound.num_resarray;
    /* One past the last op the sequence will carry.  Normally the whole
     * remainder; an op that ends the run (see nfs4_vfs_op_ends_run) pulls it
     * in, and what is left is dispatched op by op afterwards. */
    nenc = num;

    if (first >= num) {
        return 0;
    }
    credential_putfh = first;
    if (!req->fhlen && req->args_compound->argarray[first].argop == OP_TEST_STATEID) {
        while (credential_putfh < num &&
               req->args_compound->argarray[credential_putfh].argop == OP_TEST_STATEID) {
            credential_putfh++;
        }
    }

    /* A remainder longer than the sequence can hold is not refused: the scan's
     * own vfs_ops budget ends the run when it fills up, and the rest is
     * dispatched op by op. */

    /* The dispatcher fails an op with NFS4ERR_RESOURCE rather than running it
     * when the reply buffer is nearly full; leave that to it. */
    avail = req->encoding->dbuf->size - req->encoding->dbuf->used;

    if (avail < 8192) {
        return 0;
    }

    /* When the remainder opens with a PUTFH, that handle names the export;
    * otherwise the sequence inherits whatever the COMPOUND already had. */
    if (credential_putfh < num && req->args_compound->argarray[credential_putfh].argop == OP_PUTFH) {
        uint8_t seed_fh[CHIMERA_VFS_FH_SIZE];
        int     seed_len;

        if (chimera_nfs_fh_unwrap(
                req->args_compound->argarray[credential_putfh].opputfh.object.data,
                (int) req->args_compound->argarray[credential_putfh].opputfh.object.len,
                &seq_export, seed_fh, &seed_len,
                thread->shared->fh_key,
                thread->shared->fh_sign) != CHIMERA_NFS_FH_OK) {
            return 0;
        }
    }

    if (req->saved_fhlen && req->saved_export_id == seq_export &&
        !fh_is_nfs4_root(req->saved_fh, req->saved_fhlen) &&
        !chimera_nfs4_fh_is_attrdir(req->saved_fh, req->saved_fhlen) &&
        chimera_vfs_fh_is_plausible(thread->vfs_thread, req->saved_fh, req->saved_fhlen)) {
        have_saved = 2; /* inherited rather than established in this run */
        vfs_ops   += 2; /* PUTFH(saved), SAVEFH before the normal seed */
    }

    for (i = first; i < num; i++) {
        argop = &req->args_compound->argarray[i];

        const struct stateid4 *wire_sid = nfs4_vfs_arg_stateid(argop);
        /* Session I/O validates the decoded class at execution. Unsupported
        * classes (including MDS layout stateids) return BAD_STATEID without
        * entering the legacy path that assumes every non-OPEN is a LOCK. */
        bool                   session_io = req->minorversion > 0 &&
            (argop->argop == OP_READ || argop->argop == OP_READ_PLUS || argop->argop == OP_WRITE);
        if (!session_io && !nfs4_vfs_data_server_io(req, argop->argop) &&
            !nfs4_vfs_stateid_encodable(req, wire_sid,
                                        open_can_continue && open_at >= 0 && (int) i > open_at)) {
            nenc = i;
            break;
        }

        /* READ/WRITE and COPY carry an attempt-local anonymous admission
         * view. Other data operations still need their own claim-aware path.
         * A metadata-only SETATTR ignores its stateid and can stay in the run. */
        const struct stateid4 *input_sid      = nfs4_vfs_arg_stateid(argop);
        int                    view_supported = argop->argop == OP_READ || argop->argop == OP_WRITE ||
            argop->argop == OP_READ_PLUS || argop->argop == OP_CLOSE ||
            argop->argop == OP_OPEN_DOWNGRADE || argop->argop == OP_IO_ADVISE ||
            (argop->argop == OP_SETATTR &&
             (!argop->opsetattr.obj_attributes.num_attrmask ||
              !(argop->opsetattr.obj_attributes.attrmask[0] & (1U << FATTR4_SIZE))));
        if (saw_close && input_sid && !view_supported &&
            !chimera_nfs4_stateid_is_current(input_sid) && nfs4_stateid_is_special(input_sid)) {
            nenc = i;
            break;
        }

        if (!nfs4_vfs_op_encodable(argop->argop)) {
            nenc = i;
            break;
        }

        /* The per-op gates decide statuses this path has no vocabulary for, so
         * anything they would reject goes back to the per-op path to be
         * rejected there. */
        if (nfs4_op_check_minor(argop->argop, req->minorversion, i,
                                req->seen_sequence) != NFS4_OK) {
            nenc = i;
            break;
        }

        uint16_t saved_export = req->export_id;
        req->export_id = seq_export;
        nfsstat4 ro_status = nfs4_rofs_gate(req, argop);
        req->export_id = saved_export;
        if (ro_status != NFS4_OK) {
            nenc = i;
            break;
        }

        reply_bound += nfs4_vfs_op_reply_bound(argop);

        /* An upper bound on the VFS ops one NFSv4 op encodes to.  Counted up
         * front: a sequence discovered to be too long only once it was half
         * built would have to be abandoned after staging xattr names into the
         * reply buffer, which cannot be taken back.
         *
         * The encoder opens the objects it addresses, so most ops now cost an
         * OPEN as well as themselves, and the ops that take a type gate before
         * touching data cost two opens -- a metadata one for the stat and a
         * data one for the operation, which are different handles from
         * different caches.  Over-counting only declines to sequence a long
         * compound, which then takes the op-at-a-time path and is correct;
         * under-counting would fail the build half way. */
        if (argop->argop == OP_GETATTR && chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
            vfs_ops++;
        }
        if (argop->argop == OP_SETATTR && chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
            vfs_ops++;
        }
        if (argop->argop == OP_SECINFO && req->minorversion == 0) {
            vfs_ops += 2; /* Capture and restore the directory around lookup. */
        }
        if (chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
            if (argop->argop == OP_REMOVE) {
                /* GETFH(parent), LOOKUP(victim), COORDINATE, PUTFH(parent). */
                vfs_ops += 4;
            } else if (argop->argop == OP_RENAME) {
                /* Target directory OPEN/GETFH, source RESTOREFH, and two
                 * LOOKUP/COORDINATE/PUTFH walks before the base RENAME. */
                vfs_ops += 9;
            }
        }
        switch (argop->argop) {
            case OP_OPEN:
                vfs_ops += 7;
                break;
            case OP_READ_PLUS:
                /* type gate, data open, classifier, conditional data read */
                vfs_ops += 5;
                break;
            case OP_OPEN_DOWNGRADE:
            case OP_LOCK:
                vfs_ops += 3;
                break;
            case OP_COPY:
            case OP_CLONE:
                /* Save destination, source open/stat, restore, destination
                 * open, transfer. Opens may be skipped for stateid handles. */
                vfs_ops += 8;
                break;
            case OP_COMMIT:
            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
            case OP_WRITE_SAME:
                /* open(meta) + getattr + open(data) + the op */
                vfs_ops += 4;
                break;
            case OP_READLINK:
                /* open + getattr + readlink */
                vfs_ops += 3;
                break;
            case OP_PUTFH:
                /* The seed PUTFH below is the leading one's own op. */
                vfs_ops += (i == first) ? 2 : 3;
                break;
            case OP_GETFH:
            case OP_SAVEFH:
            case OP_RESTOREFH:
            case OP_RENAME:
            case OP_LINK:
                /* Cursor work, or two objects named by file handle: no open. */
                vfs_ops += 1;
                break;
            default:
                /* open + the op */
                vfs_ops += 2;
                break;
        } /* switch */

        if (vfs_ops > NFS4_VFS_COMPOUND_MAX_OPS) {
            nenc = i;
            break;
        }

        if (have_saved == 2 && (argop->argop == OP_RESTOREFH || argop->argop == OP_COPY ||
                                argop->argop == OP_CLONE || argop->argop == OP_LINK || argop->argop == OP_RENAME)) {
            seed_saved = 1;
        }

        switch (argop->argop) {
            case OP_PUTFH:
                /* A later PUTFH is allowed only back into the SAME export.
                 *
                 * What a second one threatens is the credential: a different
                 * export has a different squash policy, and the sequence runs
                 * under one credential fixed when it was submitted.  Into the
                 * same export there is no such change -- same policy, same
                 * credential, same security flavor, all of them already
                 * established by the first.
                 *
                 * That is not a narrow escape hatch.  RENAME and LINK both
                 * require their two objects to be in the same filesystem
                 * (NFS4ERR_XDEV otherwise), so same-export is the only shape
                 * either of them is ever legally given -- and they are what a
                 * second PUTFH is nearly always for. */
                if (i != first) {
                    cur_moved = 1;
                    uint8_t  later_fh[CHIMERA_VFS_FH_SIZE];
                    int      later_len;
                    uint16_t later_export;

                    if (fh_is_nfs4_root(argop->opputfh.object.data,
                                        argop->opputfh.object.len) ||
                        argop->opputfh.object.len > NFS4_FHSIZE ||
                        chimera_nfs_fh_unwrap(argop->opputfh.object.data,
                                              (int) argop->opputfh.object.len,
                                              &later_export,
                                              later_fh, &later_len,
                                              thread->shared->fh_key,
                                              thread->shared->fh_sign) !=
                        CHIMERA_NFS_FH_OK ||
                        later_export != seq_export ||
                        chimera_nfs4_fh_is_attrdir(later_fh, later_len) ||
                        !chimera_vfs_fh_is_plausible(thread->vfs_thread,
                                                     later_fh, later_len)) {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_LOOKUP:
                if (chimera_nfs4_validate_name(&argop->oplookup.objname) !=
                    NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                /* Only export component names can shadow a real entry at
                 * the namespace root. Other names stay on the current export
                 * even when an earlier operation moved the private cursor. */
                if (nfs4_vfs_possible_junction(thread, &argop->oplookup.objname)) {
                    nenc = i;
                    stop = 1;
                    break;
                }
                cur_moved = 1;
                break;

            case OP_LOOKUPP:
                /* An export root and the pseudo-root have parents the VFS
                 * cannot name -- the namespace root, or the "/" export's real
                 * root -- so a LOOKUPP is encodable only from an object we can
                 * test here, which means the one the sequence starts from.
                 * Once a LOOKUP (or a RESTOREFH) has moved the current object,
                 * what a later LOOKUPP would climb from is not known until the
                 * sequence has already run past it. */
                if (cur_moved) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                have_lookupp = 1;
                cur_moved    = 1;
                break;

            case OP_SAVEFH:
                have_saved = 1;
                break;

            case OP_RESTOREFH:
                /* Same-export inherited slots are seeded before the run.
                 * Cross-export restore still needs credential transitions. */
                if (!have_saved) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                cur_moved = 1;
                break;

            case OP_READDIR:
                /* The per-op path answers each of these itself, with statuses
                 * (TOOSMALL, BAD_COOKIE, an invalid attribute request) that
                 * belong to it rather than to any VFS result. */
                if (argop->opreaddir.maxcount < 16 ||
                    argop->opreaddir.cookie == 1 ||
                    argop->opreaddir.cookie == 2) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (chimera_nfs4_validate_getattr_request(
                        argop->opreaddir.num_attr_request,
                        argop->opreaddir.attr_request) != NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* Per-entry ACLs need no exception: the page is marshalled
                 * inside the enumeration, while the backend still owns the
                 * ACL it is handing over -- the same moment the per-op path
                 * encodes it. */
                break;

            case OP_GETXATTR:
                if (!nfs4_vfs_xattr_name_ok(argop->opgetxattr.gxa_name.len)) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_SETXATTR:
                if (!nfs4_vfs_xattr_name_ok(argop->opsetxattr.sxa_key.len)) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* RFC 8276 §8.3: only the three defined option values are
                 * valid, and the per-op path is what says INVAL. */
                if (argop->opsetxattr.sxa_option != SETXATTR4_EITHER &&
                    argop->opsetxattr.sxa_option != SETXATTR4_CREATE &&
                    argop->opsetxattr.sxa_option != SETXATTR4_REPLACE) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_REMOVEXATTR:
                if (!nfs4_vfs_xattr_name_ok(argop->opremovexattr.rxa_name.len)) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_GETATTR:
                if (chimera_nfs4_validate_getattr_request(
                        argop->opgetattr.num_attr_request,
                        argop->opgetattr.attr_request) != NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                break;

            case OP_CREATE:
            {
                const struct CREATE4args *ca = &argop->opcreate;

                /* Only the types a CREATE actually makes, named explicitly:
                 * everything else is a status the per-op path decides and this
                 * path has no vocabulary for -- NFS4ERR_BADTYPE for a regular
                 * file (that is what OPEN is for), NOTSUPP for the synthetic
                 * named-attribute objects.  An allow-list, because guessing
                 * what an unlisted type meant is how NF4REG became a FIFO. */
                switch (ca->objtype.type) {
                    case NF4DIR:
                    case NF4BLK:
                    case NF4CHR:
                    case NF4SOCK:
                    case NF4FIFO:
                        break;
                    case NF4LNK:
                        /* An empty target is NFS4ERR_INVAL, decided before any
                         * VFS call. */
                        if (ca->objtype.linkdata.len == 0) {
                            nenc = i;
                            stop = 1;
                        }
                        break;
                    default:
                        nenc = i;
                        stop = 1;
                        break;
                } /* switch */

                if (stop) {
                    break;
                }

                if (chimera_nfs4_validate_name(&ca->objname) != NFS4_OK ||
                    chimera_nfs4_validate_createattrs(
                        ca->createattrs.num_attrmask,
                        ca->createattrs.attrmask) != NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                cur_moved = 1;
                break;
            }

            case OP_SECINFO:
                if (chimera_nfs4_validate_name(&argop->opsecinfo.name) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* At a "/" export's root a name matching a sibling export is a
                 * junction, and SECINFO answers with THAT export's flavors --
                 * a name the VFS cannot resolve and a policy it does not hold.
                 */
                if (nfs4_vfs_possible_junction(thread, &argop->opsecinfo.name)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Only v4.1+ consumes the protocol cursor. */
                if (req->minorversion >= 1) {
                    cur_moved = 1;
                }
                break;

            case OP_COPY:
            case OP_CLONE:
            {
                const struct stateid4 *src = argop->argop == OP_COPY ?
                    &argop->opcopy.ca_src_stateid : &argop->opclone.cl_src_stateid;
                const struct stateid4 *dst = argop->argop == OP_COPY ?
                    &argop->opcopy.ca_dst_stateid : &argop->opclone.cl_dst_stateid;
                /* COPY's generic fallback owns each read buffer until WRITE
                * completion; proxy transports clone those borrowed refs. */
                if (!have_saved ||
                    !nfs4_vfs_stateid_encodable(req, src, open_at >= 0) ||
                    !nfs4_vfs_stateid_encodable(req, dst, open_at >= 0) ||
                    (argop->argop == OP_CLONE &&
                     (nfs4_stateid_is_special(src) || nfs4_stateid_is_special(dst)))) {
                    nenc = i;
                    stop = 1;
                }
                break;
            }

            case OP_LOCKT:
            {
                /* v4.0 pins the client named by this operation during
                 * construction; v4.1+ uses the request's pinned session client. */
                if (req->minorversion && (!nfs4_session_is_live(req->session))) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                break;
            }

            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
            case OP_WRITE_SAME:
            {
                /* The per-op path answers each of these itself, with a status
                 * that belongs to the operation rather than to any VFS
                 * result. */
                if (argop->argop == OP_WRITE_SAME) {
                    const struct app_data_block4 *adb =
                        &argop->opwrite_same.wsa_adb;

                    /* Per-block-number stamping is the unsupported arm of the
                     * union, the ADB geometry has to be sane, and the total
                     * must not overflow -- three answers the operation owes
                     * that no VFS result carries. */
                    if (adb->adb_reloff_blocknum != NFS4_UINT64_MAX ||
                        adb->adb_block_size == 0 ||
                        adb->adb_block_size > UINT32_MAX ||
                        adb->adb_reloff_pattern > adb->adb_block_size ||
                        adb->adb_reloff_pattern + adb->adb_pattern.len >
                        adb->adb_block_size ||
                        (adb->adb_block_count != 0 &&
                         adb->adb_block_size >
                         NFS4_UINT64_MAX / adb->adb_block_count)) {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (argop->argop == OP_SEEK &&
                    argop->opseek.sa_what != NFS4_CONTENT_DATA &&
                    argop->opseek.sa_what != NFS4_CONTENT_HOLE) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* RFC 7862: the range is [offset, offset + length); one whose
                 * end does not fit in a uint64 names no such interval. */
                if (argop->argop == OP_ALLOCATE &&
                    argop->opallocate.aa_length &&
                    argop->opallocate.aa_offset >
                    UINT64_MAX - argop->opallocate.aa_length) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                if (argop->argop == OP_DEALLOCATE &&
                    argop->opdeallocate.da_length &&
                    argop->opdeallocate.da_offset >
                    UINT64_MAX - argop->opdeallocate.da_length) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                break;
            }

            case OP_READ:
            case OP_READ_PLUS:
            case OP_WRITE:
            {
                if (nfs_recovery_io_check(&thread->shared->nfs4_recovery) != NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                break;
            }

            case OP_SETATTR:
            {
                const struct SETATTR4args *sa = &argop->opsetattr;
                int                        wants_size;

                if (chimera_nfs4_validate_createattrs(
                        sa->obj_attributes.num_attrmask,
                        sa->obj_attributes.attrmask) != NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                wants_size = sa->obj_attributes.num_attrmask >= 1 &&
                    (sa->obj_attributes.attrmask[0] & (1U << FATTR4_SIZE));

                if (wants_size) {
                    if (nfs_recovery_io_check(&thread->shared->nfs4_recovery) !=
                        NFS4_OK) {
                        nenc = i;
                        stop = 1;
                        break;
                    }


                }
                break;
            }

            case OP_REMOVE:
                if (chimera_nfs4_validate_name(&argop->opremove.target) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* pNFS REMOVE still owns post-MDS data-server cleanup. */
                if (chimera_vfs_pnfs_enabled(thread->shared->vfs)) {
                    nenc = i;
                    stop = 1;
                    break;
                }
                break;

            case OP_RENAME:
                if (chimera_nfs4_validate_name(&argop->oprename.oldname) !=
                    NFS4_OK ||
                    chimera_nfs4_validate_name(&argop->oprename.newname) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The source directory is the saved filehandle, and only a
                 * slot this sequence filled will do -- the same rule, and for
                 * the same reason, as RESTOREFH's above. */
                if (!have_saved) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                break;

            case OP_LINK:
                if (chimera_nfs4_validate_name(&argop->oplink.newname) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The object to link is the saved filehandle; see RENAME. */
                if (!have_saved) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* No delegation or pNFS gate: a LINK displaces nothing.  The
                 * name it creates must not already exist -- RFC 7530 SS16.9.5
                 * makes that NFS4ERR_EXIST -- so unlike RENAME there is never
                 * a victim to recall a delegation on or a backing to delete. */
                break;

            case OP_LOCK:
            case OP_LOCKU:
            {
                if (!nfs4_session_is_live(req->session)) {
                    nenc = i; stop = 1; break;
                }
                const struct stateid4 *sid = nfs4_vfs_arg_stateid(argop);
                lock_public_sids[i - first] = chimera_nfs4_stateid_is_current(sid) ? scan_current : sid;
                have_locks                  = 1;
                break;
            }

            case OP_CLOSE:
            case OP_OPEN_CONFIRM:
            case OP_OPEN_DOWNGRADE:
            {
                const struct stateid4 *sid = nfs4_vfs_arg_stateid(argop);
                if (!nfs4_session_is_live(req->session)) {
                    nenc = i; stop = 1; break;
                }
                if (chimera_nfs4_stateid_is_current(sid)) {
                    sid = scan_current;
                }
                if (argop->argop == OP_OPEN_DOWNGRADE) {
                    downgrade_public_sids[i - first] = sid;
                }
                /* Parent mutation shares the child journals even without an
                 * explicit LOCK in this run. Existing children are frozen at
                 * construction and retire only after accepted CLOSE. */
                lock_public_sids[i - first] = sid;
                have_locks                  = 1;
                saw_close                   = 1;
                break;
            }

            case OP_OPEN:
            {
                struct OPEN4args *oa = &argop->opopen;

                /* Unsupported legacy replay still needs its entry decision
                 * before submission. Journaled OPEN classifies at execution. */
                journal_open[i - first] = req->minorversion > 0 || nfs4_vfs_v40_open_journalable(req, oa);
                if (req->minorversion == 0 && !journal_open[i - first] && i != first) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* Ordinary named/FH opens and reclaim use execution checks.
                 * Delegation claims still need external coordination. */
                if (oa->claim.claim != CLAIM_NULL &&
                    oa->claim.claim != CLAIM_FH &&
                    oa->claim.claim != CLAIM_PREVIOUS) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* RFC 8881 §18.16.3: an EXCLUSIVE4_1 attribute outside
                 * suppattr_exclcreat is NFS4ERR_INVAL, and the per-op path is
                 * what says so.  Without this the verifier would silently
                 * clobber a time_access_set or time_modify_set the client
                 * asked for. */
                if (oa->openhow.opentype == OPEN4_CREATE &&
                    oa->openhow.how.mode == EXCLUSIVE4_1 &&
                    (chimera_nfs4_validate_createattrs(
                         oa->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                         oa->openhow.how.ch_createboth.cva_attrs.attrmask) !=
                     NFS4_OK ||
                     chimera_nfs4_validate_exclcreat_attrs(
                         oa->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                         oa->openhow.how.ch_createboth.cva_attrs.attrmask) !=
                     NFS4_OK)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Statuses the per-op path decides before it opens anything. */
                if ((oa->share_access & (OPEN4_SHARE_ACCESS_READ |
                                         OPEN4_SHARE_ACCESS_WRITE)) == 0) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (oa->claim.claim == CLAIM_NULL &&
                    chimera_nfs4_validate_name(&oa->claim.file) != NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (oa->openhow.opentype == OPEN4_CREATE &&
                    (oa->openhow.how.mode == UNCHECKED4 ||
                     oa->openhow.how.mode == GUARDED4)) {
                    if (chimera_nfs4_validate_createattrs(
                            oa->openhow.how.createattrs.num_attrmask,
                            oa->openhow.how.createattrs.attrmask) != NFS4_OK) {
                        {
                            nenc = i;
                            stop = 1;
                            break;
                        }
                    }

                }

                /* v4.0 delegation grants still use the legacy callback-probe
                 * and owner-replay publication path. Its complete accepted
                 * delegation response is now owned by the replay cache. */
                if (req->minorversion == 0 &&
                    chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                open_at           = (int) i;
                open_can_continue = journal_open[i - first] && nfs4_session_is_live(req->session);
                if (!open_can_continue) {
                    nenc = i + 1;
                }
                break;
            }

            default:
                break;
        } /* switch */

        /* Track only known public identities for CLOSE shape classification.
         * NULL also represents a provisional OPEN, whose shape is lock-free. */
        switch (argop->argop) {
            case OP_SAVEFH: scan_saved      = scan_current; break;
            case OP_RESTOREFH: scan_current = scan_saved; break;
            case OP_PUTFH:
            case OP_LOOKUP:
            case OP_LOOKUPP:
            case OP_CREATE:
            case OP_OPEN:
            case OP_OPEN_DOWNGRADE:
            case OP_CLOSE:
            case OP_LOCK:
            case OP_LOCKU:
                scan_current = NULL;
                break;
            default: break;
        } /* switch */

        if (stop || (nfs4_vfs_op_ends_run(argop->argop) &&
                     !(argop->argop == OP_OPEN && open_can_continue))) {
            break;
        }
    }

    /* Nothing at all was expressible, so there is no sequence to build. */
    if (nenc <= first) {
        return 0;
    }

    /*
     * Reply-buffer headroom for a sequence that stages part of its answer while
     * it runs.  Those stagings all happen before
     * any result has been marshalled, so the headroom READDIR, GETXATTR and
     * LISTXATTRS size their answers from is not the headroom the per-op path
     * would have left them.
     *
     * Rather than model the per-op path's allocation order, encode only when
     * the buffer is roomy enough that the question cannot arise: if the whole
     * sequence's worst case plus the dispatcher's 8192-byte floor fits in what
     * is free now, then each of those ops saturates its own cap in both paths
     * and neither path can reach the floor.  Same sizes, same statuses, same
     * entries -- whatever order the allocations happen in.
     */
    if (reply_bound + 8192 > avail) {
        return 0;
    }

    /* With a "/" export configured, LOOKUPP has to recognize the namespace root
     * and hand back export roots' parents from it (nfs4_root_export_fh_get);
     * neither is visible to the VFS. */
    if (have_lookupp && thread->shared->root_export_id != 0) {
        return 0;
    }

    /* Establish the object the sequence starts from. */
    lead_putfh = (req->args_compound->argarray[first].argop == OP_PUTFH);

    if (lead_putfh) {
        struct PUTFH4args *pa = &req->args_compound->argarray[first].opputfh;

        /* Mirrors chimera_nfs4_putfh.  The pseudo-root is not a wrapped VFS
         * handle at all; the decode authenticates the wire handle, recovers the
         * inner VFS handle, and re-derives the export id and squashed
         * credential the sequence will run under; a named-attribute directory
         * handle addresses a synthetic object the VFS does not have. */
        if (fh_is_nfs4_root(pa->object.data, pa->object.len) ||
            pa->object.len > NFS4_FHSIZE) {
            return 0;
        }

        if (chimera_nfs_fh_decode(req, pa->object.data, pa->object.len,
                                  cur_fh, &cur_fhlen) != CHIMERA_NFS_FH_OK) {
            return 0;
        }

        if (chimera_nfs4_fh_is_attrdir(cur_fh, cur_fhlen) ||
            !chimera_vfs_fh_is_plausible(thread->vfs_thread, cur_fh,
                                         cur_fhlen)) {
            return 0;
        }
    } else if (req->fhlen == 0 && req->args_compound->argarray[first].argop == OP_TEST_STATEID) {
        /* Select the eventual filesystem credential without exposing a future
         * filehandle to earlier, handle-free TEST_STATEID checkpoints. */
        if (credential_putfh < nenc &&
            req->args_compound->argarray[credential_putfh].argop == OP_PUTFH) {
            struct PUTFH4args *pa = &req->args_compound->argarray[credential_putfh].opputfh;
            uint8_t            decoded[NFS4_FHSIZE];
            int                decoded_len;
            if (chimera_nfs_fh_decode(req, pa->object.data, pa->object.len,
                                      decoded, &decoded_len) != CHIMERA_NFS_FH_OK ||
                chimera_nfs4_fh_is_attrdir(decoded, decoded_len) ||
                !chimera_vfs_fh_is_plausible(thread->vfs_thread, decoded, decoded_len)) {
                return 0;
            }
        }
    } else if (req->fhlen == 0 &&
               req->args_compound->argarray[first].argop == OP_RESTOREFH && seed_saved) {
        /* RESTOREFH needs only its same-export saved slot. The hidden cursor
         * seeds do no filesystem work; this first wire op establishes the
         * protocol cursor before any operation can use it. */
        memcpy(cur_fh, req->saved_fh, req->saved_fhlen);
        cur_fhlen = req->saved_fhlen;
    } else {
        if (req->fhlen == 0 ||
            fh_is_nfs4_root(req->fh, req->fhlen) ||
            chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
            return 0;
        }

        memcpy(cur_fh, req->fh, req->fhlen);
        cur_fhlen = req->fhlen;
    }

    /* An export root's parent is the NFSv4 namespace root, not the backend's
     * physical parent -- a graft the VFS knows nothing about, so its ".." is
     * the wrong answer.  Only reachable for a LOOKUPP from the object the
     * sequence starts from, which is the only one a LOOKUPP is encoded for. */
    if (have_lookupp &&
        chimera_nfs4_fh_is_vfs_mount_root(thread->vfs, cur_fh,
                                          (uint32_t) cur_fhlen)) {
        return 0;
    }

    /* ---- ops [first, nenc) are expressible: build the sequence ---- */

    /* The 4.0 OPEN's entry-time seqid classification.  Deliberately the last
     * thing before the sequence is built: it pins the open_owner on the request
     * for chimera_nfs4_open_finish to advance, so it must not run ahead of a
     * decision that could still send this COMPOUND back to the per-op path --
     * which would resolve the same owner a second time and pin it twice. */
    if (open_at >= 0 && req->minorversion == 0 && !journal_open[open_at - first]) {
        nfsstat4 entry_status;

        if (chimera_nfs4_open_4_0_entry(thread, req, (uint32_t) open_at,
                                        &entry_status)) {
            /* Answered without any VFS work.  The OPEN is the first op of the
             * run, so there is nothing before it that still had to happen. */
            req->index = open_at;
            chimera_nfs4_compound_complete(req, entry_status);
            return 1;
        }

        open_4_0_pinned = 1;
    }

    /* Construction may stage names and variable-length checkpoint results.
     * A refused build returns all of that reply space to the legacy path. */
    build_mark = req->encoding->dbuf->used;
    compound   = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    ctx = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate NFSv4 compound context");
    ctx->req = req;
    if (req->session) {
        if (!nfs4_session_is_live(req->session) ||
            nfs4_client_reserve_compound(&thread->shared->nfs4_shared_clients,
                                         req->session->nfs4_session_clientid, &ctx->client) != NFS4_OK) {
            goto refuse;
        }
    }
    ctx->initial_stateid             = req->current_stateid;
    ctx->initial_stateid_valid       = req->current_stateid_valid;
    ctx->initial_saved_stateid       = req->saved_current_stateid;
    ctx->initial_saved_stateid_valid = req->saved_current_stateid_valid;
    ctx->initial_fh_consumed         = cur_fhlen == 0;
    if (req->minorversion == 0) {
        for (i = first; i < nenc; i++) {
            argop = &req->args_compound->argarray[i];
            if (argop->argop != OP_LOCKT) {
                continue;
            }
            struct nfs4_vfs_op *map = &ctx->ops[i - first];
            map->lockt_client_status = nfs4_client_reserve_compound(&thread->shared->nfs4_shared_clients,
                                                                    argop->oplockt.owner.clientid, &map->lockt_client);
            if (map->lockt_client_status != NFS4_OK && map->lockt_client_status != NFS4ERR_STALE_CLIENTID) {
                goto refuse;
            }
        }
    }
    /* Freeze owners before resolving public CLOSEs. The owner journal also
     * supplies handles to operations preceding the first coalescing OPEN. */
    uint32_t total_existing = 0;
    for (i = first; i < nenc; i++) {
        struct nfs4_vfs_op                *map = &ctx->ops[i - first];
        argop = &req->args_compound->argarray[i];
        if (argop->argop != OP_OPEN) {
            continue;
        }
        struct OPEN4args                  *oa    = &argop->opopen;
        struct nfs_open_owner_reservation *group = NULL;
        if (journal_open[i - first] && req->session && ctx->client) {
            for (uint32_t g = 0; g < ctx->num_groups; g++) {
                struct nfs_open_owner *owner = ctx->groups[g]->owner;
                if (owner->owner_len == oa->owner.owner.len &&
                    !memcmp(owner->owner, oa->owner.owner.data, owner->owner_len)) {
                    group = ctx->groups[g];
                    break;
                }
            }
            if (!group) {
                uint32_t count = 0;
                for (uint32_t j = i; j < nenc; j++) {
                    struct nfs_argop4 *next_op = &req->args_compound->argarray[j];
                    if (next_op->argop == OP_OPEN &&
                        next_op->opopen.owner.owner.len == oa->owner.owner.len &&
                        !memcmp(next_op->opopen.owner.owner.data, oa->owner.owner.data,
                                oa->owner.owner.len)) {
                        count++;
                    }
                }
                nfsstat4 reserve_status = (have_locks ? nfs_open_owner_reserve_compound_locks :
                                           nfs_open_owner_reserve_compound)(
                    ctx->client, oa->owner.owner.data, oa->owner.owner.len,
                    req->principal_flavor, req->principal_machinename, req->principal_machinename_len,
                    ctx, count, &thread->shared->nfs4_state_table, thread->vfs_thread, &group);
                if (reserve_status == NFS4_OK) {
                    ctx->groups[ctx->num_groups++] = group;
                    total_existing                += group->num_existing;
                    if (total_existing > NFS4_VFS_COMPOUND_MAX_OPS) {
                        goto refuse;
                    }
                }
            }
        }
        if (!group) {
            if (req->minorversion == 0 && journal_open[i - first]) {
                goto refuse;
            }
            nenc = ctx->num_reserved ? i : i + 1;
            break;
        }
        uint32_t candidate = 0;
        for (uint32_t j = first; j < i; j++) {
            candidate += ctx->ops[j - first].open_group == group;
        }
        map->open_group    = group;
        map->reserved_open = group->candidates[candidate];
        ctx->num_reserved++;
    }
    ctx->attempt_mark = req->encoding->dbuf->used;
    nfs4_vfs_attempt_reset(compound, ctx);
    /* A downgrade can be the first state mutation in the run. Freeze its
     * existing owner even when no OPEN requested candidate slots. */
    for (i = first; i < nenc; i++) {
        struct nfs4_vfs_op                *map = &ctx->ops[i - first];
        const struct stateid4             *sid = downgrade_public_sids[i - first];
        if (!sid || nfs4_vfs_private_open(ctx, sid)) {
            continue;
        }
        void                              *state_void;
        uint8_t                            type, owner_bytes[NFS4_OPAQUE_LIMIT];
        uint16_t                           owner_len = 0;
        struct nfs_open_owner_reservation *group;
        nfsstat4                           status = nfs_state_table_acquire_no_renew(&thread->shared->nfs4_state_table,
                                                                                     sid, NFS4_SLOT_TYPE_OPEN, &
                                                                                     state_void, &type);
        if (status == NFS4_OK) {
            struct nfs_open_state *state = state_void;
            status = nfs_state_check_client(state, type, ctx->client);
            if (status == NFS4_OK) {
                owner_len = state->owner->owner_len;
                memcpy(owner_bytes, state->owner->owner, owner_len);
            }
            nfs_state_table_release(&thread->shared->nfs4_state_table, state, type, thread->vfs_thread);
        }
        if (req->minorversion == 0 && status == NFS4ERR_BAD_STATEID) {
            goto refuse;
        }
        if (status != NFS4_OK) {
            map->stateid_reservation_status = status;
            continue;
        }
        status = (have_locks ? nfs_open_owner_reserve_compound_locks : nfs_open_owner_reserve_compound)(ctx->client,
                                                                                                        owner_bytes,
                                                                                                        owner_len,
                                                                                                        req->
                                                                                                        principal_flavor,
                                                                                                        req->
                                                                                                        principal_machinename,
                                                                                                        req->
                                                                                                        principal_machinename_len,
                                                                                                        ctx, 0,
                                                                                                        &thread->shared
                                                                                                        ->
                                                                                                        nfs4_state_table,
                                                                                                        thread->
                                                                                                        vfs_thread, &
                                                                                                        group);
        if (status != NFS4_OK) {
            nenc = i;
            break;
        }
        ctx->groups[ctx->num_groups++] = group;
        total_existing                += group->num_existing;
        if (total_existing > NFS4_VFS_COMPOUND_MAX_OPS) {
            goto refuse;
        }
        nfs4_vfs_attempt_reset(compound, ctx);
    }
    if (have_locks) {
        uint32_t modifications = 0;
        for (i = first; i < nenc; i++) {
            if (req->args_compound->argarray[i].argop == OP_LOCK ||
                req->args_compound->argarray[i].argop == OP_LOCKU) {
                modifications++;
            }
            if (!lock_public_sids[i - first]) {
                continue;
            }
            nfsstat4 status = nfs4_vfs_freeze_lock_parent(ctx, lock_public_sids[i - first]);
            if (status == NFS4ERR_DELAY || status == NFS4ERR_RESOURCE) {
                goto refuse;
            }
            if (status != NFS4_OK && req->minorversion == 0 &&
                req->args_compound->argarray[i].argop == OP_CLOSE) {
                struct nfs4_replay_cache replay;
                struct CLOSE4args       *close = &req->args_compound->argarray[i].opclose;
                if (nfs_state_table_lookup_replay(&thread->shared->nfs4_state_table,
                                                  &close->open_stateid, OP_CLOSE, close->seqid, &replay) == NFS4_OK) {
                    ctx->ops[i - first].close_response = malloc(sizeof(replay));
                    if (!ctx->ops[i - first].close_response) {
                        goto refuse;
                    }
                    *ctx->ops[i - first].close_response = replay;
                }
            }
            ctx->ops[i - first].stateid_reservation_status = status;
            total_existing                                 = 0;
            for (uint32_t g = 0; g < ctx->num_groups; g++) {
                total_existing += ctx->groups[g]->num_existing;
            }
            if (total_existing > NFS4_VFS_COMPOUND_MAX_OPS) {
                goto refuse;
            }
            nfs4_vfs_attempt_reset(compound, ctx);
        }
        /* A wire lock-owner may already anchor other files or OPEN owners.
         * Freeze those parents too, then follow any additional shared owners
         * reached through the new groups. No lock-owner journal is installed
         * until this bounded closure is complete. */
        for (i = first; i < nenc; i++) {
            argop = &req->args_compound->argarray[i];
            if (argop->argop == OP_LOCK && argop->oplock.locker.new_lock_owner) {
                const struct state_owner4 *owner = &argop->oplock.locker.open_owner.lock_owner;
                if (!nfs4_vfs_expand_lock_parents(compound, ctx, owner->owner.data, owner->owner.len)) {
                    goto refuse;
                }
            }
        }
        for (uint32_t g = 0; g < ctx->num_groups; g++) {
            struct nfs_open_owner_reservation *parent = ctx->groups[g];
            for (uint32_t j = 0; j < parent->num_locks; j++) {
                struct nfs_lock_owner *owner = parent->locks[j]->lock_owner;
                if (!nfs4_vfs_expand_lock_parents(compound, ctx, owner->owner, owner->owner_len)) {
                    goto refuse;
                }
            }
        }
        /* Every frozen child needs a private view, including siblings used by
         * later I/O but not explicitly modified by a LOCK in this run. */
        for (uint32_t g = 0; g < ctx->num_groups; g++) {
            struct nfs_open_owner_reservation *parent = ctx->groups[g];
            for (uint32_t j = 0; j < parent->num_locks; j++) {
                struct nfs_lock_owner             *owner = parent->locks[j]->lock_owner;
                struct nfs_lock_owner_reservation *group;
                if (nfs4_vfs_reserve_lock_owner(ctx, owner->owner, owner->owner_len,
                                                first, nenc, &group) != NFS4_OK) {
                    goto refuse;
                }
            }
        }
        for (i = first; i < nenc; i++) {
            struct nfs4_vfs_op        *map = &ctx->ops[i - first];
            argop = &req->args_compound->argarray[i];
            if (argop->argop != OP_LOCK || !argop->oplock.locker.new_lock_owner) {
                continue;
            }
            const struct state_owner4 *owner = &argop->oplock.locker.open_owner.lock_owner;
            if (nfs4_vfs_reserve_lock_owner(ctx, owner->owner.data, owner->owner.len,
                                            first, nenc, &map->lock_group) != NFS4_OK) {
                goto refuse;
            }
            uint32_t                   candidate = 0;
            for (uint32_t j = first; j < i; j++) {
                candidate += ctx->ops[j - first].lock_group == map->lock_group;
            }
            map->lock_candidate = map->lock_group->candidates[candidate];
        }
        ctx->range_capacity = 2 * modifications + 1;
        for (uint32_t g = 0; g < ctx->num_lock_groups; g++) {
            struct nfs_lock_owner_reservation *group = ctx->lock_groups[g];
            for (uint32_t j = 0; j < group->num_existing + group->num_candidates; j++) {
                if (ctx->num_locks == 2 * NFS4_VFS_COMPOUND_MAX_OPS) {
                    goto refuse;
                }
                struct nfs4_vfs_lock *entry = &ctx->locks[ctx->num_locks++];
                entry->group            = group;
                entry->initially_public = j < group->num_existing;
                entry->target           = entry->initially_public ? group->existing[j] : group->candidates[j - group->
                                                                                                           num_existing]
                ;
                entry->ranges = nfs_lock_range_journal_alloc(entry->initially_public ? entry->target : NULL,
                                                             modifications);
                if (!entry->ranges) {
                    goto refuse;
                }
                uint32_t count;
                nfs_lock_range_journal_previous(entry->ranges, &count);
                ctx->range_capacity += count;
            }
        }
        ctx->range_previous = calloc(ctx->range_capacity, sizeof(*ctx->range_previous));
        ctx->range_current  = calloc(ctx->range_capacity, sizeof(*ctx->range_current));
        if (!ctx->range_previous || !ctx->range_current) {
            goto refuse;
        }
        nfs4_vfs_attempt_reset(compound, ctx);
    }
    for (i = first; i < nenc; i++) {
        struct nfs4_vfs_op *map = &ctx->ops[i - first];
        argop = &req->args_compound->argarray[i];
        if (argop->argop == OP_OPEN && !map->open_group && ctx->num_groups) {
            /* A legacy tail cannot coalesce behind a reserved owner's
             * acceptance boundary, including downgrade-only reservations. */
            nenc = i;
            break;
        }
    }

    if (nenc <= first) {
        goto refuse;
    }

    if (seed_saved) {
        if (chimera_vfs_compound_add_putfh(compound, req->saved_fh, req->saved_fhlen) < 0 ||
            chimera_vfs_compound_add_savefh(compound) < 0) {
            goto refuse;
        }
    }

    /* Seed the current object.  When the remainder opens with a PUTFH this is
     * that PUTFH; otherwise it re-states the COMPOUND's current filehandle and
     * belongs to whichever op comes first. */
    idx = cur_fhlen ? chimera_vfs_compound_add_putfh(compound, cur_fh, cur_fhlen) :
        chimera_vfs_compound_add_checkpoint(compound);

    if (idx < 0) {
        goto refuse;
    }

    next = 0;

    for (i = first, k = 0; i < nenc; i++, k++) {
        struct nfs4_vfs_op *map = &ctx->ops[k];

        argop = &req->args_compound->argarray[i];

        map->ctx           = ctx;
        map->res_index     = i;
        map->vfs_lo        = next;
        map->prepare_index = k == 0 ? idx + (argop->argop != OP_PUTFH) : next;
        map->vfs_aux       = -1;
        map->verify_status = 0;

        switch (argop->argop) {
            case OP_PUTFH:
                if (i == first) {
                    /* The seed PUTFH above is this op. */
                    map->vfs_res = idx;
                } else {
                    /* A later PUTFH, into the same export -- see the scan.  It
                     * moves the sequence's current object the way any other op
                     * does, so it is an op of its own. */
                    uint8_t  later_fh[CHIMERA_VFS_FH_SIZE];
                    int      later_len;
                    uint16_t later_export;

                    if (chimera_nfs_fh_unwrap(argop->opputfh.object.data,
                                              (int) argop->opputfh.object.len,
                                              &later_export,
                                              later_fh, &later_len,
                                              thread->shared->fh_key,
                                              thread->shared->fh_sign) !=
                        CHIMERA_NFS_FH_OK) {
                        goto refuse;
                    }

                    idx = chimera_vfs_compound_add_putfh(compound,
                                                         later_fh,
                                                         later_len);
                    map->vfs_res = idx;

                    if (idx < 0) {
                        goto refuse;
                    }
                }

                /* The PUTFH moved the current object; nothing is open on the
                 * new one. */
                cur_open_flags = 0;

                /* Stat the handle so the zero-link staleness rule has something
                 * to test; the gate reads it as this op finishes. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_NLINK);
                map->vfs_aux = idx;
                break;

            case OP_LOOKUP:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_lookup(compound, (const char *) argop->oplookup.objname.data, (
                                                          int) argop->oplookup.objname.len, 0, 0);
                map->vfs_res = idx;

                /* The child is the current object now. */
                cur_open_flags = 0;
                break;

            case OP_GETATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound,
                    chimera_nfs4_attr2mask(argop->opgetattr.attr_request,
                                           argop->opgetattr.num_attr_request) |
                    CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_SIZE);
                map->vfs_res = idx;
                if (idx >= 0 && ctx->client &&
                    chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
                    idx = chimera_vfs_compound_add_coordinate(compound, nfs4_vfs_getattr_coordinate,
                                                              map);
                    map->getattr_coordinate = idx;
                }
                break;

            case OP_ACCESS:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_access(
                    compound,
                    chimera_nfs4_access4_to_mask(argop->opaccess.access));
                map->vfs_res = idx;
                break;

            case OP_GETFH:
                idx          = chimera_vfs_compound_add_getfh(compound);
                map->vfs_res = idx;
                break;

            case OP_READLINK:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                /* The type gate READLINK applies before it reads.  It and the
                 * READLINK act on the same object, so they share the open. */
                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                if (idx >= 0) {
                    idx = chimera_vfs_compound_add_readlink(compound);
                }
                map->vfs_res = idx;
                break;

            case OP_LOOKUPP:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx          = chimera_vfs_compound_add_lookupp(compound, 0);
                map->vfs_res = idx;

                /* The parent is the current object now. */
                cur_open_flags = 0;
                break;

            case OP_CREATE:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_create_op(compound, argop, map);
                map->vfs_res = idx;

                /* The created object is the current one now. */
                cur_open_flags = 0;
                break;

            case OP_SECINFO:
                /* The name lookup is the whole of the VFS work: it produces
                 * NFS4ERR_NOTDIR for a current object that is not a directory
                 * and NFS4ERR_NOENT for a name that is not there, which are the
                 * two errors SECINFO is specified to return.  The flavors
                 * themselves come from the export, which the request already
                 * names. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                if (req->minorversion == 0) {
                    map->namespace_parent = chimera_vfs_compound_add_getfh(compound);
                    if (map->namespace_parent < 0) {
                        goto refuse;
                    }
                }

                idx = chimera_vfs_compound_add_lookup(compound, (const char *) argop->opsecinfo.name.data, (
                                                          int) argop->opsecinfo.name.len, 0, 0);
                map->vfs_res = idx;

                if (req->minorversion == 0) {
                    const uint8_t placeholder = 0;
                    if (idx < 0) {
                        goto refuse;
                    }
                    idx = chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
                    if (idx < 0) {
                        goto refuse;
                    }
                    /* Do not use SAVEFH: the wire saved cursor is independent. */
                    chimera_vfs_compound_set_op_prepare(compound, idx, nfs4_vfs_namespace_restore, map);
                }

                cur_open_flags = 0;
                break;

            case OP_LOCK:
                idx = chimera_vfs_compound_add_checkpoint(compound);
                if (idx < 0) {
                    goto refuse;
                }
                map->prepare_index = idx;
                idx                = chimera_vfs_compound_add_reserve_handle(compound, NULL, &map->lock_claim);
                if (idx < 0) {
                    goto refuse;
                }
                map->lock_reservation = idx;
                chimera_vfs_compound_set_op_callbacks(compound, idx,
                                                      nfs4_vfs_lock_reserve_prepare, nfs4_vfs_lock_reserve_complete, map
                                                      );
                idx = chimera_vfs_compound_add_checkpoint(compound);
                if (idx < 0) {
                    goto refuse;
                }
                chimera_vfs_compound_set_op_callbacks(compound, idx, NULL, nfs4_vfs_lock_commit, map);
                map->vfs_res = idx;
                break;

            case OP_LOCKU:
                idx          = chimera_vfs_compound_add_checkpoint(compound);
                map->vfs_res = idx;
                break;

            case OP_LOCKT:
                /* The completion gate checks type and live claims before the
                 * executor advances to the next operation. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_res = idx;
                break;

            case OP_READ_PLUS:
            {
                struct evpl_iovec *riov = xdr_dbuf_alloc_space(
                    sizeof(*riov) * NFS4_VFS_READ_MAX_IOV, req->encoding->dbuf);

                chimera_nfs_abort_if(riov == NULL, "Failed to allocate space");
                map->io_authorize = 1;
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }
                idx          = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;
                if (idx < 0) {
                    goto refuse;
                }
                cur_open_flags = 0;
                idx            = chimera_vfs_compound_add_read_plus(
                    compound, NULL, argop->opread_plus.rpa_offset,
                    argop->opread_plus.rpa_count);
                map->vfs_res = idx;
                if (idx < 0) {
                    goto refuse;
                }
                idx = chimera_vfs_compound_add_read(compound, NULL, argop->opread_plus.rpa_offset, argop
                                                    ->opread_plus.rpa_count, riov, NFS4_VFS_READ_MAX_IOV, 0, NULL, NULL,
                                                    0);
                map->read_plus_data = idx;
                break;
            }

            case OP_READ:
            case OP_WRITE:
            {
                map->io_authorize = 1;
                cur_open_flags    = 0;

                if (argop->argop == OP_READ) {
                    {
                        struct evpl_iovec *riov = xdr_dbuf_alloc_space(
                            sizeof(*riov) * NFS4_VFS_READ_MAX_IOV,
                            req->encoding->dbuf);

                        chimera_nfs_abort_if(riov == NULL,
                                             "Failed to allocate space");

                        /* From the reply's own memory, because the descriptors
                         * the read writes here are where the reply will read
                         * them from and cannot be moved afterwards. */
                        idx = chimera_vfs_compound_add_read(compound, map->io_handle, argop->opread.offset, argop->
                                                            opread.count, riov, NFS4_VFS_READ_MAX_IOV, 0, NULL, NULL, 0)
                        ;
                    }
                } else {
                    /* Ownership of the payload moves off the RPC2 message, so
                     * that freeing the message does not release iovecs this
                     * sequence is about to hand to the backend.  A no-op unless
                     * the data arrived in an RDMA read chunk. */
                    evpl_rpc2_encoding_take_read_chunk(req->encoding, NULL,
                                                       NULL);

                    idx = chimera_vfs_compound_add_write(compound, map->io_handle, argop->opwrite.offset, argop->opwrite
                                                         .data.length, argop->opwrite.stable, argop->opwrite.data.iov,
                                                         argop->opwrite.data.niov, 0, 0, NULL);
                }

                map->vfs_res = idx;
                break;
            }

            case OP_SETATTR:
            {
                struct chimera_vfs_attrs sattr;

                memset(&sattr, 0, sizeof(sattr));
                if (nfs4_vfs_decode_attrs(map, &argop->opsetattr.obj_attributes,
                                          &sattr) != NFS4_OK) {
                    goto refuse;
                }

                map->io_authorize = !!(sattr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
                cur_open_flags    = 0;

                if (map->io_authorize && chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
                    idx = chimera_vfs_compound_add_coordinate(compound, nfs4_vfs_layout_coordinate, map);
                    if (idx < 0) {
                        goto refuse;
                    }
                }
                idx          = chimera_vfs_compound_add_setattr(compound, map->io_handle, &sattr, 0, 0);
                map->vfs_res = idx;
                break;
            }

            case OP_REMOVE:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                if (chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
                    map->namespace_parent = chimera_vfs_compound_add_getfh(compound);
                    if (map->namespace_parent < 0 ||
                        nfs4_vfs_namespace_add_target(compound, map,
                                                      argop->opremove.target.data, argop->opremove.target.len) < 0) {
                        goto refuse;
                    }
                    cur_open_flags = 0;
                }

                idx = chimera_vfs_compound_add_remove(compound, (const char *) argop->opremove.target.data, (
                                                          int) argop->opremove.target.len, 0, 0, 0);
                map->vfs_res = idx;
                if (idx >= 0 && map->namespace_count) {
                    chimera_vfs_compound_set_op_prepare(compound, idx, nfs4_vfs_namespace_ready, map);
                }
                break;

            /* Source from the saved filehandle, target from the current one --
             * which is what the VFS op takes, so neither needs anything said
             * about where the other directory is. */
            case OP_RENAME:
                if (chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
                    /* Legacy RENAME first verifies the target directory. The
                     * internal source walk uses RESTOREFH but never SAVEFH,
                     * preserving the protocol's saved directory for mutation. */
                    if (nfs4_vfs_open_for(compound, &cur_open_flags, NFS4_VFS_OPEN_DIR) < 0) {
                        goto refuse;
                    }
                    map->namespace_parent = chimera_vfs_compound_add_getfh(compound);
                    if (map->namespace_parent < 0 || chimera_vfs_compound_add_restorefh(compound) < 0 ||
                        nfs4_vfs_namespace_add_target(compound, map,
                                                      argop->oprename.oldname.data, argop->oprename.oldname.len) < 0 ||
                        nfs4_vfs_namespace_add_target(compound, map,
                                                      argop->oprename.newname.data, argop->oprename.newname.len) < 0) {
                        goto refuse;
                    }
                    cur_open_flags = 0;
                }
                idx = chimera_vfs_compound_add_rename(compound, (const char *) argop->oprename.oldname.data, (
                                                          int) argop->oprename.oldname.len, (const char *)
                                                      argop->oprename.newname.data, (int) argop->oprename.newname.len, 0
                                                      , 0, 0);
                map->vfs_res = idx;
                if (idx >= 0 && map->namespace_count) {
                    chimera_vfs_compound_set_op_prepare(compound, idx, nfs4_vfs_namespace_ready, map);
                }
                break;

            case OP_LINK:
                /* NFS4's LINK reply is a change_info for the directory; the
                 * linked object's own attributes have no reader. */
                idx = chimera_vfs_compound_add_link(compound, (const char *) argop->oplink.newname.data, (int)
                                                    argop->oplink.newname.len, 0, 0, 0);
                map->vfs_res = idx;
                break;

            case OP_OPEN_DOWNGRADE:
                map->downgrade_scratch = calloc(1, sizeof(*map->downgrade_scratch));
                if (!map->downgrade_scratch) {
                    goto refuse;
                }
                map->reserved_open = map->downgrade_scratch;
                idx                = chimera_vfs_compound_add_checkpoint(compound);
                if (idx < 0) {
                    goto refuse;
                }
                idx                   = chimera_vfs_compound_add_reserve_handle(compound, NULL, &map->open_claim);
                map->open_reservation = idx;
                if (idx < 0) {
                    goto refuse;
                }
                chimera_vfs_compound_set_op_callbacks(compound, idx,
                                                      nfs4_vfs_downgrade_reserve_prepare, nfs4_vfs_open_reserve_complete
                                                      , map);
                idx = chimera_vfs_compound_add_checkpoint(compound);
                if (idx >= 0) {
                    chimera_vfs_compound_set_op_callbacks(compound, idx, NULL, nfs4_vfs_open_commit, map);
                }
                map->vfs_res = idx;
                break;

            case OP_TEST_STATEID:
                if (argop->optest_stateid.num_ts_stateids) {
                    map->test_stateid_statuses = xdr_dbuf_alloc_space(
                        (uint64_t) argop->optest_stateid.num_ts_stateids * sizeof(nfsstat4), req->encoding->dbuf);
                    if (!map->test_stateid_statuses) {
                        goto refuse;
                    }
                }
                idx          = chimera_vfs_compound_add_checkpoint(compound);
                map->vfs_res = idx;
                break;

            case OP_OPEN_CONFIRM:
            case OP_IO_ADVISE:
            case OP_SECINFO_NO_NAME:
            case OP_CLOSE:
                idx          = chimera_vfs_compound_add_checkpoint(compound);
                map->vfs_res = idx;
                break;

            case OP_OPEN:
                if (req->minorversion == 0 && map->open_group) {
                    map->open_response = calloc(1, sizeof(*map->open_response));
                    if (!map->open_response) {
                        goto refuse;
                    }
                }
                /* A named OPEN resolves in the current directory; an unnamed
                * one re-opens the current object and needs nothing first. */
                if (nfs4_vfs_open_is_named(argop) &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                if (!map->reserved_open) {
                    ctx->open_present   = 1;
                    ctx->open_res_index = i;
                }
                idx = nfs4_vfs_add_open_op(req, compound,
                                           argop, map);
                map->vfs_res = idx;
                if (map->reserved_open && idx >= 0) {
                    struct chimera_vfs_attrs trunc = { 0 };
                    chimera_vfs_compound_set_op_callbacks(compound, idx, NULL,
                                                          nfs4_vfs_open_checked, map);
                    idx = chimera_vfs_compound_add_reserve(compound, map->vfs_res,
                                                           &map->open_claim);
                    map->open_reservation = idx;
                    if (idx < 0) {
                        goto refuse;
                    }
                    chimera_vfs_compound_set_op_callbacks(compound, idx,
                                                          nfs4_vfs_open_reserve_prepare, nfs4_vfs_open_reserve_complete,
                                                          map);
                    idx = chimera_vfs_compound_add_open(compound, NULL, 0, 0, 0, NULL, 0, 0, 0);
                    if (idx < 0) {
                        goto refuse;
                    }
                    chimera_vfs_compound_set_op_callbacks(compound, idx,
                                                          nfs4_vfs_open_union_prepare, nfs4_vfs_open_union_complete, map
                                                          );
                    if (map->open_trunc_if_existed) {
                        trunc.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
                        trunc.va_req_mask = CHIMERA_VFS_ATTR_SIZE;
                        trunc.va_size     = 0;
                        idx               = chimera_vfs_compound_add_setattr(compound, NULL, &trunc, 0, 0);
                        if (idx < 0) {
                            goto refuse;
                        }
                        chimera_vfs_compound_set_op_callbacks(compound, idx,
                                                              nfs4_vfs_open_truncate_prepare, NULL, map);
                    }
                    idx = chimera_vfs_compound_add_checkpoint(compound);
                    if (idx < 0) {
                        goto refuse;
                    }
                    chimera_vfs_compound_set_op_callbacks(compound, idx, NULL,
                                                          nfs4_vfs_open_commit, map);
                }


                if (map->open_response) {
                    idx = chimera_vfs_compound_add_putfh(compound, cur_fh, cur_fhlen);
                    if (idx < 0) {
                        goto refuse;
                    }
                    map->open_replay_restore = idx;
                    chimera_vfs_compound_set_op_prepare(compound, idx, nfs4_vfs_open_replay_restore, map);
                }

                /* The OPEN result owns the data handle; the VFS cursor can
                 * borrow it until another operation selects a different one. */
                cur_open_flags = 0;
                break;

            case OP_SAVEFH:
                idx          = chimera_vfs_compound_add_savefh(compound);
                map->vfs_res = idx;
                break;

            case OP_RESTOREFH:
                idx          = chimera_vfs_compound_add_restorefh(compound);
                map->vfs_res = idx;

                /* A different object is current; nothing is open on it. */
                cur_open_flags = 0;
                break;

            case OP_COMMIT:
                /* The regular-file gate COMMIT applies before it flushes, from
                 * a stat of the object taken through a path open -- the same
                 * two-open shape the per-op path has. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                /* The flush itself needs the data open, which is a different
                 * handle from a different cache than the stat above. */
                if (idx >= 0 &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DATA) < 0) {
                    goto refuse;
                }

                if (idx >= 0) {
                    idx = chimera_vfs_compound_add_commit(compound, argop->opcommit.offset, argop->opcommit.count,
                                                          CHIMERA_VFS_ATTR_MODE, 0);
                }
                map->vfs_res = idx;
                break;

            case OP_READDIR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_readdir_stream(compound, argop->opreaddir.cookie,
                                                              nfs4_vfs_readdir_verifier(&argop->opreaddir),
                                                              chimera_nfs4_attr2mask(argop->opreaddir.attr_request,
                                                                                     argop
                                                                                     ->
                                                                                     opreaddir.num_attr_request) |
                                                              CHIMERA_VFS_ATTR_FH, 0, 0, NULL, 0, nfs4_vfs_readdir_reset
                                                              ,
                                                              nfs4_vfs_readdir_append, ctx);
                map->vfs_res = idx;
                break;

            case OP_VERIFY:
            case OP_NVERIFY:
                /* The whole of VERIFY is a comparison against attributes the
                 * client sent; the VFS work is the stat it compares.  The
                 * comparison itself runs in the gate, so a mismatch stops the
                 * ops behind it -- which is what VERIFY exists to do. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound,
                    chimera_nfs4_attr2mask(
                        argop->opverify.obj_attributes.attrmask,
                        argop->opverify.obj_attributes.num_attrmask));
                map->vfs_aux = idx;
                map->vfs_res = idx;
                break;

            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
            case OP_WRITE_SAME:
            {
                map->io_authorize = 1;

                /* The regular-file gate, from a stat of the current object --
                 * the same shape COMMIT uses. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                cur_open_flags = 0;

                if (idx >= 0) {
                    if (argop->argop == OP_SEEK) {
                        idx = chimera_vfs_compound_add_seek(
                            compound, map->io_handle,
                            argop->opseek.sa_offset,
                            argop->opseek.sa_what == NFS4_CONTENT_HOLE ? 1 : 0);
                    } else if (argop->argop == OP_ALLOCATE) {
                        idx = chimera_vfs_compound_add_allocate(
                            compound, map->io_handle,
                            argop->opallocate.aa_offset,
                            argop->opallocate.aa_length,
                            0, 0, 0);
                    } else if (argop->argop == OP_DEALLOCATE) {
                        idx = chimera_vfs_compound_add_allocate(
                            compound, map->io_handle,
                            argop->opdeallocate.da_offset,
                            argop->opdeallocate.da_length,
                            CHIMERA_VFS_ALLOCATE_DEALLOCATE, 0, 0);
                    } else {
                        idx = chimera_vfs_compound_add_write_same(
                            compound, map->io_handle,
                            argop->opwrite_same.wsa_adb.adb_offset,
                            argop->opwrite_same.wsa_adb.adb_block_size,
                            argop->opwrite_same.wsa_adb.adb_block_count,
                            argop->opwrite_same.wsa_adb.adb_pattern.data,
                            argop->opwrite_same.wsa_adb.adb_pattern.len,
                            argop->opwrite_same.wsa_adb.adb_reloff_pattern,
                            argop->opwrite_same.wsa_stable,
                            0, 0);
                    }
                }

                map->vfs_res = idx;
                break;
            }

            case OP_COPY:
            case OP_CLONE:
            {
                int      copy       = argop->argop == OP_COPY;
                uint64_t src_offset = copy ? argop->opcopy.ca_src_offset : argop->opclone.cl_src_offset;
                uint64_t dst_offset = copy ? argop->opcopy.ca_dst_offset : argop->opclone.cl_dst_offset;
                uint64_t length     = copy ? argop->opcopy.ca_count : argop->opclone.cl_count;
                /* The first GETFH privately remembers the destination while
                 * we inspect the source. The wire saved slot is untouched. */
                idx = chimera_vfs_compound_add_getfh(compound);
                if (idx < 0 || chimera_vfs_compound_add_restorefh(compound) < 0) {
                    goto refuse;
                }
                map->range_src_open = chimera_vfs_compound_add_open(compound, NULL, 0, NFS4_VFS_OPEN_DATA |
                                                                    CHIMERA_VFS_OPEN_READ_ONLY, 0, NULL, 0, 0, 0);
                map->range_src_attr = chimera_vfs_compound_add_getattr(compound,
                                                                       CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_MODE);
                map->range_restore  = chimera_vfs_compound_add_putfh(compound, cur_fh, cur_fhlen);
                map->range_dst_open = chimera_vfs_compound_add_open(compound, NULL, 0, NFS4_VFS_OPEN_DATA |
                                                                    CHIMERA_VFS_OPEN_WRITE_ONLY, 0, NULL, 0, 0, 0);
                if (map->range_src_open < 0 || map->range_src_attr < 0 ||
                    map->range_restore < 0 || map->range_dst_open < 0) {
                    goto refuse;
                }
                idx = copy ? chimera_vfs_compound_add_copy_range(compound, NULL, src_offset,
                                                                 NULL, dst_offset, length, 0, 0, 0) :
                    chimera_vfs_compound_add_clone_range(compound, NULL, src_offset,
                                                         NULL, dst_offset, length, 0, 0);
                map->vfs_res   = idx;
                cur_open_flags = 0;
                break;
            }

            case OP_GETXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_SETXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_LISTXATTRS:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_listxattrs(
                    compound,
                    argop->oplistxattrs.lxa_cookie,
                    chimera_nfs4_xattr_stage_max(
                        req, argop->oplistxattrs.lxa_maxcount));
                map->vfs_res = idx;
                break;

            case OP_REMOVEXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            default:
                idx = -1;
                break;
        } /* switch */

        if (idx < 0) {
            goto refuse;
        }

        map->vfs_hi = idx;
        next        = idx + 1;
    }

    ctx->num_ops = k;

    ctx->attempt_mark = req->encoding->dbuf->used;
    /* Construction allocated buffers already included in the static bound. */
    ctx->reply_bound = reply_bound -
        (ctx->attempt_mark - (req->encoding->dbuf->size - avail));
    chimera_vfs_compound_set_attempt_reset(compound, nfs4_vfs_attempt_reset, ctx);
    chimera_vfs_compound_set_gate(compound, nfs4_vfs_replay_complete, ctx);
    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op *map = &ctx->ops[k];
        if (map->vfs_aux >= 0) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_aux,
                                                  NULL, nfs4_vfs_compound_gate, ctx);
        }
        if (req->args_compound->argarray[map->res_index].argop == OP_READDIR) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_res,
                                                  NULL, nfs4_vfs_compound_gate, ctx);
        } else if (req->args_compound->argarray[map->res_index].argop == OP_SECINFO ||
                   req->args_compound->argarray[map->res_index].argop == OP_SECINFO_NO_NAME) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_res,
                                                  NULL, nfs4_vfs_secinfo_checked, map);
        } else if (req->args_compound->argarray[map->res_index].argop == OP_LOCKT) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_res,
                                                  NULL, nfs4_vfs_lockt_complete, map);
        } else if (req->args_compound->argarray[map->res_index].argop == OP_GETATTR &&
                   (map->getattr_coordinate ||
                    (req->args_compound->argarray[map->res_index].opgetattr.num_attr_request &&
                     (req->args_compound->argarray[map->res_index].opgetattr.attr_request[0] & (1U << FATTR4_ACL))))) {
            chimera_vfs_compound_set_op_callbacks(compound,
                                                  map->getattr_coordinate ? map->getattr_coordinate : map->vfs_res,
                                                  NULL, nfs4_vfs_getattr_complete, map);
        }
    }
    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op *map    = &ctx->ops[k];
        uint32_t            opcode = req->args_compound->argarray[map->res_index].argop;
        if (req->minorversion == 0 && (opcode == OP_LOCK || opcode == OP_OPEN_DOWNGRADE)) {
            chimera_vfs_compound_set_op_prepare(compound, map->vfs_hi,
                                                nfs4_vfs_replay_skip_prepare, map);
        }
        if (map->open_response) {
            for (int r = map->prepare_index; r <= map->vfs_hi; r++) {
                if (!chimera_vfs_compound_op(compound, r)->prepare) {
                    chimera_vfs_compound_set_op_prepare(compound, r, nfs4_vfs_replay_skip_prepare, map);
                }
            }
        }
        if (opcode == OP_COPY || opcode == OP_CLONE) {
            for (int r = map->prepare_index; r <= map->vfs_hi; r++) {
                chimera_vfs_compound_set_op_prepare(compound, r, nfs4_vfs_range_prepare, map);
            }
            continue;
        }
        chimera_vfs_compound_set_op_prepare(compound, map->prepare_index,
                                            nfs4_vfs_operation_prepare, map);
        if ((map->io_authorize || opcode == OP_LOOKUP || opcode == OP_SECINFO) &&
            map->vfs_res != map->prepare_index) {
            chimera_vfs_compound_set_op_prepare(compound, map->vfs_res,
                                                nfs4_vfs_operation_prepare, map);
        }
        if (req->args_compound->argarray[map->res_index].argop == OP_READ_PLUS) {
            chimera_vfs_compound_set_op_prepare(compound, map->read_plus_data,
                                                nfs4_vfs_operation_prepare, map);
        }
    }
    chimera_nfs_debug("NFS4_VFS_SUBMIT tag=%.*s start=%u count=%u compound=%p",
                      (int) req->args_compound->tag.len, req->args_compound->tag.data,
                      first, nenc - first, compound);
    chimera_nfs_debug("NFS4 VFS compound %p: wire first=%u count=%u vfs_ops=%u reserved_open=%d",
                      compound, first, nenc - first, chimera_vfs_compound_num_ops(compound),
                      ctx->num_reserved);

    chimera_vfs_compound_submit(compound, nfs4_vfs_compound_complete, ctx);

    return 1;

 refuse:

    /* The build gave up after the 4.0 entry pinned the owner.  The per-op path
     * is about to resolve it again, so drop this reference rather than leave
     * two outstanding against one chimera_nfs4_open_finish. */
    if (open_4_0_pinned && req->open_4_0_owner) {
        nfs_open_owner_put(req->open_4_0_owner);
        req->open_4_0_owner = NULL;
    }

    /* The build gave up partway; anything it resolved goes back.  The WRITE
     * payloads stay put -- the per-op path is about to run and releases them
     * itself. */
    for (k = 0; k < NFS4_VFS_COMPOUND_MAX_OPS; k++) {
        if (ctx->ops[k].io_handle) {
            chimera_vfs_release(thread->vfs_thread, ctx->ops[k].io_handle);
            ctx->ops[k].io_handle = NULL;
        }
    }

    nfs4_vfs_dispose(compound, ctx);

    req->encoding->dbuf->used = build_mark;

    return 0;
} /* chimera_nfs4_compound_try_vfs */
