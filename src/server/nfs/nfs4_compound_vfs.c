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
 * Delegation grants run after accepted finish. Explicit export selections use
 * credential groups. pNFS data-server cleanup follows accepted namespace work.
 *
 * Finish-time rejection takes precedence over ordinary per-operation errors:
 * EAGAIN restarts the attempt only after the finish adapter has aborted backend
 * effects. An accepted operation error preserves the successful prefix.
 */

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>
#include "vfs/vfs_claim.h"
#include "vfs/sdk/vfs_access.h"
#include "common/evpl_iovec_cursor.h"

#include "nfs4_procs.h"
#include "nfs4_reply.h"
#include "nfs4_protocol.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_access.h"
#include "nfs4_named_attr.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_cb.h"
#include "nfs4_callback.h"
#include "nfs4_op_matrix.h"
#include "nfs4_pnfs_compound.h"
#include "server/server.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_pnfs.h"
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

/* Named claims resolve in the execution cursor's directory. */
static int
nfs4_vfs_open_is_named(const struct nfs_argop4 *argop)
{
    return argop->opopen.claim.claim == CLAIM_NULL || argop->opopen.claim.claim == CLAIM_DELEGATE_CUR;
} /* nfs4_vfs_open_is_named */

static bool
nfs4_vfs_open_is_delegated(const struct OPEN4args *args)
{
    return args->claim.claim == CLAIM_DELEGATE_CUR || args->claim.claim == CLAIM_DELEG_CUR_FH;
} /* nfs4_vfs_open_is_delegated */

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

/* Per-wire planning and state journals have a finite 128-operation bound.
 * Each wire operation may expand into several helpers, independently bounded
 * by CHIMERA_VFS_COMPOUND_MAX_OPS. */
#define NFS4_VFS_COMPOUND_MAX_OPS 128      /* Wire maps and state journals, not VFS helper operations. */

struct nfs4_vfs_compound_ctx;
struct nfs4_vfs_coordination {
    struct nfs4_vfs_coordination          *next;
    struct nfs4_vfs_compound_ctx          *ctx;
    struct chimera_vfs_compound           *compound;
    uint64_t                               token;
    uint32_t                               index, fh_len;
    uint8_t                                fh[CHIMERA_VFS_FH_SIZE];
    bool                                   layout_barrier;
    struct nfs_layout_recall_view          layout_view;
    bool                                   stream_holder;
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
    struct nfs4_vfs_coordination      *stream_guard;
    int                                initially_public, active, closed, version_dirty;
};

struct nfs4_vfs_lock {
    struct nfs_lock_state             *target;
    struct nfs_lock_owner_reservation *group;
    struct nfs4_vfs_state             *parent;
    struct nfs_lock_range_journal     *ranges;
    uint32_t                           seqid;
    int                                initially_public, active, dirty, retired;
};

/* Borrowed by VFS groups, immutable until the compound is freed. */
struct nfs4_vfs_identity {
    struct nfs4_vfs_identity *next;
    struct chimera_nfs_export export;
    struct chimera_vfs_cred   cred;
    bool                      present;
};

struct nfs4_vfs_op {
    struct nfs4_vfs_identity          *identity, *saved_identity;
    struct nfs4_vfs_identity          *planned_identity, *junction_target;
    int                                saved_seed, current_seed, group_lo, junction_start, junction_end;
    bool                               group_start, runtime_lookup, runtime_parent, parent_crossing;
    nfsstat4                           terminal_status;
    struct GETDEVICEINFO4res           device_result;
    bool                               pseudo, result_pseudo, planned_pseudo;
    bool                               maybe_pseudo;
    int                                parent_seed, root_readdir_start;
    nfsstat4                           root_readdir_status;
    uint8_t                            result_fh[NFS4_FHSIZE];
    int                                result_fhlen;
    bool                               junction, secinfo_junction;
    struct nfs4_vfs_identity          *secinfo_export;

    struct nfs4_vfs_compound_ctx      *ctx;
    struct nfs_delegation_retirement  *deleg_retirement;
    struct nfs_client                 *retirement_client;
    nfsstat4                           retirement_status;
    struct stateid4                    retirement_stateid;
    bool                               retirement_passed;
    struct nfs4_layoutget             *layoutget;
    struct nfs_lock_owner_reservation *lock_group;
    struct nfs_lock_state             *lock_candidate;
    struct nfs4_vfs_lock              *lock_state;
    struct chimera_vfs_claim           lock_claim;
    struct nfs4_owner_replay_journal  *open_replay, *lock_replay;
    uint32_t                           open_seqid, lock_seqid;
    struct nfs4_replay_cache          *open_response, *close_response;
    /* v4.0 owner identities are independent of the connection's binding.
     * owner_client/session retain immutable construction inputs; client is
     * the attempt's selected identity and owns no additional reference. */
    struct nfs_client                 *owner_client, *client;
    struct nfs4_session               *owner_session;
    nfsstat4                           owner_status;
    bool                               owner_checked;
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
    /* Variable replies are private to an execution attempt. No accepted
     * completion allocates their payload after successors have run. */
    union {
        struct READ_PLUS4res  read_plus;
        struct GETXATTR4res   getxattr;
        struct LISTXATTRS4res listxattrs;
    } variable_result;
    uint32_t                           reply_mark;
    bool                               reply_staged;
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
    struct nfs4_root_readdir_state    *root_readdir;
    uint64_t                           readdir_verifier;
    bool                               readdir_eof;
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
    int                                open_deleg_lookup;
    bool                               open_stream;
    struct nfs4_vfs_coordination      *stream_guard;
    bool                               attrdir, result_attrdir;
    bool                               possible_junction;
    nfsstat4                           input_status;
    /* READ, WRITE, SETATTR: the handle this op resolved from its stateid, and
     * holds a reference to for as long as the sequence runs.  Borrowed by the
     * VFS op; released here when the sequence is over, whatever the outcome. */
    struct chimera_vfs_open_handle    *io_handle;
};

struct nfs4_vfs_compound_ctx {
    struct nfs_request                   *req;
    struct nfs4_vfs_identity             *identities, *current_identity, *saved_identity;
    struct nfs4_vfs_identity             *initial_identity, *initial_saved_identity;
    bool                                  dynamic_identity;
    uint8_t                               saved_fh[NFS4_FHSIZE];
    int                                   saved_fhlen;
    bool                                  single, pseudo, initial_pseudo, saved_pseudo;
    struct nfs4_vfs_identity             *namespace_identity;
    chimera_vfs_compound_op_callback_t    namespace_prepare[CHIMERA_VFS_COMPOUND_MAX_OPS];
    void                                 *namespace_context[CHIMERA_VFS_COMPOUND_MAX_OPS];
    struct chimera_nfs_export            *entry_export;
    struct chimera_nfs_export            *namespace_root;
    int                                   root_probe;
    uint8_t                               namespace_fh[NFS4_FHSIZE];
    uint32_t                              namespace_fhlen;
    const struct nfs4_vfs_op             *last_root;
    nfs4_root_export_fh_callback_t        root_resolve;
    struct nfs4_vfs_coordination         *coordination;
    struct nfs_delegation_combine_journal combines[NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_combines;
    struct nfs_delegation_retirement      retirements[NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_retirements;
    bool                                  accepted;
    /* Fixed session/entry identity and the private v4.0 OPEN binding cursor.
     * Only accepted publication changes req->session or the connection. */
    struct nfs_client                    *client, *current_client;
    struct nfs_layout_journal             layouts;
    struct nfs_client                    *io_clients[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    bool                                  io_renew[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_io_clients;
    unsigned int                          cleanup_retries, cleanup_next;
    char                                  backing_name[CHIMERA_VFS_PNFS_BACKING_NAME_MAX];
    uint8_t                               backing_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                              backing_fhlen;
    uint32_t                              num_reserved;
    struct nfs_open_state                *closed_states[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    const struct chimera_vfs_claim       *closed_claims[2 * NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t                              num_closed_states, num_closed_claims;
    struct nfs_open_owner_reservation   **groups;
    uint32_t                              num_groups, group_capacity;
    struct nfs_lock_owner_reservation   **lock_groups;
    uint32_t                              num_lock_groups, lock_group_capacity;
    struct nfs4_vfs_lock                 *locks;
    struct chimera_vfs_file_state       **retired_files;
    uint32_t                              num_locks, lock_capacity;
    const struct chimera_vfs_claim      **range_previous, **range_current;
    uint32_t                              range_capacity, num_range_previous, num_range_current;
    struct nfs4_vfs_state                *states;
    uint32_t                              state_capacity;
    uint32_t                              num_states;
    struct stateid4                       initial_stateid, initial_saved_stateid;
    struct stateid4                       current_stateid, saved_stateid;
    int                                   initial_stateid_valid, initial_saved_stateid_valid;
    int                                   current_stateid_valid, saved_stateid_valid;
    uint32_t                              attempt_mark;
    uint64_t                              reply_bound;
    uint64_t                              reply_staged_bytes;
    uint64_t                              reply_staged_bound;
    uint32_t                              reply_read_iov;
    bool                                  reply_read_seen;
    uint64_t                              reply_bytes;
    uint64_t                              reply_chunk_bytes;
    uint32_t                              retries;
    uint32_t                              num_ops;
    struct chimera_vfs_compound          *accepted_compound;
    uint32_t                              grant_next;
    int                                   accepted_index, accepted_fhlen;
    uint8_t                               accepted_fh[NFS4_FHSIZE];
    nfsstat4                              accepted_status;
    struct nfs4_vfs_op                    ops[NFS4_VFS_COMPOUND_MAX_OPS];

    /* A SECINFO that succeeded has taken the current filehandle away. */
    int                                   fh_consumed;
    int                                   initial_fh_consumed;

    /* Protocol cursor after the last successful wire operation. The VFS
     * cursor names the base inode while this cursor names its attribute
     * directory, and may also move temporarily during a failed operation. */
    uint8_t                               fh[NFS4_FHSIZE];
    int                                   fh_len;

};

static struct nfs_client *
nfs4_vfs_owner_client(
    const struct nfs4_vfs_compound_ctx *ctx,
    const struct nfs4_vfs_op           *map)
{
    return ctx->req->minorversion ? ctx->client : map->owner_client;
} /* nfs4_vfs_owner_client */

static void nfs4_vfs_attempt_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data);

static int nfs4_vfs_stateid_closed(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid);

static bool nfs4_vfs_build_reset(
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx);

/* Caller holds exports_lock. Each credential group freezes its export policy;
* a changed export must be selected afresh, never replayed with old policy. */
static bool
nfs4_vfs_export_matches(
    struct chimera_server_nfs_shared *shared,
    const struct chimera_nfs_export  *snapshot)
{
    const struct chimera_nfs_export *current = shared->exports_by_id[snapshot->id];

    return current && !strcmp(current->name, snapshot->name) && !strcmp(current->path, snapshot->path) &&
           current->access == snapshot->access && current->squash == snapshot->squash &&
           current->anonuid == snapshot->anonuid && current->anongid == snapshot->anongid &&
           current->sec_allowed == snapshot->sec_allowed;
} /* nfs4_vfs_export_matches */

static struct nfs4_vfs_identity *
nfs4_vfs_identity_get(
    struct nfs4_vfs_compound_ctx *ctx,
    uint16_t                      id)
{
    struct chimera_server_nfs_shared *shared = ctx->req->thread->shared;
    struct nfs4_vfs_identity         *identity;

    for (identity = ctx->identities; identity; identity = identity->next) {
        if (identity->export.id == id) {
            return identity;
        }
    }
    identity = calloc(1, sizeof(*identity));
    if (!identity) {
        return NULL;
    }
    identity->export.id = id;
    identity->cred      = ctx->req->orig_cred;
    evpl_mutex_lock(&shared->exports_lock);
    if (shared->exports_by_id[id]) {
        identity->present = true;
        identity->export  = *shared->exports_by_id[id];
        chimera_nfs_squash_cred(&identity->cred, &identity->export);
    }
    evpl_mutex_unlock(&shared->exports_lock);
    identity->next  = ctx->identities;
    ctx->identities = identity;
    return identity;
} /* nfs4_vfs_identity_get */

static void
nfs4_vfs_identity_apply(
    struct nfs_request             *req,
    const struct nfs4_vfs_identity *identity)
{
    if (identity) {
        req->export_id = identity->export.id;
        req->cred      = identity->cred;
    }
} /* nfs4_vfs_identity_apply */

static uint16_t
nfs4_vfs_export_id(const struct nfs4_vfs_op *map)
{
    return map->identity ? map->identity->export.id : map->ctx->req->export_id;
} /* nfs4_vfs_export_id */

/* Root resolution and export entry use the same path encoding. */
static int
nfs4_vfs_export_path(
    struct chimera_vfs_compound *compound,
    const char                  *path,
    bool                         follow)
{
    while (*path == '/') {
        path++;
    }
    return *path ? chimera_vfs_compound_add_lookup_path(compound, path, strlen(path),
                                                        CHIMERA_VFS_ATTR_FH,
                                                        follow ? CHIMERA_VFS_LOOKUP_FOLLOW : 0) :
           chimera_vfs_compound_add_getfh(compound);
} /* nfs4_vfs_export_path */

/* Caller holds exports_lock; never publish a result under a reused export id. */
static bool
nfs4_vfs_root_publish(
    struct nfs4_vfs_compound_ctx         *ctx,
    const struct chimera_nfs_export      *root,
    const struct chimera_vfs_compound_op *op)
{
    struct chimera_server_nfs_shared *shared = ctx->req->thread->shared;

    if (shared->root_export_id != root->id ||
        !nfs4_vfs_export_matches(shared, root)) {
        return false;
    }
    if (op && op->fh_len) {
        memcpy(shared->root_export_fh, op->fh, op->fh_len);
        shared->root_export_fh_len = op->fh_len;
        shared->root_export_fh_id  = root->id;
    }
    return true;
} /* nfs4_vfs_root_publish */

static void
nfs4_vfs_root_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx     *ctx    = private_data;
    struct chimera_server_nfs_shared *shared = ctx->req->thread->shared;

    const struct chimera_nfs_export  *root = ctx->root_resolve ? ctx->entry_export : ctx->namespace_root;

    evpl_mutex_lock(&shared->exports_lock);
    bool                              current = shared->root_export_id == root->id &&
        nfs4_vfs_export_matches(shared, root);
    if (current && !ctx->root_resolve && index == 0 && shared->root_export_fh_id == root->id) {
        ctx->namespace_fhlen = shared->root_export_fh_len;
        memcpy(ctx->namespace_fh, shared->root_export_fh, ctx->namespace_fhlen);
    }
    evpl_mutex_unlock(&shared->exports_lock);
    if (!current) {
        *status = CHIMERA_VFS_EAGAIN;
        if (!ctx->root_resolve) {
            ctx->ops[0].verify_status = NFS4ERR_DELAY;
        }
    } else if (!ctx->root_resolve && ctx->namespace_fhlen) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_root_prepare */

static void
nfs4_vfs_root_probe_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx         *ctx = private_data;
    const struct chimera_vfs_compound_op *op  = chimera_vfs_compound_op(compound, index);

    if (*status == CHIMERA_VFS_OK && !op->skipped) {
        ctx->namespace_fhlen = op->fh_len;
        memcpy(ctx->namespace_fh, op->fh, op->fh_len);
    }
    /* An unavailable root view does not prevent ordinary namespace work.
     * LOOKUPP from a mount root diagnoses the missing parent at its own gate.
     * Policy changes must still fail the attempt before any wire op runs. */
    if (*status != CHIMERA_VFS_EAGAIN) {
        *status = CHIMERA_VFS_OK;
    } else {
        ctx->ops[0].verify_status = NFS4ERR_DELAY;
    }
} /* nfs4_vfs_root_probe_complete */

/* The synthetic marker belongs only to the NFS cursor. Backend operations
 * always receive the real base handle, including PUTFH and saved cursors. */
static bool
nfs4_vfs_cursor_base(
    struct chimera_server_nfs_thread *thread,
    uint8_t                          *fh,
    int                              *fh_len)
{
    if (chimera_nfs4_fh_is_attrdir(fh, *fh_len)) {
        *fh_len -= CHIMERA_NFS4_ATTRDIR_MAGIC_LEN;
        memmove(fh, fh + CHIMERA_NFS4_ATTRDIR_MAGIC_LEN, *fh_len);
    }
    return chimera_vfs_fh_is_plausible(thread->vfs_thread, fh, *fh_len);
} /* nfs4_vfs_cursor_base */

static int
nfs4_vfs_cursor_fh(
    bool           attrdir,
    bool           pseudo,
    uint8_t       *out,
    const uint8_t *base,
    int            base_len)
{
    if (pseudo) {
        uint32_t length;
        nfs4_root_get_fh(out, &length);
        return length;
    }
    if (attrdir) {
        return chimera_nfs4_make_attrdir_fh(out, base, base_len);
    }
    memcpy(out, base, base_len);
    return base_len;
} /* nfs4_vfs_cursor_fh */

/* State belongs to the protocol object, not the backend cursor used to
 * implement its synthetic directory. In particular, an attribute directory
 * must never authorize or retire its base inode's OPEN, delegation or layout. */
static const uint8_t *
nfs4_vfs_state_fh(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    uint32_t                    *length)
{
    if (map->pseudo || map->attrdir) {
        *length = map->ctx->fh_consumed ? 0 : map->ctx->fh_len;
        return *length ? map->ctx->fh : NULL;
    }
    return chimera_vfs_compound_current_fh(compound, length);
} /* nfs4_vfs_state_fh */

static void
nfs4_vfs_attrdir_attrs(struct chimera_vfs_attrs *attr)
{
    attr->va_mode  = S_IFDIR | 0755;
    attr->va_nlink = 2;
    attr->va_size  = 0;
    /* GETATTR may include the backend's base FH. Let the marshaller insert
     * the synthetic cursor instead of returning that different object. */
    attr->va_set_mask &= ~CHIMERA_VFS_ATTR_FH;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_NLINK | CHIMERA_VFS_ATTR_SIZE;
} /* nfs4_vfs_attrdir_attrs */

static void
nfs4_vfs_dispose(
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx)
{
    nfs4_change_finish(ctx->req->thread->shared->nfs4_state_table.change_table,
                       &ctx->req->change_observations, ctx->accepted);
    chimera_vfs_compound_free(compound);
    for (uint32_t i = 0; i < ctx->num_retirements; i++) {
        nfs_delegation_retirement_finish(&ctx->retirements[i], false,
                                         &ctx->req->thread->shared->nfs4_state_table, ctx->req->thread->vfs_thread);
    }
    nfs_layout_journal_finish(&ctx->layouts, false);
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
        free(entry->layout_view.excluded);
        if (entry->namespace_file) {
            if (entry->stream_holder) {
                chimera_vfs_state_stream_holder_dec(entry->namespace_file);
            }
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
        if (map->retirement_client) {
            nfs_client_finish_compound(map->retirement_client, &ctx->req->thread->shared->nfs4_state_table,
                                       ctx->req->thread->vfs_thread);
        }
        nfs4_layoutget_free(map->layoutget);
        free(map->input_acl);
        free(map->downgrade_scratch);
        free(map->open_response);
        free(map->close_response);
        nfs4_root_readdir_free(map->root_readdir);
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
    free(ctx->states);
    free(ctx->locks);
    free(ctx->retired_files);
    free(ctx->lock_groups);
    free(ctx->range_previous);
    free(ctx->range_current);
    for (uint32_t i = 0; i < ctx->num_groups; i++) {
        nfs_open_owner_finish_compound(ctx->groups[i],
                                       &ctx->req->thread->shared->nfs4_state_table, ctx->req->thread->vfs_thread);
    }
    free(ctx->groups);
    for (uint32_t i = 0; i < ctx->num_io_clients; i++) {
        nfs_client_finish_compound(ctx->io_clients[i], &ctx->req->thread->shared->nfs4_state_table,
                                   ctx->req->thread->vfs_thread);
    }
    if (ctx->client) {
        nfs_client_finish_compound(ctx->client, &ctx->req->thread->shared->nfs4_state_table,
                                   ctx->req->thread->vfs_thread);
    }
    /* Owner journals must release their states before dropping these client
    * pins; otherwise the last pin could start client teardown too early. */
    for (uint32_t i = 0; i < NFS4_VFS_COMPOUND_MAX_OPS; i++) {
        struct nfs4_vfs_op *map = &ctx->ops[i];
        if (map->owner_client) {
            nfs_client_finish_compound(map->owner_client, &ctx->req->thread->shared->nfs4_state_table,
                                       ctx->req->thread->vfs_thread);
        }
        if (map->owner_session) {
            nfs4_session_put(map->owner_session);
        }
    }
    while (ctx->identities) {
        struct nfs4_vfs_identity *identity = ctx->identities;
        ctx->identities = identity->next;
        free(identity);
    }
    free(ctx->entry_export);
    free(ctx->namespace_root);
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
        if (entry->retired || entry->parent->closed) {
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
    struct nfs_state_table         *table       = &ctx->req->thread->shared->nfs4_state_table;
    struct chimera_vfs_thread      *thread      = ctx->req->thread->vfs_thread;
    struct chimera_vfs_file_state **retired     = ctx->retired_files;
    uint32_t                        num_retired = 0;

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
        if (entry->active && entry->parent && (entry->retired || entry->parent->closed)) {
            struct chimera_vfs_file_state *file = nfs_lock_range_journal_retire(entry->ranges, thread);
            if (file) {
                retired[num_retired++] = file;
            }
        }
    }

    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock *entry = &ctx->locks[i];
        if (entry->active && entry->retired && entry->initially_public) {
            nfs_lock_state_destroy(entry->target, table, thread);
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
            if (entry->stream_guard && !entry->target->base_stream_file_state) {
                struct nfs4_vfs_coordination *guard = entry->stream_guard;
                chimera_nfs_abort_if(!guard || !guard->namespace_file, "stream OPEN lost its base guard");
                entry->target->base_stream_file_state = guard->namespace_file;
                guard->namespace_file                 = NULL;
            }
            nfs_open_owner_apply_compound(entry->group, entry->target, &update, table, thread);
        } else if (entry->version_dirty) {
            nfs_open_owner_confirm_compound(entry->group, entry->target, state->seqid);
        }
    }
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock           *entry = &ctx->locks[i];
        if (!entry->active || entry->retired || !entry->dirty || (entry->parent && entry->parent->closed)) {
            continue;
        }
        chimera_nfs_abort_if(!entry->parent || entry->parent->closed,
                             "accepted lock has no live parent");
        struct chimera_vfs_open_handle *handle = entry->parent->shadow.handle;
        chimera_vfs_dup_handle(thread, handle);
        nfs_lock_state_apply_compound(entry->group, entry->target, entry->parent->target,
                                      handle, entry->seqid, entry->ranges, table, thread);
    }
    for (uint32_t i = 0; i < ctx->num_lock_groups; i++) {
        if (ctx->lock_groups[i]->release_pending) {
            nfs_lock_owner_release_compound(ctx->lock_groups[i]);
        }
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

/* Only construction grows these pointer arrays. Reservation objects and
 * state identities stay put; operation maps acquire journal pointers only
 * after the complete connected owner set has been frozen. */
static bool
nfs4_vfs_owner_slot(struct nfs4_vfs_compound_ctx *ctx)
{
    if (ctx->num_groups < ctx->group_capacity) {
        return true;
    }
    uint32_t                            capacity = ctx->group_capacity ? ctx->group_capacity * 2 : 16;
    if (capacity < ctx->group_capacity) {
        return false;
    }
    struct nfs_open_owner_reservation **groups = realloc(ctx->groups, (size_t) capacity * sizeof(*groups));
    if (!groups) {
        return false;
    }
    ctx->groups         = groups;
    ctx->group_capacity = capacity;
    return true;
} /* nfs4_vfs_owner_slot */

static bool
nfs4_vfs_lock_owner_slot(struct nfs4_vfs_compound_ctx *ctx)
{
    if (ctx->num_lock_groups < ctx->lock_group_capacity) {
        return true;
    }
    uint32_t                            capacity = ctx->lock_group_capacity ? ctx->lock_group_capacity * 2 : 16;
    if (capacity < ctx->lock_group_capacity) {
        return false;
    }
    struct nfs_lock_owner_reservation **groups = realloc(ctx->lock_groups, (size_t) capacity * sizeof(*groups));
    if (!groups) {
        return false;
    }
    ctx->lock_groups         = groups;
    ctx->lock_group_capacity = capacity;
    return true;
} /* nfs4_vfs_lock_owner_slot */

static nfsstat4
nfs4_vfs_freeze_lock_parent(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs_client            *client,
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
    if (!nfs4_vfs_owner_slot(ctx)) {
        return NFS4ERR_RESOURCE;
    }
    status = nfs_state_table_acquire_no_renew(table, sid, 0, &state, &type);
    if (status != NFS4_OK) {
        return status;
    }
    status = nfs_state_check_client(state, type, client);
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
    if (!req->minorversion) {
        nfsstat4 principal_error;
        status = nfs_open_owner_reserve_compound_replay(client, owner, owner_len,
                                                        req->principal_flavor, req->principal_machinename,
                                                        req->principal_machinename_len, ctx, 0, table,
                                                        req->thread->vfs_thread, &group, &principal_error);
        /* Each state checkpoint validates its own immutable principal after
         * replay classification; reservation must not bypass consuming errors. */
    } else {
        status = nfs_open_owner_reserve_compound_locks(client, owner, owner_len,
                                                       req->principal_flavor, req->principal_machinename,
                                                       req->principal_machinename_len, ctx, 0, table,
                                                       req->thread->vfs_thread, &group);
    }
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
    struct nfs_client            *client,
    const uint8_t                *owner,
    uint32_t                      owner_len)
{
    struct nfs_request *req = ctx->req;
    struct stateid4     parents[NFS4_VFS_COMPOUND_MAX_OPS];
    uint32_t            count;

    if (owner_len > NFS4_OPAQUE_LIMIT) {
        return 0;
    }
    for (;;) {
        nfsstat4 status = nfs_lock_owner_compound_parents(client, owner, owner_len,
                                                          ctx, &req->thread->shared->nfs4_state_table,
                                                          parents, NFS4_VFS_COMPOUND_MAX_OPS, &count);
        if (status != NFS4_OK && (status != NFS4ERR_RESOURCE || !count)) {
            return 0;
        }
        /* RESOURCE with a full batch means more parents remain. Freezing
         * this batch removes them from the next snapshot, so large standing
         * owners do not depend on the current wire-operation limit. */
        for (uint32_t i = 0; i < count; i++) {
            if (nfs4_vfs_freeze_lock_parent(ctx, client, &parents[i]) != NFS4_OK ||
                !nfs4_vfs_build_reset(compound, ctx)) {
                return 0;
            }
        }
        if (status == NFS4_OK) {
            return 1;
        }
    }
} /* nfs4_vfs_expand_lock_parents */

static nfsstat4
nfs4_vfs_reserve_lock_owner(
    struct nfs4_vfs_compound_ctx       *ctx,
    struct nfs_client                  *client,
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
        if (lo->client == client && lo->owner_len == owner_len && !memcmp(lo->owner, owner, owner_len)) {
            *out = ctx->lock_groups[i];
            return NFS4_OK;
        }
    }
    for (uint32_t i = first; i < end; i++) {
        const struct nfs_argop4 *op = &req->args_compound->argarray[i];
        if (op->argop == OP_LOCK && op->oplock.locker.new_lock_owner &&
            nfs4_vfs_owner_client(ctx, &ctx->ops[i - first]) == client &&
            op->oplock.locker.open_owner.lock_owner.owner.len == owner_len &&
            !memcmp(op->oplock.locker.open_owner.lock_owner.owner.data, owner, owner_len)) {
            candidates++;
        }
    }
    if (owner_len > NFS4_OPAQUE_LIMIT || !nfs4_vfs_lock_owner_slot(ctx)) {
        return NFS4ERR_RESOURCE;
    }
    nfsstat4 status = nfs_lock_owner_reserve_compound(client,
                                                      owner, owner_len, ctx, candidates, &req->thread->shared->
                                                      nfs4_state_table,
                                                      req->thread->vfs_thread, out);
    if (status == NFS4_OK) {
        ctx->lock_groups[ctx->num_lock_groups++] = *out;
    }
    return status;
} /* nfs4_vfs_reserve_lock_owner */

static bool nfs4_vfs_data_op(
    uint32_t opcode);

/* Preserve the synthetic-object statuses of standalone dispatch without
 * allowing any backend helper to address the attribute-directory base inode. */
static nfsstat4
nfs4_vfs_synthetic_status(
    uint32_t opcode,
    bool     pseudo,
    bool     attrdir)
{
    if (!pseudo && !attrdir) {
        return NFS4_OK;
    }
    if (opcode == OP_LAYOUTCOMMIT) {
        return NFS4ERR_BAD_STATEID;
    }
    if (opcode == OP_OPENATTR) {
        return NFS4ERR_NOTSUPP;
    }
    if ((opcode == OP_REMOVE && pseudo) ||
        (attrdir && (opcode == OP_ACCESS || opcode == OP_SECINFO)) ||
        opcode == OP_READLINK || opcode == OP_VERIFY || opcode == OP_NVERIFY ||
        opcode == OP_GETXATTR || opcode == OP_SETXATTR || opcode == OP_LISTXATTRS || opcode == OP_REMOVEXATTR ||
        opcode == OP_COMMIT || opcode == OP_CREATE || opcode == OP_LINK || opcode == OP_RENAME ||
        opcode == OP_LOCKT || (opcode == OP_LOOKUPP && attrdir) || opcode == OP_COPY || opcode == OP_CLONE ||
        nfs4_vfs_data_op(opcode)) {
        return NFS4ERR_STALE;
    }
    return NFS4_OK;
} /* nfs4_vfs_synthetic_status */

static bool
nfs4_vfs_protocol_op(uint32_t opcode)
{
    return opcode == OP_GETDEVICEINFO || opcode == OP_GETDEVICELIST ||
           opcode == OP_LAYOUTSTATS || opcode == OP_LAYOUTERROR || opcode == OP_DELEGPURGE;
} /* nfs4_vfs_protocol_op */

/* These protocol state operations neither consume nor establish a filehandle. */
static bool
nfs4_vfs_retirement_handle_free(uint32_t opcode)
{
    return opcode == OP_FREE_STATEID || opcode == OP_RELEASE_LOCKOWNER || opcode == OP_LAYOUTRETURN;
} /* nfs4_vfs_retirement_handle_free */

static int
nfs4_vfs_op_encodable(uint32_t argop)
{
    if (nfs4_vfs_protocol_op(argop)) {
        return 1;
    }
    switch (argop) {
        case OP_PUTROOTFH:
        case OP_PUTPUBFH:
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
        case OP_LAYOUTCOMMIT:
        case OP_LAYOUTGET:
        case OP_READDIR:
        case OP_GETXATTR:
        case OP_SETXATTR:
        case OP_LISTXATTRS:
        case OP_REMOVEXATTR:
        case OP_OPEN:
        case OP_OPENATTR:
        case OP_CLOSE:
        case OP_OPEN_CONFIRM:
        case OP_OPEN_DOWNGRADE:
        case OP_DELEGRETURN:
        case OP_FREE_STATEID:
        case OP_RELEASE_LOCKOWNER:
        case OP_LAYOUTRETURN:
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

/* Static scratch/result reservations. Variable payloads are staged at their
 * execution checkpoint and charged at actual size before any successor runs.
 * Names and iovec descriptors allocated during construction remain bounded
 * here; the attempt budget subtracts those bytes once they have been spent. */
static uint64_t
nfs4_vfs_op_reply_bound(const struct nfs_argop4 *argop)
{
    /* Slack for the small fixed allocations around each result -- attrmask
     * arrays, opaque headers, and xdr_dbuf_alloc_space's own rounding. */
    const uint64_t slack = 512;

    switch (argop->argop) {
        case OP_PUTFH:
            /* Its result is entirely in the already allocated response slot. */
            return 0;
        case OP_GETATTR:
            /* The entire variable result is charged during execution. */
            return 0;
        case OP_LAYOUTGET:
            /* lg_encode emits at most eight flex layouts. Each has 84 fixed
             * XDR bytes plus a padded FH, and one layout4 descriptor. The
             * single block/SCSI layout (4 + 44 bytes per segment) is smaller. */
            return CHIMERA_VFS_LAYOUT_MAX_SEGMENTS *
                   (sizeof(struct layout4) + ((84 + CHIMERA_NFS_FH_MAX + 7) & ~7U));
        case OP_GETDEVICEINFO:
            return 1024 + slack;
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
            return sizeof(struct evpl_iovec) * CHIMERA_NFS4_READ_MAX_IOV + slack;
        case OP_READ_PLUS:
            return sizeof(struct evpl_iovec) * CHIMERA_NFS4_READ_MAX_IOV;
        case OP_READDIR:
        case OP_LISTXATTRS:
            return 0;
        case OP_GETXATTR:
            /* Only the qualified input name exists before execution staging. */
            return ((uint64_t) CHIMERA_VFS_XATTR_USER_PREFIX_LEN +
                    argop->opgetxattr.gxa_name.len + 7) & ~UINT64_C(7);
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

/* Reset this enumeration before its successors run. A whole-compound retry
 * rewinds the attempt arena and clears each mark, including staged GETATTRs
 * and any other READDIR pages. */
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
        ctx->req, nfs4_vfs_export_id(map),
        &ctx->req->args_compound->argarray[map->res_index].opreaddir,
        &map->readdir_cursor,
        vop->fh, (int) vop->fh_len,
        cookie, name, namelen, attrs);
} /* nfs4_vfs_readdir_append */

/* Stage one backend page before successors execute. The frontend may fit
 * fewer entries than the backend page, so each returned entry carries its own
 * continuation cookie. No whole-directory snapshot is retained or replayed. */
static nfsstat4
nfs4_vfs_readdir_attrdir(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx         *ctx  = map->ctx;
    struct nfs_request                   *req  = ctx->req;
    struct READDIR4args                  *args = &req->args_compound->argarray[map->res_index].opreaddir;
    const struct chimera_vfs_attrs       *base = &chimera_vfs_compound_op(compound, map->vfs_aux)->attr;
    const struct chimera_vfs_compound_op *list = chimera_vfs_compound_op(compound, map->vfs_res);
    uint32_t                              in = 0, count = 0;
    uint64_t                              cookie    = args->cookie;
    bool                                  page_full = false;

    nfs4_vfs_readdir_reset(compound, map->vfs_res, ctx);
    map->readdir_eof      = list->eof;
    map->readdir_verifier = list->r_verifier;
    if (!list->buffer_count && list->eof && args->cookie > list->r_cookie) {
        return NFS4ERR_BAD_COOKIE;
    }
    if (list->buffer_len > list->buffer_max || (list->buffer_len && !list->buffer)) {
        return NFS4ERR_IO;
    }
    while (in < list->buffer_len) {
        struct chimera_vfs_stream_entry entry;
        if (list->buffer_len - in < sizeof(entry)) {
            return NFS4ERR_IO;
        }
        memcpy(&entry, (const uint8_t *) list->buffer + in, sizeof(entry));
        uint64_t                        reclen = (sizeof(entry) + (uint64_t) entry.name_len + entry.fh_len + 7) & ~7ULL;
        if (reclen > list->buffer_len - in || entry.fh_len > CHIMERA_VFS_FH_SIZE) {
            return NFS4ERR_IO;
        }
        const char                     *name = (const char *) list->buffer + in + sizeof(entry);
        const uint8_t                  *fh   = (const uint8_t *) name + entry.name_len;
        in += (uint32_t) reclen;
        count++;
        if (entry.cookie < 3 || entry.cookie <= cookie) {
            return NFS4ERR_IO;
        }
        cookie = entry.cookie;
        if (!entry.name_len) {
            continue; /* The default data fork is not a named attribute. */
        }
        /* want_fh requires stream identity, never the base's inherited FH. */
        if (!entry.fh_len) {
            return NFS4ERR_IO;
        }
        if (page_full) {
            continue;
        }
        struct chimera_vfs_attrs attr = *base;
        attr.va_size       = entry.size;
        attr.va_space_used = entry.alloc;
        memcpy(attr.va_fh, fh, entry.fh_len);
        attr.va_fh_len    = entry.fh_len;
        attr.va_ino       = chimera_vfs_hash(fh, entry.fh_len);
        attr.va_set_mask |= CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_SPACE_USED |
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_INUM;
        if (chimera_nfs4_readdir_entry_fill(req, nfs4_vfs_export_id(map), args, &map->readdir_cursor, ctx->fh, ctx->
                                            fh_len,
                                            cookie, name, entry.name_len, &attr) != 0) {
            map->readdir_eof = false;
            page_full        = true;
        }
    }
    if (count != list->buffer_count || (count && cookie != list->r_cookie)) {
        return NFS4ERR_IO;
    }
    return NFS4_OK;
} /* nfs4_vfs_readdir_attrdir */

/* Publish an already validated page only after accepted compound finish. */
static void
nfs4_vfs_readdir_fill(
    struct READDIR4res                   *res,
    const struct chimera_vfs_compound_op *vop,
    const struct nfs4_vfs_op             *map)
{
    uint64_t cv;

    if (map->root_readdir && map->pseudo) {
        nfs4_root_readdir_fill(map->root_readdir, res);
        return;
    }

    /* The too-small judgement (RFC 7530 16.24.4) is the gate's: a READDIR that
     * reaches here passed it. */
    cv = map->attrdir ? map->readdir_verifier : (vop->r_verifier ? vop->r_verifier : vop->r_cookie);
    memcpy(res->resok4.cookieverf, &cv, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof     = map->attrdir ? map->readdir_eof : vop->eof;
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
 * Map a failed VFS op onto its operation-specific NFSv4 status. Handle
 * installation, enumeration limits and type checks have their own errors.
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
    if (argop == OP_PUTROOTFH || argop == OP_PUTPUBFH) {
        /* A configured root whose backend path cannot be resolved is a
         * server configuration failure, never a synthetic-root fallback. */
        return NFS4ERR_SERVERFAULT;
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
    uint8_t                               fh[NFS4_FHSIZE];
    int                                   fh_len = map->result_fhlen;

    memcpy(fh, map->result_fh, fh_len);

    if (nfs4_vfs_protocol_op(argop->argop)) {
        if (argop->argop == OP_GETDEVICEINFO) {
            resop->opgetdeviceinfo = map->device_result;
        } else {
            resop->opillegal.status = NFS4_OK;
        }
        return NFS4_OK;
    }

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
            struct OPEN4args                     *oargs  = &argop->opopen;
            struct OPEN4res                      *ores   = &resop->opopen;
            const struct chimera_vfs_compound_op *lookup = map->open_deleg_lookup ?
                chimera_vfs_compound_op(compound, map->open_deleg_lookup) : NULL;
            req->index = (int) map->res_index;
            memcpy(req->fh, vop->fh, vop->fh_len);
            req->fhlen           = vop->fh_len;
            ores->resok4.stateid = map->reserved_stateid;
            chimera_nfs4_set_current_stateid(req, &map->reserved_stateid);
            ores->status                            = NFS4_OK;
            ores->resok4.rflags                     = OPEN4_RESULT_LOCKTYPE_POSIX;
            ores->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;
            ores->resok4.num_attrset                = 0;

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

                int rc = xdr_dbuf_alloc_array(&ores->resok4, attrset, 4,
                                              req->encoding->dbuf);
                chimera_nfs_abort_if(rc, "Failed to allocate array");

                ores->resok4.num_attrset = chimera_nfs4_mask2attr(
                    &applied, n_mask, mask, ores->resok4.attrset);
            }

            if (map->open_by_name && !map->open_stream) {
                struct chimera_vfs_attrs pre  = lookup ? lookup->dir_post_attr : vop->dir_pre_attr;
                struct chimera_vfs_attrs post = lookup ? lookup->dir_post_attr : vop->dir_post_attr;

                chimera_nfs4_set_changeinfo(&ores->resok4.cinfo, &pre, &post);
            } else {
                /* An open-by-handle changed no directory. */
                ores->resok4.cinfo.atomic = 0;
                ores->resok4.cinfo.before = 0;
                ores->resok4.cinfo.after  = 0;
            }

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

        case OP_DELEGRETURN:
            resop->opdelegreturn.status = NFS4_OK;
            return NFS4_OK;
        case OP_FREE_STATEID:
            resop->opfree_stateid.fsr_status = NFS4_OK;
            return NFS4_OK;
        case OP_RELEASE_LOCKOWNER:
            resop->oprelease_lockowner.status = NFS4_OK;
            return NFS4_OK;
        case OP_LAYOUTRETURN:
            resop->oplayoutreturn.lorr_status              = NFS4_OK;
            resop->oplayoutreturn.lorr_stateid.lrs_present = 0;
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
            chimera_nfs_abort_if(!map->reply_staged, "unstaged READ_PLUS result");
            resop->opread_plus = map->variable_result.read_plus;
            return resop->opread_plus.rp_status;

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

            export = map->secinfo_junction ? &map->secinfo_export->export :
                map->identity ? &map->identity->export : NULL;

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

        case OP_LAYOUTGET:
        {
            const struct LAYOUTGET4res *result = nfs4_layoutget_result(map->layoutget);
            chimera_nfs_abort_if(!result, "accepted LAYOUTGET has no private reply");
            resop->oplayoutget = *result;
            return NFS4_OK;
        }

        case OP_LAYOUTCOMMIT:
        {
            struct LAYOUTCOMMIT4res *res = &resop->oplayoutcommit;
            res->locr_status                             = NFS4_OK;
            res->locr_resok4.locr_newsize.ns_sizechanged = vop->status == CHIMERA_VFS_OK &&
                (vop->applied_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE);
            res->locr_resok4.locr_newsize.ns_size =
                (vop->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? vop->attr.va_size : 0;
            return NFS4_OK;
        }

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
            /* Streams have no backend directory change pair. Preserve the
             * synthetic directory's non-atomic, zero change_info. */
            if (!map->attrdir) {
                chimera_nfs4_set_changeinfo(&rres->resok4.cinfo, &pre, &post);
            }
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

        case OP_PUTROOTFH:
        case OP_PUTPUBFH:
        {
            struct chimera_server_nfs_shared *shared = req->thread->shared;
            /* Earlier roots may have been replaced inside the accepted span.
             * Publish only its last successful root resolution. */
            if (map == ctx->last_root) {
                evpl_mutex_lock(&shared->exports_lock);
                nfs4_vfs_root_publish(ctx, ctx->namespace_root, vop);
                evpl_mutex_unlock(&shared->exports_lock);
            }
            resop->opillegal.status = NFS4_OK;
            return NFS4_OK;
        }

        case OP_PUTFH:
            /* The staleness rule already ran as the precheck; nothing else in
             * a PUTFH4res but its status. */
            resop->opputfh.status = NFS4_OK;
            return NFS4_OK;

        case OP_OPENATTR:
            resop->opopenattr.status = NFS4_OK;
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
            chimera_nfs4_savefh_apply(req, fh, fh_len);
            resop->opsavefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_RESTOREFH:
            /* The accepted cursor and its frozen identity publish together below. */
            resop->oprestorefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_COMMIT:
            /* The regular-file rule already ran as the precheck; this re-tests
             * it against the attributes the flush itself reported, as the
             * per-op completion does, and stamps the write verifier. */
            status = chimera_nfs4_commit_fill(req, &resop->opcommit,
                                              &vop->pre_attr);
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
            chimera_nfs_abort_if(!map->reply_staged, "unstaged GETXATTR result");
            resop->opgetxattr = map->variable_result.getxattr;
            return resop->opgetxattr.gxr_status;

        case OP_SETXATTR:
            chimera_nfs4_setxattr_fill(&resop->opsetxattr, &vop->pre_ctime,
                                       &vop->post_ctime);
            resop->opsetxattr.sxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_LISTXATTRS:
            chimera_nfs_abort_if(!map->reply_staged, "unstaged LISTXATTRS result");
            resop->oplistxattrs = map->variable_result.listxattrs;
            return resop->oplistxattrs.lxr_status;

        case OP_REMOVEXATTR:
            chimera_nfs4_removexattr_fill(&resop->opremovexattr,
                                          &vop->pre_ctime, &vop->post_ctime);
            resop->opremovexattr.rxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_GETATTR:
        {
            if (map->getattr_staged) {
                resop->opgetattr = map->getattr_result;
                return resop->opgetattr.status;
            }
            struct chimera_vfs_attrs attr = vop->attr;
            if (map->attrdir) {
                nfs4_vfs_attrdir_attrs(&attr);
            }
            status = chimera_nfs4_getattr_fill(req, nfs4_vfs_export_id(map), &argop->opgetattr,
                                               &resop->opgetattr, &attr,
                                               fh, fh_len, false);
            resop->opgetattr.status = status;
            return status;
        }

        case OP_ACCESS:
            if (map->pseudo) {
                resop->opaccess.status           = NFS4_OK;
                resop->opaccess.resok4.supported = argop->opaccess.access & chimera_nfs4_access_meaningful(1, 0);
                resop->opaccess.resok4.access    = resop->opaccess.resok4.supported &
                    ~(ACCESS4_MODIFY | ACCESS4_EXTEND | ACCESS4_DELETE);
                return NFS4_OK;
            }
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
                                             fh, fh_len);
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
 * time. Operations outside this run retain their payloads for dispatch.
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
    for (uint32_t i = 0; i < ctx->num_retirements; i++) {
        struct nfs_delegation_retirement *entry = &ctx->retirements[i];
        if (entry->retired && entry->deleg->lease_held) {
            nfs4_vfs_exclude_claim(ctx, &entry->deleg->claim);
        }
    }
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
    const struct chimera_vfs_attrs       *attr  = map->open_deleg_lookup ?
        &chimera_vfs_compound_op(compound, map->open_deleg_lookup)->attr : &op->attr;

    state->share_access = oa->share_access & OPEN4_SHARE_ACCESS_BOTH;
    state->share_deny   = oa->share_deny;
    state->share_combos = nfs_open_combo_bit(state->share_access, state->share_deny);

    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if ((op->existed || map->open_deleg_lookup) && nfs4_vfs_open_is_exclusive(oa)) {
        if (!nfs4_vfs_open_verifier_matches(oa, attr)) {
            error = NFS4ERR_EXIST;
        }
    } else if (!map->open_stream && (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
               !S_ISREG(attr->va_mode)) {
        error = chimera_nfs4_open_nonreg_status(req->minorversion, attr->va_mode);
    }
    if (error == NFS4_OK && map->open_by_name && !op->created) {
        uint32_t required = 0;
        if (state->share_access & OPEN4_SHARE_ACCESS_READ) {
            required |= CHIMERA_ACE_READ_DATA;
        }
        if (state->share_access & OPEN4_SHARE_ACCESS_WRITE) {
            required |= CHIMERA_ACE_WRITE_DATA;
        }
        if (!chimera_vfs_access_allowed(attr, map->identity ? &map->identity->cred : &req->cred, required)) {
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
        /* Preserve the same-owner self-conflict rule. */
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
        chimera_nfs_abort_if(ctx->num_states >= ctx->state_capacity,
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

/* Validate against the object resolved in this attempt, without renewing the
 * delegation or publishing owner state. A named claim subsequently opens this
 * filehandle, so a rename cannot redirect it to an unchecked replacement. */
static void
nfs4_vfs_open_delegated_checked(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                   *map  = private_data;
    struct nfs_request                   *req  = map->ctx->req;
    const struct OPEN4args               *args = &req->args_compound->argarray[map->res_index].opopen;
    const struct chimera_vfs_compound_op *op   = chimera_vfs_compound_op(compound, index);
    struct chimera_claim_actor            actor;

    if (*status != CHIMERA_VFS_OK || map->replay_hit) {
        return;
    }
    map->verify_status = nfs4_vfs_stateid_closed(map->ctx, &args->claim.delegate_cur_info.delegate_stateid) ?
        NFS4ERR_BAD_STATEID : nfs_state_table_delegation_io(&req->thread->shared->nfs4_state_table,
                                                            &args->claim.delegate_cur_info.delegate_stateid, map->client
                                                            ,
                                                            op->fh, op->fh_len, args->share_access &
                                                            OPEN4_SHARE_ACCESS_BOTH,
                                                            &actor);
    if (map->verify_status == NFS4_OK && args->openhow.opentype == OPEN4_CREATE) {
        if (args->openhow.how.mode == GUARDED4 ||
            (nfs4_vfs_open_is_exclusive(args) && !nfs4_vfs_open_verifier_matches(args, &op->attr))) {
            map->verify_status = NFS4ERR_EXIST;
        }
    }
    if (map->verify_status != NFS4_OK) {
        *status = CHIMERA_VFS_EINVAL;
    }
} /* nfs4_vfs_open_delegated_checked */

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
    entry->active    = 1;
    entry->last_open = map;
    if (map->stream_guard) {
        entry->stream_guard = map->stream_guard;
    }
    map->open_committed             = 1;
    map->ctx->current_stateid       = map->reserved_stateid;
    map->ctx->current_stateid_valid = 1;
    nfs4_vfs_rebuild_state_view(map->ctx, NULL);
} /* nfs4_vfs_open_commit */

static bool
nfs4_vfs_open_will_truncate(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    const struct chimera_vfs_compound_op *open = chimera_vfs_compound_op(compound, map->vfs_res);

    return !map->replay_hit && map->open_trunc_if_existed &&
           (open->existed || map->open_deleg_lookup || (map->open_stream && !open->created));
} /* nfs4_vfs_open_will_truncate */

/* OPEN's deferred size=0 SETATTR must observe the same pNFS return/barrier
 * contract as a wire SETATTR. A fresh creation and a v4.0 replay do not
 * truncate, so neither can recall unrelated layouts or acquire a barrier. */
static void
nfs4_vfs_open_layout_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                   *map = private_data;

    if (!nfs4_vfs_open_will_truncate(compound, map)) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }
    const struct chimera_vfs_compound_op *open = chimera_vfs_compound_op(compound, map->vfs_res);
    if (nfs_layout_journal_truncate_conflict(&map->ctx->layouts, open->fh, open->fh_len)) {
        map->verify_status = NFS4ERR_DELAY;
        *status            = CHIMERA_VFS_EAGAIN;
    }
} /* nfs4_vfs_open_layout_prepare */

static void
nfs4_vfs_open_truncate_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op             *map = private_data;

    if (!nfs4_vfs_open_will_truncate(compound, map)) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    }

    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);

    (void) status;
    op->in_handle = map->reserved_open->handle;
    /* Truncation is the admitted opener's I/O. Carry its identity so the
     * claim layer does not recall this client's own delegation. */
    memset(&op->io_owner, 0, sizeof(op->io_owner));
    op->io_owner.owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    op->io_owner.owner.client_key = map->reserved_open->owner->client->client_id;
    op->io_owner.owner.owner_lo   = op->in_handle->fh_hash;
    op->have_io_owner             = 1;
    op->io_view.excluded          = map->ctx->closed_claims;
    op->io_view.num_excluded      = map->ctx->num_closed_claims;
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
    if (!args->length ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        map->verify_status = NFS4ERR_INVAL;
        *status            = CHIMERA_VFS_EINVAL;
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
    struct nfs4_vfs_coordination     *entry;

    if (!map->client) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
        return;
    }
    entry = calloc(1, sizeof(*entry));
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
        thread, fh, fh_len, map->client->client_id, &entry->holder_client);
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

/* A stream open must pin its base until publication (or disposal). This is
 * explicit coordination, memoized per operation and resolved base FH across
 * backend retries, rather than a side effect of a repeatable OPEN callout. */
static void
nfs4_vfs_stream_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct nfs4_vfs_op           *map   = private_data;
    struct nfs4_vfs_compound_ctx *ctx   = map->ctx;
    struct chimera_vfs_state     *state = ctx->req->thread->vfs->vfs_state;
    struct nfs4_vfs_coordination *entry = calloc(1, sizeof(*entry));

    if (!entry) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    entry->namespace_file = chimera_vfs_state_get(state, fh, fh_len, chimera_vfs_hash(fh, fh_len), true);
    if (!entry->namespace_file) {
        free(entry);
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    entry->index  = index;
    entry->fh_len = fh_len;
    memcpy(entry->fh, fh, fh_len);
    entry->stream_holder = true;
    chimera_vfs_state_stream_holder_inc(entry->namespace_file);
    entry->next       = ctx->coordination;
    ctx->coordination = entry;
    chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
} /* nfs4_vfs_stream_coordinate */

static void
nfs4_vfs_stream_coordinated(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                   *map = private_data;
    const struct chimera_vfs_compound_op *op  = chimera_vfs_compound_op(compound, index);

    if (*status != CHIMERA_VFS_OK || map->replay_hit) {
        if (*status == CHIMERA_VFS_ENOSPC) {
            map->verify_status = NFS4ERR_RESOURCE;
        }
        return;
    }
    for (struct nfs4_vfs_coordination *entry = map->ctx->coordination; entry; entry = entry->next) {
        if (entry->index == index && entry->fh_len == op->fh_len &&
            !memcmp(entry->fh, op->fh, op->fh_len)) {
            map->stream_guard = entry;
            return;
        }
    }
    map->verify_status = NFS4ERR_SERVERFAULT;
    *status            = CHIMERA_VFS_EIO;
} /* nfs4_vfs_stream_coordinated */

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

    if (!map->client) {
        return NFS4_OK;
    }
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
                                              map->client->client_id,
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

/* Keep the fixed completion work for every unstaged operation available.
* Execution-time payloads already charged at actual size are excluded from
* that reservation, as are allocations made by other executed helpers. */
static uint64_t
nfs4_vfs_reply_reserved(
    const struct nfs4_vfs_compound_ctx *ctx,
    uint32_t                            mark,
    uint64_t                            fixed)
{
    uint64_t consumed  = (uint64_t) mark - ctx->attempt_mark - ctx->reply_staged_bytes;
    uint64_t accounted = ctx->reply_staged_bound + fixed + consumed;

    return chimera_nfs4_reply_transport_bytes(ctx->req) +
           (accounted < ctx->reply_bound ? ctx->reply_bound - accounted : 0);
} /* nfs4_vfs_reply_reserved */

static bool
nfs4_vfs_reply_fits(
    const struct nfs4_vfs_compound_ctx *ctx,
    uint64_t                            reserved)
{
    /* reserved includes the generated adapter's transport array as well as
     * pending result storage. Both must fit before any successor can mutate. */
    return (uint64_t) ctx->req->encoding->dbuf->size - ctx->req->encoding->dbuf->used >= reserved;
} /* nfs4_vfs_reply_fits */

/* Stage the whole variable result while a resource error can still stop its
* successor. READDIR's streaming callbacks already built its compact page;
* the other operations copy their compound-owned data into the attempt arena
* here. Nothing is published, and reset invalidates every staged pointer. */
static void
nfs4_vfs_variable_complete(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    enum chimera_vfs_error      *status)
{
    struct nfs4_vfs_compound_ctx         *ctx   = map->ctx;
    struct nfs_request                   *req   = ctx->req;
    const struct nfs_argop4              *argop = &req->args_compound->argarray[map->res_index];
    const struct chimera_vfs_compound_op *vop   = chimera_vfs_compound_op(compound, map->vfs_res);
    uint32_t                              mark  = argop->argop == OP_READDIR ? map->reply_mark : req->encoding->dbuf->
        used;
    /* Construction scratch for GETXATTR/READ_PLUS has already left the
     * budget; their response payloads have no additional static reservation. */
    const uint64_t                        fixed    = 0;
    uint64_t                              reserved = nfs4_vfs_reply_reserved(ctx, mark, fixed);
    nfsstat4                              error    = NFS4_OK;

    switch (argop->argop) {
        case OP_READ:
            /* Each inline READ flushes the preceding XDR fields, even when
             * it returns no payload. The next flush includes its padding;
             * one final descriptor was reserved at wire request entry.
             * Count against the TCP shape on RDMA too: later READs can be
             * inline and the optional first write chunk can only save slots.
             * This attempt-local admission happens before a successor runs,
             * while the sequence still owns every READ buffer. */
            /* RFC 8267 assigns the single offered Write chunk to the first
             * successful READ, including an empty one. Do not let a short
             * chunk become a transport error after a later WRITE executes. */
            if (!ctx->reply_read_seen && req->encoding->write_chunk &&
                (req->encoding->write_chunk->max_length || req->encoding->write_chunk->num_segments) &&
                vop->read_len > req->encoding->write_chunk->max_length) {
                map->verify_status = NFS4ERR_RESOURCE;
                *status            = CHIMERA_VFS_ENOSPC;
                return;
            }
            if (vop->niov < 0 || (uint32_t) vop->niov >= CHIMERA_NFS4_REPLY_MAX_IOV ||
                ctx->reply_read_iov > CHIMERA_NFS4_REPLY_MAX_IOV - (uint32_t) vop->niov - 1) {
                map->verify_status = NFS4ERR_RESOURCE;
                *status            = CHIMERA_VFS_ENOSPC;
                return;
            }
            ctx->reply_read_iov += (uint32_t) vop->niov + 1;
            ctx->reply_read_seen = true;
            return;
        case OP_READDIR:
            break;
        case OP_READ_PLUS:
        {
            struct READ_PLUS4res                 *res    = &map->variable_result.read_plus;
            const struct chimera_vfs_compound_op *data   = chimera_vfs_compound_op(compound, map->read_plus_data);
            uint32_t                              length = vop->is_data ? data->read_len : vop->read_len;
            res->rp_status         = NFS4_OK;
            res->rp_resok4.rpr_eof = vop->is_data && vop->read_len ? data->eof_read : vop->eof_read;
            if (!length) {
                break;
            }
            struct read_plus_content             *content = xdr_dbuf_alloc_space(sizeof(*content), req->encoding->dbuf);
            if (!content) {
                error = NFS4ERR_RESOURCE;
                break;
            }
            memset(content, 0, sizeof(*content));
            if (vop->is_data) {
                struct evpl_iovec_cursor cursor;
                content->rpc_content       = NFS4_CONTENT_DATA;
                content->rpc_data.d_offset = argop->opread_plus.rpa_offset;
                if (xdr_dbuf_alloc_opaque(&content->rpc_data.d_data, length, req->encoding->dbuf)) {
                    error = NFS4ERR_RESOURCE;
                    break;
                }
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
            break;
        }
        case OP_GETXATTR:
            error = chimera_nfs4_getxattr_fill(req, &map->variable_result.getxattr,
                                               vop->buffer, vop->buffer_len);
            map->variable_result.getxattr.gxr_status = error;
            break;
        case OP_LISTXATTRS:
        {
            /* Result names point into this copy after the VFS buffer is freed. */
            char *names = xdr_dbuf_alloc_space(vop->buffer_len ? vop->buffer_len : 1, req->encoding->dbuf);
            if (!names) {
                error = NFS4ERR_RESOURCE;
                break;
            }
            memcpy(names, vop->buffer, vop->buffer_len);
            error = chimera_nfs4_listxattrs_fill(req, &map->variable_result.listxattrs,
                                                 names, vop->buffer_count, vop->eof, vop->r_cookie);
            map->variable_result.listxattrs.lxr_status = error;
            break;
        }
        default:
            return;
    } /* switch */
    if (error == NFS4_OK && !nfs4_vfs_reply_fits(ctx, reserved)) {
        error = NFS4ERR_RESOURCE;
    }
    if (error != NFS4_OK) {
        req->encoding->dbuf->used = mark;
        map->verify_status        = error;
        *status                   = CHIMERA_VFS_ENOSPC;
        return;
    }
    map->reply_staged        = true;
    ctx->reply_staged_bound += fixed;
    ctx->reply_staged_bytes += req->encoding->dbuf->used - mark;
} /* nfs4_vfs_variable_complete */

/* Marshal every GETATTR output while failure can still stop successors.
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
    uint64_t                              fixed, reserved;
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
    struct chimera_vfs_attrs attr = op->attr;
    uint8_t                  fh[NFS4_FHSIZE];
    int                      fh_len = nfs4_vfs_cursor_fh(map->attrdir, map->pseudo, fh, op->fh, op->fh_len
                                                         );
    if (map->pseudo) {
        nfs4_root_getattr(req->thread, &attr,
                          chimera_nfs4_attr2mask(argop->opgetattr.attr_request, argop->opgetattr.num_attr_request));
    }
    if (map->attrdir) {
        nfs4_vfs_attrdir_attrs(&attr);
    }
    struct nfs_delegation_combine_journal *combine_target = NULL, pending;
    struct nfs4_change_observation        *observation = NULL;
    map->verify_status = nfs4_change_project(req->thread->shared->nfs4_state_table.change_table,
                                             fh, fh_len, &attr, &req->change_observations, &observation);
    if (map->verify_status != NFS4_OK) {
        *status = CHIMERA_VFS_EAGAIN;
        return;
    }
    if (map->getattr_coordinate && !map->pseudo) {
        map->verify_status = nfs4_vfs_getattr_combine(map, op, &attr, &combine_target, &pending);
        if (map->verify_status != NFS4_OK) {
            *status = CHIMERA_VFS_EAGAIN;
            return;
        }
    }
    fixed    = nfs4_vfs_op_reply_bound(argop);
    reserved = nfs4_vfs_reply_reserved(ctx, mark, fixed);

    map->getattr_result.status = chimera_nfs4_getattr_fill(
        req, nfs4_vfs_export_id(map), &argop->opgetattr, &map->getattr_result, &attr,
        fh, fh_len, true);
    if (map->getattr_result.status == NFS4_OK && !nfs4_vfs_reply_fits(ctx, reserved)) {
        map->getattr_result.status = NFS4ERR_RESOURCE;
    }
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
    map->getattr_staged      = 1;
    ctx->reply_staged_bound += fixed;
    ctx->reply_staged_bytes += req->encoding->dbuf->used - mark;
} /* nfs4_vfs_getattr_complete */

static void
nfs4_vfs_attempt_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;

    (void) compound;
    for (uint32_t i = 0; i < ctx->num_retirements; i++) {
        ctx->retirements[i].retired = false;
    }
    nfs4_change_finish(ctx->req->thread->shared->nfs4_state_table.change_table,
                       &ctx->req->change_observations, false);
    for (uint32_t i = 0; i < ctx->num_combines; i++) {
        nfs_delegation_combine_reset(&ctx->combines[i]);
    }
    nfs_layout_journal_reset(&ctx->layouts);
    ctx->req->encoding->dbuf->used = ctx->attempt_mark;
    ctx->reply_staged_bytes        = 0;
    ctx->reply_staged_bound        = 0;
    ctx->reply_read_iov            = ctx->req->reply_read_iov;
    ctx->reply_read_seen           = ctx->req->reply_read_seen;
    ctx->reply_bytes               = ctx->req->reply_bytes;
    ctx->reply_chunk_bytes         = ctx->req->reply_chunk_bytes;
    ctx->fh_consumed               = ctx->initial_fh_consumed;
    ctx->last_root                 = NULL;
    ctx->pseudo                    = ctx->initial_pseudo;
    ctx->saved_pseudo              = fh_is_nfs4_root(ctx->req->saved_fh, ctx->req->saved_fhlen);
    ctx->namespace_fhlen           = 0;
    ctx->current_identity          = ctx->initial_identity;
    ctx->saved_identity            = ctx->initial_saved_identity;
    ctx->saved_fhlen               = ctx->req->saved_fhlen;
    memcpy(ctx->saved_fh, ctx->req->saved_fh, ctx->saved_fhlen);
    ctx->fh_len = ctx->req->fhlen;
    memcpy(ctx->fh, ctx->req->fh, ctx->fh_len);
    ctx->current_stateid = ctx->initial_stateid;
    ctx->current_client  = ctx->client;
    memset(ctx->io_renew, 0, sizeof(ctx->io_renew));
    ctx->saved_stateid         = ctx->initial_saved_stateid;
    ctx->current_stateid_valid = ctx->initial_stateid_valid;
    ctx->saved_stateid_valid   = ctx->initial_saved_stateid_valid;
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
        nfs4_layoutget_reset(map->layoutget);
        map->open_replay       = map->lock_replay = NULL;
        map->replay_hit        = map->replay_recorded = 0;
        map->close_passed      = 0;
        map->retirement_passed = false;
        map->lockt_checked     = 0;
        map->owner_checked     = false;
        map->client            = NULL;
        map->secinfo_junction  = false;
        map->parent_crossing   = false;
        map->pseudo            = map->result_pseudo = false;
        memset(&map->device_result, 0, sizeof(map->device_result));
        map->result_fhlen = 0;
        map->identity     = map->planned_identity;
        if (map->runtime_lookup) {
            map->junction = false;
        }
        for (uint32_t n = 0; n < map->namespace_count; n++) {
            map->namespace_targets[n].found = false;
        }
        map->lock_state     = NULL;
        map->open_state     = NULL;
        map->open_committed = 0;
        map->stream_guard   = NULL;
        if (map->reserved_open) {
            map->reserved_open->handle = NULL;
            map->reserved_open->fh_len = 0;
        }
        ctx->ops[i].have_io_owner  = 0;
        ctx->ops[i].verify_status  = 0;
        ctx->ops[i].getattr_staged = 0;
        ctx->ops[i].reply_staged   = false;
        ctx->ops[i].reply_mark     = 0;
        memset(&ctx->ops[i].variable_result, 0, sizeof(ctx->ops[i].variable_result));
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
        ctx->lock_groups[i]->release_pending = false;
    }
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock *entry = &ctx->locks[i];
        entry->active = entry->initially_public;
        entry->dirty  = entry->retired = 0;
        entry->seqid  = entry->initially_public ? entry->target->seqid : 0;
        entry->parent = entry->initially_public ? nfs4_vfs_open_target(ctx, entry->target->open_state) : NULL;
        nfs_lock_range_journal_reset(entry->ranges);
    }
} /* nfs4_vfs_attempt_reset */

/* Existing states are bounded by the owner's open-file population, not by
 * this wire compound's operation count. Grow only during construction; the
 * resulting addresses remain stable throughout execution and finish retries. */
static bool
nfs4_vfs_build_reset(
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx)
{
    uint32_t capacity = ctx->num_reserved;

    for (uint32_t i = 0; i < ctx->num_groups; i++) {
        if (UINT32_MAX - capacity < ctx->groups[i]->num_existing) {
            return false;
        }
        capacity += ctx->groups[i]->num_existing;
    }
    if (capacity > ctx->state_capacity) {
        struct nfs4_vfs_state *states = realloc(ctx->states, (size_t) capacity * sizeof(*states));
        if (!states) {
            return false;
        }
        ctx->states         = states;
        ctx->state_capacity = capacity;
    }
    nfs4_vfs_attempt_reset(compound, ctx);
    return true;
} /* nfs4_vfs_build_reset */

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

        vop = chimera_vfs_compound_op(compound, index);
        uint32_t                              opcode = req->args_compound->argarray[map->res_index].argop;
        if (map->result_pseudo && opcode != OP_READDIR) {
            return;
        }
        if (opcode == OP_PUTROOTFH || opcode == OP_PUTPUBFH) {
            if (!vop->fh_len) {
                map->verify_status = NFS4ERR_SERVERFAULT;
                *status            = CHIMERA_VFS_EFAULT;
            }
            return;
        }
        if (opcode == OP_LOOKUPP && map->parent_crossing) {
            return;
        }
        if (opcode == OP_CREATE || opcode == OP_LOOKUPP || (opcode == OP_LOOKUP && !map->attrdir)) {
            if (map->runtime_lookup || map->runtime_parent) {
                vop = chimera_vfs_compound_op(compound, map->runtime_lookup && map->junction ? map->junction_end : map->
                                              vfs_res - 1);
            }
            if (!(vop->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) || !vop->attr.va_fh_len) {
                map->verify_status = NFS4ERR_SERVERFAULT;
                *status            = CHIMERA_VFS_EFAULT;
            }
            return;
        }
        if (req->args_compound->argarray[map->res_index].argop != OP_READDIR) {
            break;
        }
        if (map->root_readdir && map->pseudo) {
            return;
        }
        if (map->attrdir) {
            map->verify_status = nfs4_vfs_readdir_attrdir(compound, map);
            if (map->verify_status != NFS4_OK) {
                *status = CHIMERA_VFS_EINVAL;
                return;
            }
        }

        if (map->readdir_cursor.change_status != NFS4_OK) {
            map->verify_status = map->readdir_cursor.change_status;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }

        /* RFC 7530 16.24.4, applied here so it stops the sequence rather than
         * being discovered once the ops behind it have already run. */
        if (!(map->attrdir ? map->readdir_eof : vop->eof) && map->readdir_cursor.entries == NULL) {
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
                /* Synthetic directories validate the base's existence, as
                 * their standalone PUTFH did, without a link-count test. */
                if (map->result_attrdir) {
                    break;
                }
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

            case OP_OPENATTR:
                if (!(aux->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) ||
                    !S_ISREG(aux->attr.va_mode)) {
                    map->verify_status = NFS4ERR_NOTSUPP;
                    *status            = CHIMERA_VFS_EINVAL;
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
                nfsstat4 vs = chimera_nfs4_verify_status(req, nfs4_vfs_export_id(map), map->res_index,
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

/* Variable read-only results can be measured after execution. Every state-
 * changing operation instead keeps its pre-execution success reservation.
 * Optional OPEN delegations get separate admission at publication. This callback
 * only measures borrowed results; it never transfers VFS ownership. */
static void
nfs4_vfs_reply_complete(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    bool                         first_read,
    enum chimera_vfs_error      *status)
{
    struct nfs4_vfs_compound_ctx         *ctx         = map->ctx;
    struct nfs_request                   *req         = ctx->req;
    const struct nfs_argop4              *argop       = &req->args_compound->argarray[map->res_index];
    const struct chimera_vfs_compound_op *vop         = chimera_vfs_compound_op(compound, map->vfs_res);
    struct nfs_resop4                     result      = { .resop = argop->argop };
    uint64_t                              bytes       = chimera_nfs4_reply_op_bound(req, argop);
    uint64_t                              chunk_bytes = ctx->reply_chunk_bytes;
    struct secinfo4                       security[4];

    if (*status != CHIMERA_VFS_OK) {
        bytes = chimera_nfs4_reply_error_bytes(argop->argop);
        if ((argop->argop == OP_LOCK || argop->argop == OP_LOCKT) &&
            map->verify_status == NFS4ERR_DENIED) {
            bytes += 32 + chimera_nfs4_reply_pad(map->lockt_owner_len);
        } else if (argop->argop == OP_GETDEVICEINFO) {
            result.opgetdeviceinfo = map->device_result;
            bytes                  = marshall_length_nfs_resop4(&result);
        }
    } else {
        switch (argop->argop) {
            case OP_GETFH: {
                uint8_t fh[NFS4_FHSIZE], wire[CHIMERA_NFS_FH_MAX];
                int     length = nfs4_vfs_cursor_fh(map->result_attrdir, map->result_pseudo,
                                                    fh, vop->fh, vop->fh_len);
                if (!fh_is_nfs4_root(fh, length)) {
                    chimera_nfs_fh_wrap(wire, &length, nfs4_vfs_export_id(map), fh, length,
                                        req->thread->shared->fh_key, req->thread->shared->fh_sign);
                }
                bytes = 12 + chimera_nfs4_reply_pad(length);
                break;
            }
            case OP_READ:
                bytes = 16 + chimera_nfs4_reply_pad(vop->read_len);
                if (first_read && req->encoding->write_chunk && (req->encoding->write_chunk->max_length || req->encoding
                                                                 ->write_chunk->num_segments)) {
                    chunk_bytes += chimera_nfs4_reply_pad(vop->read_len);
                }
                break;
            case OP_READLINK:
                bytes = 12 + chimera_nfs4_reply_pad(vop->target_len);
                break;
            case OP_GETATTR:
                result.opgetattr = map->getattr_result;
                bytes            = marshall_length_nfs_resop4(&result);
                break;
            case OP_READDIR:
                nfs4_vfs_readdir_fill(&result.opreaddir, vop, map);
                bytes = marshall_length_nfs_resop4(&result);
                break;
            case OP_READ_PLUS:
                result.opread_plus = map->variable_result.read_plus;
                bytes              = marshall_length_nfs_resop4(&result);
                break;
            case OP_GETXATTR:
                result.opgetxattr = map->variable_result.getxattr;
                bytes             = marshall_length_nfs_resop4(&result);
                break;
            case OP_LISTXATTRS:
                result.oplistxattrs = map->variable_result.listxattrs;
                bytes               = marshall_length_nfs_resop4(&result);
                break;
            case OP_GETDEVICEINFO:
                result.opgetdeviceinfo = map->device_result;
                bytes                  = marshall_length_nfs_resop4(&result);
                break;
            case OP_SECINFO:
            case OP_SECINFO_NO_NAME: {
                const struct chimera_nfs_export *export = map->secinfo_junction ? &map->secinfo_export->export :
                    map->identity ? &map->identity->export : NULL;
                result.opsecinfo.resok4     = security;
                result.opsecinfo.num_resok4 = chimera_nfs_fill_secinfo(security,
                                                                       export ? export->sec_allowed : 0, req->thread->
                                                                       shared->gss_enabled);
                bytes = marshall_length_nfs_resop4(&result);
                break;
            }
            default:
                break;
        } /* switch */
    }
    nfsstat4 error = chimera_nfs4_reply_check(req, map->res_index, ctx->reply_bytes + bytes,
                                              chunk_bytes, *status == CHIMERA_VFS_OK);
    if (error != NFS4_OK) {
        map->verify_status = error;
        *status            = CHIMERA_VFS_ENOSPC;
    } else if (*status == CHIMERA_VFS_OK) {
        ctx->reply_bytes      += bytes;
        ctx->reply_chunk_bytes = chunk_bytes;
    }
} /* nfs4_vfs_reply_complete */

/* The protocol cursor and owner sequence numbers are attempt-local state.
 * Capture the cursor only after a whole wire operation succeeds. Record both
 * successful operations and consuming errors before the next wire operation;
 * only accepted finish copies these journals to their reserved owners. */
static void
nfs4_vfs_operation_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;

    if (*status == CHIMERA_VFS_OK) {
        for (uint32_t i = 0; i < ctx->num_ops; i++) {
            struct nfs4_vfs_op *map = &ctx->ops[i];
            if (index == (uint32_t) map->vfs_hi) {
                bool                                  first_read = !ctx->reply_read_seen;
                nfs4_vfs_variable_complete(compound, map, status);
                nfs4_vfs_reply_complete(compound, map, first_read, status);
                if (*status != CHIMERA_VFS_OK) {
                    break;
                }
                const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
                ctx->fh_len = ctx->fh_consumed ? 0 :
                    nfs4_vfs_cursor_fh(map->result_attrdir, map->result_pseudo, ctx->fh, op->fh, op->fh_len);
                ctx->pseudo       = map->result_pseudo;
                map->result_fhlen = ctx->fh_len;
                memcpy(map->result_fh, ctx->fh, ctx->fh_len);
                ctx->current_identity = map->identity;
                uint32_t                              opcode = ctx->req->args_compound->argarray[map->res_index].argop;
                if (opcode == OP_LAYOUTGET) {
                    const struct LAYOUTGET4res *result = nfs4_layoutget_result(map->layoutget);
                    chimera_nfs_abort_if(!result, "completed LAYOUTGET has no private reply");
                    ctx->current_stateid       = result->logr_resok4.logr_stateid;
                    ctx->current_stateid_valid = 1;
                }
                if (opcode == OP_OPEN && !ctx->req->minorversion) {
                    ctx->current_client = map->client;
                }
                if (opcode == OP_SAVEFH) {
                    ctx->saved_pseudo   = ctx->pseudo;
                    ctx->saved_identity = map->identity;
                    ctx->saved_fhlen    = ctx->fh_len;
                    memcpy(ctx->saved_fh, ctx->fh, ctx->fh_len);
                }
                if (!ctx->pseudo && (opcode == OP_PUTROOTFH || opcode == OP_PUTPUBFH)) {
                    ctx->last_root = map;
                }
                break;
            }
        }
    }
    if (*status != CHIMERA_VFS_OK) {
        for (uint32_t i = 0; i < ctx->num_ops; i++) {
            struct nfs4_vfs_op *map = &ctx->ops[i];
            if (index >= (uint32_t) map->prepare_index && index <= (uint32_t) map->vfs_hi) {
                nfs4_vfs_reply_complete(compound, map, false, status);
                break;
            }
        }
    }
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
                if (map->open_by_name && !map->open_stream) {
                    const struct chimera_vfs_compound_op *lookup = map->open_deleg_lookup ?
                        chimera_vfs_compound_op(compound, map->open_deleg_lookup) : NULL;
                    struct chimera_vfs_attrs              pre  = lookup ? lookup->dir_post_attr : opened->dir_pre_attr;
                    struct chimera_vfs_attrs              post = lookup ? lookup->dir_post_attr : opened->dir_post_attr;
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
} /* nfs4_vfs_operation_complete */

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
    uint64_t                   hash = chimera_vfs_hash(fh, fh_len);
    entry->namespace_file = chimera_vfs_state_get(ctx->req->thread->vfs->vfs_state,
                                                  fh, fh_len, hash, true);
    if (!entry->namespace_file) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    struct chimera_vfs_io_view view = { .excluded     = ctx->closed_claims,
                                        .num_excluded = ctx->num_closed_claims };
    bool                       blocked = chimera_vfs_claim_break_caching_view(ctx->req->thread->vfs->vfs_state,
                                                                              fh, fh_len, hash, &view);
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
        if ((uint32_t) map->namespace_targets[i].coordinate != index) {
            continue;
        }
        if (!map->namespace_targets[i].found) {
            chimera_vfs_compound_op_skip(compound, index);
            return;
        }
        return;
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
    struct nfs4_vfs_op             *map  = private_data;
    struct chimera_vfs_compound_op *args = chimera_vfs_compound_op_args(compound, index);

    /* Lower namespace helpers may recall again after their own lookups.
     * Keep the same private retirement view through those asynchronous gates. */
    args->io_view.excluded     = map->ctx->closed_claims;
    args->io_view.num_excluded = map->ctx->num_closed_claims;
    for (uint32_t i = 0; i < map->namespace_count; i++) {
        if (!map->namespace_targets[i].found || map->namespace_targets[i].coordinate < 0) {
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
        struct chimera_vfs_io_view            view = { .excluded     = map->ctx->closed_claims,
                                                       .num_excluded = map->ctx->num_closed_claims };
        if (!entry || !entry->namespace_file || chimera_vfs_claim_has_caching_view(entry->namespace_file, &view)) {
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }
    }
} /* nfs4_vfs_namespace_ready */

static void
nfs4_vfs_remove_pin_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                   *map    = private_data;
    const struct chimera_vfs_compound_op *lookup =
        chimera_vfs_compound_op(compound, map->namespace_targets[0].lookup);
    uint64_t                              needed = CHIMERA_VFS_ATTR_PNFS_LAYOUT | CHIMERA_VFS_ATTR_NLINK |
        CHIMERA_VFS_ATTR_INUM;

    (void) status;
    if (!map->namespace_targets[0].found || (lookup->attr.va_set_mask & needed) != needed ||
        lookup->attr.va_nlink != 1 || !lookup->attr.va_pnfs_len) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_remove_pin_prepare */

static int
nfs4_vfs_namespace_add_target(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    const char                  *name,
    uint32_t                     name_len)
{
    uint32_t which = map->namespace_count++;
    uint64_t mask  = CHIMERA_VFS_ATTR_FH;

    if (map->ctx->req->args_compound->argarray[map->res_index].argop == OP_REMOVE &&
        chimera_vfs_pnfs_enabled(map->ctx->req->thread->vfs)) {
        mask |= CHIMERA_VFS_ATTR_PNFS_LAYOUT | CHIMERA_VFS_ATTR_NLINK | CHIMERA_VFS_ATTR_INUM;
    }
    int      lookup = chimera_vfs_compound_add_lookup(compound, name, name_len, mask, 0);

    if (lookup < 0) {
        return -1;
    }
    map->namespace_targets[which].lookup = lookup;
    chimera_vfs_compound_set_op_callbacks(compound, lookup, NULL, nfs4_vfs_namespace_lookup_complete, map);
    if (map->ctx->req->args_compound->argarray[map->res_index].argop == OP_REMOVE &&
        chimera_vfs_pnfs_enabled(map->ctx->req->thread->vfs)) {
        /* Keep the unlinked MDS inode alive until its accepted DS cleanup.
         * Otherwise a later CREATE/WRITE in this same span could reuse its
         * inode-based backing name before the old cleanup removes that name. */
        int pin_open = chimera_vfs_compound_add_open_current(compound, NFS4_VFS_OPEN_META, 0);
        int pin      = chimera_vfs_compound_add_gethandle(compound);
        if (pin_open < 0 || pin < 0) {
            return -1;
        }
        chimera_vfs_compound_set_op_prepare(compound, pin_open, nfs4_vfs_remove_pin_prepare, map);
        chimera_vfs_compound_set_op_prepare(compound, pin, nfs4_vfs_remove_pin_prepare, map);
    }
    int coordinate = -1;
    if (chimera_server_config_get_nfs4_delegations(map->ctx->req->thread->shared->config)) {
        coordinate = chimera_vfs_compound_add_coordinate(compound, nfs4_vfs_namespace_coordinate, map);
        if (coordinate < 0) {
            return -1;
        }
        chimera_vfs_compound_set_op_prepare(compound, coordinate, nfs4_vfs_namespace_coordinate_prepare, map);
    }
    map->namespace_targets[which].coordinate = coordinate;
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
    entry->next       = ctx->coordination;
    ctx->coordination = entry;
    if (!nfs_layout_journal_recall_view(&ctx->layouts, fh, fh_len, &entry->layout_view)) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_ENOSPC);
        return;
    }
    entry->layout_barrier = nfs_layout_table_barrier_acquire_view(
        &ctx->req->thread->shared->nfs4_layout_table, fh, fh_len, &entry->layout_view);
    if (!entry->layout_barrier) {
        chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_EAGAIN);
        return;
    }
    chimera_nfs4_cb_recall_and_wait_view(ctx->req->thread, fh, fh_len,
                                         nfs4_vfs_layout_recalled, entry, &entry->layout_view);
} /* nfs4_vfs_layout_coordinate */

/* Final accepted teardown is shared by ordinary replies and pNFS cleanup.
 * Backing removal is a distinct maintenance compound after the MDS transaction
 * has committed. Its failure cannot undo an accepted namespace REMOVE. */
static void
nfs4_vfs_finish_accepted(struct nfs4_vfs_compound_ctx *ctx)
{
    struct nfs_request *req    = ctx->req;
    nfsstat4            status = ctx->accepted_status;

    nfs4_vfs_dispose(ctx->accepted_compound, ctx);
    chimera_nfs4_compound_complete(req, status);
} /* nfs4_vfs_finish_accepted */

static void nfs4_vfs_cleanup_backing(
    struct nfs4_vfs_compound_ctx *ctx);

static void
nfs4_vfs_backing_removed(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx    = private_data;
    enum chimera_vfs_error        status = chimera_vfs_compound_finish_status(compound);

    if (status == CHIMERA_VFS_EAGAIN && ctx->cleanup_retries++ < 8 &&
        chimera_vfs_compound_retry(compound)) {
        return;
    }
    if (status == CHIMERA_VFS_OK) {
        status = chimera_vfs_compound_status(compound);
    }
    if (status != CHIMERA_VFS_OK && status != CHIMERA_VFS_ENOENT) {
        chimera_nfs_error("pNFS: failed to delete data-server backing file %s (err=%d)",
                          ctx->backing_name, status);
    }
    chimera_vfs_compound_free(compound);
    nfs4_vfs_cleanup_backing(ctx);
} /* nfs4_vfs_backing_removed */

static void
nfs4_vfs_backing_lookup_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) compound;
    (void) index;
    (void) private_data;
    if (*status == CHIMERA_VFS_ENOENT) {
        *status = CHIMERA_VFS_OK;
    }
} /* nfs4_vfs_backing_lookup_complete */

static void
nfs4_vfs_backing_remove_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx         *ctx    = private_data;
    const struct chimera_vfs_compound_op *lookup = chimera_vfs_compound_op(compound, 1);

    (void) status;
    if (!(lookup->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) ||
        lookup->attr.va_fh_len != ctx->backing_fhlen ||
        memcmp(lookup->attr.va_fh, ctx->backing_fh, ctx->backing_fhlen)) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_backing_remove_prepare */

static void
nfs4_vfs_cleanup_backing(struct nfs4_vfs_compound_ctx *ctx)
{
    struct nfs_request *req = ctx->req;

    /* The accepted compound retains every lookup snapshot until all cleanup
     * finishes. Walk successful REMOVE results, including the accepted prefix
     * of a compound whose later operation failed. Each DS deletion has its own
     * maintenance finish: a missing/unavailable backing never suppresses the
     * cleanup for a later accepted victim. */
    while (ctx->cleanup_next < ctx->num_ops) {
        struct nfs4_vfs_op *map = &ctx->ops[ctx->cleanup_next++];
        if ((int) map->res_index > ctx->accepted_index) {
            break;
        }
        if (map->attrdir || !map->namespace_count ||
            req->args_compound->argarray[map->res_index].argop != OP_REMOVE ||
            req->res_compound.resarray[map->res_index].opremove.status != NFS4_OK) {
            continue;
        }
        const struct chimera_vfs_compound_op *target =
            chimera_vfs_compound_op(ctx->accepted_compound, map->namespace_targets[0].lookup);
        const struct chimera_vfs_attrs       *attr     = &target->attr;
        uint64_t                              required = CHIMERA_VFS_ATTR_PNFS_LAYOUT | CHIMERA_VFS_ATTR_NLINK |
            CHIMERA_VFS_ATTR_INUM;
        if (map->namespace_targets[0].found && (attr->va_set_mask & required) == required &&
            attr->va_nlink == 1 && attr->va_pnfs_len >= CHIMERA_VFS_DEVICEID_SIZE) {
            const uint8_t               *deviceid, *backing_fh;
            uint32_t                     backing_len;
            if (chimera_vfs_pnfs_blob_unpack(attr->va_pnfs, attr->va_pnfs_len,
                                             &deviceid, &backing_fh, &backing_len) || backing_len > CHIMERA_VFS_FH_SIZE)
            {
                continue;
            }
            const struct chimera_vfs_ds *ds = chimera_vfs_pnfs_find_device(req->thread->vfs, deviceid);
            if (ds && ds->root_fh_len) {
                chimera_vfs_pnfs_backing_name(ctx->backing_name, target->fh, attr->va_ino);
                const struct chimera_vfs_cred *cred    = map->identity ? &map->identity->cred : &req->cred;
                struct chimera_vfs_compound   *cleanup = chimera_vfs_compound_alloc(req->thread->vfs_thread, cred);
                ctx->cleanup_retries = 0;
                ctx->backing_fhlen   = backing_len;
                memcpy(ctx->backing_fh, backing_fh, backing_len);
                chimera_vfs_compound_add_putfh(cleanup, ds->root_fh, ds->root_fh_len);
                uint64_t                       caps = chimera_vfs_module_capabilities(req->thread->vfs_thread,
                                                                                      ds->root_fh, ds->root_fh_len);
                if (!(caps & CHIMERA_VFS_CAP_REMOVE_MATCH_FH)) {
                    /* The NFS DS protocol has no atomic conditional REMOVE.
                     * Compare its native identity, while the MDS victim pin
                     * excludes legitimate backing-name reuse. An unrelated
                     * external DS namespace writer still needs backend help
                     * for atomic protection between this lookup and REMOVE. */
                    int lookup = chimera_vfs_compound_add_lookup(cleanup, ctx->backing_name,
                                                                 strlen(ctx->backing_name), CHIMERA_VFS_ATTR_FH, 0);
                    chimera_vfs_compound_set_op_callbacks(cleanup, lookup, NULL,
                                                          nfs4_vfs_backing_lookup_complete, ctx);
                    chimera_vfs_compound_add_putfh(cleanup, ds->root_fh, ds->root_fh_len);
                }
                int remove = chimera_vfs_compound_add_remove(cleanup, ctx->backing_name,
                                                             strlen(ctx->backing_name), 0, 0, 0);
                if (caps & CHIMERA_VFS_CAP_REMOVE_MATCH_FH) {
                    chimera_vfs_compound_op_set_remove_match(cleanup, remove, backing_fh, backing_len, 1, NULL);
                } else {
                    chimera_vfs_compound_set_op_prepare(cleanup, remove, nfs4_vfs_backing_remove_prepare, ctx);
                }
                chimera_vfs_compound_submit(cleanup, nfs4_vfs_backing_removed, ctx);
                return;
            }
        }
    }
    nfs4_vfs_finish_accepted(ctx);
} /* nfs4_vfs_cleanup_backing */

/* Delegation grants are protocol publication. They run only after accepted
* finish, while owner/client reservations and the reply remain retained. */
static void
nfs4_vfs_publish_delegations(struct nfs_request *req)
{
    struct nfs4_vfs_compound_ctx *ctx         = req->compound_probe_private;
    struct chimera_vfs_compound  *compound    = ctx->accepted_compound;
    uint64_t                      reply_bytes = ctx->reply_bytes;

    /* Execution accounts successful results. Include the terminal error too
     * before spending any spare bytes on an optional delegation. */
    if (ctx->accepted_status != NFS4_OK) {
        reply_bytes += marshall_length_nfs_resop4(&req->res_compound.resarray[ctx->accepted_index]);
    }

    while (ctx->grant_next < ctx->num_ops) {
        struct nfs4_vfs_op *map = &ctx->ops[ctx->grant_next];
        struct nfs_argop4  *arg = &req->args_compound->argarray[map->res_index];
        struct OPEN4res    *res = &req->res_compound.resarray[map->res_index].opopen;
        if ((int) map->res_index > ctx->accepted_index) {
            break;
        }
        if (arg->argop == OP_OPEN && res->status == NFS4_OK && map->owner_session &&
            req->session != map->owner_session) {
            nfs4_session_bind_conn(req->conn, map->owner_session);
            req->session = map->owner_session;
            chimera_nfs_debug("NFS4_VFS_CLIENT_BIND tag=%.*s index=%u clientid=%" PRIu64,
                              (int) req->args_compound->tag.len, req->args_compound->tag.data,
                              map->res_index, map->owner_client->client_id);
        }
        if (arg->argop == OP_OPEN && map->reserved_open &&
            res->status == NFS4_OK && !map->replay_hit &&
            map->open_state && !map->open_stream) {
            const struct chimera_vfs_compound_op *op =
                chimera_vfs_compound_op(compound, map->vfs_res);
            nfs4_vfs_identity_apply(req, map->identity);
            req->index = map->res_index;
            req->fhlen = op->fh_len;
            memcpy(req->fh, op->fh, op->fh_len);
            bool                                  allow_grant = !map->open_state->closed;
            if (req->minorversion && allow_grant) {
                allow_grant = chimera_nfs4_reply_check(req, ctx->accepted_index,
                                                       reply_bytes + CHIMERA_NFS4_OPEN_DELEGATION_BYTES,
                                                       ctx->reply_chunk_bytes, ctx->accepted_status == NFS4_OK) ==
                    NFS4_OK;
            }
            if (chimera_nfs4_open_grant_delegation(req, res,
                                                   map->open_by_name ? &op->attr : NULL, allow_grant)) {
                return;
            }
            if (req->minorversion &&
                (res->resok4.delegation.delegation_type == OPEN_DELEGATE_READ ||
                 res->resok4.delegation.delegation_type == OPEN_DELEGATE_WRITE)) {
                /* Retain the reservation across later grants and a parked
                 * callback probe. v4.0 already reserved grants for replay. */
                reply_bytes      += CHIMERA_NFS4_OPEN_DELEGATION_BYTES;
                ctx->reply_bytes += CHIMERA_NFS4_OPEN_DELEGATION_BYTES;
            }
            if (map->open_response) {
                /* The owner stays reserved across a parked CB_NULL probe.
                 * Cache the accepted delegation, including its owned WHO
                 * bytes, without overwriting a later operation's replay. */
                chimera_nfs_abort_if(!nfs4_replay_record_open(map->open_response, map->open_seqid,
                                                              res, op->fh, op->fh_len),
                                     "delegated OPEN reply exceeds replay snapshot");
                if (map->open_replay->replay.op == OP_OPEN &&
                    map->open_replay->replay.seqid == map->open_seqid) {
                    map->open_replay->replay = *map->open_response;
                }
                /* A repeated OPEN within this same wire compound was filled
                 * before grants. It must report the same final delegation. */
                for (uint32_t j = ctx->grant_next + 1; j < ctx->num_ops; j++) {
                    struct nfs4_vfs_op *replay = &ctx->ops[j];
                    if ((int) replay->res_index <= ctx->accepted_index && replay->replay_hit &&
                        replay->open_response && replay->open_group == map->open_group &&
                        replay->open_response->seqid == map->open_seqid) {
                        req->res_compound.resarray[replay->res_index].opopen = *res;
                    }
                }
            }
        }
        ctx->grant_next++;
    }
    if (req->minorversion == 0) {
        for (uint32_t i = 0; i < ctx->num_groups; i++) {
            nfs_open_owner_publish_replay(ctx->groups[i]);
        }
    }
    nfs4_vfs_identity_apply(req, ctx->current_identity);
    req->index = ctx->accepted_index;
    req->fhlen = ctx->accepted_fhlen;
    memcpy(req->fh, ctx->accepted_fh, req->fhlen);
    req->compound_probe_resume  = NULL;
    req->compound_probe_private = NULL;
    nfs4_vfs_cleanup_backing(ctx);
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
        nfs4_vfs_dispose(compound, ctx);
        req->root_junction_resume = NULL;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    ctx->accepted          = true;
    req->reply_read_iov    = ctx->reply_read_iov;
    req->reply_read_seen   = ctx->reply_read_seen;
    req->reply_chunk_bytes = ctx->reply_chunk_bytes;
    if (ctx->root_resolve) {
        /* This is a namespace prelude, not a completed wire operation. Keep
         * the caller's cursor and credential intact, and resume only after
         * accepted finish and a final locked snapshot check. */
        nfs4_root_export_fh_callback_t    callback = ctx->root_resolve;
        struct chimera_server_nfs_shared *shared   = thread->shared;
        enum chimera_vfs_error            result   = chimera_vfs_compound_status(compound);
        uint8_t                           fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                          fh_len = 0;
        vop = chimera_vfs_compound_op(compound, 1);
        if (result == CHIMERA_VFS_OK && !vop->fh_len) {
            result = CHIMERA_VFS_EIO;
        }
        evpl_mutex_lock(&shared->exports_lock);
        if (!nfs4_vfs_root_publish(ctx, ctx->entry_export, result == CHIMERA_VFS_OK ? vop : NULL)) {
            result = CHIMERA_VFS_EAGAIN;
        }
        evpl_mutex_unlock(&shared->exports_lock);
        if (result == CHIMERA_VFS_OK) {
            fh_len = vop->fh_len;
            memcpy(fh, vop->fh, fh_len);
        }
        nfs4_vfs_dispose(compound, ctx);
        callback(result, fh_len ? fh : NULL, fh_len, thread, req);
        return;
    }
    if (ctx->root_probe >= 0) {
        struct chimera_server_nfs_shared *shared = thread->shared;
        vop = chimera_vfs_compound_op(compound, ctx->root_probe);
        evpl_mutex_lock(&shared->exports_lock);
        nfs4_vfs_root_publish(ctx, ctx->namespace_root,
                              !ctx->last_root && vop->status == CHIMERA_VFS_OK && !vop->skipped &&
                              ctx->namespace_fhlen ? vop : NULL);
        evpl_mutex_unlock(&shared->exports_lock);
    }
    for (uint32_t i = 0; i < ctx->num_retirements; i++) {
        nfs_delegation_retirement_finish(&ctx->retirements[i], true,
                                         &req->thread->shared->nfs4_state_table, req->thread->vfs_thread);
    }
    nfs4_vfs_publish_states(compound, ctx);
    nfs_layout_journal_finish(&ctx->layouts, true);
    for (k = 0; k < ctx->num_ops; k++) {
        nfs4_layoutget_publish(compound, ctx->ops[k].layoutget);
    }

    for (k = 0; k < ctx->num_io_clients; k++) {
        if (ctx->io_renew[k]) {
            nfs_client_touch(ctx->io_clients[k]);
        }
    }

    if (ctx->client && req->minorversion) {
        nfs_client_touch(ctx->client);
    }

    for (k = 0; k < ctx->num_ops; k++) {
        if (ctx->ops[k].retirement_passed && ctx->ops[k].retirement_client) {
            nfs_client_touch(ctx->ops[k].retirement_client);
        }
        if (ctx->ops[k].lockt_checked) {
            nfs_client_touch(ctx->ops[k].lockt_client);
        }
        if (ctx->ops[k].owner_checked && ctx->ops[k].owner_client) {
            nfs_client_touch(ctx->ops[k].owner_client);
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

        /* Execution-time staging preserved the fixed result reservations.
         * Completion must publish those accepted results without a second
         * admission floor that could retroactively fail a successful op. */
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
                if (status == NFS4ERR_OP_ILLEGAL) {
                    resop->resop = OP_ILLEGAL;
                }
                if (argop->argop == OP_GETDEVICEINFO) {
                    resop->opgetdeviceinfo             = map->device_result;
                    resop->opgetdeviceinfo.gdir_status = status;
                }

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

        nfs4_vfs_identity_apply(req, map->identity);
        status = nfs4_vfs_op_fill(req, compound, ctx, map, argop, resop);

        if (status != NFS4_OK) {
            fail_res = map->res_index;
            failed   = 1;
        }
    }

    /* Publish only the cursor left by a successful wire operation. Internal
     * LOOKUPs, stream bases and failed OPENs never escape through req->fh. */
    nfs4_vfs_identity_apply(req, ctx->current_identity);
    req->fhlen = ctx->fh_len;
    memcpy(req->fh, ctx->fh, ctx->fh_len);

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
     * does for a per-op handler. */
    req->index = failed ? (int) fail_res : (int) ctx->ops[ctx->num_ops - 1].res_index;

    nfs4_vfs_compound_give_back(thread, ctx);

    ctx->accepted_compound = compound;
    ctx->accepted_index    = req->index;
    ctx->accepted_fhlen    = req->fhlen;
    memcpy(ctx->accepted_fh, req->fh, req->fhlen);
    ctx->accepted_status = failed ? status : NFS4_OK;
    /* OPEN's accepted connection binding and replay publication are needed
     * even when delegation grants are disabled. The grant helper handles
     * that configuration itself without parking the request. */
    req->compound_probe_private = ctx;
    req->compound_probe_resume  = nfs4_vfs_publish_delegations;
    nfs4_vfs_publish_delegations(req);
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
 * Append one xattr op, staging its qualified name for every execution attempt.
 * Returns the VFS op index, or -1 (which refuses the whole sequence) if the
 * name will not fit in the reply buffer -- the length itself was already
 * accepted by chimera_nfs4_xattr_validate_name during the scan.
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

    if (map->input_status == NFS4_OK) {
        if (!(args->share_access & OPEN4_SHARE_ACCESS_BOTH)) {
            map->input_status = NFS4ERR_INVAL;
        } else if ((map->open_stream && args->claim.claim != CLAIM_NULL) ||
                   (args->claim.claim != CLAIM_NULL && args->claim.claim != CLAIM_FH &&
                    args->claim.claim != CLAIM_PREVIOUS && !nfs4_vfs_open_is_delegated(args))) {
            map->input_status = NFS4ERR_NOTSUPP;
        } else if (args->openhow.opentype == OPEN4_CREATE && !nfs4_vfs_open_is_named(argop)) {
            map->input_status = NFS4ERR_INVAL;
        } else if (nfs4_vfs_open_is_named(argop)) {
            map->input_status = chimera_nfs4_validate_name(args->claim.claim == CLAIM_NULL ?
                                                           &args->claim.file : &args->claim.delegate_cur_info.file)
            ;
        }
    }
    if (map->input_status == NFS4_OK && args->openhow.opentype == OPEN4_CREATE &&
        args->openhow.how.mode != EXCLUSIVE4) {
        const struct fattr4 *wire = args->openhow.how.mode == EXCLUSIVE4_1 ?
            &args->openhow.how.ch_createboth.cva_attrs : &args->openhow.how.createattrs;
        map->input_status = chimera_nfs4_validate_createattrs(wire->num_attrmask, wire->attrmask);
        if (map->input_status == NFS4_OK && args->openhow.how.mode == EXCLUSIVE4_1) {
            map->input_status = chimera_nfs4_validate_exclcreat_attrs(wire->num_attrmask, wire->attrmask);
        }
    }
    if (map->input_status != NFS4_OK) {
        return chimera_vfs_compound_add_checkpoint(compound);
    }

    if (args->claim.claim == CLAIM_NULL) {
        name              = (const char *) args->claim.file.data;
        namelen           = (int) args->claim.file.len;
        map->open_by_name = 1;
    } else if (args->claim.claim == CLAIM_DELEGATE_CUR) {
        /* The preceding LOOKUP validated the delegation and selected its FH. */
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
             * that whatever is in the way it can go and look at it. */
            flags |= CHIMERA_VFS_OPEN_CREATE_REGULAR;

            map->input_status = nfs4_vfs_decode_attrs(map, &args->openhow.how.createattrs, &attr);
            if (map->input_status != NFS4_OK) {
                return chimera_vfs_compound_add_checkpoint(compound);
            }
        } else {
            /* EXCLUSIVE4 and EXCLUSIVE4_1 stamp the client's verifier into the
             * object's atime and mtime, which is how a repeat of the same
             * create recognises its own earlier one. A collision therefore
             * has to be inspected rather than refused, which is what
             * EXCLUSIVE_RETRY is for. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY;

            map->input_status = nfs4_vfs_open_exclusive_attrs(args, &attr, map);
            if (map->input_status != NFS4_OK) {
                return chimera_vfs_compound_add_checkpoint(compound);
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

    if (args->claim.claim == CLAIM_DELEGATE_CUR) {
        /* No create-by-name race after validating the delegated inode.
         * GUARDED/exclusive collisions were checked on LOOKUP. Only a
         * deferred UNCHECKED truncate may mutate the existing file. */
        flags &= ~(CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE | CHIMERA_VFS_OPEN_CREATE_REGULAR);
        flags |= CHIMERA_VFS_OPEN_REGULAR_ONLY;
        opts   = 0;
        memset(&attr, 0, sizeof(attr));
    }

    (void) req;

    /* Type, permission and change information required by OPEN. Exclusive
     * creation also needs the timestamps holding the verifier. */
    attr_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
        CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID | CHIMERA_VFS_ATTR_ACL |
        CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME;

    if (opts & CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY) {
        attr_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
    }

    if (map->open_stream) {
        /* Stream metadata belongs to its base. Never apply create mode/owner
         * attributes to the base inode. Truncation follows share admission. */
        flags &= ~CHIMERA_VFS_OPEN_CREATE_REGULAR;
        return chimera_vfs_compound_add_open_stream(compound, name, namelen, flags, NULL,
                                                    CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL);
    }
    return chimera_vfs_compound_add_open(compound, name, namelen, flags, opts, &attr, attr_mask, 0, 0);
} /* nfs4_vfs_add_open_op */

static int
nfs4_vfs_stateid_closed(
    struct nfs4_vfs_compound_ctx *ctx,
    const struct stateid4        *sid)
{
    struct nfs4_vfs_lock *lock = nfs4_vfs_private_lock(ctx, sid);

    if (lock && lock->retired) {
        return 1;
    }
    for (uint32_t i = 0; i < ctx->num_ops; i++) {
        struct nfs4_vfs_op *map = &ctx->ops[i];
        if (map->retirement_passed &&
            !memcmp(map->retirement_stateid.other, sid->other, sizeof(sid->other))) {
            return 1;
        }
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
    const uint8_t                *fh = nfs4_vfs_state_fh(compound, map, &fhlen);
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
    if (state->owner->client != map->client || state->fh_len != fhlen || memcmp(state->fh, fh, fhlen)) {
        return NFS4ERR_BAD_STATEID;
    }
    if (!nfs_open_state_check_principal(state, ctx->req->principal_flavor,
                                        ctx->req->principal_machinename, ctx->req->principal_machinename_len)) {
        return NFS4ERR_ACCESS;
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
    const uint8_t                *fh = nfs4_vfs_state_fh(compound, map, &length);

    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (!entry || entry->closed || entry->group->owner_replay.confirmed ||
        entry->shadow.owner->client != map->client ||
        entry->shadow.fh_len != length || memcmp(entry->shadow.fh, fh, length)) {
        return NFS4ERR_BAD_STATEID;
    }
    if (!nfs_open_state_check_principal(&entry->shadow, ctx->req->principal_flavor,
                                        ctx->req->principal_machinename, ctx->req->principal_machinename_len)) {
        return NFS4ERR_ACCESS;
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
    const uint8_t                    *fh     = nfs4_vfs_state_fh(compound, map, &fh_len);
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
    if (state->owner->client != map->client || state->fh_len != fh_len ||
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
    const uint8_t                *fh     = nfs4_vfs_state_fh(compound, map, &fhlen);
    uint32_t                      mode   = locking ? args->locktype : op->oplocku.locktype;
    uint64_t                      offset = locking ? args->offset : op->oplocku.offset;
    uint64_t                      length = locking ? args->length : op->oplocku.length;
    nfsstat4                      status;

    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (locking) {
        status = nfs_recovery_open_check(&req->thread->shared->nfs4_recovery,
                                         map->client, args->reclaim != 0);
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
        if (map->lock_group && map->lock_group->release_pending) {
            map->lock_group->release_pending = false;
        }
        parent = nfs4_vfs_private_open(ctx, sid);
        if (!parent || parent->closed || !map->lock_group) {
            return map->stateid_reservation_status ? map->stateid_reservation_status : NFS4ERR_BAD_STATEID;
        }
        seqid = parent->shadow.seqid;
        for (uint32_t i = 0; i < ctx->num_locks; i++) {
            struct nfs4_vfs_lock *candidate = &ctx->locks[i];
            if (candidate->active && !candidate->retired && candidate->group == map->lock_group && candidate->parent &&
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
    if (!entry || entry->retired || !parent || parent->closed ||
        parent->shadow.owner->client != map->client ||
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
        map->lock_claim.cb_private  = map->client;
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
nfs4_vfs_advise_stateid(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    const struct stateid4       *sid)
{
    struct nfs4_vfs_compound_ctx *ctx   = map->ctx;
    struct nfs_request           *req   = ctx->req;
    struct nfs_state_table       *table = &req->thread->shared->nfs4_state_table;
    struct stateid4               current;
    struct nfs_open_state        *open_state;
    struct nfs_open_state        *reserved;
    uint32_t                      fh_len, seqid;
    const uint8_t                *fh = nfs4_vfs_state_fh(compound, map, &fh_len);
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
    if (nfs_layout_journal_test(&ctx->layouts, sid, &status)) {
        return status == NFS4_OK ? nfs_layout_journal_check(&ctx->layouts, fh, fh_len, sid) : status;
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
} /* nfs4_vfs_advise_stateid */

static nfsstat4
nfs4_vfs_io_advise_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    return nfs4_vfs_advise_stateid(compound, map,
                                   &map->ctx->req->args_compound->argarray[map->res_index].opio_advise.iaa_stateid);
} /* nfs4_vfs_io_advise_prepare */

static nfsstat4
nfs4_vfs_layoutget_authorize(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx *ctx  = map->ctx;
    const struct LAYOUTGET4args  *args = &ctx->req->args_compound->argarray[map->res_index].oplayoutget;
    struct stateid4               sid  = args->loga_stateid;
    struct nfs4_stateid_view      view;
    uint32_t                      fh_len;
    const uint8_t                *fh = nfs4_vfs_state_fh(compound, map, &fh_len);

    if (chimera_nfs4_stateid_is_current(&sid)) {
        if (!ctx->current_stateid_valid) {
            return NFS4ERR_BAD_STATEID;
        }
        sid       = ctx->current_stateid;
        sid.seqid = 0;
    }
    uint32_t own_barriers = 0;
    for (struct nfs4_vfs_coordination *entry = ctx->coordination; entry; entry = entry->next) {
        if (entry->layout_barrier && entry->fh_len == fh_len && !memcmp(entry->fh, fh, fh_len)) {
            own_barriers++;
        }
    }
    nfsstat4 status = nfs4_layoutget_admit(compound, map->layoutget, ctx->client,
                                           nfs4_vfs_export_id(map), &sid, fh, fh_len, own_barriers);
    if (status != NFS4_OK) {
        return status;
    }
    if (map->pseudo || map->attrdir || nfs4_stateid_is_special(&sid) ||
        nfs4_vfs_stateid_closed(ctx, &sid)) {
        return NFS4ERR_BAD_STATEID;
    }
    nfs4_stateid_decode(&view, &sid);
    if (view.type == NFS4_STATEID_TYPE_LAYOUT) {
        /* The reservation/check step sees both public and earlier private
         * grants. It precedes backend reads and retains the accepted identity. */
        return NFS4_OK;
    }
    if (view.type == NFS4_STATEID_TYPE_DELEG) {
        struct chimera_claim_actor actor;
        return nfs_state_table_delegation_io(&ctx->req->thread->shared->nfs4_state_table,
                                             &sid, ctx->client, fh, fh_len, args->loga_iomode == LAYOUTIOMODE4_RW ?
                                             OPEN4_SHARE_ACCESS_WRITE : OPEN4_SHARE_ACCESS_READ, &actor);
    }
    return nfs4_vfs_advise_stateid(compound, map, &sid);
} /* nfs4_vfs_layoutget_authorize */

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
        } else if (nfs_layout_journal_test(&ctx->layouts, sid, &status)) {
            /* Earlier LAYOUTGETs own reserved slots, not public state yet. */
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

/* Pin v4.0 I/O clients now; renew leases only after accepted finish. */
static nfsstat4
nfs4_vfs_stage_io_renewal(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs_client            *client)
{
    if (ctx->req->minorversion && ctx->req->session) {
        /* Session renewal belongs to the authenticated session client. */
        return NFS4_OK;
    }
    uint32_t i;
    for (i = 0; i < ctx->num_io_clients; i++) {
        if (ctx->io_clients[i] == client) {
            break;
        }
    }
    if (i == ctx->num_io_clients) {
        if (i == 2 * NFS4_VFS_COMPOUND_MAX_OPS) {
            return NFS4ERR_RESOURCE;
        }
        if (!nfs_client_reserve_compound(client)) {
            return NFS4ERR_EXPIRED;
        }
        ctx->io_clients[ctx->num_io_clients++] = client;
    }
    ctx->io_renew[i] = true;
    return NFS4_OK;
} /* nfs4_vfs_stage_io_renewal */

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
        struct nfs_client *client = ctx->client;
        if (!req->minorversion) {
            status = nfs_state_table_acquire_no_renew(table, sid, NFS4_SLOT_TYPE_DELEG,
                                                      &state_void, &state_type);
            if (status != NFS4_OK) {
                return status;
            }
            client = ((struct nfs_delegation *) state_void)->client;
            status = nfs4_vfs_stage_io_renewal(ctx, client);
            nfs_state_table_release(table, state_void, state_type, thread->vfs_thread);
            if (status != NFS4_OK) {
                return status;
            }
        }
        status = nfs_state_table_delegation_io(table, sid, client,
                                               fh, fhlen, check_access, out_owner);
        *have_owner = status == NFS4_OK;
        return status;
    }
    if (view.type != NFS4_STATEID_TYPE_OPEN && view.type != NFS4_STATEID_TYPE_LOCK) {
        return NFS4ERR_BAD_STATEID;
    }

    status = nfs_state_table_acquire_no_renew(table, sid, 0, &state_void, &state_type);

    if (status != NFS4_OK) {
        return status;
    }

    open_state = state_type == NFS4_SLOT_TYPE_OPEN ? state_void :
        ((struct nfs_lock_state *) state_void)->open_state;
    status = nfs4_vfs_stage_io_renewal(ctx, open_state->owner->client);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type, thread->vfs_thread);
        return status;
    }
    status = nfs_state_check_client(state_void, state_type,
                                    req->minorversion ? ctx->client : NULL);

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
        case OP_DELEGRETURN: return &argop->opdelegreturn.deleg_stateid;
        case OP_LAYOUTRETURN: return argop->oplayoutreturn.lora_layoutreturn.lr_returntype == LAYOUTRETURN4_FILE ?
                   &argop->oplayoutreturn.lora_layoutreturn.lr_layout.lrf_stateid : NULL;
        case OP_CLOSE: return &argop->opclose.open_stateid;
        case OP_OPEN_CONFIRM: return &argop->opopen_confirm.open_stateid;
        case OP_OPEN_DOWNGRADE: return &argop->opopen_downgrade.open_stateid;
        case OP_FREE_STATEID: return &argop->opfree_stateid.fsa_stateid;
        case OP_IO_ADVISE: return &argop->opio_advise.iaa_stateid;
        case OP_LAYOUTGET: return &argop->oplayoutget.loga_stateid;
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

/* v4.0 has no session-bound identity. Every owner operation uses its wire
 * clientid or concrete stateid, independently of earlier OPENs in this run
 * and the connection's implicit binding. v4.1 retains its session identity. */
static bool
nfs4_vfs_owner_operation(uint32_t opcode)
{
    return opcode == OP_OPEN || opcode == OP_CLOSE || opcode == OP_OPEN_CONFIRM ||
           opcode == OP_OPEN_DOWNGRADE || opcode == OP_LOCK || opcode == OP_LOCKU ||
           opcode == OP_FREE_STATEID || opcode == OP_RELEASE_LOCKOWNER;
} /* nfs4_vfs_owner_operation */

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
    if (op->argop != OP_OPEN && (map->pseudo || map->attrdir)) {
        return NFS4ERR_BAD_STATEID;
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
            if (candidate->active && !candidate->retired && candidate->group == map->lock_group &&
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

/* Only names that can denote an export need a conditional namespace path.
 * Ordinary names need no export selection, regardless of the execution cursor. */
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

/* A component mapping and its policy must still match the frozen snapshot.
 * Missing mappings are snapshots too: a new export must not turn a retried
 * ordinary lookup into a different namespace operation. */
static bool
nfs4_vfs_component_matches(
    struct nfs4_vfs_op             *map,
    const xdr_opaque               *name,
    const struct nfs4_vfs_identity *identity)
{
    struct chimera_server_nfs_shared *shared = map->ctx->req->thread->shared;
    struct chimera_nfs_export         current;
    bool                              found = chimera_nfs_get_export_by_component(shared, name->data, name->len, &
                                                                                  current) == 0;

    if (!identity) {
        return !found;
    }
    evpl_mutex_lock(&shared->exports_lock);
    bool                              matches = found && current.id == identity->export.id && identity->present &&
        nfs4_vfs_export_matches(shared, &identity->export);
    evpl_mutex_unlock(&shared->exports_lock);
    return matches;
} /* nfs4_vfs_component_matches */

static bool
nfs4_vfs_secinfo_junction(
    struct nfs4_vfs_op     *map,
    const xdr_opaque       *name,
    enum chimera_vfs_error *status)
{
    if (!nfs4_vfs_component_matches(map, name, map->secinfo_export)) {
        map->verify_status = NFS4ERR_DELAY;
        *status            = CHIMERA_VFS_EAGAIN;
    } else if (!map->secinfo_export) {
        map->verify_status = NFS4ERR_NOENT;
        *status            = CHIMERA_VFS_ENOENT;
    } else {
        /* SECINFO advertises even an inaccessible or unresolved export. It
         * neither enters the export nor performs its backing lookup. */
        map->secinfo_junction = true;
    }
    return *status != CHIMERA_VFS_OK;
} /* nfs4_vfs_secinfo_junction */

static uint32_t
nfs4_vfs_namespace_root_fh(
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx,
    uint8_t                      *fh)
{
    struct chimera_server_nfs_shared *shared = ctx->req->thread->shared;
    uint32_t                          length = 0;

    if (ctx->root_probe >= 0) {
        length = ctx->namespace_fhlen;
        memcpy(fh, ctx->namespace_fh, length);
    } else {
        evpl_mutex_lock(&shared->exports_lock);
        if (ctx->namespace_root && shared->root_export_fh_id == ctx->namespace_root->id) {
            length = shared->root_export_fh_len;
            memcpy(fh, shared->root_export_fh, length);
        }
        evpl_mutex_unlock(&shared->exports_lock);
    }
    if (ctx->last_root) {
        const struct chimera_vfs_compound_op *root = chimera_vfs_compound_op(compound, ctx->last_root->vfs_res);
        if (root->status == CHIMERA_VFS_OK) {
            length = root->fh_len;
            memcpy(fh, root->fh, length);
        }
    }
    return length;
} /* nfs4_vfs_namespace_root_fh */

static struct nfs4_vfs_identity *
nfs4_vfs_selected_identity(struct nfs4_vfs_op *map)
{
    if (map->terminal_status) {
        return map->ctx->current_identity;
    }
    if (map == &map->ctx->ops[0] && map->ctx->entry_export) {
        return map->planned_identity;
    }
    switch (map->ctx->req->args_compound->argarray[map->res_index].argop) {
        case OP_PUTFH:
        case OP_PUTROOTFH:
        case OP_PUTPUBFH: return map->planned_identity;
        case OP_RESTOREFH: return map->ctx->saved_identity;
        default: return map->parent_crossing ? map->ctx->namespace_identity :
                   map->junction ? map->junction_target : map->ctx->current_identity;
    } /* switch */
} /* nfs4_vfs_selected_identity */

/* Group selection only chooses immutable inputs and private cursor identity.
 * Authorization and changed-snapshot errors belong to the first prepare. */
static const struct chimera_vfs_cred *
nfs4_vfs_group_cred(
    struct chimera_vfs_compound *compound,
    uint32_t                     group,
    void                        *context)
{
    struct nfs4_vfs_op           *map = context;
    struct nfs4_vfs_compound_ctx *ctx = map->ctx;

    (void) group;
    map->pseudo = ctx->pseudo;
    uint32_t                      opcode = ctx->req->args_compound->argarray[map->res_index].argop;
    if (map->runtime_parent && !ctx->fh_consumed && !ctx->pseudo) {
        uint8_t  root[NFS4_FHSIZE];
        uint32_t length  = nfs4_vfs_namespace_root_fh(compound, ctx, root);
        bool     at_root = length && length == (uint32_t) ctx->fh_len && !memcmp(root, ctx->fh, length);
        map->parent_crossing = !at_root &&
            chimera_nfs4_fh_is_vfs_mount_root(ctx->req->thread->vfs, ctx->fh, ctx->fh_len);
    }
    if (map->runtime_lookup) {
        uint8_t  root[NFS4_FHSIZE];
        uint32_t length = nfs4_vfs_namespace_root_fh(compound, ctx, root);
        map->junction = !ctx->fh_consumed && (ctx->pseudo ||
                                              (ctx->namespace_root && ctx->current_identity &&
                                               ctx->current_identity->export.id == ctx->namespace_root->id &&
                                               length && length == (uint32_t) ctx->fh_len && !memcmp(ctx->fh, root,
                                                                                                     length)));
    }
    map->result_pseudo = opcode == OP_PUTFH || opcode == OP_PUTROOTFH || opcode == OP_PUTPUBFH ?
        map->planned_pseudo : opcode == OP_RESTOREFH ? ctx->saved_pseudo :
        map->parent_crossing ? !ctx->namespace_root : map->junction ? false : ctx->pseudo;
    map->identity       = nfs4_vfs_selected_identity(map);
    map->saved_identity = ctx->saved_identity;
    return map->identity ? &map->identity->cred : NULL;
} /* nfs4_vfs_group_cred */

/* Check namespace selections against the private execution cursor, including
* cursors produced by LOOKUP and RESTOREFH. Parent crossings and junctions
* choose frozen credential groups; SECINFO only selects an answer policy.
* Cold root comparisons run before cursor seeds and repeat on every retry. */
static bool
nfs4_vfs_namespace_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map,
    uint32_t                     index,
    enum chimera_vfs_error      *status)
{
    struct nfs4_vfs_compound_ctx     *ctx    = map->ctx;
    struct nfs_request               *req    = ctx->req;
    struct chimera_server_nfs_shared *shared = req->thread->shared;
    const struct nfs_argop4          *argop  = &req->args_compound->argarray[map->res_index];
    const xdr_opaque                 *name   = argop->argop == OP_LOOKUP ? &argop->oplookup.objname :
        argop->argop == OP_SECINFO ? &argop->opsecinfo.name : NULL;
    bool                              parent = argop->argop == OP_LOOKUPP;
    uint8_t                           root_fh[NFS4_FHSIZE];
    uint32_t                          root_len = 0, fh_len;

    if (ctx->pseudo && argop->argop == OP_LOOKUP && !map->junction && !ctx->fh_consumed) {
        if (!nfs4_vfs_component_matches(map, name, NULL)) {
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
        } else {
            map->verify_status = NFS4ERR_NOENT;
            *status            = CHIMERA_VFS_ENOENT;
        }
        return true;
    }
    if (ctx->pseudo && argop->argop == OP_SECINFO && !ctx->fh_consumed) {
        return nfs4_vfs_secinfo_junction(map, name, status);
    }
    if (ctx->pseudo || map->junction || (!name && !parent) || ctx->fh_consumed || map->attrdir ||
        (ctx->entry_export && map == &ctx->ops[0])) {
        return false;
    }
    const uint8_t *fh = chimera_vfs_compound_current_fh(compound, &fh_len);
    if (ctx->dynamic_identity && index == (uint32_t) map->prepare_index) {
        fh     = ctx->fh;
        fh_len = ctx->fh_len;
    }
    if (!fh_len) {
        return false;
    }
    evpl_mutex_lock(&shared->exports_lock);
    bool current = ctx->namespace_root ?
        shared->root_export_id == ctx->namespace_root->id && nfs4_vfs_export_matches(shared, ctx->namespace_root) :
        shared->root_export_id == 0;
    evpl_mutex_unlock(&shared->exports_lock);
    if (!current || (name && map->possible_junction != nfs4_vfs_possible_junction(req->thread, name))) {
        map->verify_status = NFS4ERR_DELAY;
        *status            = CHIMERA_VFS_EAGAIN;
        return true;
    }
    root_len = nfs4_vfs_namespace_root_fh(compound, ctx, root_fh);
    bool at_root = root_len && root_len == fh_len && !memcmp(root_fh, fh, fh_len);
    if (parent && at_root) {
        map->verify_status = NFS4ERR_NOENT;
        *status            = CHIMERA_VFS_ENOENT;
        return true;
    }
    bool mount_root = parent && chimera_nfs4_fh_is_vfs_mount_root(req->thread->vfs, fh, fh_len);
    if (mount_root && ctx->namespace_root && !root_len) {
        map->verify_status = NFS4ERR_SERVERFAULT;
        *status            = CHIMERA_VFS_EIO;
        return true;
    }
    if (mount_root && map->runtime_parent) {
        if (ctx->namespace_identity && ctx->namespace_root &&
            !chimera_nfs_export_sec_ok(&ctx->namespace_identity->export, req->sec_bit)) {
            map->verify_status = NFS4ERR_WRONGSEC;
            *status            = CHIMERA_VFS_EACCES;
            return true;
        }
        return false;
    }
    /* Every LOOKUPP has conditional parent selection. A possible junction
     * LOOKUP has conditional export selection; SECINFO only selects policy.
     * A multi-op run resolves cold root comparisons before either cursor is
     * seeded, unless an earlier ROOT/PUB supplies the root itself. No namespace
     * operation needs to accept a prefix and redispatch an unexecuted suffix. */
    bool unresolved = ctx->root_probe < 0 && !root_len;
    if (!parent && map->possible_junction && ctx->namespace_root &&
        ctx->namespace_root->id == nfs4_vfs_export_id(map) && (unresolved || at_root)) {
        if (argop->argop == OP_SECINFO && at_root) {
            return nfs4_vfs_secinfo_junction(map, name, status);
        }
        if (ctx->single || map->runtime_lookup) {
            /* The handler or credential-group selector already resolved this
            * crossing. A changed selection must be retried by the client. */
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
            return true;
        }
    }
    return false;
} /* nfs4_vfs_namespace_prepare */

/* LAYOUTCOMMIT trusts only a live writable layout for the session and the
 * execution cursor. Acquire without lease renewal and release the snapshot
 * before returning; the enclosing compound pins the session client. A retry
 * repeats the check, including any intervening LAYOUTRETURN or version bump. */
static nfsstat4
nfs4_vfs_layoutcommit_authorize(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs_request             *req    = map->ctx->req;
    const struct LAYOUTCOMMIT4args *args   = &req->args_compound->argarray[map->res_index].oplayoutcommit;
    struct nfs_state_table         *table  = &req->thread->shared->nfs4_state_table;
    struct nfs_layout_state        *layout = NULL;
    uint32_t                        fh_len;
    const uint8_t                  *fh     = chimera_vfs_compound_current_fh(compound, &fh_len);
    nfsstat4                        status = NFS4_OK;
    struct stateid4                 sid    = args->loca_stateid;

    if (chimera_nfs4_stateid_is_current(&sid)) {
        if (!map->ctx->current_stateid_valid) {
            return NFS4ERR_BAD_STATEID;
        }
        sid       = map->ctx->current_stateid;
        sid.seqid = 0;
    }

    if (!chimera_vfs_pnfs_feature_enabled(req->thread->vfs)) {
        return NFS4ERR_NOTSUPP;
    }
    if (!fh || !fh_len) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (nfs_layout_journal_granted(&map->ctx->layouts, fh, fh_len)) {
        status = nfs_layout_journal_commit_check(&map->ctx->layouts, fh, fh_len, &sid);
        goto validate_inputs;
    }
    if (!req->session || nfs_state_table_acquire_no_renew(table, &sid,
                                                          NFS4_SLOT_TYPE_LAYOUT, (void **) &layout, NULL) != NFS4_OK ||
        !layout) {
        return NFS4ERR_BAD_STATEID;
    }
    evpl_mutex_lock(&layout->client->lock);
    if (atomic_load(&layout->destroyed) || layout->client != map->ctx->client ||
        layout->fh_len != fh_len || memcmp(layout->fh, fh, fh_len)) {
        status = NFS4ERR_BAD_STATEID;
    } else if (sid.seqid && sid.seqid < layout->seqid) {
        status = NFS4ERR_OLD_STATEID;
    } else if (sid.seqid && sid.seqid > layout->seqid) {
        status = NFS4ERR_BAD_STATEID;
    } else if (layout->iomode != LAYOUTIOMODE4_RW) {
        status = NFS4ERR_BADLAYOUT;
    }
    evpl_mutex_unlock(&layout->client->lock);
    nfs_state_table_release(table, layout, NFS4_SLOT_TYPE_LAYOUT, req->thread->vfs_thread);
 validate_inputs:
    if (status == NFS4_OK &&
        ((args->loca_last_write_offset.no_newoffset && args->loca_last_write_offset.no_offset == UINT64_MAX) ||
         (args->loca_time_modify.nt_timechanged && args->loca_time_modify.nt_time.nseconds >= 1000000000))) {
        status = NFS4ERR_INVAL;
    }
    return status;
} /* nfs4_vfs_layoutcommit_authorize */

/* Metadata catches up with writes already authorized through the layout.
 * Recompute the high-water mark from this attempt's GETATTR; never shrink or
 * recall the very layout whose completed writes are being committed. */
static void
nfs4_vfs_layoutcommit_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op             *map  = private_data;
    const struct LAYOUTCOMMIT4args *args =
        &map->ctx->req->args_compound->argarray[map->res_index].oplayoutcommit;
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, map->vfs_aux)->attr;
    struct chimera_vfs_compound_op *op   = chimera_vfs_compound_op_args(compound, index);

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        *status = CHIMERA_VFS_EIO;
        return;
    }
    uint64_t                        want = args->loca_last_write_offset.no_newoffset ?
        args->loca_last_write_offset.no_offset + 1 : attr->va_size;
    memset(&op->set_attr, 0, sizeof(op->set_attr));
    if (want > attr->va_size) {
        op->set_attr.va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        op->set_attr.va_size      = want;
    }
    if (args->loca_time_modify.nt_timechanged) {
        op->set_attr.va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
        op->set_attr.va_mtime.tv_sec  = args->loca_time_modify.nt_time.seconds;
        op->set_attr.va_mtime.tv_nsec = args->loca_time_modify.nt_time.nseconds;
    }
    if (!op->set_attr.va_set_mask) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_layoutcommit_prepare */

/* Retirement checkpoints mutate only the private owner/state overlay. Public
 * slots, ranges and cache claims survive until the enclosing finish accepts. */
static const struct stateid4 *
nfs4_vfs_retirement_sid(
    struct nfs4_vfs_op    *map,
    const struct stateid4 *sid)
{
    if (chimera_nfs4_stateid_is_current(sid)) {
        return map->ctx->current_stateid_valid ? &map->ctx->current_stateid : NULL;
    }
    return sid;
} /* nfs4_vfs_retirement_sid */

static nfsstat4
nfs4_vfs_delegreturn_prepare(
    struct chimera_vfs_compound *compound,
    struct nfs4_vfs_op          *map)
{
    struct nfs4_vfs_compound_ctx *ctx = map->ctx;
    const struct stateid4        *sid = nfs4_vfs_retirement_sid(map,
                                                                &ctx->req->args_compound->argarray[map->res_index].
                                                                opdelegreturn.deleg_stateid);

    if (!sid || nfs4_vfs_stateid_closed(ctx, sid)) {
        return NFS4ERR_BAD_STATEID;
    }
    if (map->retirement_status != NFS4_OK) {
        return map->retirement_status;
    }
    if (!map->deleg_retirement || !map->deleg_retirement->deleg) {
        return NFS4ERR_BAD_STATEID;
    }
    struct nfs_delegation *deleg = map->deleg_retirement->deleg;
    uint32_t               fh_len;
    const uint8_t         *fh = nfs4_vfs_state_fh(compound, map, &fh_len);
    if (!fh) {
        return NFS4ERR_NOFILEHANDLE;
    }
    if (deleg->fh_len != fh_len || memcmp(deleg->fh, fh, fh_len)) {
        return NFS4ERR_BAD_STATEID;
    }
    if (atomic_load_explicit(&deleg->revoked, memory_order_acquire)) {
        return NFS4ERR_DELEG_REVOKED;
    }
    nfsstat4 status = nfs4_stateid_check_seqid(deleg->seqid, sid->seqid);
    if (status != NFS4_OK) {
        return status;
    }
    map->deleg_retirement->retired = true;
    map->retirement_stateid        = *sid;
    map->retirement_passed         = true;
    nfs4_vfs_rebuild_state_view(ctx, NULL);
    return NFS4_OK;
} /* nfs4_vfs_delegreturn_prepare */

static nfsstat4
nfs4_vfs_free_stateid_prepare(struct nfs4_vfs_op *map)
{
    struct nfs4_vfs_compound_ctx *ctx = map->ctx;
    struct nfs_request           *req = ctx->req;
    const struct stateid4        *sid = nfs4_vfs_retirement_sid(map,
                                                                &req->args_compound->argarray[map->res_index].
                                                                opfree_stateid.fsa_stateid);

    if (!sid || nfs4_stateid_is_special(sid) || nfs4_vfs_stateid_closed(ctx, sid)) {
        return NFS4ERR_BAD_STATEID;
    }
    struct nfs4_vfs_lock         *lock = nfs4_vfs_private_lock(ctx, sid);
    struct nfs4_vfs_state        *open = nfs4_vfs_private_open(ctx, sid);
    nfsstat4                      status;
    if (lock || open) {
        struct nfs4_vfs_state *parent = lock ? lock->parent : open;
        if (!parent || parent->closed || parent->shadow.owner->client != ctx->client) {
            return NFS4ERR_BAD_STATEID;
        }
        if (!nfs_open_state_check_principal(&parent->shadow, req->principal_flavor,
                                            req->principal_machinename, req->principal_machinename_len)) {
            return NFS4ERR_ACCESS;
        }
        status = nfs4_stateid_check_seqid(lock ? lock->seqid : parent->shadow.seqid, sid->seqid);
        if (status != NFS4_OK) {
            return status;
        }
        if (!lock || !nfs_lock_range_journal_empty(lock->ranges)) {
            return NFS4ERR_LOCKS_HELD;
        }
        lock->retired = true;
    } else if (map->deleg_retirement) {
        struct nfs_delegation *deleg = map->deleg_retirement->deleg;
        status = nfs4_stateid_check_seqid(deleg->seqid, sid->seqid);
        if (status != NFS4_OK) {
            return status;
        }
        if (!atomic_load_explicit(&deleg->revoked, memory_order_acquire)) {
            return NFS4ERR_LOCKS_HELD;
        }
        map->deleg_retirement->retired = true;
    } else {
        if (map->retirement_status != NFS4_OK) {
            status = map->retirement_status;
        } else if (nfs_layout_journal_test(&ctx->layouts, sid, &status)) {
            status = status == NFS4_OK ? NFS4ERR_LOCKS_HELD : status;
        } else if (map->stateid_reservation_status != NFS4_OK) {
            /* Preserve OPEN/LOCK admission errors when no private state was
             * installed. A public lookup cannot authorize the denied owner. */
            status = map->stateid_reservation_status;
        } else {
            void   *state;
            uint8_t type;
            status = nfs_state_table_acquire_retirement(&req->thread->shared->nfs4_state_table, sid, &state, &type);
            if (status == NFS4_OK) {
                status = nfs_state_check_client(state, type, ctx->client);
                if (status == NFS4_OK) {
                    status = NFS4ERR_LOCKS_HELD;
                }
                nfs_state_table_release(&req->thread->shared->nfs4_state_table, state, type, req->thread->vfs_thread);
            }
        }
        return status == NFS4ERR_STALE_STATEID ? NFS4ERR_BAD_STATEID : status;
    }
    map->retirement_stateid = *sid;
    map->retirement_passed  = true;
    return NFS4_OK;
} /* nfs4_vfs_free_stateid_prepare */

static nfsstat4
nfs4_vfs_release_lockowner_prepare(struct nfs4_vfs_op *map)
{
    struct nfs4_vfs_compound_ctx *ctx   = map->ctx;
    const struct state_owner4    *owner = &ctx->req->args_compound->argarray[map->res_index].oprelease_lockowner.
        lock_owner;

    if (!map->client || owner->clientid != map->client->client_id) {
        return NFS4ERR_STALE_CLIENTID;
    }
    if (!map->lock_group) {
        return map->retirement_status ? map->retirement_status : NFS4ERR_DELAY;
    }
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock *lock = &ctx->locks[i];
        if (lock->group == map->lock_group && lock->active && !lock->retired &&
            lock->parent && !lock->parent->closed && !nfs_lock_range_journal_empty(lock->ranges)) {
            return NFS4ERR_LOCKS_HELD;
        }
    }
    for (uint32_t i = 0; i < ctx->num_locks; i++) {
        struct nfs4_vfs_lock *lock = &ctx->locks[i];
        if (lock->group == map->lock_group && lock->active) {
            lock->retired = true;
        }
    }
    map->lock_group->release_pending    = true;
    map->lock_group->owner_replay.seqid = 0;
    memset(&map->lock_group->owner_replay.replay, 0, sizeof(map->lock_group->owner_replay.replay));
    map->lock_group->owner_replay.dirty = true;
    return NFS4_OK;
} /* nfs4_vfs_release_lockowner_prepare */

static nfsstat4
nfs4_vfs_layoutreturn_check(struct nfs4_vfs_op *map)
{
    const struct LAYOUTRETURN4args *args = &map->ctx->req->args_compound->argarray[map->res_index].oplayoutreturn;

    if (!chimera_vfs_pnfs_feature_enabled(map->ctx->req->thread->shared->vfs)) {
        return NFS4ERR_NOTSUPP;
    }
    if (args->lora_layout_type != LAYOUT4_FLEX_FILES && args->lora_layout_type != LAYOUT4_BLOCK_VOLUME &&
        args->lora_layout_type != LAYOUT4_SCSI) {
        return NFS4ERR_UNKNOWN_LAYOUTTYPE;
    }
    if (!map->ctx->client) {
        return NFS4ERR_BAD_STATEID;
    }
    if (args->lora_layoutreturn.lr_returntype != LAYOUTRETURN4_ALL &&
        args->lora_layoutreturn.lr_returntype != LAYOUTRETURN4_FILE &&
        args->lora_layoutreturn.lr_returntype != LAYOUTRETURN4_FSID) {
        return NFS4ERR_INVAL;
    }
    return NFS4_OK;
} /* nfs4_vfs_layoutreturn_check */

/* Reservation is an explicit, memoized coordination effect, not a retryable
 * frontend callback. Only the following checkpoint stages the return. */
static void
nfs4_vfs_layoutreturn_coordinate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data)
{
    struct nfs4_vfs_op             *map    = private_data;
    struct nfs4_vfs_compound_ctx   *ctx    = map->ctx;
    const struct LAYOUTRETURN4args *args   = &ctx->req->args_compound->argarray[map->res_index].oplayoutreturn;
    nfsstat4                        status = nfs4_vfs_layoutreturn_check(map);

    (void) index;
    fh = nfs4_vfs_state_fh(compound, map, &fh_len);
    if (status == NFS4_OK && args->lora_layoutreturn.lr_returntype == LAYOUTRETURN4_FILE && !fh) {
        status = NFS4ERR_NOFILEHANDLE;
    }
    if (status == NFS4_OK && args->lora_layoutreturn.lr_returntype != LAYOUTRETURN4_FSID) {
        status = nfs_layout_journal_reserve_return(&ctx->layouts, ctx->client,
                                                   &ctx->req->thread->shared->nfs4_state_table, &ctx->req->thread->
                                                   shared->nfs4_layout_table,
                                                   fh, fh_len, args->lora_layoutreturn.lr_returntype ==
                                                   LAYOUTRETURN4_ALL);
    }
    map->retirement_status = status;
    chimera_vfs_compound_coordinate_done(compound, token, CHIMERA_VFS_OK);
} /* nfs4_vfs_layoutreturn_coordinate */

static void
nfs4_vfs_layoutreturn_stage(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op             *map  = private_data;
    struct nfs4_vfs_compound_ctx   *ctx  = map->ctx;
    const struct LAYOUTRETURN4args *args = &ctx->req->args_compound->argarray[map->res_index].oplayoutreturn;
    uint32_t                        fh_len;
    const uint8_t                  *fh     = nfs4_vfs_state_fh(compound, map, &fh_len);
    nfsstat4                        result = map->retirement_status;

    (void) index;
    if (result == NFS4_OK && args->lora_layoutreturn.lr_returntype != LAYOUTRETURN4_FSID) {
        const struct stateid4 *sid = args->lora_layoutreturn.lr_returntype == LAYOUTRETURN4_FILE ?
            nfs4_vfs_retirement_sid(map, &args->lora_layoutreturn.lr_layout.lrf_stateid) : NULL;
        if (!sid && args->lora_layoutreturn.lr_returntype == LAYOUTRETURN4_FILE) {
            result = NFS4ERR_BAD_STATEID;
        } else {
            result = nfs_layout_journal_return(&ctx->layouts, fh, fh_len, sid, args->lora_layout_type,
                                               args->lora_layoutreturn.lr_returntype == LAYOUTRETURN4_ALL);
        }
    }
    if (result != NFS4_OK) {
        map->verify_status = result;
        *status            = CHIMERA_VFS_EINVAL;
    }
} /* nfs4_vfs_layoutreturn_stage */

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

    bool                            root = argop->argop == OP_PUTROOTFH || argop->argop == OP_PUTPUBFH;

    if (index == (uint32_t) map->prepare_index) {
        error = nfs4_op_check_minor(argop->argop, req->minorversion, map->res_index, req->seen_sequence);
        if (error != NFS4_OK) {
            map->verify_status = error;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        map->client = req->minorversion ? ctx->client :
            nfs4_vfs_owner_operation(argop->argop) ? map->owner_client : ctx->current_client;
    }

    if (ctx->dynamic_identity && index == (uint32_t) map->prepare_index) {
        map->identity       = nfs4_vfs_selected_identity(map);
        map->saved_identity = ctx->saved_identity;
        map->pseudo         = ctx->pseudo;
        map->result_pseudo  = argop->argop == OP_PUTFH || root ? map->planned_pseudo :
            argop->argop == OP_RESTOREFH ? ctx->saved_pseudo :
            map->parent_crossing ? !ctx->namespace_root : map->junction ? false : ctx->pseudo;
    }

    if (argop->argop == OP_PUTFH && map->planned_pseudo && ctx->namespace_root) {
        map->verify_status = NFS4ERR_STALE;
        *status            = CHIMERA_VFS_ESTALE;
        return;
    }
    if (map->result_pseudo || map->pseudo) {
        struct chimera_server_nfs_shared *shared = req->thread->shared;
        evpl_mutex_lock(&shared->exports_lock);
        bool                              current = shared->root_export_id == 0;
        evpl_mutex_unlock(&shared->exports_lock);
        if (!current) {
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }
    }
    if (map->identity) {
        struct chimera_server_nfs_shared *shared   = req->thread->shared;
        const struct nfs4_vfs_identity   *identity = map->identity;
        evpl_mutex_lock(&shared->exports_lock);
        bool                              current = identity->present ? nfs4_vfs_export_matches(shared, &identity->
                                                                                                export) :
            shared->exports_by_id[identity->export.id] == NULL;
        if (root) {
            current = current && shared->root_export_id == identity->export.id;
        }
        if (map->saved_identity && (argop->argop == OP_RENAME || argop->argop == OP_LINK)) {
            current = current && (map->saved_identity->present ?
                                  nfs4_vfs_export_matches(shared, &map->saved_identity->export) :
                                  shared->exports_by_id[map->saved_identity->export.id] == NULL);
        }
        evpl_mutex_unlock(&shared->exports_lock);
        if (!current) {
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }
    }

    if (map->junction && index == (uint32_t) map->prepare_index) {
        struct chimera_server_nfs_shared *shared = req->thread->shared;
        uint8_t                           root_fh[NFS4_FHSIZE];
        uint32_t                          root_len = nfs4_vfs_namespace_root_fh(compound, ctx, root_fh);
        evpl_mutex_lock(&shared->exports_lock);
        bool                              current = map->pseudo ? shared->root_export_id == 0 :
            ctx->namespace_root && shared->root_export_id == ctx->namespace_root->id &&
            nfs4_vfs_export_matches(shared, ctx->namespace_root);
        evpl_mutex_unlock(&shared->exports_lock);
        if (!map->identity || !current || ctx->fh_consumed ||
            (!map->pseudo && (!root_len || ctx->fh_len != (int) root_len || memcmp(ctx->fh, root_fh, ctx->fh_len))) ||
            !nfs4_vfs_component_matches(map, &argop->oplookup.objname, map->identity)) {
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }
        if (!chimera_nfs_export_sec_ok(&map->identity->export, req->sec_bit)) {
            map->verify_status = NFS4ERR_WRONGSEC;
            *status            = CHIMERA_VFS_EACCES;
            return;
        }
        const char *path = map->identity->export.path;
        while (*path == '/') {
            path++;
        }
        if (!*path) {
            map->verify_status = NFS4ERR_NOENT;
            *status            = CHIMERA_VFS_ENOENT;
            return;
        }
    }

    if (map->saved_seed == (int) index || map->current_seed == (int) index) {
        bool    saved = map->saved_seed == (int) index;
        int     len   = saved ? ctx->saved_fhlen : ctx->fh_len;
        uint8_t base[NFS4_FHSIZE];
        memcpy(base, saved ? ctx->saved_fh : ctx->fh, len);
        if ((!saved && !len) || fh_is_nfs4_root(base, len)) {
            chimera_vfs_compound_op_skip(compound, index);
        } else {
            if (!nfs4_vfs_cursor_base(req->thread, base, &len)) {
                map->verify_status = NFS4ERR_BADHANDLE;
                *status            = CHIMERA_VFS_EINVAL;
                return;
            }
            memcpy(args->arg_fh, base, len);
            args->arg_fh_len = len;
        }
    }

    if (!map->terminal_status &&
        (!(map->runtime_lookup || map->runtime_parent) || index == (uint32_t) map->prepare_index) &&
        nfs4_vfs_namespace_prepare(compound, map, index, status)) {
        return;
    }

    if (index == (uint32_t) map->prepare_index) {
        if (map->identity) {
            error = nfs4_rofs_policy(argop, map->identity->export.access & CHIMERA_NFS_EXPORT_ACCESS_RO,
                                     map->saved_identity != NULL,
                                     map->saved_identity && (map->saved_identity->export.access &
                                                             CHIMERA_NFS_EXPORT_ACCESS_RO));
        }
        if (error == NFS4_OK && argop->argop == OP_LAYOUTCOMMIT &&
            !chimera_vfs_pnfs_feature_enabled(req->thread->shared->vfs)) {
            error = NFS4ERR_NOTSUPP;
        }
        if (error == NFS4_OK && (argop->argop == OP_READ || argop->argop == OP_READ_PLUS ||
                                 argop->argop == OP_WRITE)) {
            error = nfs_recovery_io_check(&req->thread->shared->nfs4_recovery);
        }
        if (error == NFS4_OK && map->terminal_status &&
            (argop->argop == OP_SEEK || argop->argop == OP_WRITE_SAME)) {
            error = map->terminal_status;
        }
        if (error != NFS4_OK) {
            map->verify_status = error;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        if (ctx->fh_consumed && argop->argop != OP_PUTFH && argop->argop != OP_RESTOREFH &&
            argop->argop != OP_PUTROOTFH && argop->argop != OP_PUTPUBFH &&
            argop->argop != OP_TEST_STATEID && !nfs4_vfs_protocol_op(argop->argop) &&
            !nfs4_vfs_retirement_handle_free(argop->argop)) {
            map->verify_status = NFS4ERR_NOFILEHANDLE;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        if (argop->argop == OP_PUTFH || argop->argop == OP_RESTOREFH ||
            argop->argop == OP_PUTROOTFH || argop->argop == OP_PUTPUBFH) {
            ctx->fh_consumed = 0;
        }

        if (!map->terminal_status && map->identity && (argop->argop == OP_PUTFH ||
                                                       (root && !chimera_nfs4_next_op_handles_wrongsec(req, map->
                                                                                                       res_index))) &&
            !chimera_nfs_export_sec_ok(&map->identity->export, req->sec_bit)) {
            map->verify_status = NFS4ERR_WRONGSEC;
            *status            = CHIMERA_VFS_EACCES;
            return;
        }
        if (map->terminal_status) {
            map->verify_status = map->terminal_status;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        if (argop->argop == OP_SETATTR && argop->opsetattr.obj_attributes.num_attrmask &&
            (argop->opsetattr.obj_attributes.attrmask[0] & (1U << FATTR4_SIZE))) {
            error = nfs_recovery_io_check(&req->thread->shared->nfs4_recovery);
            if (error != NFS4_OK) {
                map->verify_status = error;
                *status            = CHIMERA_VFS_EINVAL;
                return;
            }
        }
        error = nfs4_vfs_synthetic_status(argop->argop, map->pseudo, map->attrdir);
        if (error == NFS4_OK && (argop->argop == OP_COPY || argop->argop == OP_CLONE ||
                                 argop->argop == OP_LINK || argop->argop == OP_RENAME) &&
            (ctx->saved_pseudo || chimera_nfs4_fh_is_attrdir(ctx->saved_fh, ctx->saved_fhlen))) {
            /* LINK compares mounts before source lookup. RENAME does too
             * unless delegation recall first resolves the saved source. */
            error = argop->argop == OP_LINK ||
                (argop->argop == OP_RENAME &&
                 !chimera_server_config_get_nfs4_delegations(req->thread->shared->config)) ?
                NFS4ERR_XDEV : NFS4ERR_STALE;
        }

        if (error != NFS4_OK) {
            if (argop->argop == OP_LAYOUTCOMMIT &&
                !chimera_vfs_pnfs_feature_enabled(req->thread->shared->vfs)) {
                error = NFS4ERR_NOTSUPP;
            }
            map->verify_status = error;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        if (nfs4_vfs_protocol_op(argop->argop)) {
            if (argop->argop == OP_GETDEVICEINFO) {
                struct chimera_vfs_layout_device        device;
                const struct chimera_vfs_layout_device *private_device = NULL;
                for (struct nfs4_vfs_op *prior = ctx->ops; prior < map; prior++) {
                    if (prior->layoutget && nfs4_layoutget_device(prior->layoutget, compound,
                                                                  argop->opgetdeviceinfo.gdia_device_id, &device)) {
                        private_device = &device;
                        break;
                    }
                }
                memset(&map->device_result, 0, sizeof(map->device_result));
                error = chimera_nfs4_getdeviceinfo_fill(req->thread, req, &argop->opgetdeviceinfo,
                                                        &map->device_result, private_device);
            } else {
                error = chimera_nfs4_protocol_status(req, argop);
            }
            map->verify_status = error;
            if (error != NFS4_OK) {
                *status = CHIMERA_VFS_EINVAL;
            }
            return;
        }
        map->owner_checked = map->owner_client != NULL;
        error              = nfs4_vfs_replay_prepare(compound, map);
        if (error != NFS4_OK || map->replay_hit) {
            map->verify_status = error;
            if (error != NFS4_OK) {
                *status = CHIMERA_VFS_EINVAL;
            }
            return;
        }

        /* Failed admission belongs to this wire operation. In particular,
         * a stale later OPEN client must not discard a valid earlier prefix.
         * CLOSE tombstone replay was allowed to answer before this gate. */
        if (map->owner_status != NFS4_OK) {
            map->verify_status = map->owner_status;
            *status            = CHIMERA_VFS_EINVAL;
            return;
        }
        switch (argop->argop) {
            case OP_READDIR:
                if (argop->opreaddir.maxcount < 16) {
                    error = NFS4ERR_TOOSMALL;
                } else {
                    error = chimera_nfs4_validate_getattr_request(argop->opreaddir.num_attr_request,
                                                                  argop->opreaddir.attr_request);
                    if (error == NFS4_OK && (argop->opreaddir.cookie == 1 || argop->opreaddir.cookie == 2)) {
                        error = NFS4ERR_BAD_COOKIE;
                    }
                    if (error == NFS4_OK) {
                        error = map->pseudo && map->root_readdir ? map->root_readdir_status : map->input_status;
                    }
                }
                break;

            case OP_OPENATTR:
            {
                uint32_t       length;
                const uint8_t *fh = chimera_vfs_compound_current_fh(compound, &length);
                if (map->attrdir || !chimera_server_config_get_named_streams(req->thread->shared->config) ||
                    !(chimera_vfs_module_capabilities(req->thread->vfs_thread, fh, length) &
                      CHIMERA_VFS_CAP_NAMED_STREAMS) ||
                    length > CHIMERA_VFS_FH_SIZE - CHIMERA_NFS4_ATTRDIR_MAGIC_LEN) {
                    /* The wrapped NFS handle must have room for the marker as
                    * well as the base, including when returned by GETATTR. */
                    error = NFS4ERR_NOTSUPP;
                }
                ctx->current_stateid_valid = 0;
                break;
            }
            case OP_OPEN:
            {
                bool reclaim = argop->opopen.claim.claim == CLAIM_PREVIOUS;
                error = nfs_recovery_open_check(&req->thread->shared->nfs4_recovery,
                                                map->client, reclaim);
                if (error == NFS4_OK && req->minorversion > 0 && req->session) {
                    bool done = nfs4_client_reclaim_complete(&req->thread->shared->nfs4_shared_clients,
                                                             req->session->nfs4_session_clientid);
                    if (reclaim && done) {
                        error = NFS4ERR_NO_GRACE;
                    } else if (!reclaim && !done) {
                        error = NFS4ERR_GRACE;
                    }
                }
                if (error == NFS4_OK && map->pseudo) {
                    /* Keep the standalone root OPEN's recovery/owner ordering
                     * and never let its prebuilt helpers address another cursor. */
                    error = nfs4_vfs_open_is_delegated(&argop->opopen) ?
                        NFS4ERR_BAD_STATEID : NFS4ERR_STALE;
                }
                if (error == NFS4_OK) {
                    error = map->input_status;
                }
                if (error == NFS4_OK && argop->opopen.claim.claim == CLAIM_DELEG_CUR_FH) {
                    struct chimera_claim_actor actor;
                    uint32_t                   length;
                    const uint8_t             *fh = chimera_vfs_compound_current_fh(compound, &length);
                    error = nfs4_vfs_stateid_closed(ctx, &argop->opopen.claim.oc_delegate_stateid) ?
                        NFS4ERR_BAD_STATEID : nfs_state_table_delegation_io(&req->thread->shared->nfs4_state_table,
                                                                            &argop->opopen.claim.oc_delegate_stateid,
                                                                            map->client, fh,
                                                                            length,
                                                                            argop->opopen.share_access &
                                                                            OPEN4_SHARE_ACCESS_BOTH, &actor);
                }
                break;
            }
            case OP_LOCKT:
            {
                error = nfs_recovery_io_check(&req->thread->shared->nfs4_recovery);
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
            case OP_DELEGRETURN:
                error = nfs4_vfs_delegreturn_prepare(compound, map);
                break;
            case OP_FREE_STATEID:
                error = nfs4_vfs_free_stateid_prepare(map);
                break;
            case OP_RELEASE_LOCKOWNER:
                error = nfs4_vfs_release_lockowner_prepare(map);
                break;
            case OP_LAYOUTRETURN:
                error = nfs4_vfs_layoutreturn_check(map);
                if (error == NFS4_OK && argop->oplayoutreturn.lora_layoutreturn.lr_returntype == LAYOUTRETURN4_FILE) {
                    if (ctx->fh_consumed || !ctx->fh_len) {
                        error = NFS4ERR_NOFILEHANDLE;
                    } else if (map->pseudo || map->attrdir) {
                        /* Synthetic identities cannot own a layout. Reject before
                         * COORDINATE, which requires a real backend cursor, and
                         * never reserve the attribute directory's base layout. */
                        error = NFS4ERR_BAD_STATEID;
                    }
                }
                break;
            case OP_TEST_STATEID:
                nfs4_vfs_test_stateids(map);
                break;
            case OP_IO_ADVISE:
                error = nfs4_vfs_io_advise_prepare(compound, map);
                break;
            case OP_LAYOUTGET:
                error = nfs4_vfs_layoutget_authorize(compound, map);
                break;
            case OP_LAYOUTCOMMIT:
                error = nfs4_vfs_layoutcommit_authorize(compound, map);
                break;
            case OP_SECINFO_NO_NAME:
                if (argop->opsecinfo_no_name != SECINFO_STYLE4_CURRENT_FH &&
                    argop->opsecinfo_no_name != SECINFO_STYLE4_PARENT) {
                    error = NFS4ERR_INVAL;
                } else if (ctx->pseudo && argop->opsecinfo_no_name == SECINFO_STYLE4_PARENT) {
                    error = NFS4ERR_NOENT;
                }
                break;
            case OP_CLOSE:
                error = nfs4_vfs_close_prepare(compound, map, &argop->opclose.open_stateid);
                break;
            case OP_PUTFH:
            case OP_PUTROOTFH:
            case OP_PUTPUBFH:
            case OP_LOOKUP:
            case OP_LOOKUPP:
                if (ctx->pseudo && argop->argop == OP_LOOKUPP) {
                    error = NFS4ERR_NOENT;
                }
                ctx->current_stateid_valid = 0;
                break;
            case OP_CREATE:
                error                      = map->input_status;
                ctx->current_stateid_valid = 0;
                break;
            case OP_SETATTR:
                error = map->input_status;
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
            if (error == NFS4_OK && !data_server_io && !nfs4_stateid_is_special(sid) &&
                argop->argop != OP_READ && argop->argop != OP_READ_PLUS && argop->argop != OP_WRITE) {
                struct nfs4_stateid_view view;
                nfs4_stateid_decode(&view, sid);
                if (view.type != NFS4_STATEID_TYPE_OPEN && view.type != NFS4_STATEID_TYPE_LOCK) {
                    error = NFS4ERR_BAD_STATEID;
                }
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
    if (index == (uint32_t) map->prepare_index && argop->argop == OP_SETATTR && map->io_authorize) {
        uint32_t       fh_len;
        const uint8_t *fh = chimera_vfs_compound_current_fh(compound, &fh_len);
        if (nfs_layout_journal_truncate_conflict(&ctx->layouts, fh, fh_len)) {
            /* The client cannot return a newly granted layout before receiving
             * this reply. Accept its prefix and report DELAY in the same finish
             * instead of entering a recall wait on our own unpublished grant. */
            map->verify_status = NFS4ERR_DELAY;
            *status            = CHIMERA_VFS_EAGAIN;
            return;
        }
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
    if ((argop->argop == OP_SETATTR || argop->argop == OP_LINK) && index == (uint32_t) map->vfs_res) {
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

static void
nfs4_vfs_lookup_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op *map = private_data;

    if (index == (uint32_t) map->prepare_index || index == (uint32_t) map->current_seed) {
        nfs4_vfs_operation_prepare(compound, index, status, map);
    }
    if (*status != CHIMERA_VFS_OK || index < (uint32_t) map->junction_start || index == (uint32_t) map->vfs_hi) {
        return;
    }
    bool                export_path = index <= (uint32_t) map->junction_end;
    if (export_path != map->junction) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_lookup_prepare */

static void
nfs4_vfs_parent_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op           *map = private_data;
    struct nfs4_vfs_compound_ctx *ctx = map->ctx;

    if (index == (uint32_t) map->prepare_index || index == (uint32_t) map->current_seed) {
        nfs4_vfs_operation_prepare(compound, index, status, map);
    }
    if (*status != CHIMERA_VFS_OK || index < (uint32_t) map->parent_seed) {
        return;
    }
    if (index == (uint32_t) map->parent_seed) {
        if (map->parent_crossing && ctx->namespace_root) {
            struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(compound, index);
            op->arg_fh_len = nfs4_vfs_namespace_root_fh(compound, ctx, op->arg_fh);
        } else {
            chimera_vfs_compound_op_skip(compound, index);
        }
    } else if (index != (uint32_t) map->vfs_hi && map->parent_crossing) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_parent_prepare */

/* Every conditional SECINFO helper completes, including skipped helpers, so
 * its wire map has a contiguous accepted result range. The v4.0 directory
 * restore is needed only when an ordinary backend name lookup actually ran. */
static void
nfs4_vfs_secinfo_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op *map = private_data;

    if (index == (uint32_t) map->prepare_index || index == (uint32_t) map->vfs_res) {
        nfs4_vfs_operation_prepare(compound, index, status, map);
    }
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (map->secinfo_junction) {
        chimera_vfs_compound_op_skip(compound, index);
    } else if (map->ctx->req->minorversion == 0 && index == (uint32_t) map->vfs_hi) {
        nfs4_vfs_namespace_restore(compound, index, status, map);
    }
} /* nfs4_vfs_secinfo_prepare */

/* Keep synthetic namespace cursors out of backend operations. This wrapper
 * only selects already-built branches; callbacks and output stay private until
 * the one enclosing finish accepts them. Each retry repeats the selection. */
static void
nfs4_vfs_namespace_op_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_op                *map     = private_data;
    struct nfs4_vfs_compound_ctx      *ctx     = map->ctx;
    uint32_t                           opcode  = ctx->req->args_compound->argarray[map->res_index].argop;
    chimera_vfs_compound_op_callback_t prepare = ctx->namespace_prepare[index];

    if (index == (uint32_t) map->prepare_index) {
        const struct nfs_argop4 *argop = &ctx->req->args_compound->argarray[map->res_index];
        uint64_t                 bound = chimera_nfs4_reply_op_bound(ctx->req, argop);
        nfsstat4                 error = chimera_nfs4_reply_check(ctx->req, map->res_index,
                                                                  ctx->reply_bytes + bound,
                                                                  ctx->reply_chunk_bytes, true);
        if (error != NFS4_OK) {
            map->verify_status = error;
            *status            = CHIMERA_VFS_ENOSPC;
            return;
        }
        map->reply_mark = ctx->req->encoding->dbuf->used;
        if (opcode == OP_READDIR) {
            uint64_t reserved = nfs4_vfs_reply_reserved(ctx, map->reply_mark, 0);
            map->readdir_cursor.reply_reserve = reserved;
            nfs4_root_readdir_reset(map->root_readdir, reserved);
        }
        /* The saved callback may perform namespace/state admission itself.
         * Let it establish this wire operation's private source/result kind. */
        if (prepare) {
            prepare(compound, index, status, ctx->namespace_context[index]);
        }
    } else if (index == (uint32_t) map->saved_seed || index == (uint32_t) map->current_seed) {
        nfs4_vfs_operation_prepare(compound, index, status, map);
    } else if (map->saved_seed >= 0 && index == (uint32_t) map->saved_seed + 1 && ctx->saved_pseudo) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    } else if (map->root_readdir && index >= (uint32_t) map->root_readdir_start && !map->pseudo) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    } else if (map->pseudo && !map->junction && opcode != OP_PUTFH && opcode != OP_PUTROOTFH &&
               opcode != OP_PUTPUBFH && opcode != OP_RESTOREFH &&
               !(map->root_readdir && index >= (uint32_t) map->root_readdir_start) &&
               opcode != OP_TEST_STATEID && !nfs4_vfs_protocol_op(opcode) &&
               !nfs4_vfs_retirement_handle_free(opcode) && opcode != OP_DELEGRETURN &&
               opcode != OP_SECINFO && opcode != OP_SECINFO_NO_NAME) {
        chimera_vfs_compound_op_skip(compound, index);
        return;
    } else if (prepare) {
        prepare(compound, index, status, ctx->namespace_context[index]);
    }
    if (*status != CHIMERA_VFS_OK) {
        return;
    }
    if (map->saved_seed >= 0 && index == (uint32_t) map->saved_seed + 1 && ctx->saved_pseudo) {
        chimera_vfs_compound_op_skip(compound, index);
    } else if (map->result_pseudo && (opcode == OP_PUTFH || opcode == OP_PUTROOTFH ||
                                      opcode == OP_PUTPUBFH || opcode == OP_RESTOREFH)) {
        chimera_vfs_compound_op_skip(compound, index);
    } else if (map->pseudo && !map->junction && opcode != OP_PUTFH && opcode != OP_PUTROOTFH &&
               opcode != OP_PUTPUBFH && opcode != OP_RESTOREFH &&
               opcode != OP_TEST_STATEID && !nfs4_vfs_protocol_op(opcode) &&
               !nfs4_vfs_retirement_handle_free(opcode) && opcode != OP_DELEGRETURN &&
               opcode != OP_SECINFO_NO_NAME &&
               !(map->root_readdir && index >= (uint32_t) map->root_readdir_start)) {
        chimera_vfs_compound_op_skip(compound, index);
    }
} /* nfs4_vfs_namespace_op_prepare */

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

    if (index == (uint32_t) map->prepare_index) {
        nfs4_vfs_operation_prepare(compound, index, status, map);
        if (*status != CHIMERA_VFS_OK) {
            return;
        }
    }

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
        const struct stateid4 *endpoints[] = { src_sid, dst_sid };
        for (uint32_t i = 0; i < 2; i++) {
            if (nfs4_stateid_is_special(endpoints[i])) {
                if (copy) {
                    continue;
                }
            } else {
                struct nfs4_stateid_view view;
                nfs4_stateid_decode(&view, endpoints[i]);
                if (view.type == NFS4_STATEID_TYPE_OPEN || view.type == NFS4_STATEID_TYPE_LOCK) {
                    continue;
                }
            }
            error = NFS4ERR_BAD_STATEID;
            goto denied;
        }
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
        } else if (!copy && (op->src_handle->vfs_module != op->in_handle->vfs_module ||
                             !(op->in_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE))) {
            error = NFS4ERR_NOTSUPP;
        } else if (!op->length) {
            chimera_vfs_compound_op_skip(compound, index);
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

    map->input_status = nfs4_vfs_decode_attrs(map, &args->createattrs, &attr);
    if (map->input_status != NFS4_OK) {
        /* The prepare callout reports the decode error before opening the
         * parent or creating anything, including on a rejected-finish retry. */
        return chimera_vfs_compound_add_checkpoint(compound);
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
nfs4_vfs_data_op(uint32_t opcode)
{
    return opcode == OP_READ || opcode == OP_READ_PLUS || opcode == OP_WRITE ||
           opcode == OP_SETATTR || opcode == OP_ALLOCATE || opcode == OP_DEALLOCATE ||
           opcode == OP_SEEK || opcode == OP_WRITE_SAME;
} /* nfs4_vfs_data_op */

/* Immutable request checks shared by the scan and single-op dispatch. */
static nfsstat4
nfs4_vfs_data_input_status(const struct nfs_argop4 *argop)
{
    switch (argop->argop) {
        case OP_ALLOCATE:
            return argop->opallocate.aa_offset > UINT64_MAX - argop->opallocate.aa_length ?
                   NFS4ERR_INVAL : NFS4_OK;
        case OP_DEALLOCATE:
            return argop->opdeallocate.da_offset > UINT64_MAX - argop->opdeallocate.da_length ?
                   NFS4ERR_INVAL : NFS4_OK;
        case OP_SEEK:
            return argop->opseek.sa_what == NFS4_CONTENT_DATA || argop->opseek.sa_what == NFS4_CONTENT_HOLE ?
                   NFS4_OK : NFS4ERR_INVAL;
        case OP_WRITE_SAME:
        {
            const struct app_data_block4 *adb = &argop->opwrite_same.wsa_adb;
            if (adb->adb_reloff_blocknum != NFS4_UINT64_MAX) {
                return NFS4ERR_UNION_NOTSUPP;
            }
            if (!adb->adb_block_size || adb->adb_block_size > UINT32_MAX ||
                adb->adb_reloff_pattern > adb->adb_block_size ||
                adb->adb_pattern.len > adb->adb_block_size - adb->adb_reloff_pattern ||
                (adb->adb_block_count && adb->adb_block_size > UINT64_MAX / adb->adb_block_count)) {
                return NFS4ERR_INVAL;
            }
            return NFS4_OK;
        }
        case OP_SETATTR:
            return chimera_nfs4_validate_createattrs(argop->opsetattr.obj_attributes.num_attrmask,
                                                     argop->opsetattr.obj_attributes.attrmask);
        default:
            return NFS4_OK;
    } /* switch */
} /* nfs4_vfs_data_input_status */

/* Inputs which cannot require backend work are represented by a failing
 * checkpoint. Runtime FH and export gates still run before this stored status. */
static nfsstat4
nfs4_vfs_input_status(
    struct chimera_server_nfs_thread *thread,
    const struct nfs_argop4          *argop,
    bool                              have_saved)
{
    nfsstat4 status;

    switch (argop->argop) {
        case OP_LOOKUP: return chimera_nfs4_validate_name(&argop->oplookup.objname);
        case OP_SECINFO: return chimera_nfs4_validate_name(&argop->opsecinfo.name);
        case OP_REMOVE: return chimera_nfs4_validate_name(&argop->opremove.target);
        case OP_CREATE: return chimera_nfs4_validate_create(&argop->opcreate);
        case OP_GETATTR:
            return chimera_nfs4_validate_getattr_request(argop->opgetattr.num_attr_request,
                                                         argop->opgetattr.attr_request);
        case OP_SETATTR:
            return chimera_nfs4_validate_createattrs(argop->opsetattr.obj_attributes.num_attrmask,
                                                     argop->opsetattr.obj_attributes.attrmask);
        case OP_GETXATTR: return chimera_nfs4_xattr_validate_name(argop->opgetxattr.gxa_name.len);
        case OP_REMOVEXATTR: return chimera_nfs4_xattr_validate_name(argop->opremovexattr.rxa_name.len);
        case OP_SETXATTR:
            if (argop->opsetxattr.sxa_option != SETXATTR4_EITHER &&
                argop->opsetxattr.sxa_option != SETXATTR4_CREATE &&
                argop->opsetxattr.sxa_option != SETXATTR4_REPLACE) {
                return NFS4ERR_INVAL;
            }
            return chimera_nfs4_xattr_validate_name(argop->opsetxattr.sxa_key.len);
        case OP_RESTOREFH: return have_saved ? NFS4_OK : NFS4ERR_RESTOREFH;
        case OP_COPY:
        case OP_CLONE: return have_saved ? NFS4_OK : NFS4ERR_NOFILEHANDLE;
        case OP_LINK:
            return have_saved ? chimera_nfs4_validate_name(&argop->oplink.newname) : NFS4ERR_NOFILEHANDLE;
        case OP_RENAME:
            if (!have_saved) {
                return NFS4ERR_NOFILEHANDLE;
            }
            status = chimera_nfs4_validate_name(&argop->oprename.oldname);
            return status == NFS4_OK ? chimera_nfs4_validate_name(&argop->oprename.newname) : status;
        case OP_ALLOCATE:
        case OP_DEALLOCATE:
        case OP_SEEK:
        case OP_WRITE_SAME: return nfs4_vfs_data_input_status(argop);
        case OP_PUTFH: {
            uint8_t  fh[CHIMERA_VFS_FH_SIZE];
            int      length;
            uint16_t export_id;
            if (fh_is_nfs4_root(argop->opputfh.object.data, argop->opputfh.object.len)) {
                return NFS4_OK;
            }
            if (argop->opputfh.object.len > NFS4_FHSIZE ||
                chimera_nfs_fh_unwrap(argop->opputfh.object.data, argop->opputfh.object.len,
                                      &export_id, fh, &length, thread->shared->fh_key,
                                      thread->shared->fh_sign) != CHIMERA_NFS_FH_OK ||
                !nfs4_vfs_cursor_base(thread, fh, &length)) {
                return NFS4ERR_BADHANDLE;
            }
            return NFS4_OK;
        }
        default: return NFS4_OK;
    } /* switch */
} /* nfs4_vfs_input_status */

static void
nfs4_vfs_reserve_retirement(
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs4_vfs_op           *map,
    const struct nfs_argop4      *argop)
{
    struct nfs_request     *req   = ctx->req;
    struct nfs_state_table *table = &req->thread->shared->nfs4_state_table;

    map->ctx = ctx;
    if (argop->argop == OP_LAYOUTRETURN) {
        map->retirement_status = nfs4_vfs_layoutreturn_check(map);
        if (map->retirement_status == NFS4_OK && argop->oplayoutreturn.lora_layoutreturn.lr_returntype ==
            LAYOUTRETURN4_ALL) {
            map->retirement_status = nfs_layout_journal_reserve_return(&ctx->layouts, ctx->client,
                                                                       table, &req->thread->shared->nfs4_layout_table,
                                                                       NULL, 0, true);
        }
        return;
    }
    if (argop->argop != OP_DELEGRETURN && argop->argop != OP_FREE_STATEID) {
        return;
    }
    const struct stateid4   *sid = nfs4_vfs_arg_stateid(argop);
    if (chimera_nfs4_stateid_is_current(sid)) {
        /* OPEN/LOCK current-stateids are served by the private owner journal. */
        return;
    }
    struct nfs4_stateid_view view;
    nfs4_stateid_decode(&view, sid);
    if (argop->argop == OP_FREE_STATEID && view.type != NFS4_STATEID_TYPE_DELEG) {
        return;
    }
    void                    *state;
    uint8_t                  type;
    nfsstat4                 status = nfs_state_table_acquire_retirement(table, sid, &state, &type);
    if (status != NFS4_OK) {
        map->retirement_status = status;
        return;
    }
    if (type != NFS4_SLOT_TYPE_DELEG ||
        nfs_state_check_client(state, type, req->minorversion ? ctx->client : NULL) != NFS4_OK) {
        map->retirement_status = NFS4ERR_BAD_STATEID;
        nfs_state_table_release(table, state, type, req->thread->vfs_thread);
        return;
    }
    struct nfs_delegation *deleg = state;
    status = nfs4_client_reserve_compound(&req->thread->shared->nfs4_shared_clients,
                                          deleg->claim.owner.client_key, &map->retirement_client);
    if (status == NFS4_OK && map->retirement_client != deleg->client) {
        status = NFS4ERR_BAD_STATEID;
    }
    if (status == NFS4_OK) {
        for (uint32_t i = 0; i < ctx->num_retirements; i++) {
            if (ctx->retirements[i].deleg == deleg) {
                map->deleg_retirement = &ctx->retirements[i];
                break;
            }
        }
        if (!map->deleg_retirement) {
            struct nfs_delegation_retirement *entry = &ctx->retirements[ctx->num_retirements];
            status = nfs_delegation_retirement_reserve(deleg, ctx, entry);
            if (status == NFS4_OK) {
                map->deleg_retirement = entry;
                ctx->num_retirements++;
            }
        }
    }
    map->retirement_status = status;
    nfs_state_table_release(table, state, type, req->thread->vfs_thread);
} /* nfs4_vfs_reserve_retirement */

static int
nfs4_vfs_submit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    bool                              single,
    nfsstat4                         *failure,
    const struct chimera_nfs_export  *entry_export,
    const char                       *entry_path,
    nfs4_root_export_fh_callback_t    root_resolve)
{
    struct chimera_vfs_compound  *compound;
    struct nfs4_vfs_compound_ctx *ctx;
    struct nfs_argop4            *argop;
    uint32_t                      first, num, nenc, i, k, credential_putfh;
    uint8_t                       cur_fh[NFS4_FHSIZE];
    int                           cur_fhlen = 0;
    int                           lead_putfh;
    int                           have_saved = 0;
    int                           seed_saved = 0;
    /* What the sequence's current open handle carries, as it is built.  Zero
     * means nothing is open on the current object -- see nfs4_vfs_open_for. */
    unsigned int                  cur_open_flags = 0;
    /* OPEN and OPEN_DOWNGRADE share frozen-owner journals. */
    const struct stateid4        *lock_public_sids[NFS4_VFS_COMPOUND_MAX_OPS] = { 0 };
    int                           have_locks                                  = 0;
    const struct stateid4        *scan_current                                = req->current_stateid_valid ? &req->
        current_stateid : NULL;
    const struct stateid4        *scan_saved = req->saved_current_stateid_valid ? &req
        ->saved_current_stateid :
        NULL;
    bool                          journal_open[NFS4_VFS_COMPOUND_MAX_OPS]  = { false };
    uint16_t                      seq_export                               = req->export_id;
    uint16_t                      saved_export_id                          = req->saved_export_id;
    uint16_t                      exports[NFS4_VFS_COMPOUND_MAX_OPS]       = { 0 };
    uint16_t                      saved_exports[NFS4_VFS_COMPOUND_MAX_OPS] = { 0 };
    bool                          saved_present[NFS4_VFS_COMPOUND_MAX_OPS] = { false };
    bool                          junction[NFS4_VFS_COMPOUND_MAX_OPS]      = { false };
    int                           root_cursor = -1, saved_root_cursor = -1, latest_root = -1;
    struct chimera_nfs_export     root_export;
    bool                          have_root = false, coalesced_root = false;
    /* Set when the scan meets an op the sequence cannot carry: the run ends in
     * front of it, and the dispatcher picks up from there. */
    int                           stop = 0;
    /* The initial cursor seed, or a checkpoint for a handle-free prefix. */
    uint32_t                      vfs_ops                                    = 1;
    bool                          probe_budget                               = false;
    nfsstat4                      terminal_status[NFS4_VFS_COMPOUND_MAX_OPS] = { 0 };
    uint64_t                      reply_bound                                = 0;
    uint64_t                      avail                                      = req->encoding->dbuf->size - req->encoding
        ->dbuf->used;
    uint32_t                      build_mark;
    int                           idx, next;
    bool                          scan_attrdir = !entry_export && chimera_nfs4_fh_is_attrdir(req->fh, req->
                                                                                             fhlen);
    bool                          saved_attrdir = chimera_nfs4_fh_is_attrdir(req->saved_fh,
                                                                             req->saved_fhlen);
    bool                          attrdir[NFS4_VFS_COMPOUND_MAX_OPS]        = { false };
    bool                          result_attrdir[NFS4_VFS_COMPOUND_MAX_OPS] = { false };
    uint8_t                       saved_base[NFS4_FHSIZE];
    int                           saved_base_len = req->saved_fhlen;

    bool                          root_open = single && req->args_compound->argarray[req->index].argop == OP_OPEN &&
        fh_is_nfs4_root(req->fh, req->fhlen);
    bool                          pseudo = false, dynamic_identity = true;
    int                           scan_pseudo = 0, saved_pseudo = fh_is_nfs4_root(req->saved_fh, req->saved_fhlen);
    bool                          pseudo_inputs[NFS4_VFS_COMPOUND_MAX_OPS]  = { false };
    bool                          pseudo_outputs[NFS4_VFS_COMPOUND_MAX_OPS] = { false };

    memcpy(saved_base, req->saved_fh, saved_base_len);
    first = (uint32_t) req->index;
    num   = req->res_compound.num_resarray;
    /* One past the last encoded operation; scanning may shorten the run at
     * a dispatcher boundary or its operation/reply budget. */
    nenc = num;

    if (first >= num) {
        return 0;
    }
    if (single) {
        num = nenc = first + 1;
    }
    if (!chimera_nfs4_reply_fits(req, 0)) {
        if (failure) {
            *failure = NFS4ERR_RESOURCE;
        }
        return 0;
    }
    if (root_resolve) {
        nenc = first;
        goto build;
    }
    argop = &req->args_compound->argarray[first];
    if (!entry_export && !root_open) {
        if (argop->argop == OP_PUTFH) {
            pseudo = fh_is_nfs4_root(argop->opputfh.object.data, argop->opputfh.object.len);
        } else if (argop->argop == OP_PUTROOTFH || argop->argop == OP_PUTPUBFH) {
            pseudo = chimera_nfs_get_export_copy(thread->shared, "/", &root_export) != 0;
        } else {
            pseudo = fh_is_nfs4_root(req->fh, req->fhlen) ||
                (!req->fhlen && argop->argop == OP_RESTOREFH &&
                 req->saved_export_id == 0 && fh_is_nfs4_root(req->saved_fh, req->saved_fhlen));
        }
        if (pseudo) {
            if (chimera_nfs_get_export_copy(thread->shared, "/", &root_export) == 0) {
                return 0;
            }
            seq_export   = 0;
            scan_attrdir = false;
        }
    }
    scan_pseudo      = pseudo;
    credential_putfh = first;
    if (!req->fhlen && (req->args_compound->argarray[first].argop == OP_TEST_STATEID ||
                        nfs4_vfs_protocol_op(req->args_compound->argarray[first].argop) ||
                        nfs4_vfs_retirement_handle_free(req->args_compound->argarray[first].argop))) {
        while (credential_putfh < num &&
               (req->args_compound->argarray[credential_putfh].argop == OP_TEST_STATEID ||
                nfs4_vfs_protocol_op(req->args_compound->argarray[credential_putfh].argop) ||
                nfs4_vfs_retirement_handle_free(req->args_compound->argarray[credential_putfh].argop))) {
            credential_putfh++;
        }
    }

    /* A remainder longer than the sequence can hold is not refused: the scan's
     * own vfs_ops budget ends the run when it fills up, and the rest is
     * dispatched op by op. */

    /* When the remainder opens with a PUTFH, that handle names the export;
    * otherwise the sequence inherits whatever the COMPOUND already had. */
    if (!pseudo && credential_putfh < num && req->args_compound->argarray[credential_putfh].argop == OP_PUTFH) {
        uint8_t seed_fh[CHIMERA_VFS_FH_SIZE];
        int     seed_len;

        if (chimera_nfs_fh_unwrap(
                req->args_compound->argarray[credential_putfh].opputfh.object.data,
                (int) req->args_compound->argarray[credential_putfh].opputfh.object.len,
                &seq_export, seed_fh, &seed_len,
                thread->shared->fh_key,
                thread->shared->fh_sign) != CHIMERA_NFS_FH_OK) {
            if (credential_putfh == first) {
                return 0;
            }
            seq_export = req->export_id;
        }
    }

    /* Two-object operations inherit raw cursors under the current export's
     * credential, as their former builders did. Sibling exports can share a
     * filesystem; the VFS decides cross-filesystem behavior. Keep synthetic
     * saved handles opaque so they never address the base object. */
    bool single_pair = single && (req->args_compound->argarray[first].argop == OP_LINK ||
                                  req->args_compound->argarray[first].argop == OP_RENAME ||
                                  req->args_compound->argarray[first].argop == OP_COPY ||
                                  req->args_compound->argarray[first].argop == OP_CLONE);
    if (req->saved_export_id == 0 && fh_is_nfs4_root(req->saved_fh, req->saved_fhlen)) {
        have_saved = 1;
    }
    if (req->saved_fhlen && (single_pair ||
                             (!fh_is_nfs4_root(req->saved_fh, req->saved_fhlen) &&
                              nfs4_vfs_cursor_base(thread, saved_base, &saved_base_len)))) {
        have_saved = 2; /* inherited rather than established in this run */
        vfs_ops   += 2; /* PUTFH(saved), SAVEFH before the normal seed */
    }

    have_root = chimera_nfs_get_export_copy(thread->shared, "/", &root_export) == 0;

    /* Freeze any root selection before checking the prefix's export policy.
     * Its snapshot is also the policy fence for a retried prefix. */
    for (i = first; i < num; i++) {
        uint32_t opcode = req->args_compound->argarray[i].argop;
        if (opcode == OP_PUTROOTFH || opcode == OP_PUTPUBFH) {
            if (entry_export && i == first) {
                root_export = *entry_export;
                have_root   = true;
            } else {
                have_root = chimera_nfs_get_export_copy(thread->shared, "/", &root_export) == 0;
            }
            break;
        }
    }

    for (i = first; i < num; i++) {
        /* Wire maps have a separate bound from the executor's helper budget.
         * Check it before indexing any per-wire planning array. */
        if (i - first == NFS4_VFS_COMPOUND_MAX_OPS) {
            nenc = i;
            break;
        }
        argop = &req->args_compound->argarray[i];

        nfsstat4 input_status = nfs4_op_check_minor(argop->argop, req->minorversion, i, req->seen_sequence);
        if (input_status == NFS4_OK) {
            input_status = nfs4_vfs_input_status(thread, argop, have_saved != 0);
        }
        if (input_status != NFS4_OK) {
            if (vfs_ops + 1 > CHIMERA_VFS_COMPOUND_MAX_OPS || !chimera_nfs4_reply_fits(req, reply_bound + 512)) {
                nenc = i;
                break;
            }
            terminal_status[i - first] = input_status;
            exports[i - first]         = seq_export;
            saved_exports[i - first]   = saved_export_id;
            saved_present[i - first]   = have_saved != 0;
            attrdir[i - first]         = result_attrdir[i - first] = scan_attrdir;
            pseudo_inputs[i - first]   = scan_pseudo != 0;
            pseudo_outputs[i - first]  = scan_pseudo == 1;
            reply_bound               += 512;
            nenc                       = i + 1;
            break;
        }

        /* Namespace selections are checkpoints in this run, including the
         * security-negotiation exception for ROOT/PUB. */
        if (argop->argop == OP_PUTROOTFH || argop->argop == OP_PUTPUBFH) {
            coalesced_root = have_root;
            seq_export     = have_root ? root_export.id : 0;
        }
        pseudo_inputs[i - first] = scan_pseudo != 0;
        /* Synthetic cursors use the same protocol/state checkpoints as real
         * objects. Explicit directory encodings may address the base; all
         * other helpers are rejected before they can do backend work. */
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

        if (argop->argop == OP_LOOKUP && (scan_pseudo == 1 ||
                                          (root_cursor >= 0 && root_cursor == latest_root))) {
            struct chimera_nfs_export target;
            if (chimera_nfs_get_export_by_component(thread->shared, argop->oplookup.objname.data,
                                                    argop->oplookup.objname.len, &target) == 0) {
                junction[i - first] = true;
                seq_export          = target.id;
                vfs_ops            += 2; /* Rebind the saved cursor at the credential group. */
            }
        }

        if (argop->argop == OP_LOOKUPP) {
            vfs_ops += 5; /* saved/current seeds and conditional parent selection */
        }
        if (dynamic_identity && !scan_attrdir && argop->argop == OP_LOOKUP && !junction[i - first] &&
            nfs4_vfs_possible_junction(thread, &argop->oplookup.objname)) {
            vfs_ops += 6; /* saved/current seeds, conditional export path and final checkpoint */
        }

        uint64_t op_bound = nfs4_vfs_op_reply_bound(argop);
        if (!chimera_nfs4_reply_fits(req, reply_bound + op_bound)) {
            /* A single-op fallback must preserve the same transport reserve.
             * Keep any fitted prefix; report a real resource failure when
             * even the first operation's fixed scratch cannot fit. */
            if (i == first && failure) {
                *failure = NFS4ERR_RESOURCE;
            }
            nenc = i;
            break;
        }
        reply_bound += op_bound;

        if (!single && !coalesced_root && !probe_budget &&
            (argop->argop == OP_LOOKUPP || argop->argop == OP_LOOKUP || argop->argop == OP_SECINFO)) {
            vfs_ops     += 2; /* Conditional root comparison, before the cursor seeds. */
            probe_budget = true;
        }

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
        if (argop->argop == OP_REMOVE && chimera_vfs_pnfs_enabled(thread->vfs)) {
            vfs_ops += 2; /* Conditional victim OPEN/GETHANDLE until DS cleanup. */
            if (!chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
                vfs_ops += 3; /* GETFH(parent), LOOKUP(target), PUTFH(parent). */
            }
        }
        if (chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
            if (argop->argop == OP_REMOVE) {
                /* GETFH(parent), LOOKUP(victim), COORDINATE, PUTFH(parent). */
                vfs_ops += 4;
            } else if (argop->argop == OP_RENAME) {
                /* Target directory OPEN/GETFH, source RESTOREFH, and two
                 * LOOKUP/COORDINATE/PUTFH walks before the base RENAME. */
                vfs_ops += 8;
            }
        }
        if (i != first && (argop->argop == OP_PUTFH || argop->argop == OP_PUTROOTFH ||
                           argop->argop == OP_PUTPUBFH || argop->argop == OP_RESTOREFH)) {
            vfs_ops += 2;
        }
        switch (argop->argop) {
            case OP_READDIR:
                vfs_ops += scan_attrdir ? 3 : 2; /* base GETATTR + LIST_STREAMS, or READDIR */
                break;

            case OP_OPEN:
                vfs_ops += argop->opopen.claim.claim == CLAIM_DELEGATE_CUR ? 8 : 7;
                vfs_ops += scan_attrdir; /* memoized base-stream lifetime guard */
                if (chimera_vfs_pnfs_feature_enabled(thread->shared->vfs) &&
                    argop->opopen.openhow.opentype == OPEN4_CREATE &&
                    argop->opopen.openhow.how.mode == UNCHECKED4) {
                    vfs_ops++; /* conditional truncate layout coordination */
                }
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
            case OP_LAYOUTGET:
                vfs_ops += 14; /* admission, retained MDS handle, conditional paths, result */
                break;
            case OP_LAYOUTCOMMIT:
            case OP_READLINK:
                /* open + getattr + the operation */
                vfs_ops += 3;
                break;
            case OP_PUTFH:
                /* The seed PUTFH below is the leading one's own op. */
                vfs_ops += (i == first) ? 2 : 3;
                break;
            case OP_GETFH:
            case OP_SAVEFH:
            case OP_RESTOREFH:
                /* Cursor work, or two objects named by file handle: no open. */
                vfs_ops += 1;
                break;
            default:
                /* open + the op */
                vfs_ops += 2;
                break;
        } /* switch */

        if (vfs_ops > CHIMERA_VFS_COMPOUND_MAX_OPS) {
            reply_bound -= op_bound;
            nenc         = i;
            break;
        }

        attrdir[i - first] = scan_attrdir;
        if (have_saved == 2 && (argop->argop == OP_RESTOREFH || argop->argop == OP_COPY ||
                                argop->argop == OP_CLONE || argop->argop == OP_LINK || argop->argop == OP_RENAME)) {
            seed_saved = 1;
        }

        switch (argop->argop) {
            case OP_PUTFH:
            {
                uint8_t  later_fh[CHIMERA_VFS_FH_SIZE];
                int      later_len;
                uint16_t later_export;

                if (fh_is_nfs4_root(argop->opputfh.object.data, argop->opputfh.object.len)) {
                    seq_export   = 0;
                    scan_pseudo  = 1;
                    scan_attrdir = false;
                    break;
                }
                scan_pseudo = 0;
                if (argop->opputfh.object.len > NFS4_FHSIZE ||
                    chimera_nfs_fh_unwrap(argop->opputfh.object.data,
                                          (int) argop->opputfh.object.len,
                                          &later_export,
                                          later_fh, &later_len,
                                          thread->shared->fh_key,
                                          thread->shared->fh_sign) !=
                    CHIMERA_NFS_FH_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }
                seq_export   = later_export;
                scan_attrdir = chimera_nfs4_fh_is_attrdir(later_fh, later_len);
                if (!nfs4_vfs_cursor_base(thread, later_fh, &later_len)) {
                    nenc = i;
                    stop = 1;
                }
            }
            break;

            case OP_PUTROOTFH:
            case OP_PUTPUBFH:
                scan_pseudo = !have_root;
                break;
            case OP_OPENATTR:
                break;

            case OP_LOOKUPP:
                scan_pseudo = have_root ? 0 : -1;
                break;

            case OP_SAVEFH:
                saved_export_id = seq_export;
                saved_pseudo    = scan_pseudo;
                have_saved      = 1;
                break;

            case OP_RESTOREFH:
                scan_pseudo = saved_pseudo;
                seq_export  = saved_export_id;
                break;

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

            case OP_REMOVE:
                break;

            case OP_RELEASE_LOCKOWNER:
                have_locks = 1;
                break;
            case OP_FREE_STATEID:
            {
                const struct stateid4   *sid = nfs4_vfs_arg_stateid(argop);
                struct nfs4_stateid_view view;
                if (chimera_nfs4_stateid_is_current(sid)) {
                    sid = scan_current;
                }
                if (sid && !nfs4_stateid_is_special(sid)) {
                    nfs4_stateid_decode(&view, sid);
                    if (view.type == NFS4_STATEID_TYPE_LOCK || view.type == NFS4_STATEID_TYPE_OPEN) {
                        lock_public_sids[i - first] = sid;
                    }
                }
                have_locks = 1;
                break;
            }
            case OP_LOCK:
            case OP_LOCKU:
            {
                if (req->minorversion && !nfs4_session_is_live(req->session)) {
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
                if (req->minorversion && !nfs4_session_is_live(req->session)) {
                    nenc = i; stop = 1; break;
                }
                if (chimera_nfs4_stateid_is_current(sid)) {
                    sid = scan_current;
                }
                /* Parent mutation shares the child journals even without an
                 * explicit LOCK in this run. Existing children are frozen at
                 * construction and retire only after accepted CLOSE. */
                lock_public_sids[i - first] = sid;
                have_locks                  = 1;
                break;
            }

            case OP_OPEN:
            {
                struct OPEN4args *oa = &argop->opopen;

                journal_open[i - first] = oa->owner.owner.len <= NFS4_OPAQUE_LIMIT;
                if (!journal_open[i - first] ||
                    (req->minorversion && !nfs4_session_is_live(req->session))) {
                    nenc = i;
                    stop = 1;
                    break;
                }
                /* Coalesced suffixes may use or mutate existing child locks.
                 * A standalone OPEN freezes those children with the parent,
                 * but does not need a journal for unchanged lock owners. */
                have_locks |= !single;
                break;
            }

            default:
                break;
        } /* switch */

        switch (argop->argop) {
            case OP_OPENATTR: scan_attrdir = true; break;
            case OP_OPEN:
            case OP_PUTROOTFH:
            case OP_PUTPUBFH:
            case OP_LOOKUP:
            case OP_LOOKUPP:
            case OP_CREATE:
                scan_attrdir = false;
                break;
            case OP_SAVEFH: saved_attrdir   = scan_attrdir; break;
            case OP_RESTOREFH: scan_attrdir = saved_attrdir; break;
            default: break;
        } /* switch */
        switch (argop->argop) {
            case OP_PUTROOTFH:
            case OP_PUTPUBFH: root_cursor     = latest_root = i; break;
            case OP_SAVEFH: saved_root_cursor = root_cursor; break;
            case OP_RESTOREFH: root_cursor    = saved_root_cursor; break;
            case OP_SECINFO:
                if (req->minorversion == 0) {
                    break;
                }
            /* fall through */
            case OP_SECINFO_NO_NAME:
            case OP_PUTFH:
            case OP_LOOKUP:
            case OP_LOOKUPP:
            case OP_OPENATTR:
            case OP_OPEN:
            case OP_CREATE: root_cursor = -1; break;
            default: break;
        } /* switch */
        if (argop->argop == OP_LOOKUP) {
            scan_pseudo = 0;
        }
        pseudo_outputs[i - first] = scan_pseudo == 1;
        result_attrdir[i - first] = scan_attrdir;
        exports[i - first]        = seq_export;
        saved_exports[i - first]  = saved_export_id;
        saved_present[i - first]  = have_saved != 0;

        /* Track only known public identities for CLOSE shape classification.
         * NULL also represents a provisional OPEN, whose shape is lock-free. */
        switch (argop->argop) {
            case OP_SAVEFH: scan_saved      = scan_current; break;
            case OP_RESTOREFH: scan_current = scan_saved; break;
            case OP_PUTFH:
            case OP_PUTROOTFH:
            case OP_PUTPUBFH:
            case OP_LOOKUP:
            case OP_LOOKUPP:
            case OP_OPENATTR:
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

        if (stop) {
            break;
        }
    }

    /* Nothing at all was expressible, so there is no sequence to build. */
    if (nenc <= first) {
        return 0;
    }
    if (nenc == first + 1 && nfs4_vfs_protocol_op(req->args_compound->argarray[first].argop)) {
        return 0;
    }

    /* Variable payloads are checked at execution-time staging checkpoints.
    * The scan only reserves construction scratch and fixed completions. */
    if (!chimera_nfs4_reply_fits(req, reply_bound)) {
        if (failure) {
            *failure = NFS4ERR_RESOURCE;
        }
        return 0;
    }

    /* Establish the object the sequence starts from. */
    lead_putfh = (req->args_compound->argarray[first].argop == OP_PUTFH);

    if (entry_export || pseudo || req->args_compound->argarray[first].argop == OP_PUTROOTFH ||
        req->args_compound->argarray[first].argop == OP_PUTPUBFH) {
        /* Export entry resolves from PUTROOT. Virtual namespace runs have no
         * initial backend cursor, even when READDIR visits backing exports. */
        cur_fhlen = 0;
    } else if (lead_putfh) {
        struct PUTFH4args *pa = &req->args_compound->argarray[first].opputfh;

        /* Mirrors chimera_nfs4_putfh.  The pseudo-root is not a wrapped VFS
         * handle at all; the decode authenticates the wire handle, recovers the
         * inner VFS handle, and re-derives the export id and squashed
         * credential the sequence will run under. A named-attribute directory
         * uses its base inode as the VFS cursor. */
        if (fh_is_nfs4_root(pa->object.data, pa->object.len) ||
            pa->object.len > NFS4_FHSIZE) {
            return 0;
        }

        if (chimera_nfs_fh_decode(req, pa->object.data, pa->object.len,
                                  cur_fh, &cur_fhlen) != CHIMERA_NFS_FH_OK) {
            return 0;
        }

        if (!nfs4_vfs_cursor_base(thread, cur_fh, &cur_fhlen)) {
            return 0;
        }
    } else if (req->fhlen == 0 && (req->args_compound->argarray[first].argop == OP_TEST_STATEID ||
                                   nfs4_vfs_protocol_op(req->args_compound->argarray[first].argop) ||
                                   nfs4_vfs_retirement_handle_free(req->args_compound->argarray[first].argop))) {
        /* Select the eventual filesystem credential without exposing a future
         * filehandle to earlier, handle-free TEST_STATEID checkpoints. */
        if (credential_putfh < nenc && !terminal_status[credential_putfh - first] &&
            req->args_compound->argarray[credential_putfh].argop == OP_PUTFH) {
            struct PUTFH4args *pa = &req->args_compound->argarray[credential_putfh].opputfh;
            uint8_t            decoded[NFS4_FHSIZE];
            int                decoded_len;
            if (chimera_nfs_fh_decode(req, pa->object.data, pa->object.len,
                                      decoded, &decoded_len) != CHIMERA_NFS_FH_OK ||
                !nfs4_vfs_cursor_base(thread, decoded, &decoded_len)) {
                return 0;
            }
        }
    } else if (req->fhlen == 0 &&
               req->args_compound->argarray[first].argop == OP_RESTOREFH && seed_saved) {
        /* RESTOREFH needs only its saved slot and that export's credential. The hidden cursor
         * seeds do no filesystem work; this first wire op establishes the
         * protocol cursor before any operation can use it. */
        memcpy(cur_fh, saved_base, saved_base_len);
        cur_fhlen = saved_base_len;
    } else {
        if (req->fhlen == 0 ||
            (fh_is_nfs4_root(req->fh, req->fhlen) && !root_open)) {
            return 0;
        }

        if (!root_open) {
            memcpy(cur_fh, req->fh, req->fhlen);
            cur_fhlen = req->fhlen;
            if (!nfs4_vfs_cursor_base(thread, cur_fh, &cur_fhlen)) {
                return 0;
            }
        }
    }

    /* ---- ops [first, nenc) are expressible: build the sequence ---- */

    /* Construction may stage names and variable-length checkpoint results.
     * A refused build returns that reply space to the dispatcher. */
 build:
    build_mark = req->encoding->dbuf->used;
    compound   = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    ctx = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate NFSv4 compound context");
    ctx->req              = req;
    ctx->single           = single;
    ctx->pseudo           = ctx->initial_pseudo = fh_is_nfs4_root(req->fh, req->fhlen);
    ctx->root_probe       = -1;
    ctx->dynamic_identity = dynamic_identity;
    if (entry_export) {
        ctx->entry_export = malloc(sizeof(*ctx->entry_export));
        if (!ctx->entry_export) {
            if (failure) {
                *failure = NFS4ERR_RESOURCE;
            }
            goto refuse;
        }
        *ctx->entry_export = *entry_export;
    }
    if (root_resolve) {
        ctx->root_resolve     = root_resolve;
        ctx->ops[0].res_index = first;
        ctx->attempt_mark     = build_mark;
        if (chimera_vfs_compound_add_putroot(compound) < 0 ||
            nfs4_vfs_export_path(compound, entry_path, true) < 0) {
            goto refuse;
        }
        chimera_vfs_compound_set_op_prepare(compound, 0, nfs4_vfs_root_prepare, ctx);
        chimera_vfs_compound_set_op_prepare(compound, 1, nfs4_vfs_root_prepare, ctx);
        chimera_vfs_compound_set_attempt_reset(compound, nfs4_vfs_attempt_reset, ctx);
        goto submit;
    }
    if (dynamic_identity) {
        ctx->initial_identity = nfs4_vfs_identity_get(ctx, req->export_id);
        if (req->saved_fhlen) {
            ctx->initial_saved_identity = nfs4_vfs_identity_get(ctx, req->saved_export_id);
        }
        if (!ctx->initial_identity || (req->saved_fhlen && !ctx->initial_saved_identity)) {
            goto refuse;
        }
    }
    {
        for (i = first; i < nenc; i++) {
            struct nfs4_vfs_op *map = &ctx->ops[i - first];
            map->identity = nfs4_vfs_identity_get(ctx, exports[i - first]);
            if (!map->identity) {
                goto refuse;
            }
            if (saved_present[i - first]) {
                map->saved_identity = nfs4_vfs_identity_get(ctx, saved_exports[i - first]);
                if (!map->saved_identity) {
                    goto refuse;
                }
            }
        }
        if (coalesced_root) {
            struct nfs4_vfs_identity *identity = nfs4_vfs_identity_get(ctx, root_export.id);
            if (!identity) {
                goto refuse;
            }
            identity->export  = root_export;
            identity->present = true;
            identity->cred    = req->orig_cred;
            chimera_nfs_squash_cred(&identity->cred, &root_export);
        }
        /* Entry selection happened before the scan; do not accidentally pin
         * a replacement policy observed during construction. */
        if (entry_export) {
            ctx->ops[0].identity->export  = *entry_export;
            ctx->ops[0].identity->present = true;
            ctx->ops[0].identity->cred    = req->orig_cred;
            chimera_nfs_squash_cred(&ctx->ops[0].identity->cred, entry_export);
        }
    }
    for (i = first; i < nenc; i++) {
        uint32_t opcode = req->args_compound->argarray[i].argop;
        if (opcode != OP_LOOKUP && opcode != OP_LOOKUPP && opcode != OP_SECINFO &&
            opcode != OP_PUTFH && opcode != OP_RESTOREFH &&
            opcode != OP_PUTROOTFH && opcode != OP_PUTPUBFH) {
            continue;
        }
        ctx->namespace_root = malloc(sizeof(*ctx->namespace_root));
        if (!ctx->namespace_root) {
            goto refuse;
        }
        if (have_root) {
            *ctx->namespace_root = root_export;
        } else if (chimera_nfs_get_export_copy(thread->shared, "/", ctx->namespace_root) != 0) {
            free(ctx->namespace_root);
            ctx->namespace_root = NULL;
        }
        break;
    }
    ctx->namespace_identity = nfs4_vfs_identity_get(ctx, ctx->namespace_root ? ctx->namespace_root->id : 0);
    if (!ctx->namespace_identity) {
        goto refuse;
    }
    if (ctx->namespace_root) {
        ctx->namespace_identity->export  = *ctx->namespace_root;
        ctx->namespace_identity->present = true;
        ctx->namespace_identity->cred    = req->orig_cred;
        chimera_nfs_squash_cred(&ctx->namespace_identity->cred, ctx->namespace_root);
    }
    /* Retain the entry binding for non-owner callbacks; v4.0 owner admission
    * never depends on it. A stale unrelated implicit binding is harmless. */
    if (req->session) {
        nfsstat4 status = !req->minorversion || nfs4_session_is_live(req->session) ?
            nfs4_client_reserve_compound(&thread->shared->nfs4_shared_clients,
                                         req->session->nfs4_session_clientid, &ctx->client) : NFS4ERR_STALE_CLIENTID;
        if (status != NFS4_OK && req->minorversion) {
            if (failure) {
                *failure = status;
            }
            goto refuse;
        }
    }
    if (!req->minorversion) {
        for (i = first; i < nenc; i++) {
            const struct nfs_argop4 *state_op = &req->args_compound->argarray[i];
            struct nfs4_vfs_op      *map      = &ctx->ops[i - first];
            uint64_t                 clientid;
            if (!nfs4_vfs_owner_operation(state_op->argop)) {
                continue;
            }
            if (state_op->argop == OP_OPEN) {
                clientid = state_op->opopen.owner.clientid;
            } else if (state_op->argop == OP_RELEASE_LOCKOWNER) {
                clientid = state_op->oprelease_lockowner.lock_owner.clientid;
            } else {
                map->owner_status = nfs_state_table_clientid(&thread->shared->nfs4_state_table,
                                                             nfs4_vfs_arg_stateid(state_op), &clientid);
                if (map->owner_status != NFS4_OK) {
                    continue;
                }
            }
            map->owner_status = nfs4_client_reserve_compound(&thread->shared->nfs4_shared_clients,
                                                             clientid, &map->owner_client);
            if (map->owner_status == NFS4_OK && state_op->argop == OP_OPEN) {
                map->owner_session = nfs4_session_find_by_clientid(&thread->shared->nfs4_shared_clients, clientid);
                if (!map->owner_session) {
                    map->owner_status = NFS4ERR_STALE_CLIENTID;
                }
            }
        }
    }
    for (i = first; i < nenc; i++) {
        ctx->ops[i - first].planned_identity = ctx->ops[i - first].identity;
    }
    ctx->initial_stateid             = req->current_stateid;
    ctx->initial_stateid_valid       = req->current_stateid_valid;
    ctx->initial_saved_stateid       = req->saved_current_stateid;
    ctx->initial_saved_stateid_valid = req->saved_current_stateid_valid;
    ctx->initial_fh_consumed         = cur_fhlen == 0 && !root_open && !entry_export &&
        !(pseudo && fh_is_nfs4_root(req->fh, req->fhlen));
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
    for (i = first; i < nenc; i++) {
        ctx->ops[i - first].res_index = i;
        nfs4_vfs_reserve_retirement(ctx, &ctx->ops[i - first], &req->args_compound->argarray[i]);
    }
    /* Freeze owners before resolving public CLOSEs. The owner journal also
     * supplies handles to operations preceding the first coalescing OPEN. */
    for (i = first; i < nenc; i++) {
        struct nfs4_vfs_op                *map = &ctx->ops[i - first];
        argop = &req->args_compound->argarray[i];
        if (argop->argop != OP_OPEN) {
            continue;
        }
        struct OPEN4args                  *oa     = &argop->opopen;
        struct nfs_open_owner_reservation *group  = NULL;
        struct nfs_client                 *client = nfs4_vfs_owner_client(ctx, map);
        if (map->owner_status != NFS4_OK) {
            continue;
        }
        if (journal_open[i - first] && client) {
            for (uint32_t g = 0; g < ctx->num_groups; g++) {
                struct nfs_open_owner *owner = ctx->groups[g]->owner;
                if (owner->client == client && owner->owner_len == oa->owner.owner.len &&
                    !memcmp(owner->owner, oa->owner.owner.data, owner->owner_len)) {
                    group = ctx->groups[g];
                    break;
                }
            }
            if (!group) {
                if (!nfs4_vfs_owner_slot(ctx)) {
                    if (failure) {
                        *failure = NFS4ERR_RESOURCE;
                    }
                    goto refuse;
                }
                uint32_t count = 0;
                for (uint32_t j = i; j < nenc; j++) {
                    struct nfs_argop4 *next_op = &req->args_compound->argarray[j];
                    if (next_op->argop == OP_OPEN &&
                        nfs4_vfs_owner_client(ctx, &ctx->ops[j - first]) == client &&
                        next_op->opopen.owner.owner.len == oa->owner.owner.len &&
                        !memcmp(next_op->opopen.owner.owner.data, oa->owner.owner.data,
                                oa->owner.owner.len)) {
                        count++;
                    }
                }
                nfsstat4 reserve_status = nfs_open_owner_reserve_compound_locks(
                    client, oa->owner.owner.data, oa->owner.owner.len,
                    req->principal_flavor, req->principal_machinename, req->principal_machinename_len,
                    ctx, count, &thread->shared->nfs4_state_table, thread->vfs_thread, &group);
                if (reserve_status == NFS4ERR_ACCESS && req->minorversion == 0) {
                    reserve_status = nfs_open_owner_reserve_compound_replay(
                        client, oa->owner.owner.data, oa->owner.owner.len,
                        req->principal_flavor, req->principal_machinename, req->principal_machinename_len,
                        ctx, count, &thread->shared->nfs4_state_table, thread->vfs_thread, &group,
                        &map->input_status);
                }
                if (reserve_status != NFS4_OK) {
                    map->owner_status = reserve_status;
                    continue;
                }
                ctx->groups[ctx->num_groups++] = group;
            }
        }
        if (!group) {
            map->owner_status = NFS4ERR_STALE_CLIENTID;
            continue;
        }
        uint32_t candidate = 0;
        for (uint32_t j = first; j < i; j++) {
            candidate += ctx->ops[j - first].open_group == group;
        }
        for (uint32_t j = first; j < i; j++) {
            if (ctx->ops[j - first].open_group == group && ctx->ops[j - first].input_status == NFS4ERR_ACCESS) {
                map->input_status = NFS4ERR_ACCESS;
            }
        }
        map->open_group    = group;
        map->reserved_open = group->candidates[candidate];
        ctx->num_reserved++;
    }
    ctx->attempt_mark = req->encoding->dbuf->used;
    if (!nfs4_vfs_build_reset(compound, ctx)) {
        if (failure) {
            *failure = NFS4ERR_RESOURCE;
        }
        goto refuse;
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
            struct nfs4_vfs_op *map    = &ctx->ops[i - first];
            nfsstat4            status = map->owner_status;
            if (status == NFS4_OK) {
                status = nfs4_vfs_freeze_lock_parent(ctx, nfs4_vfs_owner_client(ctx, map),
                                                     lock_public_sids[i - first]);
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
            if (!nfs4_vfs_build_reset(compound, ctx)) {
                if (failure) {
                    *failure = NFS4ERR_RESOURCE;
                }
                goto refuse;
            }
        }
        /* A wire lock-owner may already anchor other files or OPEN owners.
         * Freeze those parents too, then follow any additional shared owners
         * reached through the new groups. No lock-owner journal is installed
         * until this bounded closure is complete. */
        for (i = first; i < nenc; i++) {
            argop = &req->args_compound->argarray[i];
            struct nfs4_vfs_op *map    = &ctx->ops[i - first];
            struct nfs_client  *client = nfs4_vfs_owner_client(ctx, map);
            if (client && map->owner_status == NFS4_OK && map->stateid_reservation_status == NFS4_OK &&
                (argop->argop == OP_RELEASE_LOCKOWNER ||
                 (argop->argop == OP_LOCK && argop->oplock.locker.new_lock_owner))) {
                const struct state_owner4 *owner = argop->argop == OP_RELEASE_LOCKOWNER ?
                    &argop->oprelease_lockowner.lock_owner : &argop->oplock.locker.open_owner.lock_owner;
                if (!nfs4_vfs_expand_lock_parents(compound, ctx, client, owner->owner.data, owner->owner.len)) {
                    goto refuse;
                }
            }
        }
        for (uint32_t g = 0; g < ctx->num_groups; g++) {
            struct nfs_open_owner_reservation *parent = ctx->groups[g];
            for (uint32_t j = 0; j < parent->num_locks; j++) {
                struct nfs_lock_owner *owner = parent->locks[j]->lock_owner;
                if (!nfs4_vfs_expand_lock_parents(compound, ctx, owner->client, owner->owner, owner->owner_len)) {
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
                if (nfs4_vfs_reserve_lock_owner(ctx, owner->client, owner->owner, owner->owner_len,
                                                first, nenc, &group) != NFS4_OK) {
                    goto refuse;
                }
            }
        }
        for (i = first; i < nenc; i++) {
            struct nfs4_vfs_op        *map = &ctx->ops[i - first];
            argop = &req->args_compound->argarray[i];
            struct nfs_client         *client = nfs4_vfs_owner_client(ctx, map);
            if (!client || map->owner_status != NFS4_OK || map->stateid_reservation_status != NFS4_OK ||
                (argop->argop != OP_RELEASE_LOCKOWNER &&
                 (argop->argop != OP_LOCK || !argop->oplock.locker.new_lock_owner))) {
                continue;
            }
            const struct state_owner4 *owner = argop->argop == OP_RELEASE_LOCKOWNER ?
                &argop->oprelease_lockowner.lock_owner : &argop->oplock.locker.open_owner.lock_owner;
            if (nfs4_vfs_reserve_lock_owner(ctx, client, owner->owner.data, owner->owner.len,
                                            first, nenc, &map->lock_group) != NFS4_OK) {
                goto refuse;
            }
            if (argop->argop == OP_RELEASE_LOCKOWNER) {
                continue;
            }
            uint32_t candidate = 0;
            for (uint32_t j = first; j < i; j++) {
                candidate += req->args_compound->argarray[j].argop == OP_LOCK &&
                    ctx->ops[j - first].lock_group == map->lock_group;
            }
            map->lock_candidate = map->lock_group->candidates[candidate];
        }
        for (uint32_t g = 0; g < ctx->num_lock_groups; g++) {
            struct nfs_lock_owner_reservation *group    = ctx->lock_groups[g];
            uint64_t                           capacity = (uint64_t) ctx->lock_capacity + group->num_existing + group->
                num_candidates;
            if (capacity > UINT32_MAX) {
                goto refuse;
            }
            ctx->lock_capacity = capacity;
        }
        ctx->locks         = calloc(ctx->lock_capacity ? ctx->lock_capacity : 1, sizeof(*ctx->locks));
        ctx->retired_files = calloc(ctx->lock_capacity ? ctx->lock_capacity : 1, sizeof(*ctx->retired_files));
        if (!ctx->locks || !ctx->retired_files) {
            goto refuse;
        }
        ctx->range_capacity = 2 * modifications + 1;
        for (uint32_t g = 0; g < ctx->num_lock_groups; g++) {
            struct nfs_lock_owner_reservation *group = ctx->lock_groups[g];
            for (uint32_t j = 0; j < group->num_existing + group->num_candidates; j++) {
                if (ctx->num_locks == ctx->lock_capacity) {
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
                if (count > UINT32_MAX - ctx->range_capacity) {
                    goto refuse;
                }
                ctx->range_capacity += count;
            }
        }
        ctx->range_previous = calloc(ctx->range_capacity, sizeof(*ctx->range_previous));
        ctx->range_current  = calloc(ctx->range_capacity, sizeof(*ctx->range_current));
        if (!ctx->range_previous || !ctx->range_current) {
            goto refuse;
        }
        if (!nfs4_vfs_build_reset(compound, ctx)) {
            if (failure) {
                *failure = NFS4ERR_RESOURCE;
            }
            goto refuse;
        }
    }
    if (nenc <= first) {
        goto refuse;
    }

    if (!single && ctx->namespace_root) {
        for (i = first; i < nenc; i++) {
            argop = &req->args_compound->argarray[i];
            if (argop->argop == OP_PUTROOTFH || argop->argop == OP_PUTPUBFH) {
                break;
            }
            const xdr_opaque *name = argop->argop == OP_LOOKUP ? &argop->oplookup.objname :
                argop->argop == OP_SECINFO ? &argop->opsecinfo.name : NULL;
            bool              junction = name && exports[i - first] == ctx->namespace_root->id &&
                nfs4_vfs_possible_junction(thread, name);
            if (!attrdir[i - first] && (argop->argop == OP_LOOKUPP || junction)) {
                /* Resolve before seeding either wire cursor. A warm cache
                 * skips both operations; a retry rechecks the same snapshot. */
                if (chimera_vfs_compound_add_putroot(compound) < 0 ||
                    (ctx->root_probe = nfs4_vfs_export_path(compound, ctx->namespace_root->path, true)) < 0) {
                    goto refuse;
                }
                chimera_vfs_compound_set_op_prepare(compound, 0, nfs4_vfs_root_prepare, ctx);
                chimera_vfs_compound_set_op_callbacks(compound, ctx->root_probe,
                                                      nfs4_vfs_root_prepare, nfs4_vfs_root_probe_complete, ctx);
                break;
            }
        }
    }
    if (seed_saved) {
        if (chimera_vfs_compound_add_putfh(compound, saved_base, saved_base_len) < 0 ||
            chimera_vfs_compound_add_savefh(compound) < 0) {
            goto refuse;
        }
    }

    /* Seed the current object.  When the remainder opens with a PUTFH this is
     * that PUTFH; otherwise it re-states the COMPOUND's current filehandle and
     * belongs to whichever op comes first. */
    idx = entry_export ? chimera_vfs_compound_add_putroot(compound) :
        cur_fhlen ? chimera_vfs_compound_add_putfh(compound, cur_fh, cur_fhlen) :
        chimera_vfs_compound_add_checkpoint(compound);

    if (idx < 0) {
        goto refuse;
    }

    next = 0;

    for (i = first, k = 0; i < nenc; i++, k++) {
        struct nfs4_vfs_op *map = &ctx->ops[k];

        argop = &req->args_compound->argarray[i];

        map->terminal_status    = terminal_status[k];
        map->pseudo             = false;
        map->planned_pseudo     = pseudo_outputs[k];
        map->maybe_pseudo       = pseudo_inputs[k];
        map->runtime_parent     = !map->terminal_status && argop->argop == OP_LOOKUPP;
        map->root_readdir_start = -1;
        map->junction           = junction[k] || (entry_export && !k && argop->argop == OP_LOOKUP);
        map->junction_target    = map->junction ? map->planned_identity : NULL;
        map->saved_seed         = map->current_seed = -1;
        map->group_lo           = !k && ctx->root_probe >= 0 ? ctx->root_probe + 1 : next;
        map->ctx                = ctx;
        map->attrdir            = attrdir[k];
        map->result_attrdir     = result_attrdir[k];
        map->res_index          = i;
        map->vfs_lo             = next;
        map->prepare_index      = k == 0 ? idx + (pseudo || argop->argop != OP_PUTFH) : next;
        map->vfs_aux            = -1;
        map->verify_status      = 0;
        map->possible_junction  = argop->argop == OP_LOOKUP ?
            nfs4_vfs_possible_junction(thread, &argop->oplookup.objname) :
            argop->argop == OP_SECINFO && nfs4_vfs_possible_junction(thread, &argop->opsecinfo.name);

        if (!map->terminal_status && (argop->argop == OP_SECINFO || (argop->argop == OP_LOOKUP && !map->attrdir))) {
            const xdr_opaque         *name = argop->argop == OP_LOOKUP ? &argop->oplookup.objname : &argop->opsecinfo.
                name;
            struct chimera_nfs_export target;
            if (chimera_nfs_get_export_by_component(thread->shared, name->data, name->len, &target) == 0) {
                map->secinfo_export = nfs4_vfs_identity_get(ctx, target.id);
                if (!map->secinfo_export) {
                    goto refuse;
                }
                if (argop->argop == OP_LOOKUP) {
                    map->junction_target = map->junction ? map->planned_identity : map->secinfo_export;
                    map->runtime_lookup  = dynamic_identity && !map->junction && !(entry_export && !k);
                }
            }
        }
        map->group_start = !k || (!map->terminal_status && (dynamic_identity ?
                                                            (map->runtime_lookup || map->runtime_parent || map->junction
                                                             || argop->argop == OP_PUTFH ||
                                                             argop->argop == OP_PUTROOTFH || argop->argop == OP_PUTPUBFH
                                                             || argop->argop ==
                                                             OP_RESTOREFH) :
                                                            map->identity != ctx->ops[k - 1].identity));
        if (map->group_start && (k || map->runtime_lookup || map->runtime_parent)) {
            if (!k) {
                map->group_lo      = chimera_vfs_compound_num_ops(compound);
                map->prepare_index = map->group_lo;
            }
            cur_open_flags = 0;
            if (k ? saved_present[k - 1] : seed_saved) {
                const uint8_t placeholder = 0;
                map->saved_seed = chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
                if (map->saved_seed < 0 || chimera_vfs_compound_add_savefh(compound) < 0) {
                    goto refuse;
                }
            }
            if (map->runtime_lookup || map->runtime_parent) {
                const uint8_t placeholder = 0;
                map->current_seed = chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
                if (map->current_seed < 0) {
                    goto refuse;
                }
            }
        }

        if (map->terminal_status || nfs4_vfs_protocol_op(argop->argop)) {
            idx                = map->vfs_res = chimera_vfs_compound_add_checkpoint(compound);
            map->prepare_index = idx;
            goto mapped;
        }
        if (map->planned_pseudo && (argop->argop == OP_PUTFH ||
                                    argop->argop == OP_PUTROOTFH || argop->argop == OP_PUTPUBFH)) {
            idx = map->vfs_res = chimera_vfs_compound_add_checkpoint(compound);
            goto mapped;
        }
        switch (argop->argop) {
            case OP_PUTFH:
                if (i == first) {
                    /* The seed PUTFH above is this op. */
                    map->vfs_res = idx;
                } else {
                    /* Explicit selection starts a new credential group when needed. */
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

                    if (!nfs4_vfs_cursor_base(thread, later_fh, &later_len)) {
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

            case OP_PUTROOTFH:
            case OP_PUTPUBFH:
                if (!(entry_export && i == first) && chimera_vfs_compound_add_putroot(compound) < 0) {
                    goto refuse;
                }
                idx            = nfs4_vfs_export_path(compound, ctx->namespace_root->path, true);
                map->vfs_res   = idx;
                cur_open_flags = 0;
                break;

            case OP_LOOKUP:
                if (map->runtime_lookup) {
                    map->junction_start = chimera_vfs_compound_add_putroot(compound);
                    map->junction_end   = nfs4_vfs_export_path(compound, map->junction_target->export.path, false);
                    cur_open_flags      = 0;
                    if (map->junction_start < 0 || map->junction_end < 0 ||
                        nfs4_vfs_open_for(compound, &cur_open_flags, NFS4_VFS_OPEN_DIR) < 0 ||
                        chimera_vfs_compound_add_lookup(compound, (const char *) argop->oplookup.objname.data,
                                                        argop->oplookup.objname.len, 0, 0) < 0) {
                        goto refuse;
                    }
                    idx            = map->vfs_res = chimera_vfs_compound_add_getfh(compound);
                    cur_open_flags = 0;
                    break;
                }
                if (map->junction) {
                    if (chimera_vfs_compound_add_putroot(compound) < 0) {
                        goto refuse;
                    }
                    idx            = nfs4_vfs_export_path(compound, map->identity->export.path, false);
                    map->vfs_res   = idx;
                    cur_open_flags = 0;
                    break;
                }
                if (entry_export && i == first) {
                    idx            = nfs4_vfs_export_path(compound, entry_path, false);
                    map->vfs_res   = idx;
                    cur_open_flags = 0;
                    break;
                }
                if (map->attrdir) {
                    if (nfs4_vfs_open_for(compound, &cur_open_flags, NFS4_VFS_OPEN_META) < 0) {
                        goto refuse;
                    }
                    idx = chimera_vfs_compound_add_open_stream(compound,
                                                               (const char *) argop->oplookup.objname.data,
                                                               argop->oplookup.objname.len, 0, NULL, 0);
                    map->vfs_res   = idx;
                    cur_open_flags = 0;
                    break;
                }
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

            case OP_OPENATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags, NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }
                idx          = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = map->vfs_res = idx;
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
                if (idx >= 0 && !map->attrdir &&
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
                                      NFS4_VFS_OPEN_META | CHIMERA_VFS_OPEN_NOFOLLOW) < 0) {
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
            {
                const uint8_t placeholder = 0;
                map->parent_seed = chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
                if (map->parent_seed < 0) {
                    goto refuse;
                }
                cur_open_flags = 0;
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                if (chimera_vfs_compound_add_lookupp(compound, 0) < 0) {
                    goto refuse;
                }
                idx            = map->vfs_res = chimera_vfs_compound_add_getfh(compound);
                cur_open_flags = 0;
                break;
            }

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
                 * two errors SECINFO is specified to return. At a junction,
                 * prepare skips these helpers and selects the frozen target
                 * policy without resolving its backing path. */
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
                                      NFS4_VFS_OPEN_META | CHIMERA_VFS_OPEN_NOFOLLOW) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_res = idx;
                break;

            case OP_READ_PLUS:
            {
                struct evpl_iovec *riov = xdr_dbuf_alloc_space(
                    sizeof(*riov) * CHIMERA_NFS4_READ_MAX_IOV, req->encoding->dbuf);

                chimera_nfs_abort_if(riov == NULL, "Failed to allocate space");
                map->io_authorize = 1;
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META | CHIMERA_VFS_OPEN_NOFOLLOW) < 0) {
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
                                                    ->opread_plus.rpa_count, riov, CHIMERA_NFS4_READ_MAX_IOV, 0, NULL,
                                                    NULL,
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
                            sizeof(*riov) * CHIMERA_NFS4_READ_MAX_IOV,
                            req->encoding->dbuf);

                        chimera_nfs_abort_if(riov == NULL,
                                             "Failed to allocate space");

                        /* From the reply's own memory, because the descriptors
                         * the read writes here are where the reply will read
                         * them from and cannot be moved afterwards. */
                        idx = chimera_vfs_compound_add_read(compound, map->io_handle, argop->opread.offset, argop->
                                                            opread.count, riov, CHIMERA_NFS4_READ_MAX_IOV, 0, NULL, NULL
                                                            , 0)
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

            case OP_LAYOUTGET:
                map->layoutget = nfs4_layoutget_alloc(req, map->res_index, &ctx->layouts, &map->verify_status);
                if (!map->layoutget) {
                    goto refuse;
                }
                idx            = nfs4_layoutget_append(compound, map->layoutget);
                map->vfs_res   = idx;
                cur_open_flags = 0;
                break;

            case OP_LAYOUTRETURN:
                if (argop->oplayoutreturn.lora_layoutreturn.lr_returntype == LAYOUTRETURN4_FILE) {
                    idx = chimera_vfs_compound_add_coordinate(compound, nfs4_vfs_layoutreturn_coordinate, map);
                    if (idx < 0) {
                        goto refuse;
                    }
                } else {
                    idx = chimera_vfs_compound_add_checkpoint(compound);
                    if (idx < 0) {
                        goto refuse;
                    }
                }
                idx          = chimera_vfs_compound_add_checkpoint(compound);
                map->vfs_res = idx;
                if (idx >= 0) {
                    chimera_vfs_compound_set_op_prepare(compound, idx, nfs4_vfs_layoutreturn_stage, map);
                }
                break;

            case OP_LAYOUTCOMMIT:
            {
                struct chimera_vfs_attrs attrs = { 0 };
                if (nfs4_vfs_open_for(compound, &cur_open_flags, CHIMERA_VFS_OPEN_INFERRED) < 0) {
                    goto refuse;
                }
                map->vfs_aux = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_SIZE);
                if (map->vfs_aux < 0) {
                    goto refuse;
                }
                idx          = chimera_vfs_compound_add_setattr(compound, NULL, &attrs, 0, CHIMERA_VFS_ATTR_SIZE);
                map->vfs_res = idx;
                if (idx >= 0) {
                    chimera_vfs_compound_op_args(compound, idx)->setattr_after_write = 1;
                    chimera_vfs_compound_set_op_prepare(compound, idx, nfs4_vfs_layoutcommit_prepare, map);
                }
                break;
            }

            case OP_SETATTR:
            {
                struct chimera_vfs_attrs sattr;

                memset(&sattr, 0, sizeof(sattr));
                map->input_status = nfs4_vfs_decode_attrs(map, &argop->opsetattr.obj_attributes, &sattr);
                if (map->input_status != NFS4_OK) {
                    idx          = chimera_vfs_compound_add_checkpoint(compound);
                    map->vfs_res = idx;
                    break;
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
                if (map->attrdir) {
                    if (nfs4_vfs_open_for(compound, &cur_open_flags, NFS4_VFS_OPEN_META) < 0) {
                        goto refuse;
                    }
                    idx = chimera_vfs_compound_add_remove_stream(compound,
                                                                 (const char *) argop->opremove.target.data,
                                                                 argop->opremove.target.len);
                    map->vfs_res = idx;
                    break;
                }
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                if (chimera_server_config_get_nfs4_delegations(thread->shared->config) ||
                    chimera_vfs_pnfs_enabled(thread->vfs)) {
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
                if (nfs4_vfs_open_for(compound, &cur_open_flags, NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }
                if (chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
                    /* Legacy RENAME first verifies the target directory. The
                     * internal source walk uses RESTOREFH but never SAVEFH,
                     * preserving the protocol's saved directory for mutation. */
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
                /* Open the target directory before the source-side recall
                 * and link, matching standalone ordering. The VFS link
                 * recalls source caching leases before changing its nlink. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags, NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }
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

            case OP_DELEGRETURN:
            case OP_FREE_STATEID:
            case OP_RELEASE_LOCKOWNER:
            case OP_OPEN_CONFIRM:
            case OP_IO_ADVISE:
            case OP_SECINFO_NO_NAME:
            case OP_CLOSE:
                idx          = chimera_vfs_compound_add_checkpoint(compound);
                map->vfs_res = idx;
                break;

            case OP_OPEN:
                if (map->owner_status != NFS4_OK) {
                    idx            = chimera_vfs_compound_add_checkpoint(compound);
                    map->vfs_res   = idx;
                    cur_open_flags = 0;
                    break;
                }
                map->open_stream = map->attrdir;
                if (root_open) {
                    map->input_status = nfs4_vfs_open_is_delegated(&argop->opopen) ?
                        NFS4ERR_BAD_STATEID : NFS4ERR_STALE;
                }
                if (req->minorversion == 0 && map->open_group) {
                    map->open_response = calloc(1, sizeof(*map->open_response));
                    if (!map->open_response) {
                        goto refuse;
                    }
                }
                /* A named OPEN resolves in the current directory; an unnamed
                * one re-opens the current object and needs nothing first. */
                if (!map->open_stream && !root_open && nfs4_vfs_open_is_named(argop) &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                if (!root_open && !map->open_stream && argop->opopen.claim.claim == CLAIM_DELEGATE_CUR) {
                    const xdr_opaque *name = &argop->opopen.claim.delegate_cur_info.file;
                    idx = chimera_vfs_compound_add_lookup(compound, (const char *) name->data, name->len,
                                                          CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL |
                                                          CHIMERA_VFS_ATTR_FH,
                                                          CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME);
                    if (idx < 0) {
                        goto refuse;
                    }
                    map->open_deleg_lookup = idx;
                    chimera_vfs_compound_set_op_callbacks(compound, idx, NULL,
                                                          nfs4_vfs_open_delegated_checked, map);
                }

                if (map->open_stream) {
                    idx = chimera_vfs_compound_add_coordinate(compound, nfs4_vfs_stream_coordinate, map);
                    if (idx < 0) {
                        goto refuse;
                    }
                    chimera_vfs_compound_set_op_callbacks(compound, idx, NULL,
                                                          nfs4_vfs_stream_coordinated, map);
                }
                idx = nfs4_vfs_add_open_op(req, compound,
                                           argop, map);
                map->vfs_res = idx;
                if (map->reserved_open && idx >= 0 && map->input_status == NFS4_OK) {
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
                        if (chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
                            idx = chimera_vfs_compound_add_coordinate(compound, nfs4_vfs_layout_coordinate, map);
                            if (idx < 0) {
                                goto refuse;
                            }
                            chimera_vfs_compound_set_op_prepare(compound, idx, nfs4_vfs_open_layout_prepare, map);
                        }
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
                    uint8_t placeholder = 0; /* Filled or skipped by replay preparation. */
                    idx = chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
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
                                      NFS4_VFS_OPEN_META | CHIMERA_VFS_OPEN_NOFOLLOW) < 0) {
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
            {
                uint64_t mask = chimera_nfs4_attr2mask(argop->opreaddir.attr_request,
                                                       argop->opreaddir.num_attr_request);
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      map->attrdir ? NFS4_VFS_OPEN_META : NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }
                if (map->attrdir) {
                    map->vfs_aux = chimera_vfs_compound_add_getattr(compound, mask);
                    if (map->vfs_aux < 0) {
                        goto refuse;
                    }
                    idx = chimera_vfs_compound_add_list_streams(compound, argop->opreaddir.cookie,
                                                                nfs4_vfs_readdir_verifier(&argop->opreaddir), 64 * 1024,
                                                                true);
                } else {
                    idx = chimera_vfs_compound_add_readdir_stream(compound, argop->opreaddir.cookie,
                                                                  nfs4_vfs_readdir_verifier(&argop->opreaddir),
                                                                  mask | CHIMERA_VFS_ATTR_FH, 0, 0, NULL, 0,
                                                                  nfs4_vfs_readdir_reset, nfs4_vfs_readdir_append, ctx);
                }
                map->vfs_res = idx;
                if (map->maybe_pseudo) {
                    if (idx < 0) {
                        goto refuse;
                    }
                    map->root_readdir_start = chimera_vfs_compound_num_ops(compound);
                    idx                     = nfs4_root_readdir_add(compound, req, &argop->opreaddir,
                                                                    &map->root_readdir, &map->root_readdir_status,
                                                                    &map->verify_status);
                    /* The root walk changes only backend scratch cursors.
                     * The protocol result remains the synthetic root. */
                    cur_open_flags = 0;
                }
                break;
            }

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
                                      NFS4_VFS_OPEN_META | CHIMERA_VFS_OPEN_NOFOLLOW) < 0) {
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
                int      copy        = argop->argop == OP_COPY;
                uint8_t  placeholder = 0; /* Destination is known only during execution. */
                uint64_t src_offset  = copy ? argop->opcopy.ca_src_offset : argop->opclone.cl_src_offset;
                uint64_t dst_offset  = copy ? argop->opcopy.ca_dst_offset : argop->opclone.cl_dst_offset;
                uint64_t length      = copy ? argop->opcopy.ca_count : argop->opclone.cl_count;
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
                map->range_restore  = chimera_vfs_compound_add_putfh(compound, &placeholder, 1);
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

            /* f*xattr backends require a data descriptor, including when
             * a preceding metadata operation left a PATH handle open. */
            case OP_GETXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      CHIMERA_VFS_OPEN_INFERRED) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_SETXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      CHIMERA_VFS_OPEN_INFERRED) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_LISTXATTRS:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      CHIMERA_VFS_OPEN_INFERRED) < 0) {
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
                                      CHIMERA_VFS_OPEN_INFERRED) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            default:
                idx = -1;
                break;
        } /* switch */

 mapped:
        if (idx < 0) {
            goto refuse;
        }

        map->vfs_hi = idx;
        if (map->maybe_pseudo || map->planned_pseudo) {
            cur_open_flags = 0;
        }
        next = idx + 1;
    }

    ctx->num_ops = k;
    if (chimera_vfs_compound_num_ops(compound) > CHIMERA_VFS_COMPOUND_MAX_OPS) {
        goto refuse;
    }

    {
        if (ctx->ops[0].group_lo) {
            struct chimera_vfs_compound_group_config prelude = {
                .first_op = 0,
                .num_ops  = ctx->ops[0].group_lo,
                .cred     = ctx->root_probe >= 0 ? &ctx->namespace_identity->cred : &ctx->ops[0].planned_identity->
                    cred,
                .dependency = -1,
            };
            if (chimera_vfs_compound_add_group(compound, &prelude) < 0) {
                goto refuse;
            }
        }
        for (uint32_t start = 0, end; start < ctx->num_ops; start = end) {
            for (end = start + 1; end < ctx->num_ops && !ctx->ops[end].group_start; end++) {
            }
            struct chimera_vfs_compound_group_config group = {
                .first_op    = ctx->ops[start].group_lo,
                .num_ops     = ctx->ops[end - 1].vfs_hi + 1 - ctx->ops[start].group_lo,
                .cred        = &ctx->ops[start].planned_identity->cred,
                .select_cred = dynamic_identity ? nfs4_vfs_group_cred : NULL,
                .context     = &ctx->ops[start],
                .dependency  = -1,
            };
            if (chimera_vfs_compound_add_group(compound, &group) < 0) {
                goto refuse;
            }
        }
    }

    ctx->attempt_mark = req->encoding->dbuf->used;
    /* Construction allocated buffers already included in the static bound. */
    uint64_t construction_bytes = ctx->attempt_mark - (req->encoding->dbuf->size - avail);
    if (construction_bytes > reply_bound) {
        goto refuse;
    }
    ctx->reply_bound = reply_bound - construction_bytes;
    chimera_vfs_compound_set_attempt_reset(compound, nfs4_vfs_attempt_reset, ctx);
    chimera_vfs_compound_set_gate(compound, nfs4_vfs_operation_complete, ctx);
    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op *map = &ctx->ops[k];
        if (map->terminal_status) {
            continue;
        }
        if (map->vfs_aux >= 0) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_aux,
                                                  NULL, nfs4_vfs_compound_gate, ctx);
        }
        if (req->args_compound->argarray[map->res_index].argop == OP_READDIR ||
            req->args_compound->argarray[map->res_index].argop == OP_PUTROOTFH ||
            req->args_compound->argarray[map->res_index].argop == OP_PUTPUBFH ||
            req->args_compound->argarray[map->res_index].argop == OP_CREATE ||
            req->args_compound->argarray[map->res_index].argop == OP_LOOKUP ||
            req->args_compound->argarray[map->res_index].argop == OP_LOOKUPP) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_res,
                                                  NULL, nfs4_vfs_compound_gate, ctx);
        } else if (req->args_compound->argarray[map->res_index].argop == OP_SECINFO ||
                   req->args_compound->argarray[map->res_index].argop == OP_SECINFO_NO_NAME) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_res,
                                                  NULL, nfs4_vfs_secinfo_checked, map);
        } else if (req->args_compound->argarray[map->res_index].argop == OP_LOCKT) {
            chimera_vfs_compound_set_op_callbacks(compound, map->vfs_res,
                                                  NULL, nfs4_vfs_lockt_complete, map);
        } else if (req->args_compound->argarray[map->res_index].argop == OP_GETATTR) {
            chimera_vfs_compound_set_op_callbacks(compound,
                                                  map->getattr_coordinate ? map->getattr_coordinate : map->vfs_res,
                                                  NULL, nfs4_vfs_getattr_complete, map);
        }
    }
    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op *map    = &ctx->ops[k];
        uint32_t            opcode = req->args_compound->argarray[map->res_index].argop;
        if (map->terminal_status) {
            chimera_vfs_compound_set_op_prepare(compound, map->prepare_index,
                                                nfs4_vfs_operation_prepare, map);
            continue;
        }
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
        if (map->runtime_parent) {
            for (int r = map->prepare_index; r <= map->vfs_hi; r++) {
                chimera_vfs_compound_set_op_prepare(compound, r, nfs4_vfs_parent_prepare, map);
            }
            continue;
        }
        if (map->runtime_lookup) {
            for (int r = map->prepare_index; r <= map->vfs_hi; r++) {
                chimera_vfs_compound_set_op_prepare(compound, r, nfs4_vfs_lookup_prepare, map);
            }
            continue;
        }
        if (opcode == OP_SECINFO) {
            for (int r = map->prepare_index; r <= map->vfs_hi; r++) {
                chimera_vfs_compound_set_op_prepare(compound, r, nfs4_vfs_secinfo_prepare, map);
            }
            continue;
        }
        if (opcode == OP_COPY || opcode == OP_CLONE) {
            for (int r = map->prepare_index; r <= map->vfs_hi; r++) {
                chimera_vfs_compound_set_op_prepare(compound, r, nfs4_vfs_range_prepare, map);
            }
            continue;
        }
        chimera_vfs_compound_set_op_prepare(compound, map->prepare_index,
                                            nfs4_vfs_operation_prepare, map);
        if ((map->io_authorize || opcode == OP_LINK || opcode == OP_LOOKUP || opcode == OP_LOOKUPP || opcode ==
             OP_SECINFO ||
             opcode == OP_PUTROOTFH || opcode == OP_PUTPUBFH) &&
            map->vfs_res != map->prepare_index) {
            chimera_vfs_compound_set_op_prepare(compound, map->vfs_res,
                                                nfs4_vfs_operation_prepare, map);
        }
        if (req->args_compound->argarray[map->res_index].argop == OP_READ_PLUS) {
            chimera_vfs_compound_set_op_prepare(compound, map->read_plus_data,
                                                nfs4_vfs_operation_prepare, map);
        }
    }
    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op *map = &ctx->ops[k];
        for (int r = map->prepare_index; r <= map->vfs_hi; r++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, r);
            ctx->namespace_prepare[r] = op->prepare;
            ctx->namespace_context[r] = op->prepare_private;
            chimera_vfs_compound_set_op_prepare(compound, r, nfs4_vfs_namespace_op_prepare, map);
        }
    }
 submit:
    chimera_nfs_debug("NFS4_VFS_SUBMIT tag=%.*s start=%u count=%u compound=%p",
                      (int) req->args_compound->tag.len, req->args_compound->tag.data,
                      first, nenc - first, compound);
    chimera_nfs_debug("NFS4 VFS compound %p: wire first=%u count=%u vfs_ops=%u reserved_open=%d",
                      compound, first, nenc - first, chimera_vfs_compound_num_ops(compound),
                      ctx->num_reserved);

    chimera_vfs_compound_submit(compound, nfs4_vfs_compound_complete, ctx);

    return 1;

 refuse:

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
} /* nfs4_vfs_submit */

int
chimera_nfs4_compound_try_vfs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    return nfs4_vfs_submit(thread, req, false, NULL, NULL, NULL, NULL);
} /* chimera_nfs4_compound_try_vfs */

void
chimera_nfs4_compound_root_resolve(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    nfs4_root_export_fh_callback_t    callback)
{
    struct chimera_nfs_export root;

    if (chimera_nfs_get_export_copy(thread->shared, "/", &root) != 0) {
        callback(CHIMERA_VFS_ENOENT, NULL, 0, thread, req);
        return;
    }
    if (!nfs4_vfs_submit(thread, req, true, NULL, &root, root.path, callback)) {
        callback(CHIMERA_VFS_EAGAIN, NULL, 0, thread, req);
    }
} /* chimera_nfs4_compound_root_resolve */

void
chimera_nfs4_compound_export(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct chimera_nfs_export  *export,
    const char                       *path)
{
    nfsstat4 status = NFS4ERR_DELAY;

    while (*path == '/') {
        path++;
    }
    chimera_nfs_set_export(req, export);
    if (nfs4_vfs_submit(thread, req, false, &status, export, path, NULL) ||
        nfs4_vfs_submit(thread, req, true, &status, export, path, NULL)) {
        return;
    }
    nfs4_fail_undispatched_op(thread, &req->args_compound->argarray[req->index],
                              &req->res_compound.resarray[req->index], status);
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_compound_export */

/* The dispatcher has handled validation and namespace/export junctions.
 * Use the same builder, callouts, retry and publication as a longer
 * wire run, even when its conservative reply budget declined coalescing. */
void
chimera_nfs4_compound_single(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    /* These operations have no synthetic-directory implementation. Their
     * former standalone builders passed the synthetic FH to the VFS, whose
     * mount lookup returned STALE. Preserve that result without accidentally
     * addressing the attribute directory's base inode. */
    uint32_t op = req->args_compound->argarray[req->index].argop;

    if (op == OP_LAYOUTCOMMIT) {
        nfsstat4 status = !chimera_vfs_pnfs_feature_enabled(thread->vfs) ? NFS4ERR_NOTSUPP :
            !req->fhlen ? NFS4ERR_NOFILEHANDLE :
            (fh_is_nfs4_root(req->fh, req->fhlen) || chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) ?
            NFS4ERR_BAD_STATEID : NFS4_OK;
        if (status != NFS4_OK) {
            nfs4_fail_undispatched_op(thread, &req->args_compound->argarray[req->index],
                                      &req->res_compound.resarray[req->index], status);
            chimera_nfs4_compound_complete(req, status);
            return;
        }
    }
    if (nfs4_vfs_data_op(op)) {
        struct nfs_argop4 *args   = &req->args_compound->argarray[req->index];
        nfsstat4           status = NFS4_OK;
        if (op == OP_SEEK || op == OP_WRITE_SAME) {
            status = nfs4_vfs_data_input_status(args);
        }
        if (status == NFS4_OK && (op == OP_READ || op == OP_READ_PLUS || op == OP_WRITE)) {
            status = nfs_recovery_io_check(&thread->shared->nfs4_recovery);
        }
        if (status == NFS4_OK && !req->fhlen) {
            status = NFS4ERR_NOFILEHANDLE;
        }
        if (status == NFS4_OK) {
            status = nfs4_vfs_data_input_status(args);
        }
        if (status == NFS4_OK && op == OP_SETATTR && args->opsetattr.obj_attributes.num_attrmask &&
            (args->opsetattr.obj_attributes.attrmask[0] & (1U << FATTR4_SIZE))) {
            status = nfs_recovery_io_check(&thread->shared->nfs4_recovery);
        }
        if (status != NFS4_OK) {
            nfs4_fail_undispatched_op(thread, args, &req->res_compound.resarray[req->index], status);
            chimera_nfs4_compound_complete(req, status);
            return;
        }
    }

    nfsstat4 synthetic = nfs4_vfs_synthetic_status(op, fh_is_nfs4_root(req->fh, req->fhlen),
                                                   chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen));
    if (synthetic != NFS4_OK) {
        nfs4_fail_undispatched_op(thread, &req->args_compound->argarray[req->index],
                                  &req->res_compound.resarray[req->index], synthetic);
        chimera_nfs4_compound_complete(req, synthetic);
        return;
    }
    nfsstat4 status = NFS4ERR_DELAY;
    if (!nfs4_vfs_submit(thread, req, true, &status, NULL, NULL, NULL)) {
        nfs4_fail_undispatched_op(thread, &req->args_compound->argarray[req->index],
                                  &req->res_compound.resarray[req->index], status);
        chimera_nfs4_compound_complete(req, status);
    }
} /* chimera_nfs4_compound_single */

/* State operations use the reserved-owner journal even when a capacity or
 * reservation boundary sent dispatch here. There is no direct mutation path
 * to bypass a competing compound's reservations. */
void
chimera_nfs4_compound_state(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    nfsstat4 status = NFS4ERR_DELAY;

    uint32_t opcode = req->args_compound->argarray[req->index].argop;

    if (!req->fhlen && opcode != OP_FREE_STATEID && opcode != OP_RELEASE_LOCKOWNER && opcode != OP_LAYOUTRETURN) {
        status = NFS4ERR_NOFILEHANDLE;
    } else if (nfs4_vfs_submit(thread, req, false, &status, NULL, NULL, NULL) ||
               nfs4_vfs_submit(thread, req, true, &status, NULL, NULL, NULL)) {
        return;
    }
    nfs4_fail_undispatched_op(thread, &req->args_compound->argarray[req->index],
                              &req->res_compound.resarray[req->index], status);
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_compound_state */
