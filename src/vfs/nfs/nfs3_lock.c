// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"

/*
 * CHIMERA_VFS_CAP_CLAIM_RANGE for the NFS proxy.
 *
 * A byte-range claim reaches this backend after the claim core has arbitrated
 * it locally; taking the matching NLM lock on the upstream server is what makes
 * a conflict with some OTHER client of that server visible to the core.
 *
 * Two vocabularies meet here, and neither is the other's:
 *
 *   Length.  The claim wire spells to-EOF as UINT64_MAX and means a genuine
 *   zero-byte range by 0.  NLM, like POSIX, spells to-EOF as 0 and cannot
 *   express a zero-byte range at all.  Translated in both directions, the
 *   conflict reported back from a TEST included.
 *
 *   Identity.  NLM names a lock by (caller_name, oh, svid) and an unlock by
 *   that plus the exact range, while the claim wire names it by a token the
 *   backend mints.  struct chimera_nfs3_range holds the mapping, since a
 *   CHIMERA_VFS_OP_CLAIM_RELEASE by token arrives carrying nothing else.  A
 *   release by geometry (token 0) instead names the range the way the acquire
 *   named it, and every record of that owner's which overlaps it is undone --
 *   one NLM UNLOCK each, over the bytes that record actually covers.
 *
 * The owner handle is derived from the claim's cluster-stable owner identity
 * rather than from a node-local pointer, so one POSIX owner still coalesces on
 * the server when its requests arrive through two different chimera nodes.
 */

/*
 * Context stored in request->plugin_data for the duration of the NLM call.
 * The oh field provides a request-lifetime home for the owner handle: NLM
 * passes oh by pointer into an async RPC, so it must not live on the stack of
 * chimera_nfs3_do_lock.
 */
struct nfs3_lock_ctx {
    struct chimera_nfs_thread               *nfs_thread;
    struct chimera_nfs_shared               *shared;
    struct chimera_nfs_client_server_thread *server_thread;
    uint8_t                                  oh[CHIMERA_NFS3_LOCK_OH_SIZE];
    /* Release only: the record being undone, freed once the server answers. */
    struct chimera_nfs3_range               *range;
    /* Release by geometry only: the records matched but not yet unlocked, and
     * the first failure any of their unlocks reported. */
    struct chimera_nfs3_range               *pending;
    enum chimera_vfs_error ranged_status;
};

static void chimera_nfs3_do_lock(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request);

static void chimera_nfs3_unlock_ranged_send(
    struct chimera_vfs_request *request);

/* Claim wire to NLM: to-EOF is UINT64_MAX on the claim wire and 0 on the wire
 * NLM speaks. */
static inline uint64_t
chimera_nfs3_lock_nlm_len(uint64_t length)
{
    return length == UINT64_MAX ? 0 : length;
} /* chimera_nfs3_lock_nlm_len */

/* NLM back to the claim wire. */
static inline uint64_t
chimera_nfs3_lock_claim_len(uint64_t l_len)
{
    return l_len == 0 ? UINT64_MAX : l_len;
} /* chimera_nfs3_lock_claim_len */

/* Flatten the cluster-stable owner identity into the opaque bytes NLM carries
 * as the lock's owner handle. */
static void
chimera_nfs3_lock_owner_handle(
    const struct chimera_claim_owner *owner,
    uint8_t                          *oh)
{
    oh[0] = owner->proto;
    memcpy(oh + 1, &owner->client_key, sizeof(owner->client_key));
    memcpy(oh + 9, &owner->owner_lo, sizeof(owner->owner_lo));
    memcpy(oh + 17, &owner->owner_hi, sizeof(owner->owner_hi));
} /* chimera_nfs3_lock_owner_handle */

/* Mint a token without a record behind it, for a range that was never put on
 * the wire.  Its release finds nothing to undo and completes as a no-op. */
static uint64_t
chimera_nfs3_range_mint(struct chimera_nfs_shared *shared)
{
    uint64_t token;

    pthread_mutex_lock(&shared->nlm_range_lock);
    token = ++shared->nlm_next_token;
    pthread_mutex_unlock(&shared->nlm_range_lock);

    return token;
} /* chimera_nfs3_range_mint */

/* Remember a granted range so its unlock can be rebuilt from the token alone.
 * The claim fields are already absolute here: a SEEK_END request was resolved
 * against the server's size before the lock went out. */
static uint64_t
chimera_nfs3_range_insert(
    struct chimera_nfs_shared        *shared,
    const struct chimera_vfs_request *request,
    const uint8_t                    *oh)
{
    struct chimera_nfs3_range *range = calloc(1, sizeof(*range));

    memcpy(range->fh, request->fh, request->fh_len);
    range->fh_len = (int) request->fh_len;
    range->offset = request->claim_acquire.offset;
    range->length = request->claim_acquire.length;
    memcpy(range->oh, oh, CHIMERA_NFS3_LOCK_OH_SIZE);

    pthread_mutex_lock(&shared->nlm_range_lock);
    range->token = ++shared->nlm_next_token;
    DL_APPEND(shared->nlm_ranges, range);
    pthread_mutex_unlock(&shared->nlm_range_lock);

    return range->token;
} /* chimera_nfs3_range_insert */

/* Unlink the record named by a token and hand it to the caller, who owns it. */
static struct chimera_nfs3_range *
chimera_nfs3_range_take(
    struct chimera_nfs_shared *shared,
    uint64_t                   token)
{
    struct chimera_nfs3_range *range;

    pthread_mutex_lock(&shared->nlm_range_lock);

    for (range = shared->nlm_ranges; range; range = range->next) {
        if (range->token == token) {
            DL_DELETE(shared->nlm_ranges, range);
            break;
        }
    }

    pthread_mutex_unlock(&shared->nlm_range_lock);

    return range;
} /* chimera_nfs3_range_take */

/* Does this owner hold anything at all on this file?  Asked before a release
 * by geometry goes to the wire for a size it would have nothing to do with. */
static int
chimera_nfs3_range_owner_holds(
    struct chimera_nfs_shared *shared,
    const uint8_t             *fh,
    int                        fh_len,
    const uint8_t             *oh)
{
    struct chimera_nfs3_range *range;
    int                        held = 0;

    pthread_mutex_lock(&shared->nlm_range_lock);

    DL_FOREACH(shared->nlm_ranges, range)
    {
        if (range->fh_len == fh_len &&
            memcmp(range->fh, fh, fh_len) == 0 &&
            memcmp(range->oh, oh, CHIMERA_NFS3_LOCK_OH_SIZE) == 0) {
            held = 1;
            break;
        }
    }

    pthread_mutex_unlock(&shared->nlm_range_lock);

    return held;
} /* chimera_nfs3_range_owner_holds */

/* Unlink every record this owner holds on this file which overlaps the given
 * absolute range, and hand the list to the caller, who owns it.  The oh is the
 * owner identity flattened by chimera_nfs3_lock_owner_handle(), which is
 * injective over exactly the fields chimera_claim_owner_equal() compares, so
 * matching on it is that comparison -- against records that keep no owner
 * struct of their own.  Records carry the claim wire's spelling already
 * (UINT64_MAX = to-EOF), and a zero-byte range never became a record at all. */
static struct chimera_nfs3_range *
chimera_nfs3_range_take_overlapping(
    struct chimera_nfs_shared *shared,
    const uint8_t             *fh,
    int                        fh_len,
    const uint8_t             *oh,
    uint64_t                   offset,
    uint64_t                   length)
{
    struct chimera_nfs3_range *range, *tmp, *matched = NULL;

    pthread_mutex_lock(&shared->nlm_range_lock);

    DL_FOREACH_SAFE(shared->nlm_ranges, range, tmp)
    {
        if (range->fh_len != fh_len ||
            memcmp(range->fh, fh, fh_len) != 0 ||
            memcmp(range->oh, oh, CHIMERA_NFS3_LOCK_OH_SIZE) != 0) {
            continue;
        }

        if (!chimera_vfs_claim_range_overlap_i(range->offset, range->length,
                                               offset, length)) {
            continue;
        }

        DL_DELETE(shared->nlm_ranges, range);
        DL_APPEND(matched, range);
    }

    pthread_mutex_unlock(&shared->nlm_range_lock);

    return matched;
} /* chimera_nfs3_range_take_overlapping */

static void
chimera_nfs3_lock_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs3_lock_ctx       *ctx     = request->plugin_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    switch (res->stat) {
        case NLM4_GRANTED:
            request->claim_acquire.r_token = chimera_nfs3_range_insert(
                ctx->shared, request, ctx->oh);
            request->claim_acquire.r_granted = 1;
            request->status                  = CHIMERA_VFS_OK;
            break;
        case NLM4_DENIED:
            /* Somebody else holds it.  A refusal, not a failure; the LOCK reply
             * carries no holder, so there is no conflict detail to report. */
            request->status = CHIMERA_VFS_OK;
            break;
        case NLM4_BLOCKED:
            /* Queued upstream, to be granted later by an NLM_GRANTED callback
             * this client does not serve.  Not granted, as far as we know. */
            request->status = CHIMERA_VFS_OK;
            break;
        case NLM4_STALE_FH:
            request->status = CHIMERA_VFS_ESTALE;
            break;
        default:
            request->status = CHIMERA_VFS_EFAULT;
            break;
    } /* switch */

    request->complete(request);
} /* chimera_nfs3_lock_callback */

static void
chimera_nfs3_unlock_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs3_lock_ctx       *ctx     = request->plugin_data;

    free(ctx->range);
    ctx->range = NULL;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    request->status = (res->stat == NLM4_GRANTED) ? CHIMERA_VFS_OK : CHIMERA_VFS_EFAULT;
    request->complete(request);
} /* chimera_nfs3_unlock_callback */

static void
chimera_nfs3_test_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_testres         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nlm4_holder         *holder;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    /* A probe acquires nothing, so r_granted and r_token stay 0 and the answer
     * is the conflict block. */
    switch (res->test_stat.stat) {
        case NLM4_GRANTED:
            request->claim_acquire.r_conflict_type = CHIMERA_VFS_LOCK_UNLOCK;
            request->status                        = CHIMERA_VFS_OK;
            break;
        case NLM4_DENIED:
            holder                                 = &res->test_stat.holder;
            request->claim_acquire.r_conflict_type = holder->exclusive
                                              ? CHIMERA_VFS_LOCK_WRITE
                                              : CHIMERA_VFS_LOCK_READ;
            request->claim_acquire.r_conflict_offset = holder->l_offset;
            request->claim_acquire.r_conflict_length =
                chimera_nfs3_lock_claim_len(holder->l_len);
            request->claim_acquire.r_conflict_pid = (uint32_t) holder->svid;
            request->status                       = CHIMERA_VFS_OK;
            break;
        default:
            request->status = CHIMERA_VFS_EFAULT;
            break;
    } /* switch */

    request->complete(request);
} /* chimera_nfs3_test_callback */

static void
chimera_nfs3_do_lock(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request)
{
    struct nfs3_lock_ctx *ctx = request->plugin_data;
    struct evpl_rpc2_cred rpc2_cred;
    uint8_t              *fh;
    int                   fhlen;
    uint64_t              nlm_len;

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    nlm_len = chimera_nfs3_lock_nlm_len(request->claim_acquire.length);

    if (request->claim_acquire.flags & CHIMERA_VFS_CLAIM_TEST) {
        struct nlm4_testargs args;

        memset(&args, 0, sizeof(args));
        args.exclusive             = request->claim_acquire.exclusive;
        args.alock.caller_name.str = (char *) request->thread->vfs->machine_name;
        args.alock.caller_name.len = request->thread->vfs->machine_name_len;
        args.alock.fh.data         = fh;
        args.alock.fh.len          = fhlen;
        args.alock.oh.data         = ctx->oh;
        args.alock.oh.len          = sizeof(ctx->oh);
        args.alock.svid            = 0;
        args.alock.l_offset        = request->claim_acquire.offset;
        args.alock.l_len           = nlm_len;

        shared->nlm_v4.send_call_NLMPROC4_TEST(&shared->nlm_v4.rpc2, thread->evpl,
                                               server_thread->nlm_conn, &rpc2_cred,
                                               &args, 0, 0, NULL, 0, 0,
                                               chimera_nfs3_test_callback, request);

    } else {
        struct nlm4_lockargs args;

        memset(&args, 0, sizeof(args));
        args.block                 = (request->claim_acquire.flags & CHIMERA_VFS_CLAIM_WAIT) ? 1 : 0;
        args.exclusive             = request->claim_acquire.exclusive;
        args.reclaim               = 0;
        args.state                 = 0;
        args.alock.caller_name.str = (char *) request->thread->vfs->machine_name;
        args.alock.caller_name.len = request->thread->vfs->machine_name_len;
        args.alock.fh.data         = fh;
        args.alock.fh.len          = fhlen;
        args.alock.oh.data         = ctx->oh;
        args.alock.oh.len          = sizeof(ctx->oh);
        args.alock.svid            = 0;
        args.alock.l_offset        = request->claim_acquire.offset;
        args.alock.l_len           = nlm_len;

        shared->nlm_v4.send_call_NLMPROC4_LOCK(&shared->nlm_v4.rpc2, thread->evpl,
                                               server_thread->nlm_conn, &rpc2_cred,
                                               &args, 0, 0, NULL, 0, 0,
                                               chimera_nfs3_lock_callback, request);
    }
} /* chimera_nfs3_do_lock */

static void
chimera_nfs3_lock_getattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct GETATTR3res          *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request              *request       = private_data;
    struct nfs3_lock_ctx                    *ctx           = request->plugin_data;
    struct chimera_nfs_thread               *thread        = ctx->nfs_thread;
    struct chimera_nfs_shared               *shared        = ctx->shared;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    int64_t                                  raw_offset, raw_length, base;
    uint64_t                                 file_size;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    /* Resolve the SEEK_END range into the absolute, claim-wire form the rest of
     * this path -- and the release record -- speaks.  offset and length arrived
     * as bit-casts of signed off_t values keeping the POSIX flock conventions
     * intact: a negative l_len runs backwards from l_start, and l_len 0 is
     * to-EOF. */
    file_size  = res->resok.obj_attributes.size;
    raw_offset = (int64_t) request->claim_acquire.offset;
    raw_length = (int64_t) request->claim_acquire.length;

    base = (int64_t) file_size + raw_offset;

    if (raw_length < 0) {
        base += raw_length;
    }

    if (base < 0) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    request->claim_acquire.offset = (uint64_t) base;

    if (raw_length > 0) {
        request->claim_acquire.length = (uint64_t) raw_length;
    } else if (raw_length < 0) {
        request->claim_acquire.length = 0 - (uint64_t) raw_length;
    } else {
        request->claim_acquire.length = UINT64_MAX;  /* to-EOF */
    }

    request->claim_acquire.whence = SEEK_SET;

    chimera_nfs3_do_lock(thread, shared, server_thread, request);
} /* chimera_nfs3_lock_getattr_callback */

void
chimera_nfs3_claim_acquire(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct nfs3_lock_ctx                    *ctx;
    struct GETATTR3args                      getattr_args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (request->claim_acquire.klass != CHIMERA_VFS_CLAIM_KLASS_RANGE) {
        /* This module arbitrates ranges only; it does not declare
         * CHIMERA_VFS_CAP_CLAIM_AGGREGATE. */
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    if (!server_thread->nlm_conn) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    if (request->claim_acquire.whence != SEEK_END &&
        request->claim_acquire.length == 0) {
        /* A genuine zero-byte range, which NLM cannot express at all since an
         * l_len of 0 already means to-EOF.  The core has arbitrated it locally;
         * granting it unprojected beats refusing an SMB zero-byte lock.  A
         * probe of one likewise sees no conflict upstream. */
        if (!(request->claim_acquire.flags & CHIMERA_VFS_CLAIM_TEST)) {
            request->claim_acquire.r_token   = chimera_nfs3_range_mint(shared);
            request->claim_acquire.r_granted = 1;
        }
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Always initialize ctx: chimera_nfs3_do_lock reads ctx->oh from
     * plugin_data regardless of whence.  SEEK_CUR is normalized to SEEK_SET
     * by the POSIX layer (posix_fcntl.c) before reaching here, so only
     * SEEK_SET and SEEK_END need to be handled. */
    ctx                = request->plugin_data;
    ctx->nfs_thread    = thread;
    ctx->shared        = shared;
    ctx->server_thread = server_thread;
    ctx->range         = NULL;

    chimera_nfs3_lock_owner_handle(&request->claim_acquire.owner, ctx->oh);

    if (request->claim_acquire.whence == SEEK_END) {

        chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

        getattr_args.object.data.data = fh;
        getattr_args.object.data.len  = fhlen;

        chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                                   request->thread->vfs->machine_name,
                                   request->thread->vfs->machine_name_len);

        shared->nfs_v3.send_call_NFSPROC3_GETATTR(&shared->nfs_v3.rpc2, thread->evpl,
                                                  server_thread->nfs_conn, &rpc2_cred,
                                                  &getattr_args, 0, 0, NULL, 0, 0,
                                                  chimera_nfs3_lock_getattr_callback, request);
    } else {
        chimera_nfs3_do_lock(thread, shared, server_thread, request);
    }
} /* chimera_nfs3_claim_acquire */

static void
chimera_nfs3_unlock_ranged_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs3_lock_ctx       *ctx     = request->plugin_data;

    free(ctx->range);
    ctx->range = NULL;

    if (ctx->ranged_status == CHIMERA_VFS_OK &&
        (unlikely(status) || res->stat != NLM4_GRANTED)) {
        ctx->ranged_status = CHIMERA_VFS_EFAULT;
    }

    /* Keep going whatever this one answered: the records are already off the
     * registry, so anything left unsent would stand upstream with nothing on
     * this side left naming it. */
    chimera_nfs3_unlock_ranged_send(request);
} /* chimera_nfs3_unlock_ranged_callback */

/* Undo the next matched record, or finish once they are all undone. */
static void
chimera_nfs3_unlock_ranged_send(struct chimera_vfs_request *request)
{
    struct nfs3_lock_ctx      *ctx = request->plugin_data;
    struct chimera_nfs3_range *range;
    struct nlm4_unlockargs     args;
    struct evpl_rpc2_cred      rpc2_cred;
    uint8_t                   *fh;
    int                        fhlen;

    if (!ctx->pending) {
        request->status = ctx->ranged_status;
        request->complete(request);
        return;
    }

    range = ctx->pending;
    DL_DELETE(ctx->pending, range);
    ctx->range = range;

    chimera_nfs3_map_fh(range->fh, range->fh_len, &fh, &fhlen);

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    memset(&args, 0, sizeof(args));
    args.alock.caller_name.str = (char *) request->thread->vfs->machine_name;
    args.alock.caller_name.len = request->thread->vfs->machine_name_len;
    args.alock.fh.data         = fh;
    args.alock.fh.len          = fhlen;
    args.alock.oh.data         = ctx->oh;
    args.alock.oh.len          = sizeof(ctx->oh);
    args.alock.svid            = 0;
    args.alock.l_offset        = range->offset;
    args.alock.l_len           = chimera_nfs3_lock_nlm_len(range->length);

    ctx->shared->nlm_v4.send_call_NLMPROC4_UNLOCK(&ctx->shared->nlm_v4.rpc2,
                                                  ctx->nfs_thread->evpl,
                                                  ctx->server_thread->nlm_conn,
                                                  &rpc2_cred, &args, 0, 0, NULL, 0, 0,
                                                  chimera_nfs3_unlock_ranged_callback,
                                                  request);
} /* chimera_nfs3_unlock_ranged_send */

static void
chimera_nfs3_unlock_ranged_getattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct GETATTR3res          *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs3_lock_ctx       *ctx     = request->plugin_data;
    int64_t                     raw_offset, raw_length, base;
    uint64_t                    file_size, offset, length;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    /* The same resolution the acquire did, on the same spelling: offset and
     * length are bit-casts of the caller's signed l_start and l_len, a negative
     * l_len runs backwards from l_start, and an l_len of 0 is to-EOF. */
    file_size  = res->resok.obj_attributes.size;
    raw_offset = (int64_t) request->claim_release.offset;
    raw_length = (int64_t) request->claim_release.length;

    base = (int64_t) file_size + raw_offset;

    if (raw_length < 0) {
        base += raw_length;
    }

    if (base < 0) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    offset = (uint64_t) base;

    if (raw_length > 0) {
        length = (uint64_t) raw_length;
    } else if (raw_length < 0) {
        length = 0 - (uint64_t) raw_length;
    } else {
        length = UINT64_MAX;  /* to-EOF */
    }

    ctx->pending = chimera_nfs3_range_take_overlapping(ctx->shared,
                                                       request->fh,
                                                       (int) request->fh_len,
                                                       ctx->oh,
                                                       offset, length);

    chimera_nfs3_unlock_ranged_send(request);
} /* chimera_nfs3_unlock_ranged_getattr_callback */

/* Release by GEOMETRY (claim_release.token == 0): the caller never learned the
 * absolute bytes it holds -- a SEEK_END lock is resolved against the server's
 * size and the resolution is never reported back -- so it names the range to
 * drop in exactly the spelling it named the lock, and this side resolves EOF
 * again.  Matching nothing is success, and is answered without troubling the
 * server for a size no record would have been measured against. */
static void
chimera_nfs3_claim_release_ranged(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct nfs3_lock_ctx                    *ctx;
    struct GETATTR3args                      getattr_args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                  oh[CHIMERA_NFS3_LOCK_OH_SIZE];
    uint8_t                                 *fh;
    int                                      fhlen;

    chimera_nfs3_lock_owner_handle(&request->claim_release.owner, oh);

    if (!chimera_nfs3_range_owner_holds(shared, request->fh,
                                        (int) request->fh_len, oh)) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    ctx                = request->plugin_data;
    ctx->nfs_thread    = thread;
    ctx->shared        = shared;
    ctx->range         = NULL;
    ctx->pending       = NULL;
    ctx->ranged_status = CHIMERA_VFS_OK;

    memcpy(ctx->oh, oh, sizeof(ctx->oh));

    server_thread      = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    ctx->server_thread = server_thread;

    if (!server_thread || !server_thread->nlm_conn) {
        /* The mount these locks belonged to is gone; the server drops its locks
         * when it monitors us down, so every record this owner has on the file
         * is dead whatever the geometry says, and freeing them is all that is
         * left to do. */
        struct chimera_nfs3_range *range, *tmp;

        ctx->pending = chimera_nfs3_range_take_overlapping(shared, request->fh,
                                                           (int) request->fh_len,
                                                           oh, 0, UINT64_MAX);

        DL_FOREACH_SAFE(ctx->pending, range, tmp)
        {
            DL_DELETE(ctx->pending, range);
            free(range);
        }

        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    if (request->claim_release.whence == SEEK_END) {
        chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

        getattr_args.object.data.data = fh;
        getattr_args.object.data.len  = fhlen;

        chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                                   request->thread->vfs->machine_name,
                                   request->thread->vfs->machine_name_len);

        shared->nfs_v3.send_call_NFSPROC3_GETATTR(&shared->nfs_v3.rpc2, thread->evpl,
                                                  server_thread->nfs_conn, &rpc2_cred,
                                                  &getattr_args, 0, 0, NULL, 0, 0,
                                                  chimera_nfs3_unlock_ranged_getattr_callback,
                                                  request);
        return;
    }

    ctx->pending = chimera_nfs3_range_take_overlapping(shared, request->fh,
                                                       (int) request->fh_len, oh,
                                                       request->claim_release.offset,
                                                       request->claim_release.length);

    chimera_nfs3_unlock_ranged_send(request);
} /* chimera_nfs3_claim_release_ranged */

void
chimera_nfs3_claim_release(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs3_range               *range;
    struct nfs3_lock_ctx                    *ctx;
    struct nlm4_unlockargs                   args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    /* claim_release.retained is an AGGREGATE downgrade mask; a RANGE record is
     * binding and all-or-nothing, so the release simply drops it. */

    if (request->claim_release.token == 0 &&
        request->claim_release.klass == CHIMERA_VFS_CLAIM_KLASS_RANGE) {
        chimera_nfs3_claim_release_ranged(thread, shared, request);
        return;
    }

    range = chimera_nfs3_range_take(shared, request->claim_release.token);

    if (!range) {
        /* No record under that token: a range that was never put on the wire,
         * or a release arriving twice.  Nothing to undo. */
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    server_thread = chimera_nfs_thread_get_server_thread(thread, range->fh, range->fh_len);

    if (!server_thread || !server_thread->nlm_conn) {
        /* The mount this lock belonged to is gone; the server drops its locks
         * when it monitors us down, so the record is all that is left to free. */
        free(range);
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    ctx                = request->plugin_data;
    ctx->nfs_thread    = thread;
    ctx->shared        = shared;
    ctx->server_thread = server_thread;
    ctx->range         = range;

    memcpy(ctx->oh, range->oh, sizeof(ctx->oh));

    chimera_nfs3_map_fh(range->fh, range->fh_len, &fh, &fhlen);

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    memset(&args, 0, sizeof(args));
    args.alock.caller_name.str = (char *) request->thread->vfs->machine_name;
    args.alock.caller_name.len = request->thread->vfs->machine_name_len;
    args.alock.fh.data         = fh;
    args.alock.fh.len          = fhlen;
    args.alock.oh.data         = ctx->oh;
    args.alock.oh.len          = sizeof(ctx->oh);
    args.alock.svid            = 0;
    args.alock.l_offset        = range->offset;
    args.alock.l_len           = chimera_nfs3_lock_nlm_len(range->length);

    shared->nlm_v4.send_call_NLMPROC4_UNLOCK(&shared->nlm_v4.rpc2, thread->evpl,
                                             server_thread->nlm_conn, &rpc2_cred,
                                             &args, 0, 0, NULL, 0, 0,
                                             chimera_nfs3_unlock_callback, request);
} /* chimera_nfs3_claim_release */
