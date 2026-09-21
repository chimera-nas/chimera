// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "nfs4_callback.h"
#include "nfs4_named_attr.h"
#include "server/server.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"
#include "vfs/sdk/vfs_access.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/vfs_idmap.h"

/*
 * The claim-core SHARE reservation an OPEN asks for, built from the
 * operation's arguments and the client that sent them.
 *
 * Everything here is a pure function of the wire and the client table, which
 * is why it is separable from taking it: a sequenced OPEN builds the lease
 * when the sequence is BUILT and lets a CLAIM op of the run do the
 * arbitration, and the op-at-a-time path below builds it and acquires in the
 * same breath.  The lease is heap-allocated because the claim core keeps
 * pointers into a claim it has inserted (see struct nfs4_share_lease).
 *
 * Returns NULL when the OPEN reserves nothing at all -- which the scan makes
 * unreachable for a sequence (share_access must carry READ or WRITE) but the
 * op-at-a-time path still has to tolerate.
 */
SYMBOL_EXPORT struct nfs4_share_lease *
chimera_nfs4_open_share_lease_build(
    const struct OPEN4args *args,
    struct nfs_client      *client,
    uint32_t                share_access,
    uint32_t                share_deny)
{
    struct nfs4_share_lease   *share;
    struct chimera_claim_owner owner;
    uint8_t                    granted = 0;
    uint8_t                    denied  = 0;

    if (share_access & OPEN4_SHARE_ACCESS_READ) {
        granted |= CHIMERA_CLAIM_R;
    }
    if (share_access & OPEN4_SHARE_ACCESS_WRITE) {
        granted |= CHIMERA_CLAIM_W;
    }
    if (share_deny & OPEN4_SHARE_DENY_READ) {
        denied |= CHIMERA_CLAIM_R;
    }
    if (share_deny & OPEN4_SHARE_DENY_WRITE) {
        denied |= CHIMERA_CLAIM_W;
    }

    if (granted == 0 && denied == 0) {
        return NULL;
    }

    share = calloc(1, sizeof(*share));

    if (!share) {
        return NULL;
    }

    memset(&owner, 0, sizeof(owner));
    owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    owner.client_key = client->client_id;
    /* The open-owner string as it arrived, which is what the open_owner this
     * OPEN resolves to is keyed on -- so a lease built before the state
     * exists and one built from the state carry the same owner. */
    owner.owner_lo = XXH3_64bits(args->owner.owner.data,
                                 args->owner.owner.len);
    owner.owner_hi = 0;

    chimera_vfs_claim_init_nfs4_open(&share->claim, granted, denied, &owner);
    /* Courteous server: report this share reservation dead once the owning
     * client's lease lapses, so a conflicting open reclaims it; reclaim flags
     * the client for sweep teardown. */
    share->claim.is_alive_cb = nfs_client_lease_alive;
    share->claim.revoked_cb  = nfs_client_lease_revoked_cb;
    share->claim.cb_private  = client;

    return share;
} /* chimera_nfs4_open_share_lease_build */

/*
 * Take a built lease against `handle`, the op-at-a-time way.
 *
 * Also the sequenced path's fallback: a run that fails AFTER the OPEN's CLAIM
 * was granted has the claim released out from under it by the executor's abort
 * release, which happens before the completion callback -- so the OPEN's fill
 * finds its file state gone and arbitrates again, here, for the result it is
 * in the middle of writing.  That is the asymmetry with LOCK, which ends its
 * run rather than re-arbitrate: a claim taken for an op whose result is
 * already committed cannot be re-arbitrated, and one taken for the op
 * currently being filled can.
 */
SYMBOL_EXPORT nfsstat4
chimera_nfs4_open_share_lease_acquire(
    struct nfs_request             *req,
    struct nfs4_share_lease        *share,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_state      *vfs_state = req->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file_state;
    enum chimera_vfs_claim_result  result;

    file_state = chimera_vfs_state_get(vfs_state,
                                       handle->fh, handle->fh_len,
                                       handle->fh_hash, true);
    if (!file_state) {
        return NFS4ERR_SERVERFAULT;
    }

    result = chimera_vfs_claim_try_acquire(vfs_state, file_state,
                                           &share->claim, NULL);

    return chimera_nfs4_open_share_status(share, file_state, result);
} /* chimera_nfs4_open_share_lease_acquire */

/*
 * What an arbitration answer means to an OPEN, and what the lease owns
 * afterwards.  Shared by the acquire above and by the sequenced path, whose
 * answer comes back on a CLAIM op of the run rather than from a call.
 */
SYMBOL_EXPORT nfsstat4
chimera_nfs4_open_share_status(
    struct nfs4_share_lease       *share,
    struct chimera_vfs_file_state *file_state,
    enum chimera_vfs_claim_result  result)
{
    if (result == CHIMERA_CLAIM_GRANTED) {
        share->file_state = file_state;
        share->held       = true;
        return NFS4_OK;
    }

    /* Not granted: the lease owns nothing.  The caller frees it. */
    share->file_state = NULL;

    if (result == CHIMERA_CLAIM_BREAKING) {
        /* The conflict is a breakable holder -- an NFSv4 delegation being
         * recalled (the arbitration already kicked the break).  Tell the
         * client to retry; by the next attempt the delegation's DELEGRETURN
         * should have released the claim and the SHARE will be granted (RFC
         * 7530 §10.2 recommends NFS4ERR_DELAY while a recall is
         * outstanding). */
        return NFS4ERR_DELAY;
    }

    return NFS4ERR_SHARE_DENIED;
} /* chimera_nfs4_open_share_status */

/*
 * Acquire a cross-protocol SHARE reservation in the claim core for a freshly
 * created open_state.  Upstream's nfs_client_check_share_conflict already
 * enforces share-mode conflicts *among NFSv4 owners of the same client*;
 * this adds the NLM/SMB dimension so an NFSv4 OPEN that denies read/write
 * collides with an SMB open holding that access (and vice versa).
 *
 * Returns NFS4_OK on success (lease stored on the state, released at
 * open_state_cleanup), NFS4ERR_SHARE_DENIED on cross-protocol conflict.
 */
static nfsstat4
chimera_nfs4_open_acquire_share(
    struct nfs_request    *req,
    struct nfs_open_state *state)
{
    struct OPEN4args         *args      = &req->args_compound->argarray[req->index].opopen;
    struct chimera_vfs_state *vfs_state = req->thread->vfs->vfs_state;
    struct nfs4_share_lease  *share;
    nfsstat4                  status;

    /* share_access MUST request at least one of READ or WRITE (RFC 7530
     * §16.16.5 / §9.9); a value with neither base mode is invalid. */
    if ((args->share_access &
         (OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WRITE)) == 0) {
        return NFS4ERR_INVAL;
    }

    share = chimera_nfs4_open_share_lease_build(args, state->owner->client,
                                                args->share_access,
                                                args->share_deny);

    if (!share) {
        return NFS4_OK;
    }

    status = chimera_nfs4_open_share_lease_acquire(req, share, state->handle);

    if (status != NFS4_OK) {
        nfs4_share_lease_free(vfs_state, share);
        return status;
    }

    state->share = share;
    return NFS4_OK;
} /* chimera_nfs4_open_acquire_share */

/*
 * Report a declined delegation.  RFC 8881 §18.16: a server that supports the
 * OPEN4_SHARE_ACCESS_WANT_* flags (chimera advertises WANT_ANY_DELEG and
 * WANT_NO_DELEG in supported_attrs' open_arguments) MUST answer an OPEN that
 * carried one of them with OPEN_DELEGATE_NONE_EXT and a reason, so the client
 * can tell contention from resource exhaustion.  A 4.0 client, or a 4.1 client
 * that expressed no preference, keeps the bare OPEN_DELEGATE_NONE.
 *
 * Always returns false, so decline sites can `return
 * chimera_nfs4_open_deleg_none(...)` in place of a bare `return false`.
 */
static bool
chimera_nfs4_open_deleg_none(
    struct nfs_request *req,
    struct OPEN4res    *res,
    uint32_t            why)
{
    struct OPEN4args *args = &req->args_compound->argarray[req->index].opopen;
    uint32_t          want = args->share_access & OPEN4_SHARE_ACCESS_WANT_DELEG_MASK;

    res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;

    if (req->minorversion < 1 || want == OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE) {
        return false;
    }

    /* A want that only asked to cancel an outstanding registration is not a
     * failure to delegate; report it as such. */
    if (want == OPEN4_SHARE_ACCESS_WANT_CANCEL) {
        why = WND4_CANCELLED;
    }

    res->resok4.delegation.delegation_type    = OPEN_DELEGATE_NONE_EXT;
    res->resok4.delegation.od_whynone.ond_why = why;

    /* The union arm is selected by ond_why; chimera never registers a want, so
     * it promises neither a push nor a signal. */
    if (why == WND4_CONTENTION) {
        res->resok4.delegation.od_whynone.ond_server_will_push_deleg = 0;
    } else if (why == WND4_RESOURCE) {
        res->resok4.delegation.od_whynone.ond_server_will_signal_avail = 0;
    }

    return false;
} /* chimera_nfs4_open_deleg_none */

/*
 * Decide whether to grant an OPEN delegation and, if so, mint it and populate
 * the OPEN response.  A read open is offered an OPEN_DELEGATE_READ; an open
 * that requests write access is offered an OPEN_DELEGATE_WRITE.  A delegation
 * is granted only when:
 *   - the nfs4_delegations config knob is on,
 *   - this is a normal open (CLAIM_NULL / CLAIM_FH, not a reclaim),
 *   - the client's callback path has been validated (CB_NULL probe UP), and
 *   - no conflicting access is present on the file (a write delegation also
 *     requires no other reader/writer; conflicts are surfaced by the CACHING
 *     lease conflict matrix).
 * Otherwise the response carries a decline built by
 * chimera_nfs4_open_deleg_none().  Must be called on the
 * client's connection thread (it may kick a CB_NULL probe).
 *
 * Returns true when the OPEN's response has been parked on the cb_path's
 * probe-waiters list (a CB_NULL probe is in flight for this client and a
 * concurrent earlier OPEN already triggered it).  In that case the caller
 * MUST NOT call chimera_nfs4_open_complete -- the probe completion will
 * resume the OPEN via chimera_nfs4_open_resume_after_probe.  Returns false
 * for the normal path (granted or not), where the caller continues with
 * chimera_nfs4_open_complete.
 */
SYMBOL_EXPORT bool
chimera_nfs4_open_grant_delegation(
    struct nfs_request             *req,
    struct OPEN4res                *res,
    const struct chimera_vfs_attrs *file_attr)
{
    struct chimera_server_nfs_thread *thread    = req->thread;
    struct OPEN4args                 *args      = &req->args_compound->argarray[req->index].opopen;
    struct nfs_client                *client    = req->session ? req->session->client_unified : NULL;
    struct chimera_vfs_state         *vfs_state = thread->vfs->vfs_state;
    struct nfs_delegation            *deleg;
    struct nfs_delegation            *d;
    struct chimera_vfs_file_state    *file_state;
    struct chimera_claim_owner        owner;
    enum chimera_vfs_claim_result     result;
    struct stateid4                   deleg_stateid;
    uint64_t                          fh_hash;
    bool                              exists = false;
    uint8_t                           deleg_type;
    uint32_t                          want;
    struct nfsace4                   *perms;
    struct chimera_principal          who;
    char                             *who_str;
    int                               who_len;

    res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;

    if (!chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
        return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
    }
    if (!client) {
        return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
    }
    if (args->claim.claim != CLAIM_NULL && args->claim.claim != CLAIM_FH) {
        return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
    }

    /* RFC 8881 §18.16: honor an explicit "no delegation wanted" request.  The
     * WANT_ values are a field in share_access, not independent bits, so they
     * have to be compared under the mask; WANT_CANCEL also means "no
     * delegation" (and reports WND4_CANCELLED instead). */
    want = args->share_access & OPEN4_SHARE_ACCESS_WANT_DELEG_MASK;
    if (want == OPEN4_SHARE_ACCESS_WANT_NO_DELEG ||
        want == OPEN4_SHARE_ACCESS_WANT_CANCEL) {
        return chimera_nfs4_open_deleg_none(req, res, WND4_NOT_WANTED);
    }

    /* A write open earns a write delegation; otherwise a pure-read open earns
     * a read delegation. */
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        deleg_type = OPEN_DELEGATE_WRITE;
    } else if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        deleg_type = OPEN_DELEGATE_READ;
    } else {
        return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
    }

    /* Tri-state probe gate.  PATH_UP grants below; NO_PATH is the historical
     * "no delegation this time" path (either DOWN, or this very OPEN just
     * kicked the probe); DEFER parks the OPEN on the cb_path until the in-
     * flight CB_NULL probe completes (handled by nfs4_cb_null_complete via
     * chimera_nfs4_open_resume_after_probe).  Deferring is what lets a
     * second OPEN that lands during the probe still receive a delegation,
     * without changing the kicking OPEN's behavior -- so pynfs DELEG22's
     * second OPEN gets the delegation it expects, while DELEG15d's plain
     * c.create_file (which kicks the probe) continues to receive none. */
    switch (nfs4_cb_grant_probe(thread, client, req)) {
        case NFS4_CB_GRANT_PATH_UP:
            break;
        case NFS4_CB_GRANT_NO_PATH:
            return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
        case NFS4_CB_GRANT_DEFER:
            nfs4_cb_probe_park(client, req);
            return true;
    } /* switch */

    /* RFC 7530 §9.1.11: "The server must not bestow a delegation for any open
     * that would require confirmation", and §16.18.5: "Servers MUST NOT
     * require confirmation on OPENs that grant delegations."  The two
     * decisions are made in different places -- install_state has already
     * committed OPEN4_RESULT_CONFIRM into the reply for a 4.0 OPEN on an
     * unconfirmed open_owner -- so read that decision back and decline rather
     * than hand out a delegation stateid on open state that is still subject
     * to cancellation if OPEN_CONFIRM never arrives.  The client gets one on
     * its next OPEN.  Deliberately after the probe gate above so the first
     * OPEN of a new owner still kicks the CB_NULL probe, leaving the path
     * validated by the time that next OPEN arrives. */
    if (res->resok4.rflags & OPEN4_RESULT_CONFIRM) {
        return false;
    }

    /* fh_hash must match the open-cache / SHARE-lease hashing so the
     * delegation and conflicting opens land on the same file_state. */
    fh_hash = XXH3_64bits(req->fh, req->fhlen) & INT64_MAX;

    evpl_mutex_lock(&client->lock);
    LL_FOREACH2(client->delegations, d, next_in_client)
    {
        if (d->fh_len == req->fhlen &&
            memcmp(d->fh, req->fh, req->fhlen) == 0) {
            exists = true;
            break;
        }
    }
    evpl_mutex_unlock(&client->lock);
    if (exists) {
        return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
    }

    deleg = nfs_delegation_create(client, deleg_type,
                                  req->fh, req->fhlen, fh_hash,
                                  req->export_id,
                                  &thread->shared->nfs4_state_table,
                                  &deleg_stateid);

    memset(&owner, 0, sizeof(owner));
    owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    owner.client_key = client->client_id;
    owner.owner_lo   = fh_hash;
    owner.owner_hi   = 0;

    chimera_vfs_claim_init_delegation(&deleg->claim,
                                      deleg_type == OPEN_DELEGATE_WRITE,
                                      &owner);
    deleg->claim.break_cb = nfs4_delegation_break_cb;
    /* Courteous server: report the delegation dead once the owning client's
     * lease lapses, so a conflicting open revokes it outright rather than
     * attempting a CB_RECALL to a client that is gone. */
    deleg->claim.is_alive_cb = nfs_delegation_lease_alive;
    deleg->claim.revoked_cb  = nfs_delegation_revoked_cb;
    deleg->claim.cb_private  = deleg;

    file_state = chimera_vfs_state_get(vfs_state, req->fh, req->fhlen,
                                       fh_hash, true);
    if (!file_state) {
        nfs_delegation_destroy(deleg, &thread->shared->nfs4_state_table,
                               thread->vfs_thread);
        return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
    }
    deleg->file_state = file_state;

    result = chimera_vfs_claim_try_acquire(vfs_state, file_state,
                                           &deleg->claim, NULL);
    if (result != CHIMERA_CLAIM_GRANTED) {
        /* Contention (another open / claim): just decline to delegate. */
        chimera_vfs_state_put(vfs_state, file_state);
        deleg->file_state = NULL;
        nfs_delegation_destroy(deleg, &thread->shared->nfs4_state_table,
                               thread->vfs_thread);
        return chimera_nfs4_open_deleg_none(req, res, WND4_CONTENTION);
    }
    deleg->lease_held = true;

    /* RFC 7530/8881 §10.4.3: cache the file's change attribute at grant (sc).
     * Only meaningful for a write delegation (the holder can modify locally);
     * captured when the OPEN path supplied the change-derivation attrs.  If they
     * are absent (e.g. CLAIM_FH, or a probe-deferred resume), combine_valid
     * stays false and the first peer CB_GETATTR captures sc lazily. */
    if (deleg_type == OPEN_DELEGATE_WRITE && file_attr &&
        (file_attr->va_set_mask &
         (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME))) {
        uint64_t sc = chimera_nfs4_change_from_attrs(file_attr);
        evpl_mutex_lock(&deleg->combine_lock);
        deleg->combine_sc    = sc;
        deleg->combine_last  = sc;
        deleg->combine_valid = true;
        evpl_mutex_unlock(&deleg->combine_lock);
    }

    res->resok4.delegation.delegation_type = deleg_type;

    if (deleg_type == OPEN_DELEGATE_WRITE) {
        res->resok4.delegation.write.stateid = deleg_stateid;
        res->resok4.delegation.write.recall  = 0;
        /* No space guarantee enforced -- advertise "no limit". */
        res->resok4.delegation.write.space_limit.limitby  = NFS_LIMIT_SIZE;
        res->resok4.delegation.write.space_limit.filesize = UINT64_MAX;
        perms                                             = &res->resok4.delegation.write.permissions;
    } else {
        res->resok4.delegation.read.stateid = deleg_stateid;
        res->resok4.delegation.read.recall  = 0;
        perms                               = &res->resok4.delegation.read.permissions;
    }

    /* RFC 7530 §16.16: the permissions ACE names the users that may open the
     * delegated file without an ACCESS call.  It therefore has to name the
     * principal this delegation was handed to and carry the rights the
     * delegation actually conveys: an EVERYONE@ ACE invites the client to skip
     * the server's per-user check for every other local user, and READ_DATA on
     * a write delegation sends the holder back for an ACCESS round trip to
     * write -- the very round trip the field exists to eliminate.  The who
     * string is rendered like FATTR4_OWNER (numeric form, no domain) and lives
     * in the reply's dbuf so it outlives this frame. */
    who     = chimera_idmap_uid_principal(req->cred.uid);
    who_str = xdr_dbuf_alloc_space(CHIMERA_IDMAP_WHO_MAX,
                                   req->encoding->dbuf);
    chimera_nfs_abort_if(who_str == NULL, "Failed to allocate space");
    who_len = chimera_idmap_principal_to_who(&who, NULL, who_str,
                                             CHIMERA_IDMAP_WHO_MAX);

    perms->type        = ACE4_ACCESS_ALLOWED_ACE_TYPE;
    perms->flag        = 0;
    perms->access_mask = ACE4_GENERIC_READ;

    if (deleg_type == OPEN_DELEGATE_WRITE) {
        /* The data/attribute rights a write delegation conveys; not WRITE_ACL
         * or WRITE_OWNER, which it does not. */
        perms->access_mask |= ACE4_WRITE_DATA | ACE4_APPEND_DATA |
            ACE4_WRITE_ATTRIBUTES;
    }

    perms->who.len  = who_len > 0 ? who_len : 0;
    perms->who.data = who_str;
    return false;
} /* chimera_nfs4_open_grant_delegation */

/*
 * Install a freshly-opened VFS handle into the unified state model, returning
 * the stateid to send back to the client.  Handles same-owner re-OPEN
 * coalescing per RFC 7530 §9.1.2: if an open_state already exists for
 * (open_owner, fh), share bits are merged and stateid.seqid is bumped; the
 * caller's incoming handle ref is released and the existing handle is reused.
 *
 * On entry, `handle` must be a +1 reference returned by chimera_vfs_open_at
 * or chimera_vfs_open_fh.  On create, ownership transfers to the new
 * open_state; on coalesce, the function calls chimera_vfs_release on it.
 *
 * `share` is a SHARE reservation already granted for this OPEN, whose
 * ownership passes here -- the sequenced path, where the arbitration was a
 * CLAIM op of the run.  NULL means "take it here", which is what the
 * op-at-a-time path does.  Either way it ends up on the new state, is
 * released on a coalesce (the first OPEN of this (owner, fh) keeps the
 * reservation, which is the rule this path has always followed), and is
 * given back on every error.
 */
SYMBOL_EXPORT nfsstat4
chimera_nfs4_open_install_state(
    struct nfs_request             *req,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_attrs *attr,
    bool                            file_created,
    const uint8_t                  *base_fh,
    int                             base_fh_len,
    struct nfs4_share_lease        *share,
    struct stateid4                *out_stateid,
    uint32_t                       *out_rflags)
{
    struct OPEN4args      *args   = &req->args_compound->argarray[req->index].opopen;
    struct nfs_client     *client = req->session ? req->session->client_unified : NULL;
    struct nfs_open_owner *owner;
    struct nfs_open_state *existing;
    bool                   created = false;
    nfsstat4               status;

    *out_rflags = 0;

    if (!client) {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        nfs4_share_lease_free(req->thread->vfs->vfs_state, share);
        return NFS4ERR_STALE_CLIENTID;
    }
    if (client->expired) {
        client->expired = 0;
        nfs_client_touch(client);
    }

    /* Enforce the object's ACL against the requested share access before the
     * share-reservation check, so a permission failure surfaces as
     * NFS4ERR_ACCESS (not NFS4ERR_SHARE_DENIED).  `attr` is NULL on a reopen by
     * filehandle (CLAIM_FH), where access was already established.  A create
     * that actually made the file grants the creator the requested access
     * regardless of the mode it was created with (POSIX/RFC: the access check
     * is bypassed for the creating open), so skip it when file_created. */
    if (attr && !file_created) {
        uint32_t required = 0;

        if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
            required |= CHIMERA_ACE_READ_DATA;
        }
        if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
            required |= CHIMERA_ACE_WRITE_DATA;
        }

        if (required &&
            !chimera_vfs_access_allowed(attr, &req->cred, required)) {
            chimera_vfs_release(req->thread->vfs_thread, handle);
            nfs4_share_lease_free(req->thread->vfs->vfs_state, share);
            return NFS4ERR_ACCESS;
        }
    }

    /* Resolve the open_owner on the completion path.  For a 4.0 OPEN the
    * dispatch path already pinned it on req->open_4_0_owner; passing it as
    * the adopt candidate means a lease sweep that unpublished it mid-flight
    * republishes the SAME struct here, keeping the seqid/replay bookkeeping
    * in chimera_nfs4_open_complete and this open_state on one object. */
    owner = nfs_open_owner_find_or_adopt(client,
                                         req->open_4_0_owner,
                                         args->owner.owner.data,
                                         args->owner.owner.len,
                                         &created);

    /* Pathological double-race: the sweep unpublished the pinned owner AND
     * another OPEN already recreated the key.  Move the request's seqid/replay
     * bookkeeping onto the published object so the reply is cached where the
     * client's retransmit will look. */
    if (req->open_4_0_owner && owner != req->open_4_0_owner) {
        nfs_open_owner_put(req->open_4_0_owner);
        nfs_open_owner_get(owner);
        req->open_4_0_owner = owner;
    }

    /* RFC 7530 §9.10: check share-mode conflict against opens by *other*
     * owners on this client.  Same-owner OPEN coalesces via the
     * find_state path below and is exempt.  Cross-client (and cross-
     * protocol) deny, including revocation of a conflicting courtesy
     * client whose lease has lapsed, is enforced by the VFS share-lease
     * layer when the open state is installed. */
    status = nfs_client_check_share_conflict(client, owner,
                                             handle->fh, handle->fh_len,
                                             args->share_access,
                                             args->share_deny);
    if (status != NFS4_OK) {
        goto err_release_handle;
    }

    existing = nfs_open_owner_find_state(owner, handle->fh, handle->fh_len);

    if (existing) {
        /* RFC 7530 §9.9: a same-owner re-open is still a distinct share
         * request -- it must not ask for access the existing open denies, nor
         * deny access the existing open holds.  (Plain deny=NONE upgrades, as
         * normal clients issue, never trip this.) */
        if ((existing->share_access & args->share_deny) ||
            (args->share_access & existing->share_deny)) {
            status = NFS4ERR_SHARE_DENIED;
            goto err_release_handle;
        }
        nfs_open_state_coalesce(existing,
                                args->share_access, args->share_deny,
                                &req->thread->shared->nfs4_state_table,
                                out_stateid);
        /* Keep the write-capable handle as the primary handle.  A separate
         * read-only handle must survive when the primary is write-only: the
         * union of two OPEN grants does not turn an O_WRONLY host fd into
         * O_RDWR.  Retain both until state cleanup, also protecting in-flight
         * I/O that borrowed the old handle before this upgrade. */
        if (existing->handle && !existing->handle_superseded &&
            existing->handle->access_mode != CHIMERA_VFS_ACCESS_MODE_RW &&
            handle->access_mode != existing->handle->access_mode) {
            if (handle->access_mode == CHIMERA_VFS_ACCESS_MODE_RO) {
                existing->handle_superseded = handle;
            } else {
                existing->handle_superseded = existing->handle;
                existing->handle            = handle;
            }
        } else {
            chimera_vfs_release(req->thread->vfs_thread, handle);
        }
        /* The SHARE reservation acquired on the first OPEN of this
         * (owner, fh) stays in force.  Broadening share bits on
         * coalesce is not re-checked cross-protocol in this pass —
         * upstream's intra-client check is likewise coalesce-exempt.
         *
         * A sequence that took one anyway -- because a coalesce is a fact
         * about a file handle the OPEN had not yet resolved when the run was
         * built -- gives it straight back.  The gate spares the CLAIM op
         * whenever it can see the coalesce coming, so this is the narrow
         * case where it could not. */
        nfs4_share_lease_free(req->thread->vfs->vfs_state, share);
        share = NULL;
    } else {
        struct nfs_open_state *new_state;

        new_state = nfs_open_state_create(owner,
                                          req->principal_flavor,
                                          req->principal_machinename,
                                          req->principal_machinename_len,
                                          handle->fh, handle->fh_len,
                                          args->share_access, args->share_deny,
                                          handle,
                                          &req->thread->shared->nfs4_state_table,
                                          out_stateid);
        if (!new_state) {
            /* The lease sweeper unpublished the owner between find_or_adopt
             * and here; installing would orphan the state. */
            status = NFS4ERR_EXPIRED;
            goto err_release_handle;
        }

        /* Cross-protocol SHARE coordination.  Already arbitrated when the
         * caller handed one over -- a CLAIM op of the run took it, and the
         * state simply adopts it.  Otherwise it is taken here.  On conflict,
         * tear the just-created state back down (releasing the handle) and
         * fail. */
        if (share) {
            new_state->share = share;
            share            = NULL;
        } else {
            status = chimera_nfs4_open_acquire_share(req, new_state);
            if (status != NFS4_OK) {
                nfs_open_state_destroy(new_state,
                                       &req->thread->shared->nfs4_state_table,
                                       req->thread->vfs_thread);
                goto err_put_owner;
            }
        }

        /* Named-attribute (stream) open: take a stream holder on the BASE file so
         * a cross-protocol base delete-on-close defers while this stream is open,
         * unifying delete-on-close with SMB ADS (smb_proc_close checks
         * stream_holders).  Released in open_state_cleanup. */
        if (base_fh) {
            struct chimera_vfs_state      *vfs_state = req->thread->vfs->vfs_state;
            struct chimera_vfs_file_state *base_fs;

            base_fs = chimera_vfs_state_get(vfs_state, base_fh, base_fh_len,
                                            chimera_vfs_hash(base_fh, base_fh_len),
                                            true);
            if (base_fs) {
                chimera_vfs_state_stream_holder_inc(base_fs);
                new_state->base_stream_file_state = base_fs;
            }
        }
    }

    /* RFC 7530 §16.18.5: signal OPEN4_RESULT_CONFIRM for an unconfirmed
     * open_owner on a 4.0 client.  The client must then send OPEN_CONFIRM
     * before issuing any further state-modifying op against this owner. */
    if (req->minorversion == 0 && !owner->confirmed) {
        *out_rflags |= OPEN4_RESULT_CONFIRM;
    }

    /* Done with the synchronous borrow; release the find_or_adopt ref.  The
     * hash-table slot ref (and any open_state referencing this owner) keeps it
     * alive. */
    nfs_open_owner_put(owner);
    return NFS4_OK;

 err_release_handle:
    chimera_vfs_release(req->thread->vfs_thread, handle);
 err_put_owner:
    nfs4_share_lease_free(req->thread->vfs->vfs_state, share);
    nfs_open_owner_put(owner);
    return status;
} /* chimera_nfs4_open_install_state */

/*
 * The part of installing an OPEN's state that has to be settled BEFORE the
 * share reservation is arbitrated -- asked separately so a sequenced OPEN can
 * ask it from its gate, while the object it opened is in hand and before the
 * CLAIM op that takes the reservation has run.
 *
 * It answers the two questions whose answers OUTRANK a share conflict, in the
 * order install_state asks them (so NFS4ERR_ACCESS still beats
 * NFS4ERR_SHARE_DENIED), plus one the arbitration itself depends on: whether
 * this OPEN will COALESCE onto an open this owner already holds on the
 * object.  A coalesce takes no new reservation -- the first OPEN's stays in
 * force -- so a sequence that knows the answer here can spare its CLAIM op
 * rather than take a reservation it would immediately give back.
 *
 * Everything it reads is memory the server already holds, it changes nothing,
 * and asking it twice asks the same question: the gate's contract exactly.
 * install_state asks all of it again afterwards, which is what keeps the two
 * entrances one body of rules rather than two.
 */
SYMBOL_EXPORT nfsstat4
chimera_nfs4_open_precheck(
    struct nfs_request             *req,
    const struct OPEN4args         *args,
    struct nfs_client              *client,
    struct nfs_open_owner          *owner,
    const struct chimera_vfs_attrs *attr,
    bool                            file_created,
    const uint8_t                  *fh,
    uint16_t                        fh_len,
    int                            *out_coalesce)
{
    struct nfs_open_state *existing;
    nfsstat4               status;

    *out_coalesce = 0;

    if (!client || !owner) {
        return NFS4ERR_STALE_CLIENTID;
    }

    /* The object's ACL against the requested share access -- install_state's
     * own check, verbatim, including why a create skips it. */
    if (attr && !file_created) {
        uint32_t required = 0;

        if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
            required |= CHIMERA_ACE_READ_DATA;
        }
        if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
            required |= CHIMERA_ACE_WRITE_DATA;
        }

        if (required &&
            !chimera_vfs_access_allowed(attr, &req->cred, required)) {
            return NFS4ERR_ACCESS;
        }
    }

    /* RFC 7530 §9.10: share-mode conflict against opens by *other* owners on
     * this client. */
    status = nfs_client_check_share_conflict(client, owner, fh, fh_len,
                                             args->share_access,
                                             args->share_deny);
    if (status != NFS4_OK) {
        return status;
    }

    existing = nfs_open_owner_find_state(owner, fh, fh_len);

    if (existing) {
        /* RFC 7530 §9.9: a same-owner re-open is still a distinct share
         * request -- it must not ask for access the existing open denies, nor
         * deny access the existing open holds. */
        if ((existing->share_access & args->share_deny) ||
            (args->share_access & existing->share_deny)) {
            return NFS4ERR_SHARE_DENIED;
        }

        *out_coalesce = 1;
    }

    return NFS4_OK;
} /* chimera_nfs4_open_precheck */

/*
 * RFC 7530 §9.1.7 OPEN completion: advance open_owner.seqid and cache the
 * reply for every outcome that nfs4_seqid_should_advance() reports as
 * advancing (NFS4_OK plus most logical errors -- everything except the
 * "infrastructure" set documented on that helper).  No-op on 4.1+ and on
 * the 4.0 path when req->open_4_0_owner is NULL (entry never classified
 * NEW, e.g. NOFILEHANDLE before the owner was even looked up).
 *
 * Then hands off to the regular compound dispatcher.  Every chimera_nfs4
 * OPEN response path in this file calls this wrapper instead of
 * chimera_nfs4_compound_complete directly.
 */
/*
 * Status for an OPEN whose target is not a regular file (mode already
 * fetched).  A directory is always NFS4ERR_ISDIR.  For any other
 * non-regular object the two minor versions diverge (RFC 7530 §16.16.6
 * vs RFC 8881 §18.16.4): 4.0 reports NFS4ERR_SYMLINK for every special
 * file, while 4.1+ reports NFS4ERR_SYMLINK only for an actual symlink and
 * NFS4ERR_WRONG_TYPE for fifos, sockets, and devices.
 */
SYMBOL_EXPORT nfsstat4
chimera_nfs4_open_nonreg_status(
    uint8_t minorversion,
    mode_t  mode)
{
    if (S_ISDIR(mode)) {
        return NFS4ERR_ISDIR;
    }
    if (minorversion == 0 || S_ISLNK(mode)) {
        return NFS4ERR_SYMLINK;
    }
    return NFS4ERR_WRONG_TYPE;
} /* chimera_nfs4_open_nonreg_status */

/*
 * Everything chimera_nfs4_open_finish does except hand the request on: the
 * RFC 7530 §9.1.7 seqid advance and replay record, the encoder's owner pin,
 * and the 4.1 current stateid.
 *
 * Split out because an OPEN that a VFS sequence carries is no longer
 * necessarily the last op of its COMPOUND.  When it is not, the OPEN's
 * wrapper has to run as its RESULT is filled -- while req->index still names
 * the OPEN -- and the ops behind it are filled afterwards, with the generic
 * completion handing the request on once.
 */
SYMBOL_EXPORT void
chimera_nfs4_open_settle(
    struct nfs_request *req,
    nfsstat4            status)
{
    if (req->minorversion == 0 &&
        req->open_4_0_owner &&
        nfs4_seqid_should_advance(status)) {

        struct nfs_open_owner *owner = req->open_4_0_owner;
        struct OPEN4args      *args  =
            &req->args_compound->argarray[req->index].opopen;
        struct OPEN4res       *res =
            &req->res_compound.resarray[req->index].opopen;

        evpl_mutex_lock(&owner->lock);
        owner->seqid = args->seqid;
        nfs4_replay_record(&owner->replay, args->seqid, OP_OPEN, status,
                           status == NFS4_OK ? &res->resok4.stateid : NULL);
        /* RFC 7530 §9.1.7 wants the stored last response replayed verbatim.
         * rflags carries OPEN4_RESULT_CONFIRM (§16.18.5), which a client may
         * act on, so keep it alongside the stateid. */
        if (status == NFS4_OK) {
            owner->replay.rflags = res->resok4.rflags;
        }
        evpl_mutex_unlock(&owner->lock);
    }

    /* Drop the borrow ref transferred onto the request in
     * chimera_nfs4_open.  Unconditional -- every 4.0 completion/error path that set
     * open_4_0_owner funnels through here exactly once; NULL it to guard
     * against any double drop. */
    if (req->open_4_0_owner) {
        nfs_open_owner_put(req->open_4_0_owner);
        req->open_4_0_owner = NULL;
    }

    /* NFS4.1: a successful OPEN sets the COMPOUND's current stateid. */
    if (status == NFS4_OK) {
        struct OPEN4res *res =
            &req->res_compound.resarray[req->index].opopen;
        chimera_nfs4_set_current_stateid(req, &res->resok4.stateid);
    }
} /* chimera_nfs4_open_settle */

static void
chimera_nfs4_open_finish(
    struct nfs_request *req,
    nfsstat4            status)
{
    chimera_nfs4_open_settle(req, status);

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_open_finish */

/*
 * Tear the open state this OPEN just installed back down.  Used when the
 * deferred truncate fails: the OPEN reports the error, so it must not leave a
 * stateid behind that the client was never told about.
 */
static void
chimera_nfs4_open_unwind_state(struct nfs_request *req)
{
    struct nfs_state_table *table = &req->thread->shared->nfs4_state_table;
    struct OPEN4res        *res   =
        &req->res_compound.resarray[req->index].opopen;
    void                   *state_void;
    uint8_t                 state_type;

    if (nfs_state_table_acquire(table, &res->resok4.stateid,
                                NFS4_SLOT_TYPE_OPEN,
                                &state_void, &state_type) != NFS4_OK) {
        return;
    }

    nfs_open_state_destroy(state_void, table, req->thread->vfs_thread);
    nfs_state_table_release(table, state_void, NFS4_SLOT_TYPE_OPEN,
                            req->thread->vfs_thread);
} /* chimera_nfs4_open_unwind_state */

static void
chimera_nfs4_open_trunc_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request     *req   = private_data;
    struct nfs_state_table *table = &req->thread->shared->nfs4_state_table;
    struct OPEN4res        *res   =
        &req->res_compound.resarray[req->index].opopen;
    enum chimera_vfs_error  error_code;

    error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    nfs_state_table_release(table, req->nfs_state_ref, NFS4_SLOT_TYPE_OPEN,
                            req->thread->vfs_thread);
    req->nfs_state_ref = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_open_unwind_state(req);
        chimera_nfs4_open_finish(req, res->status);
        return;
    }

    chimera_nfs4_open_finish(req, NFS4_OK);
} /* chimera_nfs4_open_trunc_complete */

/*
 * Every OPEN outcome funnels through here, including the one that parks on the
 * 4.0 CB_NULL probe -- so this is the one place that sees "the OPEN has
 * succeeded and its share reservation is held".  An UNCHECKED4 truncate of an
 * existing file is applied here rather than as part of the open, so an OPEN
 * that fails (share reservation, type, permission) leaves the file's contents
 * alone.
 *
 * The truncate runs through the just-installed open state's handle and the
 * descriptor-originated fsetattr, so it is authorized by the access this OPEN
 * was granted rather than re-checked against the file's mode -- the ftruncate
 * rule, and the same grant the client would use for a WRITE.
 */
SYMBOL_EXPORT void
chimera_nfs4_open_complete(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct nfs_state_table *table = &req->thread->shared->nfs4_state_table;
    struct OPEN4res        *res   =
        &req->res_compound.resarray[req->index].opopen;
    struct nfs_open_state  *open_state;
    void                   *state_void;
    uint8_t                 state_type;

    if (status == NFS4_OK && req->open_trunc_pending) {
        req->open_trunc_pending = false;

        if (nfs_state_table_acquire(table, &res->resok4.stateid,
                                    NFS4_SLOT_TYPE_OPEN,
                                    &state_void, &state_type) == NFS4_OK) {
            open_state          = state_void;
            req->nfs_state_ref  = state_void;
            req->nfs_state_type = NFS4_SLOT_TYPE_OPEN;

            memset(&req->open_trunc_attr, 0, sizeof(req->open_trunc_attr));
            req->open_trunc_attr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
            req->open_trunc_attr.va_req_mask = CHIMERA_VFS_ATTR_SIZE;
            req->open_trunc_attr.va_size     = 0;

            /* One op, against the OPEN's own handle: the stateid authorizes
             * the size change the way a descriptor authorizes ftruncate(2),
             * which is what a SETATTR through a handle the CALLER named means
             * -- rather than being re-gated against the file's mode. */
            struct chimera_vfs_compound *compound =
                chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                           &req->cred);

            chimera_vfs_compound_add_setattr(compound, open_state->handle,
                                             &req->open_trunc_attr, 0, 0);

            chimera_vfs_compound_submit(compound,
                                        chimera_nfs4_open_trunc_complete, req);
            return;
        }
        /* The state was reaped before the truncate could run (a lease sweep
         * racing this OPEN); the stateid is already dead, so report it as
         * such rather than truncating on its behalf. */
        res->status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_open_finish(req, res->status);
        return;
    }

    req->open_trunc_pending = false;
    chimera_nfs4_open_finish(req, status);
} /* chimera_nfs4_open_complete */

/*
 * Resume an OPEN that parked on the cb_path probe-waiters list while a
 * 4.0 CB_NULL probe was in flight.  Called by nfs4_cb_null_complete on the
 * channel's owner thread, once cb_state has settled to UP or DOWN.  Re-runs
 * the grant decision -- it can no longer DEFER -- and completes the OPEN.
 */
void
chimera_nfs4_open_resume_after_probe(struct nfs_request *req)
{
    struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
    bool             deferred;

    /* No file attrs available on the probe-deferred resume; sc is captured
     * lazily on the first peer CB_GETATTR (combine_valid stays false). */
    deferred = chimera_nfs4_open_grant_delegation(req, res, NULL);
    chimera_nfs_abort_if(deferred,
                         "OPEN resume after probe re-deferred");
    chimera_nfs4_open_complete(req, NFS4_OK);
} /* chimera_nfs4_open_resume_after_probe */

/*
 * OPEN as one run: PUTFH, OPEN_CURRENT (the current object, as a directory),
 * OPEN.
 *
 * The chain the per-op path walked by hand -- resolve the name, judge its type,
 * then open it, and on an exclusive collision open again to read the verifier
 * back -- is what the OPEN op's three options ARE, and every one of them is
 * chosen from the OPEN's own arguments, before the run starts:
 *
 *   REGULAR_ONLY          the name is resolved and a non-regular object is
 *                         answered for by TYPE rather than opened -- a native
 *                         open of a FIFO can block, and of a socket reports
 *                         ENXIO where the protocol owes its own error.  The
 *                         mode comes back in `existing_mode`.
 *   ATTRS_ON_CREATE_ONLY  UNCHECKED4: the create attributes describe a
 *                         creation, so an open that found the name leaves the
 *                         object alone.  `existed` says which happened, and is
 *                         what decides the deferred truncate below.
 *   EXCLUSIVE_RETRY       EXCLUSIVE4 / EXCLUSIVE4_1: a collision OPENS what is
 *                         already there, so the verifier stamped in its
 *                         timestamps can be compared, instead of failing.
 *
 * So the whole of the operation is three ops, and the request is no longer
 * between them.
 */
#define NFS4_OPEN_OP_OPEN 2

struct nfs4_open_run_ctx {
    struct nfs_request       *req;
    /* The decoded create attributes.  The run copies the struct and borrows
     * the ACL it points at; this keeps the ORIGINAL request, which is what
     * says whether an UNCHECKED4 create asked for size 0. */
    struct chimera_vfs_attrs *attr;
    uint8_t                   by_name;
};

/* An exclusive create is the one that stamps a verifier and therefore the one
 * that has to look at whatever it collides with. */
static inline int
nfs4_open_is_exclusive(const struct OPEN4args *args)
{
    return args->openhow.opentype == OPEN4_CREATE &&
           (args->openhow.how.mode == EXCLUSIVE4 ||
            args->openhow.how.mode == EXCLUSIVE4_1);
} /* nfs4_open_is_exclusive */

/*
 * Which of the requested attributes the create actually applied.  The op's
 * set_attr is the executor's copy, which it blanks when the name resolved to
 * something that already existed -- so an open that created nothing reports
 * nothing set.  The requested set lives in a different arm of openhow.how for
 * each create mode, and EXCLUSIVE4 has none at all: its verifier occupies that
 * slot, so reading it as an attribute request would be reading the verifier's
 * bytes as an attribute mask.
 */
static void
chimera_nfs4_open_fill_attrset(
    struct nfs_request             *req,
    const struct OPEN4args         *args,
    struct OPEN4res                *res,
    const struct chimera_vfs_attrs *applied)
{
    struct chimera_vfs_attrs copy = *applied;
    uint32_t                 n_mask;
    uint32_t                *mask;
    int                      rc;

    res->resok4.num_attrset = 0;

    if (args->openhow.opentype != OPEN4_CREATE ||
        args->openhow.how.mode == EXCLUSIVE4) {
        return;
    }

    if (args->openhow.how.mode == EXCLUSIVE4_1) {
        n_mask = args->openhow.how.ch_createboth.cva_attrs.num_attrmask;
        mask   = args->openhow.how.ch_createboth.cva_attrs.attrmask;
    } else {
        n_mask = args->openhow.how.createattrs.num_attrmask;
        mask   = args->openhow.how.createattrs.attrmask;
    }

    rc = xdr_dbuf_alloc_array(&res->resok4, attrset, 4, req->encoding->dbuf);
    chimera_nfs_abort_if(rc, "Failed to allocate array");

    res->resok4.num_attrset = chimera_nfs4_mask2attr(&copy, n_mask, mask,
                                                     res->resok4.attrset);
} /* chimera_nfs4_open_fill_attrset */

static void
chimera_nfs4_open_run_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_open_run_ctx             *ctx  = private_data;
    struct nfs_request                   *req  = ctx->req;
    struct OPEN4args                     *args = &req->args_compound->argarray[req->index].opopen;
    struct OPEN4res                      *res  = &req->res_compound.resarray[req->index].opopen;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_open_handle       *handle;
    struct chimera_vfs_attrs              attr, dir_pre, dir_post;
    enum chimera_vfs_error                error_code;
    uint32_t                              install_rflags = 0;
    uint8_t                               created, existed;
    nfsstat4                              status;

    error_code = chimera_vfs_compound_status(compound);
    op         = chimera_vfs_compound_op(compound, NFS4_OPEN_OP_OPEN);
    existed    = op->existed;

    if (error_code != CHIMERA_VFS_OK) {
        if (existed && nfs4_open_is_exclusive(args) &&
            (error_code == CHIMERA_VFS_ENXIO ||
             error_code == CHIMERA_VFS_EISDIR ||
             error_code == CHIMERA_VFS_ELOOP)) {
            /* The exclusive-create collision was re-opened to read its
             * verifier, and the re-open refused it by type.  No object that is
             * not a regular file can carry the verifier an EXCLUSIVE4 create
             * stamped, so what those errors stand for is simply "an object that
             * is not our earlier create exists": NFS4ERR_EXIST (RFC 7530
             * §16.16.4). */
            res->status = NFS4ERR_EXIST;
        } else if (existed && op->existing_mode &&
                   !S_ISREG(op->existing_mode)) {
            /* The type gate refused the open before it happened.  The VFS
             * reports the nearest POSIX answer; NFSv4 has its own, and a
             * different one per minor version, decided from the mode the
             * refusal carried. */
            res->status = chimera_nfs4_open_nonreg_status(req->minorversion,
                                                          op->existing_mode);
        } else {
            res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        }

        chimera_vfs_compound_free(compound);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.16.6 / RFC 8881 §18.16.4: OPEN targets a regular file.  The
     * type gate already refused one for every shape that asked for it; a create
     * that did not ask is judged here on what it opened.
     *
     * An exclusive-create COLLISION is the exception, and answers for the type
     * below instead: whatever is in the way, it cannot be carrying the verifier
     * this create stamped, so what the protocol wants is NFS4ERR_EXIST (RFC
     * 7530 §16.16.4) and not a type error -- which is the answer the verifier
     * comparison reaches for it. */
    if (!(existed && nfs4_open_is_exclusive(args)) &&
        (op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(op->attr.va_mode)) {
        res->status = chimera_nfs4_open_nonreg_status(req->minorversion,
                                                      op->attr.va_mode);
        chimera_vfs_compound_free(compound);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    if (existed && nfs4_open_is_exclusive(args)) {
        const uint8_t *verf;
        uint32_t       verf_atime, verf_mtime;

        verf = (args->openhow.how.mode == EXCLUSIVE4) ?
            args->openhow.how.createverf :
            args->openhow.how.ch_createboth.cva_verf;

        memcpy(&verf_atime, verf, sizeof(verf_atime));
        memcpy(&verf_mtime, verf + sizeof(verf_atime), sizeof(verf_mtime));

        if (!(op->attr.va_set_mask & CHIMERA_VFS_ATTR_ATIME) ||
            !(op->attr.va_set_mask & CHIMERA_VFS_ATTR_MTIME) ||
            op->attr.va_atime.tv_sec != verf_atime ||
            op->attr.va_mtime.tv_sec != verf_mtime) {
            /* Somebody else's file is in the way.  The handle was never taken,
             * so the free puts it back. */
            chimera_vfs_compound_free(compound);
            res->status = NFS4ERR_EXIST;
            chimera_nfs4_open_complete(req, NFS4ERR_EXIST);
            return;
        }
    }

    /* Everything the install needs is read out of the run before it is freed:
     * the handle it takes ownership of, the object's attributes, and the
     * directory's either side of the create. */
    attr     = op->attr;
    dir_pre  = op->dir_pre_attr;
    dir_post = op->dir_post_attr;
    created  = op->created;

    /* An UNCHECKED4 create of a name that was already there opens it without
     * restyling it, except that size 0 truncates.  The truncate is deliberately
     * NOT part of the open: that call opens and applies attributes in one step,
     * while the share reservation is not admitted until install_state runs -- so
     * an OPEN destined to fail NFS4ERR_SHARE_DENIED would empty the file on its
     * way to failing.  Note it here and let chimera_nfs4_open_complete apply it
     * once the reservation is actually held. */
    if (existed && args->openhow.opentype == OPEN4_CREATE &&
        args->openhow.how.mode == UNCHECKED4 &&
        (ctx->attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        ctx->attr->va_size == 0) {
        req->open_trunc_pending = true;
    }

    if (ctx->by_name) {
        chimera_nfs4_open_fill_attrset(req, args, res, &op->set_attr);
    } else {
        /* An open-by-handle created nothing and applied nothing. */
        res->resok4.num_attrset = 0;
    }

    handle = chimera_vfs_compound_take_handle(compound, NFS4_OPEN_OP_OPEN);

    chimera_vfs_compound_free(compound);

    if (!handle) {
        res->status = NFS4ERR_SERVERFAULT;
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* Capture the file handle before install_state, which may release the
     * handle when it coalesces onto an existing open state.  CLAIM_FH and
     * CLAIM_PREVIOUS name the object by the filehandle already on the wire, so
     * req->fh is left as it was. */
    if (ctx->by_name) {
        memcpy(req->fh, handle->fh, handle->fh_len);
        req->fhlen = handle->fh_len;
    }

    /* From here install_state owns the handle, including releasing it on every
     * one of its own failures.  An open-by-handle reports no attributes, and
     * install_state reads that as "access was established when this filehandle
     * was resolved" -- which is what it must not be told by an empty attribute
     * set that looks like a real one. */
    status = chimera_nfs4_open_install_state(req, handle,
                                             ctx->by_name ? &attr : NULL,
                                             ctx->by_name ? created : false,
                                             NULL, 0, NULL,
                                             &res->resok4.stateid,
                                             &install_rflags);

    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_open_complete(req, status);
        return;
    }

    res->status = NFS4_OK;
    /* The claim core arbitrates byte ranges with POSIX semantics for every
     * backend now -- a CHIMERA_VFS_CAP_CLAIM_RANGE backend only widens that
     * arbitration past this node -- so the advertisement no longer depends on
     * the backend having a lock passthrough. */
    res->resok4.rflags = install_rflags | OPEN4_RESULT_LOCKTYPE_POSIX;

    if (ctx->by_name) {
        /* An exclusive-create retry does not modify the directory, so cinfo
         * reports before == after == the directory's (unchanged) change
         * attribute rather than a bare zero, which a revalidating client would
         * otherwise mistake for the directory changing. */
        chimera_nfs4_set_changeinfo(&res->resok4.cinfo, &dir_pre, &dir_post);
    } else {
        /* An open-by-handle changed no directory. */
        res->resok4.cinfo.atomic = 0;
        res->resok4.cinfo.before = 0;
        res->resok4.cinfo.after  = 0;
    }

    if (chimera_nfs4_open_grant_delegation(req, res,
                                           ctx->by_name ? &attr : NULL)) {
        return; /* parked; resume from nfs4_cb_null_complete */
    }

    chimera_nfs4_open_complete(req, NFS4_OK);
} /* chimera_nfs4_open_run_complete */

/*
 * Validate the delegate_stateid an OPEN with CLAIM_DELEGATE_CUR cites (RFC
 * 7530 §16.16 / §10.4.3).  The claim asserts the caller already holds a
 * delegation on the file, so the stateid has to resolve to a live delegation
 * of this very client; a stateid that designates no such state is
 * NFS4ERR_BAD_STATEID (or STALE_STATEID/EXPIRED, as the table decides) rather
 * than something to quietly serve as an ordinary open.
 */
static nfsstat4
chimera_nfs4_open_check_delegate_cur(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct OPEN4args                 *args   = &req->args_compound->argarray[req->index].opopen;
    struct nfs_state_table           *table  = &thread->shared->nfs4_state_table;
    void                             *state_void;
    uint8_t                           state_type;
    nfsstat4                          status;

    status = nfs_state_table_acquire(table,
                                     &args->claim.delegate_cur_info.delegate_stateid,
                                     NFS4_SLOT_TYPE_DELEG,
                                     &state_void,
                                     &state_type);

    if (status != NFS4_OK) {
        return status;
    }

    status = nfs_state_check_client(
        state_void, state_type,
        req->session ? req->session->client_unified : NULL);

    nfs_state_table_release(table, state_void, state_type,
                            thread->vfs_thread);

    return status;
} /* chimera_nfs4_open_check_delegate_cur */

static void
chimera_nfs4_open_issue(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct OPEN4args            *args = &req->args_compound->argarray[req->index].opopen;
    struct OPEN4res             *res  = &req->res_compound.resarray[req->index].opopen;
    struct nfs4_open_run_ctx    *ctx;
    struct chimera_vfs_compound *compound;
    struct chimera_vfs_attrs    *attr;
    unsigned int                 flags   = 0;
    uint32_t                     opts    = 0;
    const char                  *name    = NULL;
    int                          namelen = 0;
    uint64_t                     attr_mask;
    nfsstat4                     status;
    uint32_t                     verf_part;

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    attr->va_req_mask = 0;
    attr->va_set_mask = 0;

    /* Everything below is a pure function of the OPEN's own arguments -- which
     * is why it happens HERE, before the run, rather than from inside it, and
     * where the VFS-compound path's scan makes the same decisions.  The create
     * mode selects the open flags and unmarshals the create attributes;
     * share_access selects the data-access intent; the claim selects the name
     * and the two options that depend on what that name resolves to. */
    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        switch (args->openhow.how.mode) {
            case GUARDED4:
                /* GUARDED4 = create only if file doesn't exist (like O_EXCL) */
                flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            /* fallthrough */
            case UNCHECKED4:
                /* An NFSv4 OPEN only ever creates regular files, and an
                 * UNCHECKED create of an existing name must resolve the
                 * object's type without opening it: a directory is EISDIR
                 * and any other non-regular object is EXIST (RFC 7530
                 * 16.16.4).  CREATE_REGULAR is the flag the backends'
                 * open paths key that resolution on -- without it a
                 * passthrough re-opened the existing object for data and a
                 * socket answered ENXIO where EXIST belongs. */
                flags |= CHIMERA_VFS_OPEN_CREATE_REGULAR;
                status = chimera_nfs4_validate_createattrs(
                    args->openhow.how.createattrs.num_attrmask,
                    args->openhow.how.createattrs.attrmask);
                if (status != NFS4_OK) {
                    res->status = status;
                    chimera_nfs4_open_complete(req, status);
                    return;
                }
                {
                    struct chimera_acl *acl_buf      = NULL;
                    unsigned            acl_buf_aces = 0;
                    if (args->openhow.how.createattrs.num_attrmask >= 1 &&
                        (args->openhow.how.createattrs.attrmask[0] & (1 << FATTR4_ACL))) {
                        acl_buf = xdr_dbuf_alloc_space(
                            chimera_acl_size(CHIMERA_ACL_MAX_ACES), req->encoding->dbuf);
                        acl_buf_aces = acl_buf ? CHIMERA_ACL_MAX_ACES : 0;
                    }
                    chimera_nfs4_unmarshall_attrs(attr,
                                                  args->openhow.how.createattrs.num_attrmask,
                                                  args->openhow.how.createattrs.attrmask,
                                                  args->openhow.how.createattrs.attr_vals.data,
                                                  args->openhow.how.createattrs.attr_vals.len,
                                                  acl_buf,
                                                  acl_buf_aces);
                }
                break;
            case EXCLUSIVE4_1:
                flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
                status = chimera_nfs4_validate_createattrs(
                    args->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                    args->openhow.how.ch_createboth.cva_attrs.attrmask);
                if (status == NFS4_OK) {
                    /* RFC 8881 18.16.3: an attribute in cva_attrs that is not
                     * in suppattr_exclcreat MUST be rejected with
                     * NFS4ERR_INVAL.  Without this the server unmarshalled
                     * time_access_set/time_modify_set and then silently
                     * clobbered them with the verifier below. */
                    status = chimera_nfs4_validate_exclcreat_attrs(
                        args->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                        args->openhow.how.ch_createboth.cva_attrs.attrmask);
                }
                if (status != NFS4_OK) {
                    res->status = status;
                    chimera_nfs4_open_complete(req, status);
                    return;
                }
                {
                    struct chimera_acl *acl_buf      = NULL;
                    unsigned            acl_buf_aces = 0;
                    if (args->openhow.how.ch_createboth.cva_attrs.num_attrmask >= 1 &&
                        (args->openhow.how.ch_createboth.cva_attrs.attrmask[0] &
                         (1 << FATTR4_ACL))) {
                        acl_buf = xdr_dbuf_alloc_space(
                            chimera_acl_size(CHIMERA_ACL_MAX_ACES), req->encoding->dbuf);
                        acl_buf_aces = acl_buf ? CHIMERA_ACL_MAX_ACES : 0;
                    }
                    chimera_nfs4_unmarshall_attrs(attr,
                                                  args->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                                                  args->openhow.how.ch_createboth.cva_attrs.attrmask,
                                                  args->openhow.how.ch_createboth.cva_attrs.attr_vals.data,
                                                  args->openhow.how.ch_createboth.cva_attrs.attr_vals.len,
                                                  acl_buf,
                                                  acl_buf_aces);
                }
                /* TODO: Store verifier in a server-private xattr (e.g. trusted.nfs4_excl_verf)
                 * once the VFS layer exposes setxattr/getxattr.  That would remove the
                 * restriction on clients setting time_access_set/time_modify_set in cva_attrs.
                 * For now encode the verifier in atime.tv_sec (bytes 0-3) and mtime.tv_sec
                 * (bytes 4-7), which is the same strategy used by Linux nfsd. */
                attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
                memcpy(&verf_part, args->openhow.how.ch_createboth.cva_verf, 4);
                attr->va_atime.tv_sec  = verf_part;
                attr->va_atime.tv_nsec = 0;
                memcpy(&verf_part, args->openhow.how.ch_createboth.cva_verf + 4, 4);
                attr->va_mtime.tv_sec  = verf_part;
                attr->va_mtime.tv_nsec = 0;
                /* When the client's cva_attrs left the mode unset, default to
                 * owner-only (0600) -- the same undefined-until-SETATTR safe
                 * default as EXCLUSIVE4 above. */
                if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
                    attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
                    attr->va_mode      = 0600;
                }
                break;
            case EXCLUSIVE4:
                flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
                /* EXCLUSIVE4 carries no attributes -- the createverf occupies
                 * the sattr slot -- so the file's mode is undefined until the
                 * client's follow-up SETATTR (RFC 7530 16.16.5).  Create it
                 * owner-only (0600), the safe default a file whose permissions
                 * are not yet set should have; this matches Linux nfsd and
                 * NFS-Ganesha (and the quint model). */
                attr->va_set_mask = CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME |
                    CHIMERA_VFS_ATTR_MODE;
                attr->va_mode = 0600;
                memcpy(&verf_part, args->openhow.how.createverf, 4);
                attr->va_atime.tv_sec  = verf_part;
                attr->va_atime.tv_nsec = 0;
                memcpy(&verf_part, args->openhow.how.createverf + 4, 4);
                attr->va_mtime.tv_sec  = verf_part;
                attr->va_mtime.tv_nsec = 0;
                break;
        } /* switch */

        if (args->openhow.how.mode == UNCHECKED4) {
            /* An UNCHECKED4 create of a name that is already there opens it
             * without restyling it, except that size 0 truncates -- and the
             * truncate is deliberately not part of the open, so an OPEN that
             * fails afterwards leaves the file's contents alone. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY |
                CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY;
        } else if (args->openhow.how.mode != GUARDED4) {
            /* EXCLUSIVE4 and EXCLUSIVE4_1 stamp the client's verifier into the
             * object's atime and mtime, which is how a repeat of the same
             * create recognises its own earlier one.  A collision therefore has
             * to be LOOKED AT rather than refused. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY;
        }
    }

    /* Carry the requested share access into the VFS open's data-access
     * intent.  The engine's open gate authorizes exactly these bits and
     * stamps the grant on the handle for every later stateful READ/WRITE
     * through this open -- and its created-file exemption grants only what
     * was requested, so an OPEN that asks for WRITE but maps no intent here
     * would stamp a mode-restricted create with no data access at all,
     * failing the creator's own I/O with EACCES. */
    if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        flags |= CHIMERA_VFS_OPEN_WRITE_ONLY;
    }

    switch (args->claim.claim) {
        case CLAIM_NULL:
            status = chimera_nfs4_validate_name(&args->claim.file);

            if (status != NFS4_OK) {
                res->status = status;
                chimera_nfs4_open_complete(req, status);
                return;
            }

            name    = (const char *) args->claim.file.data;
            namelen = (int) args->claim.file.len;
            break;
        case CLAIM_DELEGATE_CUR:
            /* RFC 7530 §16.16: open of a file the client already holds a
             * delegation on, identified by name within the current FH's
             * directory.  Treat like CLAIM_NULL using the name carried in
             * delegate_cur_info; the delegation the client cites must be its
             * own -- verified below -- so no recall is needed. */
            status = chimera_nfs4_validate_name(&args->claim.delegate_cur_info.file);
            if (status != NFS4_OK) {
                res->status = status;
                chimera_nfs4_open_complete(req, status);
                return;
            }

            status = chimera_nfs4_open_check_delegate_cur(req);
            if (status != NFS4_OK) {
                res->status = status;
                chimera_nfs4_open_complete(req, status);
                return;
            }

            name    = (const char *) args->claim.delegate_cur_info.file.data;
            namelen = (int) args->claim.delegate_cur_info.file.len;
            break;
        case CLAIM_PREVIOUS:
        case CLAIM_FH:
        case CLAIM_DELEG_CUR_FH:
            /* CLAIM_DELEG_CUR_FH (RFC 8881 §18.16): the client is converting an
             * open it held under a delegation into concrete open state on the
             * server, identifying the file by the current filehandle (the
             * minorversion-1 analogue of CLAIM_DELEGATE_CUR, which carries a
             * name).  This is exactly an open-by-FH; the delegation-grant gate
             * already declines to hand out a *new* delegation for this claim, so
             * the client receives an ordinary open stateid it can then return
             * the delegation against.  A client issues this in response to a
             * CB_RECALL, so failing it (NFS4ERR_NOTSUPP) stalls the recall and
             * prevents a clean DELEGRETURN.
             *
             * A nameless OPEN re-opens the current object itself, which is what
             * chimera_vfs_open_fh did here, and it takes no options: there is no
             * name to resolve first. */
            break;
        default:
            /* CLAIM_DELEGATE_PREV (delegation reclaim across a client reboot)
             * and any unknown claim are not supported -- reject rather than
             * abort the server. */
            res->status = NFS4ERR_NOTSUPP;
            chimera_nfs4_open_complete(req, NFS4ERR_NOTSUPP);
            return;
    } /* switch */

    if (!namelen) {
        /* An open-by-handle resolves no name, so neither option applies. */
        opts = 0;
    } else if (args->openhow.opentype != OPEN4_CREATE) {
        /* A plain open must classify a non-regular object before a backend
         * tries to open it. */
        opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY;
    }

    /* The same attributes the per-op path's open asked for -- no more, so an
     * object is not stat'd more thoroughly on one path than the other.  An
     * exclusive create adds the two the verifier lives in. */
    attr_mask = namelen ? (CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
                           CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME) : 0;

    if (opts & CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY) {
        attr_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
    }

    ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");

    ctx->req     = req;
    ctx->attr    = attr;
    ctx->by_name = namelen ? 1 : 0;

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_open(compound, name, namelen, flags, opts,
                                  attr, attr_mask,
                                  namelen ? (CHIMERA_VFS_ATTR_CHANGE |
                                             CHIMERA_VFS_ATTR_CTIME) : 0,
                                  namelen ? (CHIMERA_VFS_ATTR_CHANGE |
                                             CHIMERA_VFS_ATTR_CTIME) : 0);

    chimera_vfs_compound_submit(compound, chimera_nfs4_open_run_complete, ctx);
} /* chimera_nfs4_open_issue */

/*
 * OPEN of a named attribute (CLAIM_NULL with the current fh being a synthetic
 * named-attribute directory): create/open the named stream of that name on the
 * base file and install ordinary NFSv4 open state keyed on the stream fh.  No
 * delegation is offered for named attributes.
 *
 * One run -- PUTFH the base, PATH-open it, OPEN_STREAM -- and the handle the
 * stream open produced is TAKEN from it, because that handle is what the open
 * state holds from here on and must outlive the sequence.
 */
#define NFS4_ATTRDIR_OPEN_OP_STREAM 2

static void
chimera_nfs4_open_attrdir_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct OPEN4res                      *res =
        &req->res_compound.resarray[req->index].opopen;
    const struct chimera_vfs_compound_op *sop;
    struct chimera_vfs_open_handle       *oh;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                error_code;
    uint8_t                               base_fh[NFS4_FHSIZE];
    int                                   base_fh_len;
    const uint8_t                        *base;
    uint32_t                              install_rflags = 0;
    uint8_t                               created;
    nfsstat4                              status;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    sop     = chimera_vfs_compound_op(compound, NFS4_ATTRDIR_OPEN_OP_STREAM);
    attr    = sop->attr;
    created = sop->created;
    oh      = chimera_vfs_compound_take_handle(compound,
                                               NFS4_ATTRDIR_OPEN_OP_STREAM);

    chimera_vfs_compound_free(compound);

    if (!oh) {
        res->status = NFS4ERR_SERVERFAULT;
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* Capture the base fh (for the stream holder) before req->fh is overwritten
     * with the stream fh. */
    chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_fh_len);
    memcpy(base_fh, base, base_fh_len);

    memcpy(req->fh, oh->fh, oh->fh_len);
    req->fhlen = oh->fh_len;

    status = chimera_nfs4_open_install_state(req, oh, &attr, created,
                                             base_fh, base_fh_len, NULL,
                                             &res->resok4.stateid,
                                             &install_rflags);

    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_open_complete(req, status);
        return;
    }

    res->status              = NFS4_OK;
    res->resok4.cinfo.atomic = 0;
    res->resok4.cinfo.before = 0;
    res->resok4.cinfo.after  = 0;
    res->resok4.rflags       = install_rflags | OPEN4_RESULT_LOCKTYPE_POSIX;
    res->resok4.num_attrset  = 0;

    /* Named attributes are never delegated. */
    res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;
    chimera_nfs4_open_complete(req, NFS4_OK);
} /* chimera_nfs4_open_attrdir_complete */

static void
chimera_nfs4_open_attrdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct OPEN4args            *args =
        &req->args_compound->argarray[req->index].opopen;
    struct chimera_vfs_compound *compound;
    const uint8_t               *base;
    int                          base_len;
    unsigned int                 flags = 0;

    req->handle = NULL;

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        if (args->openhow.how.mode == GUARDED4 ||
            args->openhow.how.mode == EXCLUSIVE4 ||
            args->openhow.how.mode == EXCLUSIVE4_1) {
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
        }
    }

    chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, base, base_len);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    /* set_attr is NULL: a named-stream open must not stamp the base file's
    * mode/owner (memfs applies open_stream set_attr to the base inode). */
    chimera_vfs_compound_add_open_stream(compound,
                                         (const char *) args->claim.file.data,
                                         (int) args->claim.file.len,
                                         flags, NULL,
                                         CHIMERA_VFS_ATTR_MASK_STAT);

    chimera_vfs_compound_submit(compound, chimera_nfs4_open_attrdir_complete,
                                req);
} /* chimera_nfs4_open_attrdir */

/*
 * RFC 7530 §9.1.7 entry-time seqid classification for the 4.0 path.
 *
 * Runs BEFORE any VFS work, so a replay is answered from the owner's cached
 * reply without re-executing the open.  That ordering is why it is a separate
 * function: the VFS-compound path has to reach the same decision before it
 * submits a sequence, and both paths then advance the seqid through the same
 * chimera_nfs4_open_finish on the way out.
 *
 * Returns true when the OPEN is answered outright -- replay, bad seqid, stale
 * clientid, all of them in the no-advance set -- with *status carrying the
 * answer for the caller to complete the COMPOUND with.  Returns false when the
 * OPEN should proceed, having pinned the resolved owner on req->open_4_0_owner
 * (chimera_nfs4_open_finish drops it).  A no-op returning false on 4.1+.
 */
SYMBOL_EXPORT bool
chimera_nfs4_open_4_0_entry(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    uint32_t                          res_index,
    nfsstat4                         *status)
{
    struct OPEN4args *args = &req->args_compound->argarray[res_index].opopen;
    struct OPEN4res  *res  = &req->res_compound.resarray[res_index].opopen;

    if (req->minorversion != 0) {
        return false;
    }

    {
        struct nfs_client *client = NULL;

        /* Resolve the client strictly by the OPEN owner's clientid.  The
         * connection may carry a stale or unrelated implicit session (e.g. a
         * prior confirmed client), so an unconfirmed/unknown clientid must not
         * be silently accepted just because the conn is bound. */
        if (req->session && req->session->client_unified &&
            req->session->client_unified->client_id == args->owner.clientid) {
            client = req->session->client_unified;
        } else {
            struct nfs4_session *found = nfs4_session_find_by_clientid(
                &thread->shared->nfs4_shared_clients,
                args->owner.clientid);

            if (found) {
                client = found->client_unified;
                nfs4_session_bind_conn(req->conn, found);
                req->session = found;
                /* Drop the +1 ref from find_by_clientid; the conn owns it. */
                nfs4_session_put(found);
            }
        }

        if (!client) {
            /* NFS4ERR_STALE_CLIENTID is in the no-advance set; we don't
             * touch any owner state. */
            res->status = NFS4ERR_STALE_CLIENTID;
            *status     = NFS4ERR_STALE_CLIENTID;
            return true;
        }
        if (client->expired) {
            client->expired = 0;
            nfs_client_touch(client);
        }

        bool                   created;
        struct nfs_open_owner *owner = nfs_open_owner_find_or_create(
            client, args->owner.owner.data, args->owner.owner.len,
            &created);

        evpl_mutex_lock(&owner->lock);
        int                    cls = nfs4_owner_seqid_classify(owner->seqid, &owner->replay,
                                                               args->seqid);

        if (cls == NFS4_SEQID_REPLAY) {
            /* Return the cached reply.  Simplified replay (status, stateid
             * and rflags); cinfo/attrset/delegation are reconstructed as
             * zero/none.  Linux clients tolerate that since they re-fetch
             * attrs via GETATTR after OPEN.
             *
             * rflags is replayed rather than zeroed: RFC 7530 §9.1.7 requires
             * the stored last response, and dropping OPEN4_RESULT_CONFIRM
             * (§16.18.5) tells a retransmitting client the opposite of what
             * the original reply said about its OPEN_CONFIRM obligation.
             *
             * A retransmit on the SAME connection is normally answered
             * byte-exact by the v4.0 reply cache before the compound is
             * even decoded (nfs4_v40_drc.c), so this branch is reached
             * only when the retransmit arrives on a new connection. */
            res->status                            = owner->replay.status;
            res->resok4.stateid                    = owner->replay.stateid;
            res->resok4.cinfo.atomic               = 0;
            res->resok4.cinfo.before               = 0;
            res->resok4.cinfo.after                = 0;
            res->resok4.rflags                     = owner->replay.rflags;
            res->resok4.num_attrset                = 0;
            res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;
            evpl_mutex_unlock(&owner->lock);
            /* Early return before the borrow ref transfers to the request;
             * release it here. */
            nfs_open_owner_put(owner);
            *status = res->status;
            return true;
        }

        if (cls != NFS4_SEQID_NEW) {
            /* NFS4ERR_BAD_SEQID is in the no-advance set; do not touch
             * owner state. */
            evpl_mutex_unlock(&owner->lock);
            nfs_open_owner_put(owner);
            res->status = NFS4ERR_BAD_SEQID;
            *status     = NFS4ERR_BAD_SEQID;
            return true;
        }

        evpl_mutex_unlock(&owner->lock);
        /* Transfer the find_or_create ref onto the request; dropped in
         * chimera_nfs4_open_complete. */
        req->open_4_0_owner = owner;
    }
    return false;
} /* chimera_nfs4_open_4_0_entry */

void
chimera_nfs4_open(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct OPEN4args *args = &argop->opopen;
    struct OPEN4res  *res  = &resop->opopen;

    req->open_trunc_pending = false;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* RFC 7530 §9.1.7 entry-time seqid classification for the 4.0 path; see
     * chimera_nfs4_open_4_0_entry.  A replay or a rejected seqid is answered
     * here, before any VFS work. */
    {
        nfsstat4 entry_status;

        if (chimera_nfs4_open_4_0_entry(thread, req, (uint32_t) req->index,
                                        &entry_status)) {
            chimera_nfs4_compound_complete(req, entry_status);
            return;
        }
    }

    /* Gate OPEN during recovery.  Two distinct rules apply:
     *
     *   1. Server-reboot grace window (nfs_recovery_open_check): non-reclaim
     *      OPENs are refused while in_grace, and CLAIM_PREVIOUS reclaims are
     *      refused outside it.  Recovery records are persisted to the KV store
     *      (nfs4_recovery.c), so in_grace reflects a real post-restart grace
     *      period.
     *
     *   2. Per-client reclaim completion (RFC 8881 §18.51.3): once a 4.1+
     *      client establishes a new client ID it MUST send RECLAIM_COMPLETE
     *      before performing any non-reclaim locking operation.  Until it does,
     *      a non-reclaim OPEN is refused with NFS4ERR_GRACE.  Symmetrically,
     *      once it has sent RECLAIM_COMPLETE the client may no longer reclaim:
     *      a further CLAIM_PREVIOUS is NFS4ERR_NO_GRACE even while the
     *      server-wide window is still open for other clients.  This is a
     *      per-client obligation independent of the server-wide grace window,
     *      so it is enforced whether or not in_grace is set. */
    {
        bool     is_reclaim = (args->claim.claim == CLAIM_PREVIOUS);
        nfsstat4 g_status   = nfs_recovery_open_check(
            &thread->shared->nfs4_recovery,
            req->session ? req->session->client_unified : NULL,
            is_reclaim);

        if (g_status != NFS4_OK) {
            res->status = g_status;
            chimera_nfs4_open_complete(req, g_status);
            return;
        }

        if (req->minorversion > 0 && req->session) {
            bool done = nfs4_client_reclaim_complete(
                &thread->shared->nfs4_shared_clients,
                req->session->nfs4_session_clientid);

            if (is_reclaim && done) {
                res->status = NFS4ERR_NO_GRACE;
                chimera_nfs4_open_complete(req, res->status);
                return;
            }

            if (!is_reclaim && !done) {
                res->status = NFS4ERR_GRACE;
                chimera_nfs4_open_complete(req, res->status);
                return;
            }
        }
    }

    /* OPEN of a named attribute: the current fh is a synthetic named-attribute
     * directory and the claim names the attribute.  Only CLAIM_NULL is defined
     * inside a named-attr directory. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        if (args->claim.claim != CLAIM_NULL) {
            res->status = NFS4ERR_NOTSUPP;
            chimera_nfs4_open_complete(req, res->status);
            return;
        }

        chimera_nfs4_open_attrdir(thread, req);
        return;
    }

    chimera_nfs4_open_issue(thread, req);
} /* chimera_nfs4_open */
