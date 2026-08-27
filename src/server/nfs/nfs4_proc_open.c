// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "nfs4_callback.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"
#include "vfs/sdk/vfs_access.h"
#include "vfs/sdk/vfs_acl.h"

/*
 * Acquire a cross-protocol SHARE reservation in the claim core for a freshly
 * created open_state.  Upstream's nfs_client_check_share_conflict already
 * enforces share-mode conflicts *among NFSv4 owners of the same client*;
 * this adds the NLM/SMB dimension so an NFSv4 OPEN that denies read/write
 * collides with an SMB open holding that access (and vice versa).
 *
 * Returns NFS4_OK on success (claim stored on the state, released at
 * open_state_cleanup), NFS4ERR_SHARE_DENIED on cross-protocol conflict.
 */
static nfsstat4
chimera_nfs4_open_acquire_share(
    struct nfs_request    *req,
    struct nfs_open_state *state)
{
    struct OPEN4args               *args      = &req->args_compound->argarray[req->index].opopen;
    struct chimera_vfs_state       *vfs_state = req->thread->vfs->vfs_state;
    struct chimera_vfs_open_handle *handle    = state->handle;
    struct chimera_vfs_file_state  *file_state;
    struct chimera_claim_owner      owner;
    enum chimera_vfs_claim_result   result;
    uint8_t                         granted = 0;
    uint8_t                         denied  = 0;

    /* share_access MUST request at least one of READ or WRITE (RFC 7530
     * §16.16.5 / §9.9); a value with neither base mode is invalid. */
    if ((args->share_access &
         (OPEN4_SHARE_ACCESS_READ | OPEN4_SHARE_ACCESS_WRITE)) == 0) {
        return NFS4ERR_INVAL;
    }

    if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        granted |= CHIMERA_CLAIM_R;
    }
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        granted |= CHIMERA_CLAIM_W;
    }
    if (args->share_deny & OPEN4_SHARE_DENY_READ) {
        denied |= CHIMERA_CLAIM_R;
    }
    if (args->share_deny & OPEN4_SHARE_DENY_WRITE) {
        denied |= CHIMERA_CLAIM_W;
    }

    if (granted == 0 && denied == 0) {
        return NFS4_OK;
    }

    file_state = chimera_vfs_state_get(vfs_state,
                                       handle->fh, handle->fh_len,
                                       handle->fh_hash, true);
    if (!file_state) {
        return NFS4ERR_SERVERFAULT;
    }

    memset(&owner, 0, sizeof(owner));
    owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    owner.client_key = state->owner->client->client_id;
    owner.owner_lo   = XXH3_64bits(state->owner->owner,
                                   state->owner->owner_len);
    owner.owner_hi = 0;

    chimera_vfs_claim_init_nfs4_open(&state->share_claim, granted, denied,
                                     &owner);
    /* Courteous server: report this share reservation dead once the owning
     * client's lease lapses, so a conflicting open reclaims it; reclaim flags
     * the client for sweep teardown. */
    state->share_claim.is_alive_cb = nfs_client_lease_alive;
    state->share_claim.revoked_cb  = nfs_client_lease_revoked_cb;
    state->share_claim.cb_private  = state->owner->client;

    result = chimera_vfs_claim_try_acquire(vfs_state, file_state,
                                           &state->share_claim, NULL);
    if (result == CHIMERA_CLAIM_BREAKING) {
        /* The conflict is a breakable holder -- an NFSv4 delegation being
         * recalled (try_acquire already kicked the break).  Tell the client to
         * retry; by the next attempt the delegation's DELEGRETURN should have
         * released the claim and the SHARE will be granted (RFC 7530 §10.2
         * recommends NFS4ERR_DELAY while a recall is outstanding). */
        chimera_vfs_state_put(vfs_state, file_state);
        return NFS4ERR_DELAY;
    }
    if (result != CHIMERA_CLAIM_GRANTED) {
        chimera_vfs_state_put(vfs_state, file_state);
        return NFS4ERR_SHARE_DENIED;
    }

    state->share_file_state = file_state;
    state->share_claim_held = true;
    return NFS4_OK;
} /* chimera_nfs4_open_acquire_share */

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
 * Otherwise the response carries OPEN_DELEGATE_NONE.  Must be called on the
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
static bool
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
    struct nfsace4                   *perms;
    static const char                 everyone[] = "EVERYONE@";

    res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;

    if (!chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
        return false;
    }
    if (!client) {
        return false;
    }
    if (args->claim.claim != CLAIM_NULL && args->claim.claim != CLAIM_FH) {
        return false;
    }

    /* RFC 8881 §18.16: honor an explicit "no delegation wanted" request.  On
    * 4.1+ this is reported as OPEN_DELEGATE_NONE_EXT with WND4_NOT_WANTED;
    * 4.0 has no such extension, so the default OPEN_DELEGATE_NONE stands. */
    if (args->share_access & OPEN4_SHARE_ACCESS_WANT_NO_DELEG) {
        if (req->minorversion >= 1) {
            res->resok4.delegation.delegation_type                       = OPEN_DELEGATE_NONE_EXT;
            res->resok4.delegation.od_whynone.ond_why                    = WND4_NOT_WANTED;
            res->resok4.delegation.od_whynone.ond_server_will_push_deleg = 0;
        }
        return false;
    }

    /* A write open earns a write delegation; otherwise a pure-read open earns
     * a read delegation. */
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        deleg_type = OPEN_DELEGATE_WRITE;
    } else if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        deleg_type = OPEN_DELEGATE_READ;
    } else {
        return false;
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
            return false;
        case NFS4_CB_GRANT_DEFER:
            nfs4_cb_probe_park(client, req);
            return true;
    } /* switch */

    /* fh_hash must match the open-cache / SHARE-lease hashing so the
     * delegation and conflicting opens land on the same file_state. */
    fh_hash = XXH3_64bits(req->fh, req->fhlen) & INT64_MAX;

    pthread_mutex_lock(&client->lock);
    LL_FOREACH2(client->delegations, d, next_in_client)
    {
        if (d->fh_len == req->fhlen &&
            memcmp(d->fh, req->fh, req->fhlen) == 0) {
            exists = true;
            break;
        }
    }
    pthread_mutex_unlock(&client->lock);
    if (exists) {
        return false;
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
        return false;
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
        return false;
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
        pthread_mutex_lock(&deleg->combine_lock);
        deleg->combine_sc    = sc;
        deleg->combine_last  = sc;
        deleg->combine_valid = true;
        pthread_mutex_unlock(&deleg->combine_lock);
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

    perms->type        = ACE4_ACCESS_ALLOWED_ACE_TYPE;
    perms->flag        = 0;
    perms->access_mask = ACE4_READ_DATA;
    perms->who.len     = sizeof(everyone) - 1;
    perms->who.data    = (void *) everyone;
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
 */
static nfsstat4
chimera_nfs4_open_install_state(
    struct nfs_request             *req,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_attrs *attr,
    bool                            file_created,
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
        chimera_vfs_release(req->thread->vfs_thread, handle);
        /* The SHARE reservation acquired on the first OPEN of this
         * (owner, fh) stays in force.  Broadening share bits on
         * coalesce is not re-checked cross-protocol in this pass —
         * upstream's intra-client check is likewise coalesce-exempt. */
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

        /* Cross-protocol SHARE coordination.  On conflict, tear the
         * just-created state back down (releasing the handle) and fail. */
        status = chimera_nfs4_open_acquire_share(req, new_state);
        if (status != NFS4_OK) {
            nfs_open_state_destroy(new_state,
                                   &req->thread->shared->nfs4_state_table,
                                   req->thread->vfs_thread);
            goto err_put_owner;
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
    nfs_open_owner_put(owner);
    return status;
} /* chimera_nfs4_open_install_state */

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
static nfsstat4
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

static void
chimera_nfs4_open_finish(
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

        pthread_mutex_lock(&owner->lock);
        owner->seqid = args->seqid;
        nfs4_replay_record(&owner->replay, args->seqid, OP_OPEN, status,
                           status == NFS4_OK ? &res->resok4.stateid : NULL);
        pthread_mutex_unlock(&owner->lock);
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
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request     *req   = private_data;
    struct nfs_state_table *table = &req->thread->shared->nfs4_state_table;
    struct OPEN4res        *res   =
        &req->res_compound.resarray[req->index].opopen;

    (void) pre_attr;
    (void) set_attr;
    (void) post_attr;

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
static void
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

            chimera_vfs_fsetattr(req->thread->vfs_thread, &req->cred,
                                 open_state->handle,
                                 &req->open_trunc_attr,
                                 0,
                                 0,
                                 chimera_nfs4_open_trunc_complete,
                                 req);
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

static void
chimera_nfs4_open_exclusive_verify(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct nfs_request             *req           = private_data;
    struct OPEN4args               *args          = &req->args_compound->argarray[req->index].opopen;
    struct OPEN4res                *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle *parent_handle = req->handle;
    const uint8_t                  *verf;
    uint32_t                        verf_atime, verf_mtime;
    uint32_t                        lock_caps;
    nfsstat4                        status;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    if (args->openhow.how.mode == EXCLUSIVE4) {
        verf = args->openhow.how.createverf;
    } else {
        verf = args->openhow.how.ch_createboth.cva_verf;
    }

    memcpy(&verf_atime, verf, sizeof(verf_atime));
    memcpy(&verf_mtime, verf + sizeof(verf_atime), sizeof(verf_mtime));

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) ||
        !(attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) ||
        attr->va_atime.tv_sec != verf_atime ||
        attr->va_mtime.tv_sec != verf_mtime) {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        res->status = NFS4ERR_EXIST;
        chimera_nfs4_open_complete(req, NFS4ERR_EXIST);
        return;
    }

    /* Verifier matches - this is a retry, treat as success.  Capture FH and
     * lock capabilities before install_state may release the handle. */
    {
        uint32_t install_rflags = 0;
        lock_caps = handle->vfs_module->capabilities;
        memcpy(req->fh, handle->fh, handle->fh_len);
        req->fhlen = handle->fh_len;

        status = chimera_nfs4_open_install_state(req, handle, attr,
                                                 handle->r_created,
                                                 &res->resok4.stateid,
                                                 &install_rflags);
        if (status != NFS4_OK) {
            res->status = status;
            chimera_vfs_release(req->thread->vfs_thread, parent_handle);
            chimera_nfs4_open_complete(req, status);
            return;
        }

        res->status = NFS4_OK;
        /* An exclusive-create retry does not modify the directory, so cinfo
         * reports before == after == the directory's (unchanged) change
         * attribute rather than a bare zero, which a revalidating client would
         * otherwise mistake for the directory changing. */
        chimera_nfs4_set_changeinfo(&res->resok4.cinfo, dir_pre_attr, dir_post_attr);
        res->resok4.rflags = install_rflags |
            ((lock_caps & CHIMERA_VFS_CAP_FS_LOCK) ?
             OPEN4_RESULT_LOCKTYPE_POSIX : 0);
    }
    res->resok4.num_attrset = 0;

    /* Release the parent handle before grant_delegation so the cb-probe
    * defer path (which returns without completing the OPEN) does not leak
    * it -- the resume callback only knows how to call open_complete. */
    chimera_vfs_release(req->thread->vfs_thread, parent_handle);
    if (chimera_nfs4_open_grant_delegation(req, res, attr)) {
        return; /* parked; resume from nfs4_cb_null_complete */
    }
    chimera_nfs4_open_complete(req, NFS4_OK);
} /* chimera_nfs4_open_exclusive_verify */

static void
chimera_nfs4_open_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct nfs_request             *req           = private_data;
    struct OPEN4args               *args          = &req->args_compound->argarray[req->index].opopen;
    struct OPEN4res                *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle *parent_handle = req->handle;
    int                             rc;

    if (error_code != CHIMERA_VFS_OK) {
        if (error_code == CHIMERA_VFS_EEXIST &&
            args->openhow.opentype == OPEN4_CREATE &&
            (args->openhow.how.mode == EXCLUSIVE4 ||
             args->openhow.how.mode == EXCLUSIVE4_1)) {
            set_attr->va_set_mask = 0;
            set_attr->va_req_mask = 0;
            chimera_vfs_open_at(req->thread->vfs_thread, &req->cred,
                                parent_handle,
                                args->claim.file.data,
                                args->claim.file.len,
                                CHIMERA_VFS_OPEN_INFERRED,
                                set_attr,
                                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME,
                                CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                                CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                                chimera_nfs4_open_exclusive_verify,
                                req);
            return;
        }
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.16.6 / RFC 8881 §18.16.4: OPEN targets a regular file.
     * A directory yields NFS4ERR_ISDIR; other non-regular objects yield
     * NFS4ERR_SYMLINK (4.0) or NFS4ERR_SYMLINK/WRONG_TYPE (4.1+). */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISREG(attr->va_mode)) {
        res->status = chimera_nfs4_open_nonreg_status(req->minorversion,
                                                      attr->va_mode);
        chimera_vfs_release(req->thread->vfs_thread, handle);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    {
        uint32_t lock_caps      = handle->vfs_module->capabilities;
        uint32_t install_rflags = 0;
        nfsstat4 status;

        /* Capture FH before install_state may release the handle (coalesce). */
        memcpy(req->fh, handle->fh, handle->fh_len);
        req->fhlen = handle->fh_len;

        status = chimera_nfs4_open_install_state(req, handle, attr,
                                                 handle->r_created,
                                                 &res->resok4.stateid,
                                                 &install_rflags);
        if (status != NFS4_OK) {
            res->status = status;
            chimera_vfs_release(req->thread->vfs_thread, parent_handle);
            chimera_nfs4_open_complete(req, status);
            return;
        }

        res->status              = NFS4_OK;
        res->resok4.cinfo.atomic = 0;
        res->resok4.cinfo.before = 0;
        res->resok4.cinfo.after  = 0;
        res->resok4.rflags       = install_rflags |
            ((lock_caps & CHIMERA_VFS_CAP_FS_LOCK) ?
             OPEN4_RESULT_LOCKTYPE_POSIX : 0);
    }
    res->resok4.num_attrset = 0;

    if (args->openhow.opentype == OPEN4_CREATE &&
        (args->openhow.how.mode == UNCHECKED4 || args->openhow.how.mode == GUARDED4)) {
        rc = xdr_dbuf_alloc_array(&res->resok4, attrset, 4, req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to allocate array");
        res->resok4.num_attrset = chimera_nfs4_mask2attr(set_attr,
                                                         args->openhow.how.createattrs.num_attrmask,
                                                         args->openhow.how.createattrs.attrmask,
                                                         res->resok4.attrset);
    } else if (args->openhow.opentype == OPEN4_CREATE &&
               args->openhow.how.mode == EXCLUSIVE4_1) {
        rc = xdr_dbuf_alloc_array(&res->resok4, attrset, 4, req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to allocate array");
        res->resok4.num_attrset = chimera_nfs4_mask2attr(set_attr,
                                                         args->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                                                         args->openhow.how.ch_createboth.cva_attrs.attrmask,
                                                         res->resok4.attrset);
    } else {
        res->resok4.num_attrset = 0;
    }

    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, dir_pre_attr, dir_post_attr);

    /* Release the parent handle before grant_delegation so the cb-probe
    * defer path (which returns without completing the OPEN) does not leak
    * it -- the resume callback only knows how to call open_complete. */
    chimera_vfs_release(req->thread->vfs_thread, parent_handle);
    if (chimera_nfs4_open_grant_delegation(req, res, attr)) {
        return; /* parked; resume from nfs4_cb_null_complete */
    }

    chimera_nfs4_open_complete(req, NFS4_OK);
} /* chimera_nfs4_open_at_complete */

struct nfs4_open_lookup_regular_ctx {
    struct nfs_request       *req;
    struct chimera_vfs_attrs *attr;
    const char               *name;
    uint32_t                  namelen;
    unsigned int              flags;
};

struct nfs4_open_unchecked_ctx {
    struct nfs_request       *req;
    struct chimera_vfs_attrs *attr;
    const char               *name;
    uint32_t                  namelen;
    unsigned int              flags;
};

static void
chimera_nfs4_open_lookup_regular_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs4_open_lookup_regular_ctx *ctx           = private_data;
    struct nfs_request                  *req           = ctx->req;
    struct OPEN4res                     *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle      *parent_handle = req->handle;

    (void) dir_attr;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* OPEN4_NOCREATE must classify special objects by type before the
     * backend tries to open them.  Native opens of FIFOs, sockets, and
     * devices can otherwise block or report backend-specific errors.
     * Typing follows RFC 7530 §16.16.6 / RFC 8881 §18.16.4. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISREG(attr->va_mode)) {
        res->status = chimera_nfs4_open_nonreg_status(req->minorversion,
                                                      attr->va_mode);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    chimera_vfs_open_at(req->thread->vfs_thread, &req->cred,
                        parent_handle,
                        ctx->name,
                        ctx->namelen,
                        ctx->flags,
                        ctx->attr,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
                        CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                        (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                        (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                        chimera_nfs4_open_at_complete,
                        req);
} /* chimera_nfs4_open_lookup_regular_complete */

static void
chimera_nfs4_open_unchecked_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *existing_attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs4_open_unchecked_ctx *ctx           = private_data;
    struct nfs_request             *req           = ctx->req;
    struct OPEN4res                *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle *parent_handle = req->handle;

    (void) dir_attr;

    if (error_code == CHIMERA_VFS_OK) {
        /* The object already exists.  Classify special objects by type before
         * the backend tries to open them -- a native open of a FIFO, socket,
         * or device can block or report a backend-specific errno (ENXIO on a
         * FIFO with no peer), where RFC 7530 §16.16.6 / RFC 8881 §18.16.4
         * require the protocol-level type error.  Mirrors the NOCREATE path. */
        if ((existing_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
            !S_ISREG(existing_attr->va_mode)) {
            res->status = chimera_nfs4_open_nonreg_status(req->minorversion,
                                                          existing_attr->va_mode);
            chimera_vfs_release(req->thread->vfs_thread, parent_handle);
            chimera_nfs4_open_complete(req, res->status);
            return;
        }

        /* RFC 7530 OPEN/UNCHECKED recreate: create attrs are ignored for an
         * existing object, except size=0 truncates the file.
         *
         * The truncate is deliberately NOT handed to the open below.  That
         * call opens and applies attributes in one step, while the share
         * reservation is not admitted until chimera_nfs4_open_acquire_share()
         * runs on the completion -- so an OPEN destined to fail
         * NFS4ERR_SHARE_DENIED emptied the file on its way to failing.  A
         * failed OPEN must leave the object alone; note it here and let
         * chimera_nfs4_open_complete() apply it once the reservation is
         * actually held. */
        if ((ctx->attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
            ctx->attr->va_size == 0) {
            req->open_trunc_pending = true;
        }
        ctx->attr->va_set_mask = 0;
        ctx->attr->va_req_mask = 0;
    } else if (error_code != CHIMERA_VFS_ENOENT) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    chimera_vfs_open_at(req->thread->vfs_thread, &req->cred,
                        parent_handle,
                        ctx->name,
                        ctx->namelen,
                        ctx->flags,
                        ctx->attr,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
                        CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                        (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                        (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                        chimera_nfs4_open_at_complete,
                        req);
} /* chimera_nfs4_open_unchecked_lookup_complete */

static void
chimera_nfs4_open_claim_fh_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request             *req           = private_data;
    struct OPEN4res                *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle *parent_handle = req->handle;
    uint32_t                        lock_caps;
    nfsstat4                        status;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* CLAIM_FH/CLAIM_PREVIOUS: req->fh already carries the FH that was put
     * on the wire; no need to overwrite from handle->fh.  Capture lock caps
     * before install_state may release the handle. */
    {
        uint32_t install_rflags = 0;
        lock_caps = handle->vfs_module->capabilities;

        status = chimera_nfs4_open_install_state(req, handle, NULL, false,
                                                 &res->resok4.stateid,
                                                 &install_rflags);
        if (status != NFS4_OK) {
            res->status = status;
            chimera_vfs_release(req->thread->vfs_thread, parent_handle);
            chimera_nfs4_open_complete(req, status);
            return;
        }

        res->status              = NFS4_OK;
        res->resok4.cinfo.atomic = 0;
        res->resok4.cinfo.before = 0;
        res->resok4.cinfo.after  = 0;
        res->resok4.rflags       = install_rflags |
            ((lock_caps & CHIMERA_VFS_CAP_FS_LOCK) ?
             OPEN4_RESULT_LOCKTYPE_POSIX : 0);
    }
    res->resok4.num_attrset = 0;

    /* Release the parent handle before grant_delegation so the cb-probe
    * defer path (which returns without completing the OPEN) does not leak
    * it -- the resume callback only knows how to call open_complete. */
    chimera_vfs_release(req->thread->vfs_thread, parent_handle);
    /* CLAIM_FH/CLAIM_PREVIOUS does not getattr the file; sc is captured lazily
     * on the first peer CB_GETATTR (combine_valid stays false). */
    if (chimera_nfs4_open_grant_delegation(req, res, NULL)) {
        return; /* parked; resume from nfs4_cb_null_complete */
    }
    chimera_nfs4_open_complete(req, NFS4_OK);
} /* chimera_nfs4_open_claim_fh_complete */

static void
chimera_nfs4_open_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs_request       *req   = private_data;
    struct OPEN4args         *args  = &req->args_compound->argarray[req->index].opopen;
    unsigned int              flags = 0;
    nfsstat4                  status;
    struct chimera_vfs_attrs *attr;
    uint32_t                  verf_part;

    req->handle = parent_handle;

    if (error_code != CHIMERA_VFS_OK) {
        struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    attr->va_req_mask = 0;
    attr->va_set_mask = 0;

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        switch (args->openhow.how.mode) {
            case GUARDED4:
                /* GUARDED4 = create only if file doesn't exist (like O_EXCL) */
                flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            /* fallthrough */
            case UNCHECKED4:
                status = chimera_nfs4_validate_createattrs(
                    args->openhow.how.createattrs.num_attrmask,
                    args->openhow.how.createattrs.attrmask);
                if (status != NFS4_OK) {
                    struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
                    chimera_vfs_release(req->thread->vfs_thread, parent_handle);
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
                if (status != NFS4_OK) {
                    struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
                    chimera_vfs_release(req->thread->vfs_thread, parent_handle);
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
                break;
            case EXCLUSIVE4:
                flags            |= CHIMERA_VFS_OPEN_EXCLUSIVE;
                attr->va_set_mask = CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
                memcpy(&verf_part, args->openhow.how.createverf, 4);
                attr->va_atime.tv_sec  = verf_part;
                attr->va_atime.tv_nsec = 0;
                memcpy(&verf_part, args->openhow.how.createverf + 4, 4);
                attr->va_mtime.tv_sec  = verf_part;
                attr->va_mtime.tv_nsec = 0;
                break;
        } /* switch */
    }

    if (args->share_access == OPEN4_SHARE_ACCESS_READ) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }

    switch (args->claim.claim) {
        case CLAIM_NULL:
            status = chimera_nfs4_validate_name(&args->claim.file);

            if (status != NFS4_OK) {
                struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
                chimera_vfs_release(req->thread->vfs_thread, parent_handle);
                res->status = status;
                chimera_nfs4_open_complete(req, status);
                return;
            }

            if (args->openhow.opentype == OPEN4_NOCREATE) {
                struct nfs4_open_lookup_regular_ctx *ctx;

                ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
                chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
                ctx->req     = req;
                ctx->attr    = attr;
                ctx->name    = args->claim.file.data;
                ctx->namelen = args->claim.file.len;
                ctx->flags   = flags;

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred,
                                      parent_handle,
                                      args->claim.file.data,
                                      args->claim.file.len,
                                      CHIMERA_VFS_ATTR_MODE,
                                      0,
                                      chimera_nfs4_open_lookup_regular_complete,
                                      ctx);
                return;
            }

            if (args->openhow.opentype == OPEN4_CREATE &&
                args->openhow.how.mode == UNCHECKED4) {
                struct nfs4_open_unchecked_ctx *ctx;

                ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
                chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
                ctx->req     = req;
                ctx->attr    = attr;
                ctx->name    = args->claim.file.data;
                ctx->namelen = args->claim.file.len;
                ctx->flags   = flags;

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred,
                                      parent_handle,
                                      args->claim.file.data,
                                      args->claim.file.len,
                                      CHIMERA_VFS_ATTR_MODE,
                                      0,
                                      chimera_nfs4_open_unchecked_lookup_complete,
                                      ctx);
                return;
            }

            chimera_vfs_open_at(req->thread->vfs_thread, &req->cred,
                                parent_handle,
                                args->claim.file.data,
                                args->claim.file.len,
                                flags,
                                attr,
                                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
                                CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                                (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                chimera_nfs4_open_at_complete,
                                req);
            break;
        case CLAIM_DELEGATE_CUR:
            /* RFC 7530 §16.16: open of a file the client already holds a
             * delegation on, identified by name within the current FH's
             * directory.  Treat like CLAIM_NULL using the name carried in
             * delegate_cur_info; the delegation the client cites is its own,
             * so no recall is needed. */
            status = chimera_nfs4_validate_name(&args->claim.delegate_cur_info.file);
            if (status != NFS4_OK) {
                struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
                chimera_vfs_release(req->thread->vfs_thread, parent_handle);
                res->status = status;
                chimera_nfs4_open_complete(req, status);
                return;
            }
            if (args->openhow.opentype == OPEN4_NOCREATE) {
                struct nfs4_open_lookup_regular_ctx *ctx;

                ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
                chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
                ctx->req     = req;
                ctx->attr    = attr;
                ctx->name    = args->claim.delegate_cur_info.file.data;
                ctx->namelen = args->claim.delegate_cur_info.file.len;
                ctx->flags   = flags;

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred,
                                      parent_handle,
                                      args->claim.delegate_cur_info.file.data,
                                      args->claim.delegate_cur_info.file.len,
                                      CHIMERA_VFS_ATTR_MODE,
                                      0,
                                      chimera_nfs4_open_lookup_regular_complete,
                                      ctx);
                return;
            }

            if (args->openhow.opentype == OPEN4_CREATE &&
                args->openhow.how.mode == UNCHECKED4) {
                struct nfs4_open_unchecked_ctx *ctx;

                ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
                chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
                ctx->req     = req;
                ctx->attr    = attr;
                ctx->name    = args->claim.delegate_cur_info.file.data;
                ctx->namelen = args->claim.delegate_cur_info.file.len;
                ctx->flags   = flags;

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred,
                                      parent_handle,
                                      args->claim.delegate_cur_info.file.data,
                                      args->claim.delegate_cur_info.file.len,
                                      0,
                                      0,
                                      chimera_nfs4_open_unchecked_lookup_complete,
                                      ctx);
                return;
            }
            chimera_vfs_open_at(req->thread->vfs_thread, &req->cred,
                                parent_handle,
                                args->claim.delegate_cur_info.file.data,
                                args->claim.delegate_cur_info.file.len,
                                flags,
                                attr,
                                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
                                CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                                (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                (CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                chimera_nfs4_open_at_complete,
                                req);
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
             * prevents a clean DELEGRETURN. */
            chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                                req->fh,
                                req->fhlen,
                                flags,
                                chimera_nfs4_open_claim_fh_complete,
                                req);
            break;
        default:
            /* CLAIM_DELEGATE_PREV (delegation reclaim across a client reboot)
             * and any unknown claim are not supported -- reject rather than
             * abort the server. */
        {
            struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
            chimera_vfs_release(req->thread->vfs_thread, parent_handle);
            res->status = NFS4ERR_NOTSUPP;
            chimera_nfs4_open_complete(req, NFS4ERR_NOTSUPP);
        }
            return;
    } /* switch */

} /* chimera_nfs4_open_complete */

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

    /* RFC 7530 §9.1.7 entry-time seqid classification for the 4.0 path.
     * Done BEFORE any VFS work so a replay short-circuits without
     * re-executing the open.  On NEW, the resolved owner is stashed on
     * req for chimera_nfs4_open_complete to advance + cache the reply. */
    if (req->minorversion == 0) {
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
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
        if (client->expired) {
            client->expired = 0;
            nfs_client_touch(client);
        }

        bool                   created;
        struct nfs_open_owner *owner = nfs_open_owner_find_or_create(
            client, args->owner.owner.data, args->owner.owner.len,
            &created);

        pthread_mutex_lock(&owner->lock);
        int                    cls = nfs4_owner_seqid_classify(owner->seqid, &owner->replay,
                                                               args->seqid);

        if (cls == NFS4_SEQID_REPLAY) {
            /* Return the cached reply.  Simplified replay (status +
             * stateid only); cinfo/attrset/rflags/delegation are
             * reconstructed as zero/none.  Linux clients tolerate this
             * since they re-fetch attrs via GETATTR after OPEN.
             *
             * A retransmit on the SAME connection is normally answered
             * byte-exact by the v4.0 reply cache before the compound is
             * even decoded (nfs4_v40_drc.c), so this branch is reached
             * only when the retransmit arrives on a new connection --
             * where losing rflags matters least, since a reconnecting
             * client re-establishes its state anyway. */
            res->status                            = owner->replay.status;
            res->resok4.stateid                    = owner->replay.stateid;
            res->resok4.cinfo.atomic               = 0;
            res->resok4.cinfo.before               = 0;
            res->resok4.cinfo.after                = 0;
            res->resok4.rflags                     = 0;
            res->resok4.num_attrset                = 0;
            res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;
            pthread_mutex_unlock(&owner->lock);
            /* Early return before the borrow ref transfers to the request;
             * release it here. */
            nfs_open_owner_put(owner);
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        if (cls != NFS4_SEQID_NEW) {
            /* NFS4ERR_BAD_SEQID is in the no-advance set; do not touch
             * owner state. */
            pthread_mutex_unlock(&owner->lock);
            nfs_open_owner_put(owner);
            res->status = NFS4ERR_BAD_SEQID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        pthread_mutex_unlock(&owner->lock);
        /* Transfer the find_or_create ref onto the request; dropped in
         * chimera_nfs4_open_complete. */
        req->open_4_0_owner = owner;
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
     *      a non-reclaim OPEN is refused with NFS4ERR_GRACE.  This is a
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

        if (req->minorversion > 0 && !is_reclaim && req->session &&
            !nfs4_client_reclaim_complete(&thread->shared->nfs4_shared_clients,
                                          req->session->nfs4_session_clientid)) {
            res->status = NFS4ERR_GRACE;
            chimera_nfs4_open_complete(req, res->status);
            return;
        }
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_open_parent_complete,
                        req);
} /* chimera_nfs4_open */
