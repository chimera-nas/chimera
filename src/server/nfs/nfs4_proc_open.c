// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_state.h"
#include "nfs4_callback.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"
#include "vfs/vfs_access.h"
#include "vfs/vfs_acl.h"

/*
 * Acquire a cross-protocol SHARE reservation in vfs_state for a freshly
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
    struct OPEN4args               *args      = &req->args_compound->argarray[req->index].opopen;
    struct chimera_vfs_state       *vfs_state = req->thread->vfs->vfs_state;
    struct chimera_vfs_open_handle *handle    = state->handle;
    struct chimera_vfs_file_state  *file_state;
    struct chimera_vfs_lease       *conflict = NULL;
    enum chimera_vfs_lease_result   result;
    uint8_t                         granted = 0;
    uint8_t                         denied  = 0;

    if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        granted |= CHIMERA_VFS_LEASE_MODE_R;
    }
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        granted |= CHIMERA_VFS_LEASE_MODE_W;
    }
    if (args->share_deny & OPEN4_SHARE_DENY_READ) {
        denied |= CHIMERA_VFS_LEASE_MODE_R;
    }
    if (args->share_deny & OPEN4_SHARE_DENY_WRITE) {
        denied |= CHIMERA_VFS_LEASE_MODE_W;
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

    state->share_lease.kind             = CHIMERA_VFS_LEASE_SHARE;
    state->share_lease.mode.granted     = granted;
    state->share_lease.mode.denied      = denied;
    state->share_lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_NFSV4;
    state->share_lease.owner.client_key = state->owner->client->client_id;
    state->share_lease.owner.owner_lo   = XXH3_64bits(state->owner->owner,
                                                      state->owner->owner_len);
    state->share_lease.owner.owner_hi = 0;
    /* Courteous server: report this share reservation dead once the owning
     * client's lease lapses, so a conflicting open reclaims it; reclaim flags
     * the client for sweep teardown. */
    state->share_lease.owner.is_alive_cb = nfs_client_lease_alive;
    state->share_lease.owner.revoked_cb  = nfs_client_lease_revoked_cb;
    state->share_lease.owner.cb_private  = state->owner->client;

    result = chimera_vfs_state_try_insert(vfs_state, file_state,
                                          &state->share_lease, &conflict);
    if (result == CHIMERA_VFS_LEASE_BREAKING) {
        /* The conflict is a breakable holder -- an NFSv4 delegation being
         * recalled (try_insert already kicked the break).  Tell the client to
         * retry; by the next attempt the delegation's DELEGRETURN should have
         * released the lease and the SHARE will be granted (RFC 7530 §10.4.5
         * recommends NFS4ERR_DELAY while a recall is outstanding). */
        chimera_vfs_state_put(vfs_state, file_state);
        return NFS4ERR_DELAY;
    }
    if (result != CHIMERA_VFS_LEASE_GRANTED) {
        chimera_vfs_state_put(vfs_state, file_state);
        return NFS4ERR_SHARE_DENIED;
    }

    state->share_file_state = file_state;
    state->share_lease_held = true;
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
    struct nfs_request *req,
    struct OPEN4res    *res)
{
    struct chimera_server_nfs_thread *thread    = req->thread;
    struct OPEN4args                 *args      = &req->args_compound->argarray[req->index].opopen;
    struct nfs_client                *client    = req->session ? req->session->client_unified : NULL;
    struct chimera_vfs_state         *vfs_state = thread->vfs->vfs_state;
    struct nfs_delegation            *deleg;
    struct nfs_delegation            *d;
    struct chimera_vfs_file_state    *file_state;
    struct chimera_vfs_lease         *conflict = NULL;
    enum chimera_vfs_lease_result     result;
    struct stateid4                   deleg_stateid;
    uint64_t                          fh_hash;
    bool                              exists = false;
    uint8_t                           deleg_type;
    uint8_t                           lease_mode;
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
        lease_mode = CHIMERA_VFS_LEASE_MODE_W;
    } else if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        deleg_type = OPEN_DELEGATE_READ;
        lease_mode = CHIMERA_VFS_LEASE_MODE_R;
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
                                  &thread->shared->nfs4_state_table,
                                  &deleg_stateid);

    deleg->lease.kind             = CHIMERA_VFS_LEASE_CACHING;
    deleg->lease.mode.granted     = lease_mode;
    deleg->lease.mode.denied      = 0;
    deleg->lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_NFSV4;
    deleg->lease.owner.client_key = client->client_id;
    deleg->lease.owner.owner_lo   = fh_hash;
    deleg->lease.owner.owner_hi   = 0;
    deleg->lease.owner.break_cb   = nfs4_delegation_break_cb;
    /* Courteous server: report the delegation dead once the owning client's
     * lease lapses, so a conflicting open revokes it outright rather than
     * attempting a CB_RECALL to a client that is gone. */
    deleg->lease.owner.is_alive_cb = nfs_delegation_lease_alive;
    deleg->lease.owner.revoked_cb  = nfs_delegation_revoked_cb;
    deleg->lease.owner.cb_private  = deleg;

    file_state = chimera_vfs_state_get(vfs_state, req->fh, req->fhlen,
                                       fh_hash, true);
    if (!file_state) {
        nfs_delegation_destroy(deleg, &thread->shared->nfs4_state_table,
                               thread->vfs_thread);
        return false;
    }
    deleg->file_state = file_state;

    result = chimera_vfs_state_try_insert(vfs_state, file_state,
                                          &deleg->lease, &conflict);
    if (result != CHIMERA_VFS_LEASE_GRANTED) {
        /* Contention (another open / lease): just decline to delegate. */
        chimera_vfs_state_put(vfs_state, file_state);
        deleg->file_state = NULL;
        nfs_delegation_destroy(deleg, &thread->shared->nfs4_state_table,
                               thread->vfs_thread);
        return false;
    }
    deleg->lease_held = true;

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
    struct stateid4                *out_stateid,
    uint32_t                       *out_rflags)
{
    struct OPEN4args      *args   = &req->args_compound->argarray[req->index].opopen;
    struct nfs_client     *client = req->session ? req->session->client_unified : NULL;
    struct nfs_open_owner *owner;
    struct nfs_open_state *existing;
    bool                   created = false;

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
     * filehandle (CLAIM_FH), where access was already established. */
    if (attr) {
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

    owner = nfs_open_owner_find_or_create(client,
                                          args->owner.owner.data,
                                          args->owner.owner.len,
                                          &created);

    /* RFC 7530 §9.10: check share-mode conflict against opens by *other*
     * owners on this client.  Same-owner OPEN coalesces via the
     * find_state path below and is exempt. */
    {
        nfsstat4 conflict = nfs_client_check_share_conflict(
            client, owner,
            handle->fh, handle->fh_len,
            args->share_access, args->share_deny);
        if (conflict != NFS4_OK) {
            chimera_vfs_release(req->thread->vfs_thread, handle);
            return conflict;
        }
    }

    existing = nfs_open_owner_find_state(owner, handle->fh, handle->fh_len);

    if (existing) {
        /* RFC 7530 §9.9: a same-owner re-open is still a distinct share
         * request -- it must not ask for access the existing open denies, nor
         * deny access the existing open holds.  (Plain deny=NONE upgrades, as
         * normal clients issue, never trip this.) */
        if ((existing->share_access & args->share_deny) ||
            (args->share_access & existing->share_deny)) {
            chimera_vfs_release(req->thread->vfs_thread, handle);
            return NFS4ERR_SHARE_DENIED;
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
        nfsstat4               share_status;

        new_state = nfs_open_state_create(owner,
                                          req->principal_flavor,
                                          req->principal_machinename,
                                          req->principal_machinename_len,
                                          handle->fh, handle->fh_len,
                                          args->share_access, args->share_deny,
                                          handle,
                                          &req->thread->shared->nfs4_state_table,
                                          out_stateid);

        /* Cross-protocol SHARE coordination.  On conflict, tear the
         * just-created state back down (releasing the handle) and fail. */
        share_status = chimera_nfs4_open_acquire_share(req, new_state);
        if (share_status != NFS4_OK) {
            nfs_open_state_destroy(new_state,
                                   &req->thread->shared->nfs4_state_table,
                                   req->thread->vfs_thread);
            return share_status;
        }
    }

    /* RFC 7530 §16.18.5: signal OPEN4_RESULT_CONFIRM for an unconfirmed
     * open_owner on a 4.0 client.  The client must then send OPEN_CONFIRM
     * before issuing any further state-modifying op against this owner. */
    if (req->minorversion == 0 && !owner->confirmed) {
        *out_rflags |= OPEN4_RESULT_CONFIRM;
    }

    return NFS4_OK;
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
static void
chimera_nfs4_open_complete(
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

    /* NFS4.1: a successful OPEN sets the COMPOUND's current stateid. */
    if (status == NFS4_OK) {
        struct OPEN4res *res =
            &req->res_compound.resarray[req->index].opopen;
        chimera_nfs4_set_current_stateid(req, &res->resok4.stateid);
    }

    chimera_nfs4_compound_complete(req, status);
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

    deferred = chimera_nfs4_open_grant_delegation(req, res);
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
    if (chimera_nfs4_open_grant_delegation(req, res)) {
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
            chimera_vfs_open_at(req->thread->vfs_thread, &req->cred, NULL,
                                parent_handle,
                                args->claim.file.data,
                                args->claim.file.len,
                                CHIMERA_VFS_OPEN_INFERRED,
                                set_attr,
                                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME,
                                0,
                                0,
                                chimera_nfs4_open_exclusive_verify,
                                req);
            return;
        }
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.16.5: OPEN targets a regular file. A directory yields
     * NFS4ERR_ISDIR; any other non-regular object (symlink, socket, fifo,
     * block/char device) yields NFS4ERR_SYMLINK. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISREG(attr->va_mode)) {
        res->status = S_ISDIR(attr->va_mode) ? NFS4ERR_ISDIR : NFS4ERR_SYMLINK;
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
    if (chimera_nfs4_open_grant_delegation(req, res)) {
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
     * devices can otherwise block or report backend-specific errors. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISREG(attr->va_mode)) {
        res->status = S_ISDIR(attr->va_mode) ? NFS4ERR_ISDIR : NFS4ERR_SYMLINK;
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    chimera_vfs_open_at(req->thread->vfs_thread, &req->cred, NULL,
                        parent_handle,
                        ctx->name,
                        ctx->namelen,
                        ctx->flags,
                        ctx->attr,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
                        CHIMERA_VFS_ATTR_MTIME,
                        CHIMERA_VFS_ATTR_MTIME,
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

    (void) existing_attr;
    (void) dir_attr;

    if (error_code == CHIMERA_VFS_OK) {
        /* RFC 7530 OPEN/UNCHECKED recreate: create attrs are ignored for an
         * existing object, except size=0 truncates the file. */
        if ((ctx->attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
            ctx->attr->va_size == 0) {
            ctx->attr->va_set_mask = CHIMERA_VFS_ATTR_SIZE;
            ctx->attr->va_req_mask = CHIMERA_VFS_ATTR_SIZE;
        } else {
            ctx->attr->va_set_mask = 0;
            ctx->attr->va_req_mask = 0;
        }
    } else if (error_code != CHIMERA_VFS_ENOENT) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_open_complete(req, res->status);
        return;
    }

    chimera_vfs_open_at(req->thread->vfs_thread, &req->cred, NULL,
                        parent_handle,
                        ctx->name,
                        ctx->namelen,
                        ctx->flags,
                        ctx->attr,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
                        CHIMERA_VFS_ATTR_MTIME,
                        CHIMERA_VFS_ATTR_MTIME,
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

        status = chimera_nfs4_open_install_state(req, handle, NULL,
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
    if (chimera_nfs4_open_grant_delegation(req, res)) {
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

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, NULL,
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

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, NULL,
                                      parent_handle,
                                      args->claim.file.data,
                                      args->claim.file.len,
                                      0,
                                      0,
                                      chimera_nfs4_open_unchecked_lookup_complete,
                                      ctx);
                return;
            }

            chimera_vfs_open_at(req->thread->vfs_thread, &req->cred, NULL,
                                parent_handle,
                                args->claim.file.data,
                                args->claim.file.len,
                                flags,
                                attr,
                                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
                                CHIMERA_VFS_ATTR_MTIME,
                                CHIMERA_VFS_ATTR_MTIME,
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

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, NULL,
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

                chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, NULL,
                                      parent_handle,
                                      args->claim.delegate_cur_info.file.data,
                                      args->claim.delegate_cur_info.file.len,
                                      0,
                                      0,
                                      chimera_nfs4_open_unchecked_lookup_complete,
                                      ctx);
                return;
            }
            chimera_vfs_open_at(req->thread->vfs_thread, &req->cred, NULL,
                                parent_handle,
                                args->claim.delegate_cur_info.file.data,
                                args->claim.delegate_cur_info.file.len,
                                flags,
                                attr,
                                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
                                CHIMERA_VFS_ATTR_MTIME,
                                CHIMERA_VFS_ATTR_MTIME,
                                chimera_nfs4_open_at_complete,
                                req);
            break;
        case CLAIM_PREVIOUS:
        case CLAIM_FH:
            chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, NULL,
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
             * since they re-fetch attrs via GETATTR after OPEN. */
            res->status                            = owner->replay.status;
            res->resok4.stateid                    = owner->replay.stateid;
            res->resok4.cinfo.atomic               = 0;
            res->resok4.cinfo.before               = 0;
            res->resok4.cinfo.after                = 0;
            res->resok4.rflags                     = 0;
            res->resok4.num_attrset                = 0;
            res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;
            pthread_mutex_unlock(&owner->lock);
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        if (cls != NFS4_SEQID_NEW) {
            /* NFS4ERR_BAD_SEQID is in the no-advance set; do not touch
             * owner state. */
            pthread_mutex_unlock(&owner->lock);
            res->status = NFS4ERR_BAD_SEQID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        pthread_mutex_unlock(&owner->lock);
        req->open_4_0_owner = owner;
    }

    /* Gate OPEN during recovery.  Two distinct rules apply:
     *
     *   1. Server-reboot grace window (nfs_recovery_open_check): non-reclaim
     *      OPENs are refused while in_grace, and CLAIM_PREVIOUS reclaims are
     *      refused outside it.  Persistence is stubbed so in_grace is false in
     *      practice today, leaving only the reclaim-outside-grace NO_GRACE leg
     *      live.
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

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_open_parent_complete,
                        req);
} /* chimera_nfs4_open */
