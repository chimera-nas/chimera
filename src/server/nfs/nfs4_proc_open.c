// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "nfs4_callback.h"
#include "server/server.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_idmap.h"

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
    } else if (want == OPEN4_SHARE_ACCESS_WANT_NO_DELEG) {
        why = WND4_NOT_WANTED;
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
 * must retain the compound -- the probe completion will
 * resume the OPEN via chimera_nfs4_open_resume_after_probe.  Returns false
 * for the normal path (granted or not), where the caller continues with
 * continue accepted compound publication. When allow_grant is false (closed
 * state or insufficient reply space), still form the WANT-aware decline.
 */
SYMBOL_EXPORT bool
chimera_nfs4_open_grant_delegation(
    struct nfs_request             *req,
    struct OPEN4res                *res,
    const struct chimera_vfs_attrs *file_attr,
    bool                            allow_grant)
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

    if (!allow_grant || !chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
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

    struct chimera_vfs_attrs projected_attr;
    if (deleg_type == OPEN_DELEGATE_WRITE) {
        if (!nfs4_change_register(thread->shared->nfs4_state_table.change_table, req->fh, req->fhlen)) {
            return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
        }
        if (file_attr) {
            struct nfs4_change_observation *observation;
            projected_attr = *file_attr;
            if (nfs4_change_project(thread->shared->nfs4_state_table.change_table,
                                    req->fh, req->fhlen, &projected_attr, &req->change_observations,
                                    &observation) != NFS4_OK) {
                return chimera_nfs4_open_deleg_none(req, res, WND4_RESOURCE);
            }
            file_attr = &projected_attr;
        }
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

/* The compound retains all state until accepted delegation publication resumes. */
void
chimera_nfs4_open_resume_after_probe(struct nfs_request *req)
{
    chimera_nfs_abort_if(!req->compound_probe_resume, "OPEN probe lost compound continuation");
    req->compound_probe_resume(req);
} /* chimera_nfs4_open_resume_after_probe */


void
chimera_nfs4_open(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    (void) argop;
    if (!req->fhlen) {
        resop->opopen.status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOFILEHANDLE);
        return;
    }
    chimera_nfs4_compound_state(thread, req);
} /* chimera_nfs4_open */
