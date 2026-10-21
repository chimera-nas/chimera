// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_state.h"
#include "nfs4_stateid.h"
#include "nfs4_session.h"
#include "nfs4_cb.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_setattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct SETATTR4args              *args   = &req->args_compound->argarray[req->index].opsetattr;
    struct SETATTR4res               *res    = &req->res_compound.resarray[req->index].opsetattr;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS4_OK;

        res->attrsset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t), req->encoding->dbuf);
        chimera_nfs_abort_if(res->attrsset == NULL, "Failed to allocate space");

        res->num_attrsset = chimera_nfs4_mask2attr(set_attr,
                                                   args->obj_attributes.num_attrmask,
                                                   args->obj_attributes.attrmask,
                                                   res->attrsset);
    } else {
        res->status       = chimera_nfs4_errno_to_nfsstat4(error_code);
        res->num_attrsset = 0;
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_setattr_complete */

/* Unmarshall the requested attributes and apply them to `handle`.  fd_rights
 * selects descriptor semantics (chimera_vfs_fsetattr): a size change through
 * an open stateid is authorized by the OPEN, exactly as ftruncate(2) is by
 * its descriptor, and must not be re-gated against the file's current mode. */
static void
chimera_nfs4_setattr_apply(
    struct nfs_request             *req,
    struct chimera_vfs_open_handle *handle,
    int                             fd_rights)
{
    struct SETATTR4args      *args = &req->args_compound->argarray[req->index].opsetattr;
    struct SETATTR4res       *res  = &req->res_compound.resarray[req->index].opsetattr;
    struct chimera_vfs_attrs *attr;
    int                       rc;

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    req->handle = handle;

    struct chimera_acl       *acl_buf      = NULL;
    unsigned                  acl_buf_aces = 0;
    if (args->obj_attributes.num_attrmask >= 1 &&
        (args->obj_attributes.attrmask[0] & (1 << FATTR4_ACL))) {
        acl_buf = xdr_dbuf_alloc_space(chimera_acl_size(CHIMERA_ACL_MAX_ACES),
                                       req->encoding->dbuf);
        acl_buf_aces = acl_buf ? CHIMERA_ACL_MAX_ACES : 0;
    }

    rc = chimera_nfs4_unmarshall_attrs(attr,
                                       args->obj_attributes.num_attrmask,
                                       args->obj_attributes.attrmask,
                                       args->obj_attributes.attr_vals.data,
                                       args->obj_attributes.attr_vals.len,
                                       acl_buf,
                                       acl_buf_aces);

    if (rc != NFS4_OK) {
        res->status = rc;
        chimera_vfs_release(req->thread->vfs_thread, handle);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (fd_rights && req->io_owner_valid) {
        /* The size change is the stateid owner's own: carry that client's
         * claim actor -- the exact owner its delegation claim was created
         * with at OPEN, this client id and the file's hash
         * (chimera_vfs_hash of the unwrapped filehandle, which is what the
         * open cache stamps into handle->fh_hash) -- which
         * chimera_vfs_claim_trigger_ns_full exempts from the size-change
         * recall by owner identity.  Without it the recall
         * exempts only a claim anchored to this very handle, and an NFSv4
         * delegation claim has no handle anchor: the holder's own truncate
         * would recall, and after the recall deadline revoke, its own
         * delegation.  Other clients' conflicting claims are still
         * recalled. */
        struct chimera_claim_actor io_owner = {
            .owner          = {
                .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
                .client_key = req->io_owner_client_id,
                .owner_lo   = handle->fh_hash,
                .owner_hi   = 0,
            },
        };

        chimera_vfs_fsetattr_owned(req->thread->vfs_thread,
                                   &req->cred,
                                   handle,
                                   attr,
                                   0,
                                   0,
                                   chimera_nfs4_setattr_complete,
                                   req,
                                   &io_owner);
    } else if (fd_rights) {
        chimera_vfs_fsetattr(req->thread->vfs_thread,
                             &req->cred,
                             handle,
                             attr,
                             0,
                             0,
                             chimera_nfs4_setattr_complete,
                             req);
    } else {
        chimera_vfs_setattr(req->thread->vfs_thread,
                            &req->cred,
                            handle,
                            attr,
                            0,
                            0,
                            chimera_nfs4_setattr_complete,
                            req);
    }
} /* chimera_nfs4_setattr_apply */

static void
chimera_nfs4_setattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;
    struct SETATTR4res *res = &req->res_compound.resarray[req->index].opsetattr;

    if (error_code != CHIMERA_VFS_OK) {
        req->handle = handle;
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_setattr_apply(req, handle, 0);
} /* chimera_nfs4_setattr_open_callback */

/* Completion of the on-the-fly open a delegation-authorized size change
 * goes through.  The delegation carries the write grant the way an open
 * does, so the attributes are applied with descriptor rights.  A handle the
 * on-the-fly open creates has no bound grant, so chimera_vfs_setattr_common
 * re-checks DAC the way it does for a path setattr; when the open cache
 * hands back this credential's own OPEN-time handle for the file instead,
 * that handle's bound grant authorizes the size change, as it would
 * through the open stateid. */
static void
chimera_nfs4_setattr_deleg_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;
    struct SETATTR4res *res = &req->res_compound.resarray[req->index].opsetattr;

    if (error_code != CHIMERA_VFS_OK) {
        req->handle = handle;
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_setattr_apply(req, handle, 1);
} /* chimera_nfs4_setattr_deleg_open_callback */

/* Apply the attributes -- through the open stateid's own handle when the
 * size-change validation stashed one (descriptor rights); through an
 * on-the-fly open of the current filehandle when a delegation stateid
 * authorized the size change (it names the holder but carries no open
 * handle, exactly as for WRITE); otherwise via a fresh path-only open of
 * the target.  Invoked directly, or as the resume continuation once a
 * conflicting layout has been recalled. */
static void
nfs4_setattr_proceed(void *arg)
{
    struct nfs_request *req = arg;

    if (req->handle) {
        struct chimera_vfs_open_handle *handle = req->handle;

        req->handle = NULL;
        chimera_nfs4_setattr_apply(req, handle, 1);
        return;
    }

    if (req->io_owner_valid) {
        chimera_vfs_open_fh(req->thread->vfs_thread,
                            &req->cred,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED |
                            CHIMERA_VFS_OPEN_NOFOLLOW |
                            CHIMERA_VFS_OPEN_REGULAR_ONLY,
                            chimera_nfs4_setattr_deleg_open_callback,
                            req);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread,
                        &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_setattr_open_callback,
                        req);
} /* nfs4_setattr_proceed */

void
chimera_nfs4_setattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETATTR4args *args = &argop->opsetattr;
    struct SETATTR4res  *res  = &resop->opsetattr;

    /* The XDR marshaller emits num_attrsset and the attrsset array
     * unconditionally; resarray slots come from a bump allocator that does
     * not zero memory, so any early-error return must initialize these or
     * the marshaller dereferences garbage. */
    res->num_attrsset = 0;
    res->attrsset     = NULL;

    /* nfs_request is pooled; nfs4_setattr_proceed keys off req->handle to
     * decide between the stateid-handle and path-open application paths,
     * and the owner fields are set only by the stateid branch below. */
    req->handle             = NULL;
    req->io_owner_valid     = false;
    req->io_owner_client_id = 0;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Reject requests for unsupported / non-writable attributes before
     * touching the VFS. Same validation as CREATE: returns ATTRNOTSUPP for
     * unknown attrs and INVAL for read-only attrs. */
    res->status = chimera_nfs4_validate_createattrs(
        args->obj_attributes.num_attrmask,
        args->obj_attributes.attrmask);
    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->stateid);

    /* RFC 7530 §9.6.2: a size-changing SETATTR is a write (§9.1.4.3), so it
     * is refused with NFS4ERR_GRACE while the server-reboot grace window is
     * open -- the byte-range lock it would truncate through may not have been
     * reclaimed yet.  Attribute-only SETATTRs conflict with no reclaim and
     * are left alone. */
    if (args->obj_attributes.num_attrmask >= 1 &&
        (args->obj_attributes.attrmask[0] & (1 << FATTR4_SIZE))) {
        nfsstat4 g_status = nfs_recovery_io_check(
            &thread->shared->nfs4_recovery);

        if (g_status != NFS4_OK) {
            res->status = g_status;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
    }

    /* RFC 7530 section 16.32.4: when SETATTR carries FATTR4_SIZE the
     * stateid provides the locking context of a WRITE -- an open with write
     * access, or a lock stateid anchored to one.  Special stateids
     * (all-zero / all-ones) are exempt -- treated as anonymous, like the
     * pre-Phase-2 behavior. */
    /* RFC 7530 §9.1.4.3 / RFC 8881 §9.7: a size-changing SETATTR is a
     * write; through a special stateid it must honor deny-WRITE share
     * reservations held by any owner of any client (NFS4ERR_LOCKED). */
    if (args->obj_attributes.num_attrmask >= 1 &&
        (args->obj_attributes.attrmask[0] & (1 << FATTR4_SIZE)) &&
        nfs4_stateid_is_special(&args->stateid)) {
        nfsstat4 dstatus = nfs4_clients_check_io_denied(
            &thread->shared->nfs4_shared_clients,
            req->fh,
            req->fhlen,
            OPEN4_SHARE_ACCESS_WRITE);

        if (dstatus != NFS4_OK) {
            res->status = dstatus;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
    }

    if (args->obj_attributes.num_attrmask >= 1 &&
        (args->obj_attributes.attrmask[0] & (1 << FATTR4_SIZE)) &&
        !nfs4_stateid_is_special(&args->stateid)) {
        struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
        struct chimera_vfs_open_handle *state_handle;
        void                           *state_void;
        uint8_t                         state_type;
        nfsstat4                        status;

        /* Look the stateid up by any type: a client holding a byte-range
         * lock on the file sends the lock stateid here, and one holding a
         * delegation the delegation stateid, exactly as it would for a
         * WRITE, and refusing either while TEST_STATEID reports it valid
         * leaves the client resending the SETATTR forever.  A delegation
         * stateid is judged by the branch below, an open or lock stateid by
         * nfs_state_check_write_for_fh. */
        status = nfs_state_table_acquire(table, &args->stateid, 0,
                                         &state_void, &state_type);
        status = nfs4_stateid_acquire_status(status, req->minorversion);
        if (status != NFS4_OK) {
            res->status = status;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        status = nfs_state_check_client(
            state_void, state_type,
            req->session ? req->session->client_unified : NULL);
        if (status != NFS4_OK) {
            nfs_state_table_release(table, state_void, state_type,
                                    thread->vfs_thread);
            res->status = status;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        /* A delegation stateid: what a Linux client sends when it holds a
         * delegation on the file and no POSIX lock (nfs4_select_rw_stateid
         * falls back to it before the open stateid; truncate(2) with no
         * open context tries the delegation stateid before the anonymous
         * one).  A write delegation of the current filehandle authorizes
         * the size change; a read delegation grants no write
         * (NFS4ERR_OPENMODE, as a read-only open would); a delegation on
         * another file designates no state for this one.  It carries no
         * open handle, so nfs4_setattr_proceed opens the current filehandle
         * on the fly with the holder as the claim actor, which keeps the
         * holder's own delegation -- recalled or not -- out of the
         * size-change recall. */
        if (state_type == NFS4_SLOT_TYPE_DELEG) {
            struct nfs_delegation *deleg = state_void;

            if (deleg->fh_len != req->fhlen ||
                memcmp(deleg->fh, req->fh, req->fhlen) != 0) {
                status = NFS4ERR_BAD_STATEID;
            } else if (deleg->type != OPEN_DELEGATE_WRITE) {
                status = NFS4ERR_OPENMODE;
            } else {
                req->io_owner_valid     = true;
                req->io_owner_client_id = deleg->client->client_id;
            }
            nfs_state_table_release(table, state_void, state_type,
                                    thread->vfs_thread);
            if (status != NFS4_OK) {
                res->status = status;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
        } else {
            /* RFC 7530 section 9.1.4.3: the stateid must name a
             * write-capable open of the object that is the current
             * filehandle -- directly, or as the open a lock stateid is
             * anchored to -- not some other open file. */
            status = nfs_state_check_write_for_fh(state_void, state_type,
                                                  req->fh, req->fhlen);
            if (status != NFS4_OK) {
                nfs_state_table_release(table, state_void, state_type,
                                        thread->vfs_thread);
                res->status = status;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }

            /* Apply through the open's own handle: the stateid authorizes the
             * size change the way a descriptor authorizes ftruncate(2), and the
             * OPEN-time handle carries that grant (and, on a passthrough
             * backend, the writable descriptor itself) where a fresh path-only
             * open would face a per-operation permission re-check.  The
             * truncate is the stateid owner's own I/O: carry that client as the
             * claim actor so it does not recall the client's own delegation. */
            state_handle = nfs_state_io_handle(state_void, state_type,
                                               OPEN4_SHARE_ACCESS_WRITE);
            if (state_handle) {
                chimera_vfs_dup_handle(thread->vfs_thread, state_handle);
                req->handle             = state_handle;
                req->io_owner_valid     = true;
                req->io_owner_client_id =
                    nfs_state_owner_client(state_void, state_type)->client_id;
            }

            nfs_state_table_release(table, state_void, state_type,
                                    thread->vfs_thread);
        }
    }

    /* A size change conflicts with any outstanding writable layout for the
     * file -- held by this client or any other.  Recall it from every holder
     * and defer the truncate until they have flushed to the data server and
     * returned their layouts (two-stage recall); if none is held,
     * nfs4_setattr_proceed runs immediately. */
    if (args->obj_attributes.num_attrmask >= 1 &&
        (args->obj_attributes.attrmask[0] & (1 << FATTR4_SIZE)) &&
        chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
        chimera_nfs4_cb_recall_and_wait(thread, req->fh, req->fhlen,
                                        nfs4_setattr_proceed, req);
        return;
    }

    nfs4_setattr_proceed(req);
} /* chimera_nfs4_setattr */
