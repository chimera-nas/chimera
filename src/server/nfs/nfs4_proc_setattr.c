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

static void
chimera_nfs4_setattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request       *req  = private_data;
    struct SETATTR4args      *args = &req->args_compound->argarray[req->index].opsetattr;
    struct SETATTR4res       *res  = &req->res_compound.resarray[req->index].opsetattr;
    struct chimera_vfs_attrs *attr;
    int                       rc;

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    req->handle = handle;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    struct chimera_acl *acl_buf      = NULL;
    unsigned            acl_buf_aces = 0;
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

    chimera_vfs_setattr(req->thread->vfs_thread,
                        &req->cred,
                        handle,
                        attr,
                        0,
                        0,
                        chimera_nfs4_setattr_complete,
                        req);
} /* chimera_nfs4_setattr_open_callback */

/* Open the target and apply the attributes.  Invoked directly, or as the
 * resume continuation once a conflicting layout has been recalled. */
static void
nfs4_setattr_proceed(void *arg)
{
    struct nfs_request *req = arg;

    chimera_vfs_open_fh(req->thread->vfs_thread,
                        &req->cred, NULL,
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

    /* RFC 7530 §16.32.3: when SETATTR carries FATTR4_SIZE the supplied
     * stateid must identify an open with write access.  Special stateids
     * (all-zero / all-ones) are exempt -- treated as anonymous, like the
     * pre-Phase-2 behavior. */
    if (args->obj_attributes.num_attrmask >= 1 &&
        (args->obj_attributes.attrmask[0] & (1 << FATTR4_SIZE)) &&
        !nfs4_stateid_is_special(&args->stateid)) {
        struct nfs_state_table *table = &thread->shared->nfs4_state_table;
        void                   *state_void;
        uint8_t                 state_type;
        nfsstat4                status;

        status = nfs_state_table_acquire(table, &args->stateid,
                                         NFS4_SLOT_TYPE_OPEN,
                                         &state_void, &state_type);
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

        struct nfs_open_state *open_state = state_void;

        /* RFC 7530 §9.1.4.3: the stateid must name an open of the object that
         * is the current filehandle, not some other open file. */
        if (open_state->fh_len != req->fhlen ||
            memcmp(open_state->fh, req->fh, req->fhlen) != 0) {
            nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                    thread->vfs_thread);
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        bool has_write = (open_state->share_access &
                          OPEN4_SHARE_ACCESS_WRITE) != 0;

        nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);

        if (!has_write) {
            res->status = NFS4ERR_OPENMODE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
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
