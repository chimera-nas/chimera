// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_allocate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request  *req = private_data;
    struct ALLOCATE4res *res = &req->res_compound.resarray[req->index].opallocate;

    if (error_code == CHIMERA_VFS_OK) {
        res->ar_status = NFS4_OK;
    } else {
        res->ar_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    } else if (req->handle) {
        /* Special stateid: release the on-the-fly handle we opened. */
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs4_compound_complete(req, res->ar_status);
} /* chimera_nfs4_allocate_complete */

static void
chimera_nfs4_allocate_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request   *req  = private_data;
    struct ALLOCATE4args *args = &req->args_compound->argarray[req->index].opallocate;
    struct ALLOCATE4res  *res  = &req->res_compound.resarray[req->index].opallocate;

    if (error_code != CHIMERA_VFS_OK) {
        res->ar_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->ar_status);
        return;
    }

    req->handle = handle;

    chimera_vfs_allocate(req->thread->vfs_thread, &req->cred,
                         handle,
                         args->aa_offset,
                         args->aa_length,
                         0,
                         0, 0,
                         chimera_nfs4_allocate_complete,
                         req);
} /* chimera_nfs4_allocate_open_callback */

static void
chimera_nfs4_allocate_typecheck_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request  *req = private_data;
    struct ALLOCATE4res *res = &req->res_compound.resarray[req->index].opallocate;

    if (error_code != CHIMERA_VFS_OK) {
        res->ar_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->ar_status);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(attr->va_mode)) {
        res->ar_status = chimera_nfs4_data_nonreg_status(attr->va_mode);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->ar_status);
        return;
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    req->handle = NULL;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_allocate_open_callback,
                        req);
} /* chimera_nfs4_allocate_typecheck_complete */

/*
 * The current filehandle of a special-stateid ALLOCATE is not guaranteed to be
 * a regular file: nothing has OPENed it, so no earlier op rejected the type.
 * RFC 7862 §11.2: ALLOCATE operates on a regular file, so a
 * directory cfh is NFS4ERR_ISDIR rather than a backend-level success.
 */
static void
chimera_nfs4_allocate_typecheck_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req = private_data;
    struct ALLOCATE4res *res = &req->res_compound.resarray[req->index].opallocate;

    if (error_code != CHIMERA_VFS_OK) {
        res->ar_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->ar_status);
        return;
    }

    req->handle = handle;
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_MODE,
                        chimera_nfs4_allocate_typecheck_complete,
                        req);
} /* chimera_nfs4_allocate_typecheck_open_callback */

void
chimera_nfs4_allocate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct ALLOCATE4args           *args  = &argop->opallocate;
    struct ALLOCATE4res            *res   = &resop->opallocate;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->aa_stateid);

    /* Every path below acts on the current filehandle: the special-stateid
     * path opens it directly, and the state-table path must name it. */
    if (req->fhlen == 0) {
        res->ar_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->ar_status);
        return;
    }

    /*
     * RFC 8881 §8.2.3 requires ALLOCATE to honor the special stateids.  These
     * carry no state-table entry, so open the current FH on the fly instead of
     * consulting the state table.
     */
    if (nfs4_stateid_is_special(&args->aa_stateid)) {
        /* A special-stateid write op must still honor share-reservation
         * deny-WRITE modes held by any owner of any client (RFC 8881
         * §9.7): ALLOCATE/DEALLOCATE modify file data, so a conflicting
         * deny-WRITE open makes them NFS4ERR_LOCKED, exactly as WRITE. */
        nfsstat4 dstatus = nfs4_clients_check_io_denied(
            &thread->shared->nfs4_shared_clients,
            req->fh, req->fhlen, OPEN4_SHARE_ACCESS_WRITE);

        if (dstatus != NFS4_OK) {
            res->ar_status = dstatus;
            chimera_nfs4_compound_complete(req, res->ar_status);
            return;
        }
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                            CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_allocate_typecheck_open_callback,
                            req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->aa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->ar_status = status;
        chimera_nfs4_compound_complete(req, res->ar_status);
        return;
    }

    status = nfs_state_check_write_for_fh(state_void, state_type,
                                          req->fh, req->fhlen);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->ar_status = status;
        chimera_nfs4_compound_complete(req, res->ar_status);
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        state_handle = ((struct nfs_open_state *) state_void)->handle;
    } else {
        state_handle = ((struct nfs_lock_state *) state_void)->handle;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    chimera_vfs_allocate(thread->vfs_thread, &req->cred,
                         state_handle,
                         args->aa_offset,
                         args->aa_length,
                         0,
                         0, 0,
                         chimera_nfs4_allocate_complete,
                         req);
} /* chimera_nfs4_allocate */
