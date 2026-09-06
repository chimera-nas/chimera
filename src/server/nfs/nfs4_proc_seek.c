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
chimera_nfs4_seek_complete(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data)
{
    struct nfs_request *req = private_data;
    struct SEEK4res    *res = &req->res_compound.resarray[req->index].opseek;

    if (error_code == CHIMERA_VFS_OK) {
        res->sa_status        = NFS4_OK;
        res->resok4.sr_eof    = sr_eof;
        res->resok4.sr_offset = sr_offset;
    } else {
        res->sa_status = chimera_nfs4_errno_to_nfsstat4(error_code);
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

    chimera_nfs4_compound_complete(req, res->sa_status);
} /* chimera_nfs4_seek_complete */

static void
chimera_nfs4_seek_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct SEEK4args   *args = &req->args_compound->argarray[req->index].opseek;
    struct SEEK4res    *res  = &req->res_compound.resarray[req->index].opseek;

    if (error_code != CHIMERA_VFS_OK) {
        res->sa_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    req->handle = handle;

    chimera_vfs_seek(req->thread->vfs_thread, &req->cred, req->compound,
                     handle,
                     args->sa_offset,
                     args->sa_what,
                     chimera_nfs4_seek_complete,
                     req);
} /* chimera_nfs4_seek_open_callback */

static void
chimera_nfs4_seek_typecheck_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct SEEK4res    *res = &req->res_compound.resarray[req->index].opseek;

    if (error_code != CHIMERA_VFS_OK) {
        res->sa_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(attr->va_mode)) {
        res->sa_status = chimera_nfs4_data_nonreg_status(attr->va_mode);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    req->handle = NULL;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, req->compound,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_READ_ONLY,
                        chimera_nfs4_seek_open_callback,
                        req);
} /* chimera_nfs4_seek_typecheck_complete */

/*
 * The current filehandle of a special-stateid SEEK is not guaranteed to be
 * a regular file: nothing has OPENed it, so no earlier op rejected the type.
 * RFC 7862 §15.11.3 lists NFS4ERR_ISDIR for SEEK on a
 * directory; without this the offset check below runs first and reports
 * NFS4ERR_NXIO instead.
 */
static void
chimera_nfs4_seek_typecheck_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;
    struct SEEK4res    *res = &req->res_compound.resarray[req->index].opseek;

    if (error_code != CHIMERA_VFS_OK) {
        res->sa_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    req->handle = handle;
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, req->compound,
                        handle,
                        CHIMERA_VFS_ATTR_MODE,
                        chimera_nfs4_seek_typecheck_complete,
                        req);
} /* chimera_nfs4_seek_typecheck_open_callback */

void
chimera_nfs4_seek(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SEEK4args               *args  = &argop->opseek;
    struct SEEK4res                *res   = &resop->opseek;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* RFC 7862 §15.11.3: sa_what selects the content type to seek for and is
     * only defined for NFS4_CONTENT_DATA / NFS4_CONTENT_HOLE.  Reject anything
     * else with NFS4ERR_INVAL rather than passing it through to the backend. */
    if (args->sa_what != NFS4_CONTENT_DATA &&
        args->sa_what != NFS4_CONTENT_HOLE) {
        res->sa_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->sa_stateid);

    /*
     * RFC 8881 §8.2.3 requires SEEK to honor the special stateids.  These carry
     * no state-table entry, so open the current FH on the fly instead of
     * consulting the state table.
     */
    if (nfs4_stateid_is_special(&args->sa_stateid)) {
        if (req->fhlen == 0) {
            res->sa_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->sa_status);
            return;
        }

        /* A special-stateid read op must still honor share-reservation
         * deny-READ modes held by any owner of any client (RFC 8881
         * §9.7): SEEK reports the file's data/hole structure, so a
         * conflicting deny-READ open makes it NFS4ERR_LOCKED, exactly as
         * READ (and as DEALLOCATE does for deny-WRITE).  Without this the
         * offset check below runs first and reports NFS4ERR_NXIO for a
         * file the caller was never entitled to inspect. */
        nfsstat4 dstatus = nfs4_clients_check_io_denied(
            &thread->shared->nfs4_shared_clients,
            req->fh, req->fhlen, OPEN4_SHARE_ACCESS_READ);

        if (dstatus != NFS4_OK) {
            res->sa_status = dstatus;
            chimera_nfs4_compound_complete(req, res->sa_status);
            return;
        }

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred, req->compound,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                            CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_seek_typecheck_open_callback,
                            req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->sa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->sa_status = status;
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        state_handle = ((struct nfs_open_state *) state_void)->handle;
    } else {
        state_handle = ((struct nfs_lock_state *) state_void)->handle;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    chimera_vfs_seek(thread->vfs_thread, &req->cred, req->compound,
                     state_handle,
                     args->sa_offset,
                     args->sa_what,
                     chimera_nfs4_seek_complete,
                     req);
} /* chimera_nfs4_seek */
