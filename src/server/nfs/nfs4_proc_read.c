// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "server/server.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include <sys/stat.h>

static inline int
chimera_nfs4_stateid_is_anonymous(const struct stateid4 *sid)
{
    static const uint8_t zero[12] = { 0 };

    return sid->seqid == 0 && memcmp(sid->other, zero, sizeof(zero)) == 0;
} /* chimera_nfs4_stateid_is_anonymous */

static void
chimera_nfs4_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct READ4res    *res = &req->res_compound.resarray[req->index].opread;

    if (error_code == CHIMERA_VFS_OK) {
        res->status             = NFS4_OK;
        res->resok4.eof         = eof;
        res->resok4.data.length = count;
        res->resok4.data.iov    = iov;
        res->resok4.data.niov   = niov;
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        evpl_iovecs_release(req->thread->evpl, iov, niov);
    }

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    } else if (req->handle) {
        /* Anonymous stateid: release the on-the-fly handle we opened. */
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs4_compound_complete(req, res->status);

} /* chimera_nfs4_read_complete */

static void
chimera_nfs4_read_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct READ4args   *args = &req->args_compound->argarray[req->index].opread;
    struct READ4res    *res  = &req->res_compound.resarray[req->index].opread;
    struct evpl_iovec  *iov;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;

    iov = xdr_dbuf_alloc_space(sizeof(*iov) * 256, req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    chimera_vfs_read(req->thread->vfs_thread, &req->cred,
                     handle,
                     args->offset,
                     args->count,
                     iov,
                     256,
                     0,
                     chimera_nfs4_read_complete,
                     req);
} /* chimera_nfs4_read_open_callback */

static void
chimera_nfs4_read_typecheck_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct READ4res    *res = &req->res_compound.resarray[req->index].opread;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(attr->va_mode)) {
        res->status = S_ISDIR(attr->va_mode) ? NFS4ERR_ISDIR : NFS4ERR_INVAL;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    req->handle = NULL;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_READ_ONLY,
                        chimera_nfs4_read_open_callback,
                        req);
} /* chimera_nfs4_read_typecheck_complete */

static void
chimera_nfs4_read_typecheck_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req = private_data;
    struct READ4res    *res = &req->res_compound.resarray[req->index].opread;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_MODE,
                        chimera_nfs4_read_typecheck_complete,
                        req);
} /* chimera_nfs4_read_typecheck_open_callback */

void
chimera_nfs4_read(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READ4args               *args  = &argop->opread;
    struct READ4res                *res   = &resop->opread;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    struct nfs_open_state          *open_state;
    struct evpl_iovec              *iov;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /*
     * RFC 7530 9.1.4.3 / RFC 8881 8.2.3 require READ to honor the
     * anonymous stateid (all zeros).  Open the current FH on the fly
     * instead of consulting the state table.
     */
    /* A pNFS data server serves READ by file handle without consulting its
     * (empty) state table; the MDS authorizes the I/O via the layout. */
    if (chimera_nfs4_stateid_is_anonymous(&args->stateid) ||
        chimera_server_config_get_nfs_data_server(thread->shared->config)) {
        if (req->fhlen == 0) {
            res->status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        if (req->session && req->session->client_unified) {
            status = nfs_client_check_io_denied(req->session->client_unified,
                                                NULL,
                                                req->fh,
                                                req->fhlen,
                                                OPEN4_SHARE_ACCESS_READ);
            if (status != NFS4_OK) {
                res->status = status;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
        }

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_read_typecheck_open_callback,
                            req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* A delegation stateid authorizes the READ but carries no open handle
     * (only open/lock states do).  Drop the ref and open the current FH on
     * the fly, as for the anonymous stateid. */
    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        if (req->fhlen == 0) {
            res->status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_read_typecheck_open_callback,
                            req);
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        open_state   = state_void;
        state_handle = open_state->handle;
    } else {
        open_state   = ((struct nfs_lock_state *) state_void)->open_state;
        state_handle = ((struct nfs_lock_state *) state_void)->handle;
    }

    status = nfs_open_state_check_io_denied(open_state,
                                            OPEN4_SHARE_ACCESS_READ);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    iov = xdr_dbuf_alloc_space(sizeof(*iov) * 256, req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    chimera_vfs_read(thread->vfs_thread, &req->cred,
                     state_handle,
                     args->offset,
                     args->count,
                     iov,
                     256,
                     0,
                     chimera_nfs4_read_complete,
                     req);
} /* chimera_nfs4_read */
