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
#include "evpl/evpl.h"
#include "common/evpl_iovec_cursor.h"
#include <sys/stat.h>

/*
 * READ_PLUS (RFC 7862 15.10) is a sparse-aware READ: the server may report the
 * file as a sequence of DATA and HOLE segments so a client reading a sparse
 * file transfers no bytes for the holes.  Chimera projects the work into the
 * backend (chimera_vfs_read_plus), which classifies the leading byte-run at the
 * requested offset as a single DATA or HOLE segment.  Returning one segment per
 * call is RFC-legal (rpr_contents is an array; the client re-issues from the
 * last byte returned).  Backends without CAP_READ_PLUS surface NFS4ERR_NOTSUPP,
 * and the client falls back to plain READ.
 */

/* Release the acquired state ref or on-the-fly handle, then complete. */
static void
chimera_nfs4_read_plus_finish(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct READ_PLUS4res *res = &req->res_compound.resarray[req->index].opread_plus;

    res->rp_status = status;

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
        req->handle        = NULL; /* borrowed from the state; not ours to free */
    } else if (req->handle) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_read_plus_finish */

/* Completion of the data read for a DATA segment: flatten the bytes into the
 * (copying) data4 opaque and emit one NFS4_CONTENT_DATA element. */
static void
chimera_nfs4_read_plus_data_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request       *req  = private_data;
    struct READ_PLUS4args    *args = &req->args_compound->argarray[req->index].opread_plus;
    struct READ_PLUS4res     *res  = &req->res_compound.resarray[req->index].opread_plus;
    struct read_plus_content *content;

    if (error_code != CHIMERA_VFS_OK) {
        evpl_iovecs_release(req->thread->evpl, iov, niov);
        chimera_nfs4_read_plus_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    res->rp_resok4.rpr_eof = eof;

    if (count == 0) {
        res->rp_resok4.num_rpr_contents = 0;
        res->rp_resok4.rpr_contents     = NULL;
        evpl_iovecs_release(req->thread->evpl, iov, niov);
        chimera_nfs4_read_plus_finish(req, NFS4_OK);
        return;
    }

    content = xdr_dbuf_alloc_space(sizeof(*content), req->encoding->dbuf);
    chimera_nfs_abort_if(content == NULL, "Failed to allocate space");

    content->rpc_content       = NFS4_CONTENT_DATA;
    content->rpc_data.d_offset = args->rpa_offset;

    /* data4.d_data is a plain (copying) opaque, so flatten the read iovecs into
     * a contiguous dbuf-backed buffer for the marshaller. */
    chimera_nfs_abort_if(
        xdr_dbuf_alloc_opaque(&content->rpc_data.d_data, count,
                              req->encoding->dbuf) != 0,
        "Failed to allocate space");
    content->rpc_data.d_data.len = count;

    struct evpl_iovec_cursor cursor;
    evpl_iovec_cursor_init(&cursor, iov, niov);
    evpl_iovec_cursor_copy(&cursor, content->rpc_data.d_data.data, count);

    evpl_iovecs_release(req->thread->evpl, iov, niov);

    res->rp_resok4.num_rpr_contents = 1;
    res->rp_resok4.rpr_contents     = content;

    chimera_nfs4_read_plus_finish(req, NFS4_OK);
} /* chimera_nfs4_read_plus_data_complete */

/* Completion of the backend segment classification.  For a DATA run, fetch the
 * bytes with a normal read; for a HOLE run, emit a hole descriptor; at EOF,
 * emit an empty content array. */
static void
chimera_nfs4_read_plus_classify_complete(
    enum chimera_vfs_error error_code,
    uint32_t               is_data,
    uint64_t               length,
    uint32_t               eof,
    void                  *private_data)
{
    struct nfs_request       *req  = private_data;
    struct READ_PLUS4args    *args = &req->args_compound->argarray[req->index].opread_plus;
    struct READ_PLUS4res     *res  = &req->res_compound.resarray[req->index].opread_plus;
    struct read_plus_content *content;
    struct evpl_iovec        *iov;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_read_plus_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    res->rp_resok4.rpr_eof = eof;

    if (is_data && length > 0) {
        /* Fetch the data run via the normal read path (handles buffer and
         * thread ownership); the response is built in the read completion. */
        iov = xdr_dbuf_alloc_space(sizeof(*iov) * 256, req->encoding->dbuf);
        chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

        chimera_vfs_read(req->thread->vfs_thread, &req->cred,
                         req->handle,
                         args->rpa_offset,
                         (uint32_t) length,
                         iov, 256, 0,
                         chimera_nfs4_read_plus_data_complete,
                         req);
        return;
    }

    if (!is_data && length > 0) {
        content = xdr_dbuf_alloc_space(sizeof(*content), req->encoding->dbuf);
        chimera_nfs_abort_if(content == NULL, "Failed to allocate space");

        content->rpc_content        = NFS4_CONTENT_HOLE;
        content->rpc_hole.di_offset = args->rpa_offset;
        content->rpc_hole.di_length = length;

        res->rp_resok4.num_rpr_contents = 1;
        res->rp_resok4.rpr_contents     = content;
    } else {
        /* At/past EOF or empty range: no content segments. */
        res->rp_resok4.num_rpr_contents = 0;
        res->rp_resok4.rpr_contents     = NULL;
    }

    chimera_nfs4_read_plus_finish(req, NFS4_OK);
} /* chimera_nfs4_read_plus_classify_complete */

static void
chimera_nfs4_read_plus_issue(
    struct nfs_request             *req,
    struct chimera_vfs_open_handle *handle)
{
    struct READ_PLUS4args *args = &req->args_compound->argarray[req->index].opread_plus;

    /* Stash the read target so the classify completion can fetch DATA bytes.
     * For a state-based handle this is borrowed (nfs_state_ref owns release);
     * for an on-the-fly open it is already req->handle. */
    req->handle = handle;

    chimera_vfs_read_plus(req->thread->vfs_thread, &req->cred,
                          handle,
                          args->rpa_offset,
                          args->rpa_count,
                          chimera_nfs4_read_plus_classify_complete,
                          req);
} /* chimera_nfs4_read_plus_issue */

static void
chimera_nfs4_read_plus_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request   *req = private_data;
    struct READ_PLUS4res *res = &req->res_compound.resarray[req->index].opread_plus;

    if (error_code != CHIMERA_VFS_OK) {
        res->rp_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    req->handle = handle;
    chimera_nfs4_read_plus_issue(req, handle);
} /* chimera_nfs4_read_plus_open_callback */

static void
chimera_nfs4_read_plus_typecheck_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request   *req = private_data;
    struct READ_PLUS4res *res = &req->res_compound.resarray[req->index].opread_plus;

    if (error_code != CHIMERA_VFS_OK) {
        res->rp_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(attr->va_mode)) {
        res->rp_status = S_ISDIR(attr->va_mode) ? NFS4ERR_ISDIR : NFS4ERR_INVAL;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    req->handle = NULL;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_READ_ONLY,
                        chimera_nfs4_read_plus_open_callback,
                        req);
} /* chimera_nfs4_read_plus_typecheck_complete */

static void
chimera_nfs4_read_plus_typecheck_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request   *req = private_data;
    struct READ_PLUS4res *res = &req->res_compound.resarray[req->index].opread_plus;

    if (error_code != CHIMERA_VFS_OK) {
        res->rp_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    req->handle = handle;
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_MODE,
                        chimera_nfs4_read_plus_typecheck_complete,
                        req);
} /* chimera_nfs4_read_plus_typecheck_open_callback */

void
chimera_nfs4_read_plus(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READ_PLUS4args          *args  = &argop->opread_plus;
    struct READ_PLUS4res           *res   = &resop->opread_plus;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    struct nfs_open_state          *open_state;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* NFS4.1 current-stateid substitution (RFC 8881 16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->rpa_stateid);

    /* Special/anonymous stateids (and a pNFS data server) carry no state-table
     * entry: open the current FH on the fly, mirroring READ. */
    if (nfs4_stateid_is_special(&args->rpa_stateid) ||
        chimera_server_config_get_nfs_data_server(thread->shared->config)) {
        if (req->fhlen == 0) {
            res->rp_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->rp_status);
            return;
        }

        if (req->session && req->session->client_unified) {
            status = nfs_client_check_io_denied(req->session->client_unified,
                                                NULL, req->fh, req->fhlen,
                                                OPEN4_SHARE_ACCESS_READ);
            if (status != NFS4_OK) {
                res->rp_status = status;
                chimera_nfs4_compound_complete(req, res->rp_status);
                return;
            }
        }

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->fh, req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_read_plus_typecheck_open_callback,
                            req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->rpa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->rp_status = status;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    status = nfs_state_check_client(
        state_void, state_type,
        req->session ? req->session->client_unified : NULL);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->rp_status = status;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    /* A delegation stateid authorizes the READ but carries no open handle;
     * drop the ref and open the current FH on the fly, as for READ. */
    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        if (req->fhlen == 0) {
            res->rp_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->rp_status);
            return;
        }
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->fh, req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_read_plus_typecheck_open_callback,
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
        res->rp_status = status;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    if (!nfs_open_state_check_principal(open_state,
                                        req->principal_flavor,
                                        req->principal_machinename,
                                        req->principal_machinename_len)) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->rp_status = NFS4ERR_ACCESS;
        chimera_nfs4_compound_complete(req, res->rp_status);
        return;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    chimera_nfs4_read_plus_issue(req, state_handle);
} /* chimera_nfs4_read_plus */
