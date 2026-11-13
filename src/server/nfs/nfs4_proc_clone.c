// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_stateid.h"
#include "nfs4_session.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_procs.h"

/*
 * CLONE (RFC 7862 15.13) reflinks a byte range from the SAVED_FH (source) into
 * the CURRENT_FH (destination): the ranges share physical storage copy-on-write.
 * Chimera projects this onto the backend's native reflink (chimera_vfs_clone_range,
 * CAP_CLONE_RANGE).  A cross-module clone, or a backend without reflink support,
 * yields NFS4ERR_NOTSUPP.  cl_count == 0 means "to the end of the source file".
 *
 * Unlike COPY, CLONE resolves both stateids as concrete open/lock/delegation
 * state: it takes no special-stateid shortcut, so an anonymous or
 * READ-bypass selector is NFS4ERR_BAD_STATEID.  A delegation stateid still
 * satisfies the requirement (carrying no open handle of its own) and is
 * opened on the fly, the same way COPY handles a delegation endpoint.
 */

struct nfs4_clone_state_refs {
    void                           *src_state;
    uint8_t                         src_type;
    void                           *dst_state;
    uint8_t                         dst_type;
    /* Handles opened by CLONE itself for a delegation stateid, which names no
     * openable state-table entry; released with the refs. */
    struct chimera_vfs_open_handle *src_own;
    struct chimera_vfs_open_handle *dst_own;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;
    uint8_t                         src_deleg;
    uint8_t                         dst_deleg;
    struct nfs_request             *req;
};

static void
chimera_nfs4_clone_release_refs(
    struct nfs_request           *req,
    struct nfs4_clone_state_refs *refs)
{
    struct nfs_state_table *table = &req->thread->shared->nfs4_state_table;

    if (refs->src_state) {
        nfs_state_table_release(table, refs->src_state, refs->src_type,
                                req->thread->vfs_thread);
    }
    if (refs->dst_state) {
        nfs_state_table_release(table, refs->dst_state, refs->dst_type,
                                req->thread->vfs_thread);
    }
    if (refs->src_own) {
        chimera_vfs_release(req->thread->vfs_thread, refs->src_own);
    }
    if (refs->dst_own) {
        chimera_vfs_release(req->thread->vfs_thread, refs->dst_own);
    }
    free(refs);
} /* chimera_nfs4_clone_release_refs */

static void
chimera_nfs4_clone_finish(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct CLONE4res             *res  = &req->res_compound.resarray[req->index].opclone;
    struct nfs4_clone_state_refs *refs = req->nfs_state_ref;

    req->nfs_state_ref = NULL;

    if (refs) {
        chimera_nfs4_clone_release_refs(req, refs);
    }

    res->cl_status = status;
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_clone_finish */

static void
chimera_nfs4_clone_fail(
    struct nfs4_clone_state_refs *refs,
    nfsstat4                      status)
{
    struct nfs_request *req = refs->req;
    struct CLONE4res   *res = &req->res_compound.resarray[req->index].opclone;

    req->nfs_state_ref = NULL;
    chimera_nfs4_clone_release_refs(req, refs);
    res->cl_status = status;
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_clone_fail */

static void
chimera_nfs4_clone_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    chimera_nfs4_clone_finish(req, error_code == CHIMERA_VFS_OK ?
                              NFS4_OK : chimera_nfs4_errno_to_nfsstat4(error_code));
} /* chimera_nfs4_clone_complete */

static void
chimera_nfs4_clone_issue(
    struct nfs_request *req,
    uint64_t            length)
{
    struct CLONE4args            *args = &req->args_compound->argarray[req->index].opclone;
    struct nfs4_clone_state_refs *refs = req->nfs_state_ref;

    /* Reflink requires both files be served by the same module and that module
     * support clone_range; otherwise it is not a supported operation. */
    if (refs->src_handle->vfs_module != refs->dst_handle->vfs_module ||
        !(refs->dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE)) {
        chimera_nfs4_clone_finish(req, NFS4ERR_NOTSUPP);
        return;
    }

    chimera_vfs_clone_range(req->thread->vfs_thread, &req->cred,
                            refs->src_handle, args->cl_src_offset,
                            refs->dst_handle, args->cl_dst_offset,
                            length,
                            0, 0,
                            chimera_nfs4_clone_complete,
                            req);
} /* chimera_nfs4_clone_issue */

/* RFC 7862 §15.13.3: if the source offset plus count is greater than the
 * size of the source file, the operation MUST fail with NFS4ERR_INVAL.
 * cl_count == 0 means "to EOF" (quint: cnt = max(0, size - srcOff)), so an
 * offset at or past EOF in that case is simply nothing to clone -- a no-op
 * success, not an error.  The size is not known until the handle is open,
 * hence the extra round trip -- same shape as COPY's bounds check. */
static void
chimera_nfs4_clone_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct CLONE4args  *args = &req->args_compound->argarray[req->index].opclone;
    uint64_t            src_size, length;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_clone_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    src_size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;

    if (args->cl_count == 0) {
        /* Clone from cl_src_offset to the end of the source.  If the offset
         * is at or past EOF there is nothing to clone. */
        length = (args->cl_src_offset < src_size) ? (src_size - args->cl_src_offset) : 0;
        if (length == 0) {
            chimera_nfs4_clone_finish(req, NFS4_OK);
            return;
        }
    } else {
        if (args->cl_src_offset > src_size ||
            args->cl_count > src_size - args->cl_src_offset) {
            chimera_nfs4_clone_finish(req, NFS4ERR_INVAL);
            return;
        }
        length = args->cl_count;
    }

    /* If SAVED_FH and CURRENT_FH name the same file, the source and target
     * ranges must not overlap. */
    if (req->saved_fhlen == req->fhlen &&
        memcmp(req->saved_fh, req->fh, req->fhlen) == 0 &&
        args->cl_src_offset < args->cl_dst_offset + length &&
        args->cl_dst_offset < args->cl_src_offset + length) {
        chimera_nfs4_clone_finish(req, NFS4ERR_INVAL);
        return;
    }

    chimera_nfs4_clone_issue(req, length);
} /* chimera_nfs4_clone_getattr_complete */

/* Start the transfer once both endpoints have a usable handle. */
static void
chimera_nfs4_clone_begin(struct nfs_request *req)
{
    struct nfs4_clone_state_refs   *refs = req->nfs_state_ref;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;

    src_handle = refs->src_own
        ? refs->src_own
        : nfs_state_io_handle(refs->src_state, refs->src_type, OPEN4_SHARE_ACCESS_READ);
    dst_handle = refs->dst_own
        ? refs->dst_own
        : nfs_state_io_handle(refs->dst_state, refs->dst_type, OPEN4_SHARE_ACCESS_WRITE);

    if (!src_handle || !dst_handle) {
        chimera_nfs4_clone_finish(req, NFS4ERR_BAD_STATEID);
        return;
    }

    refs->src_handle = src_handle;
    refs->dst_handle = dst_handle;

    /* Always resolve the source size: it bounds a clone-to-EOF and validates
     * an explicit count against the source file's actual size. */
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        src_handle,
                        CHIMERA_VFS_ATTR_SIZE,
                        chimera_nfs4_clone_getattr_complete,
                        req);
} /* chimera_nfs4_clone_begin */

static void
chimera_nfs4_clone_dst_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request           *req  = private_data;
    struct nfs4_clone_state_refs *refs = req->nfs_state_ref;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_clone_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    refs->dst_own = handle;
    chimera_nfs4_clone_begin(req);
} /* chimera_nfs4_clone_dst_open_complete */

static void
chimera_nfs4_clone_open_dst(struct nfs_request *req)
{
    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_clone_dst_open_complete,
                        req);
} /* chimera_nfs4_clone_open_dst */

static void
chimera_nfs4_clone_src_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request           *req  = private_data;
    struct nfs4_clone_state_refs *refs = req->nfs_state_ref;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_clone_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    refs->src_own = handle;

    if (refs->dst_deleg) {
        chimera_nfs4_clone_open_dst(req);
    } else {
        chimera_nfs4_clone_begin(req);
    }
} /* chimera_nfs4_clone_src_open_complete */

static void
chimera_nfs4_clone_open_src(struct nfs_request *req)
{
    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->saved_fh,
                        req->saved_fhlen,
                        CHIMERA_VFS_OPEN_INFERRED |
                        CHIMERA_VFS_OPEN_READ_ONLY,
                        chimera_nfs4_clone_src_open_complete,
                        req);
} /* chimera_nfs4_clone_open_src */

void
chimera_nfs4_clone(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CLONE4args            *args  = &argop->opclone;
    struct CLONE4res             *res   = &resop->opclone;
    struct nfs_state_table       *table = &thread->shared->nfs4_state_table;
    struct nfs4_clone_state_refs *refs;
    nfsstat4                      status;

    req->nfs_state_ref = NULL;

    if (req->saved_fhlen == 0 || req->fhlen == 0) {
        res->cl_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->cl_status);
        return;
    }

    chimera_nfs4_resolve_current_stateid(req, &args->cl_src_stateid);
    chimera_nfs4_resolve_current_stateid(req, &args->cl_dst_stateid);

    /* Unlike COPY, CLONE takes no special-stateid shortcut: an anonymous or
     * READ-bypass selector is rejected outright with NFS4ERR_BAD_STATEID
     * (quint cloneSidGate). */
    if (nfs4_stateid_is_special(&args->cl_src_stateid) ||
        nfs4_stateid_is_special(&args->cl_dst_stateid)) {
        res->cl_status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, res->cl_status);
        return;
    }

    refs = calloc(1, sizeof(*refs));
    chimera_nfs_abort_if(refs == NULL, "clone state refs OOM");
    refs->req = req;

    req->nfs_state_ref = refs;

    status = nfs_state_table_acquire(table, &args->cl_src_stateid, 0,
                                     &refs->src_state, &refs->src_type);
    status = nfs4_stateid_acquire_status(status, req->minorversion);
    if (status != NFS4_OK) {
        chimera_nfs4_clone_fail(refs, status);
        return;
    }

    status = nfs_state_check_client(
        refs->src_state, refs->src_type,
        req->session ? req->session->client_unified : NULL);
    if (status != NFS4_OK) {
        chimera_nfs4_clone_fail(refs, status);
        return;
    }

    if (refs->src_type == NFS4_SLOT_TYPE_DELEG) {
        status = nfs_delegation_check_io_for_fh(refs->src_state,
                                                req->saved_fh,
                                                req->saved_fhlen,
                                                OPEN4_SHARE_ACCESS_READ);
        refs->src_deleg = 1;
    } else {
        status = nfs_state_check_read_for_fh(refs->src_state,
                                             refs->src_type,
                                             req->saved_fh,
                                             req->saved_fhlen);
    }
    if (status != NFS4_OK) {
        chimera_nfs4_clone_fail(refs, status);
        return;
    }

    status = nfs_state_table_acquire(table, &args->cl_dst_stateid, 0,
                                     &refs->dst_state, &refs->dst_type);
    status = nfs4_stateid_acquire_status(status, req->minorversion);
    if (status != NFS4_OK) {
        chimera_nfs4_clone_fail(refs, status);
        return;
    }

    status = nfs_state_check_client(
        refs->dst_state, refs->dst_type,
        req->session ? req->session->client_unified : NULL);
    if (status != NFS4_OK) {
        chimera_nfs4_clone_fail(refs, status);
        return;
    }

    if (refs->dst_type == NFS4_SLOT_TYPE_DELEG) {
        status = nfs_delegation_check_io_for_fh(refs->dst_state,
                                                req->fh, req->fhlen,
                                                OPEN4_SHARE_ACCESS_WRITE);
        refs->dst_deleg = 1;
    } else {
        status = nfs_state_check_write_for_fh(refs->dst_state,
                                              refs->dst_type,
                                              req->fh, req->fhlen);
    }
    if (status != NFS4_OK) {
        chimera_nfs4_clone_fail(refs, status);
        return;
    }

    if (refs->src_deleg) {
        chimera_nfs4_clone_open_src(req);
    } else if (refs->dst_deleg) {
        chimera_nfs4_clone_open_dst(req);
    } else {
        chimera_nfs4_clone_begin(req);
    }
} /* chimera_nfs4_clone */
