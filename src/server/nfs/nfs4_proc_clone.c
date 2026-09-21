// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "vfs/vfs_compound.h"

/*
 * CLONE (RFC 7862 15.13) reflinks a byte range from the SAVED_FH (source) into
 * the CURRENT_FH (destination): the ranges share physical storage copy-on-write.
 * Chimera projects this onto the backend's native reflink (chimera_vfs_clone_range,
 * CAP_CLONE_RANGE).  A cross-module clone, or a backend without reflink support,
 * yields NFS4ERR_NOTSUPP.  cl_count == 0 means "to the end of the source file".
 */

struct nfs4_clone_state_refs {
    void               *src_state;
    uint8_t             src_type;
    void               *dst_state;
    uint8_t             dst_type;
    struct nfs_request *req;
};

static void
chimera_nfs4_clone_finish(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct CLONE4res             *res   = &req->res_compound.resarray[req->index].opclone;
    struct nfs4_clone_state_refs *refs  = req->nfs_state_ref;
    struct nfs_state_table       *table = &req->thread->shared->nfs4_state_table;

    req->nfs_state_ref = NULL;

    if (refs) {
        if (refs->src_state) {
            nfs_state_table_release(table, refs->src_state, refs->src_type,
                                    req->thread->vfs_thread);
        }
        if (refs->dst_state) {
            nfs_state_table_release(table, refs->dst_state, refs->dst_type,
                                    req->thread->vfs_thread);
        }
        free(refs);
    }

    res->cl_status = status;
    chimera_nfs4_compound_complete(req, status);
} /* chimera_nfs4_clone_finish */

/*
 * CLONE is one run: the reflink, and -- when cl_count is 0 -- a stat of the
 * source in front of it, because "to the end of the source file" is a length
 * the request does not carry.  That is the gate's edit: it reads the size the
 * stat reported and writes the length into the CLONE ahead of it, or runs past
 * the CLONE entirely when the offset is already at or past EOF and there is
 * nothing to clone.  Assigned from the caller's own offset plus what the stat
 * reported, so a second execution computes the same length.
 *
 * Both objects are named by stateid, so both handles are the caller's and
 * neither addresses the current object -- which is what the range ops take.
 */
#define NFS4_CLONE_OP_GETATTR 0
#define NFS4_CLONE_OP_CLONE   1

static void
chimera_nfs4_clone_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs_request                   *req  = private_data;
    struct CLONE4args                    *args = &req->args_compound->argarray[req->index].opclone;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_compound_op       *edit;
    uint64_t                              src_size;

    if (index != NFS4_CLONE_OP_GETATTR || *status != CHIMERA_VFS_OK) {
        return;
    }

    op   = chimera_vfs_compound_op(compound, index);
    edit = chimera_vfs_compound_op_edit(compound, NFS4_CLONE_OP_CLONE);

    if (!edit) {
        return;
    }

    src_size = (op->attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ?
        op->attr.va_size : 0;

    if (src_size <= args->cl_src_offset) {
        edit->skip = 1;
    } else {
        edit->length = src_size - args->cl_src_offset;
    }
} /* chimera_nfs4_clone_gate */

static void
chimera_nfs4_clone_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request    *req = private_data;
    enum chimera_vfs_error error_code;

    error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);

    chimera_nfs4_clone_finish(req, error_code == CHIMERA_VFS_OK ?
                              NFS4_OK : chimera_nfs4_errno_to_nfsstat4(error_code));
} /* chimera_nfs4_clone_complete */

static void
chimera_nfs4_clone_issue(
    struct nfs_request *req,
    uint64_t            length,
    int                 want_size)
{
    struct CLONE4args              *args = &req->args_compound->argarray[req->index].opclone;
    struct nfs4_clone_state_refs   *refs = req->nfs_state_ref;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;
    struct chimera_vfs_compound    *compound;
    int                             idx;

    src_handle = nfs_state_io_handle(refs->src_state, refs->src_type, OPEN4_SHARE_ACCESS_READ);
    dst_handle = nfs_state_io_handle(refs->dst_state, refs->dst_type, OPEN4_SHARE_ACCESS_WRITE);

    /* Reflink requires both files be served by the same module and that module
     * support clone_range; otherwise it is not a supported operation. */
    if (src_handle->vfs_module != dst_handle->vfs_module ||
        !(dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE)) {
        chimera_nfs4_clone_finish(req, NFS4ERR_NOTSUPP);
        return;
    }

    compound = chimera_vfs_compound_alloc(req->thread->vfs_thread, &req->cred);

    if (want_size) {
        /* Resolve the source size to bound a clone-to-EOF.  The stat addresses
         * the source handle, not the current object. */
        idx = chimera_vfs_compound_add_getattr(compound,
                                               CHIMERA_VFS_ATTR_SIZE);
        chimera_vfs_compound_op_set_handle(compound, (uint32_t) idx,
                                           src_handle);
        chimera_vfs_compound_set_gate(compound, chimera_nfs4_clone_gate, req);
    }

    chimera_vfs_compound_add_clone_range(compound,
                                         src_handle, args->cl_src_offset,
                                         dst_handle, args->cl_dst_offset,
                                         length, 0, 0);

    chimera_vfs_compound_submit(compound, chimera_nfs4_clone_complete, req);
} /* chimera_nfs4_clone_issue */

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

    refs = calloc(1, sizeof(*refs));
    chimera_nfs_abort_if(refs == NULL, "clone state refs OOM");
    refs->req = req;

    status = nfs_state_table_acquire(table, &args->cl_src_stateid, 0,
                                     &refs->src_state, &refs->src_type);
    if (status != NFS4_OK) {
        free(refs);
        res->cl_status = status;
        chimera_nfs4_compound_complete(req, res->cl_status);
        return;
    }

    status = nfs_state_table_acquire(table, &args->cl_dst_stateid, 0,
                                     &refs->dst_state, &refs->dst_type);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, refs->src_state, refs->src_type,
                                thread->vfs_thread);
        free(refs);
        res->cl_status = status;
        chimera_nfs4_compound_complete(req, res->cl_status);
        return;
    }

    req->nfs_state_ref = refs;

    /* cl_count == 0 means "to the end of the source file", which the run has to
     * measure for itself. */
    chimera_nfs4_clone_issue(req, args->cl_count, args->cl_count == 0);
} /* chimera_nfs4_clone */
