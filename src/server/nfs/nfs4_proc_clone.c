// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"

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

static struct chimera_vfs_open_handle *
chimera_nfs4_clone_state_handle(
    void   *state,
    uint8_t state_type)
{
    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        return ((struct nfs_open_state *) state)->handle;
    }
    return ((struct nfs_lock_state *) state)->handle;
} /* chimera_nfs4_clone_state_handle */

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
    struct CLONE4args              *args = &req->args_compound->argarray[req->index].opclone;
    struct nfs4_clone_state_refs   *refs = req->nfs_state_ref;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;

    src_handle = chimera_nfs4_clone_state_handle(refs->src_state, refs->src_type);
    dst_handle = chimera_nfs4_clone_state_handle(refs->dst_state, refs->dst_type);

    /* Reflink requires both files be served by the same module and that module
     * support clone_range; otherwise it is not a supported operation. */
    if (src_handle->vfs_module != dst_handle->vfs_module ||
        !(dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE)) {
        chimera_nfs4_clone_finish(req, NFS4ERR_NOTSUPP);
        return;
    }

    chimera_vfs_clone_range(req->thread->vfs_thread, &req->cred,
                            src_handle, args->cl_src_offset,
                            dst_handle, args->cl_dst_offset,
                            length,
                            0, 0,
                            chimera_nfs4_clone_complete,
                            req);
} /* chimera_nfs4_clone_issue */

static void
chimera_nfs4_clone_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct CLONE4args  *args = &req->args_compound->argarray[req->index].opclone;
    uint64_t            src_size;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_clone_finish(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    src_size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;

    /* cl_count == 0: clone from cl_src_offset to the end of the source.  If the
     * offset is at or past EOF there is nothing to clone. */
    if (src_size <= args->cl_src_offset) {
        chimera_nfs4_clone_finish(req, NFS4_OK);
        return;
    }

    chimera_nfs4_clone_issue(req, src_size - args->cl_src_offset);
} /* chimera_nfs4_clone_getattr_complete */

void
chimera_nfs4_clone(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CLONE4args              *args  = &argop->opclone;
    struct CLONE4res               *res   = &resop->opclone;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    struct nfs4_clone_state_refs   *refs;
    struct chimera_vfs_open_handle *src_handle;
    nfsstat4                        status;

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

    if (args->cl_count != 0) {
        chimera_nfs4_clone_issue(req, args->cl_count);
        return;
    }

    /* Resolve the source size to bound a clone-to-EOF. */
    src_handle = chimera_nfs4_clone_state_handle(refs->src_state, refs->src_type);
    chimera_vfs_getattr(thread->vfs_thread, &req->cred,
                        src_handle,
                        CHIMERA_VFS_ATTR_SIZE,
                        chimera_nfs4_clone_getattr_complete,
                        req);
} /* chimera_nfs4_clone */
