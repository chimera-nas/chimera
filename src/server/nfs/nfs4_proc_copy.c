// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"

#define CHIMERA_NFS4_COPY_IO_SIZE (128 * 1024)
#define CHIMERA_NFS4_COPY_IOV_MAX 256

struct nfs4_copy_state_refs {
    void               *src_state;
    uint8_t             src_type;
    void               *dst_state;
    uint8_t             dst_type;
    struct nfs_request *req;
    uint64_t            src_offset;
    uint64_t            dst_offset;
    uint64_t            remaining;
    uint64_t            copied;
    uint32_t            rw_count;
    uint32_t            rw_eof;
    int                 rw_niov;
    struct evpl_iovec   rw_iov[CHIMERA_NFS4_COPY_IOV_MAX];
};

static struct chimera_vfs_open_handle *
chimera_nfs4_copy_state_handle(
    void   *state,
    uint8_t state_type)
{
    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        return ((struct nfs_open_state *) state)->handle;
    }
    return ((struct nfs_lock_state *) state)->handle;
} /* chimera_nfs4_copy_state_handle */

static void
chimera_nfs4_copy_release_refs(
    struct nfs_request          *req,
    struct nfs4_copy_state_refs *refs)
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
    free(refs);
} /* chimera_nfs4_copy_release_refs */

static void
chimera_nfs4_copy_finish(
    struct nfs_request          *req,
    struct nfs4_copy_state_refs *refs,
    enum chimera_vfs_error       error_code)
{
    struct COPY4res *res = &req->res_compound.resarray[req->index].opcopy;

    req->nfs_state_ref = NULL;

    if (error_code == CHIMERA_VFS_OK) {
        res->cr_status                                = NFS4_OK;
        res->cr_resok4.cr_response.num_wr_callback_id = 0;
        res->cr_resok4.cr_response.wr_callback_id     = NULL;
        res->cr_resok4.cr_response.wr_count           = refs->copied;
        res->cr_resok4.cr_response.wr_committed       = FILE_SYNC4;
        memcpy(res->cr_resok4.cr_response.wr_writeverf,
               &req->thread->shared->nfs_verifier,
               sizeof(res->cr_resok4.cr_response.wr_writeverf));
        res->cr_resok4.cr_requirements.cr_consecutive = false;
        res->cr_resok4.cr_requirements.cr_synchronous = true;
    } else {
        res->cr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_nfs4_copy_release_refs(req, refs);
    chimera_nfs4_compound_complete(req, res->cr_status);
} /* chimera_nfs4_copy_finish */

static void
chimera_nfs4_copy_rw_step(
    struct nfs4_copy_state_refs *refs);

static void
chimera_nfs4_copy_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs4_copy_state_refs *refs = private_data;
    struct nfs_request          *req  = refs->req;

    evpl_iovecs_release(req->thread->evpl, refs->rw_iov, refs->rw_niov);
    refs->rw_niov = 0;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_copy_finish(req, refs, error_code);
        return;
    }

    if (length != refs->rw_count) {
        chimera_nfs4_copy_finish(req, refs, CHIMERA_VFS_EIO);
        return;
    }

    refs->src_offset += length;
    refs->dst_offset += length;
    refs->copied     += length;
    if (refs->remaining != UINT64_MAX) {
        refs->remaining -= length;
    }

    if (refs->remaining == 0 || refs->rw_eof) {
        chimera_nfs4_copy_finish(req, refs, CHIMERA_VFS_OK);
        return;
    }

    chimera_nfs4_copy_rw_step(refs);
} /* chimera_nfs4_copy_write_complete */

static void
chimera_nfs4_copy_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs4_copy_state_refs    *refs = private_data;
    struct nfs_request             *req  = refs->req;
    struct chimera_vfs_open_handle *dst_handle;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_copy_finish(req, refs, error_code);
        return;
    }

    if (count == 0) {
        chimera_nfs4_copy_finish(req, refs, CHIMERA_VFS_OK);
        return;
    }

    dst_handle     = chimera_nfs4_copy_state_handle(refs->dst_state, refs->dst_type);
    refs->rw_count = count;
    refs->rw_eof   = eof;
    refs->rw_niov  = niov;

    chimera_vfs_write(req->thread->vfs_thread, &req->cred, NULL,
                      dst_handle,
                      refs->dst_offset,
                      count,
                      1,
                      0,
                      0,
                      refs->rw_iov,
                      refs->rw_niov,
                      chimera_nfs4_copy_write_complete,
                      refs);
} /* chimera_nfs4_copy_read_complete */

static void
chimera_nfs4_copy_rw_step(struct nfs4_copy_state_refs *refs)
{
    struct nfs_request             *req = refs->req;
    struct chimera_vfs_open_handle *src_handle;
    uint64_t                        chunk;

    chunk = CHIMERA_NFS4_COPY_IO_SIZE;
    if (refs->remaining < chunk) {
        chunk = refs->remaining;
    }

    src_handle    = chimera_nfs4_copy_state_handle(refs->src_state, refs->src_type);
    refs->rw_niov = CHIMERA_NFS4_COPY_IOV_MAX;

    chimera_vfs_read(req->thread->vfs_thread, &req->cred, NULL,
                     src_handle,
                     refs->src_offset,
                     (uint32_t) chunk,
                     refs->rw_iov,
                     refs->rw_niov,
                     0,
                     chimera_nfs4_copy_read_complete,
                     refs);
} /* chimera_nfs4_copy_rw_step */

static void
chimera_nfs4_copy_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request          *req  = private_data;
    struct COPY4res             *res  = &req->res_compound.resarray[req->index].opcopy;
    struct nfs4_copy_state_refs *refs = req->nfs_state_ref;

    req->nfs_state_ref = NULL;

    if (error_code == CHIMERA_VFS_OK) {
        refs->copied                                  = length;
        res->cr_status                                = NFS4_OK;
        res->cr_resok4.cr_response.num_wr_callback_id = 0;
        res->cr_resok4.cr_response.wr_callback_id     = NULL;
        res->cr_resok4.cr_response.wr_count           = length;
        res->cr_resok4.cr_response.wr_committed       = FILE_SYNC4;
        memcpy(res->cr_resok4.cr_response.wr_writeverf,
               &req->thread->shared->nfs_verifier,
               sizeof(res->cr_resok4.cr_response.wr_writeverf));
        res->cr_resok4.cr_requirements.cr_consecutive = false;
        res->cr_resok4.cr_requirements.cr_synchronous = true;
    } else {
        res->cr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_nfs4_copy_release_refs(req, refs);
    chimera_nfs4_compound_complete(req, res->cr_status);
} /* chimera_nfs4_copy_complete */

void
chimera_nfs4_copy(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct COPY4args               *args  = &argop->opcopy;
    struct COPY4res                *res   = &resop->opcopy;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    struct nfs4_copy_state_refs    *refs;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;
    nfsstat4                        status;

    if (req->saved_fhlen == 0 || req->fhlen == 0) {
        res->cr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->cr_status);
        return;
    }

    chimera_nfs4_resolve_current_stateid(req, &args->ca_src_stateid);
    chimera_nfs4_resolve_current_stateid(req, &args->ca_dst_stateid);

    refs = calloc(1, sizeof(*refs));
    chimera_nfs_abort_if(refs == NULL, "copy state refs OOM");

    status = nfs_state_table_acquire(table, &args->ca_src_stateid, 0,
                                     &refs->src_state, &refs->src_type);
    if (status != NFS4_OK) {
        free(refs);
        res->cr_status = status;
        chimera_nfs4_compound_complete(req, res->cr_status);
        return;
    }

    status = nfs_state_table_acquire(table, &args->ca_dst_stateid, 0,
                                     &refs->dst_state, &refs->dst_type);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, refs->src_state, refs->src_type,
                                thread->vfs_thread);
        free(refs);
        res->cr_status = status;
        chimera_nfs4_compound_complete(req, res->cr_status);
        return;
    }

    src_handle = chimera_nfs4_copy_state_handle(refs->src_state, refs->src_type);
    dst_handle = chimera_nfs4_copy_state_handle(refs->dst_state, refs->dst_type);

    refs->req        = req;
    refs->src_offset = args->ca_src_offset;
    refs->dst_offset = args->ca_dst_offset;
    refs->remaining  = args->ca_count ? args->ca_count : UINT64_MAX;

    req->nfs_state_ref = refs;

    if (src_handle->vfs_module == dst_handle->vfs_module &&
        (dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE)) {
        chimera_vfs_copy_range(thread->vfs_thread, &req->cred,
                               src_handle,
                               refs->src_offset,
                               dst_handle,
                               refs->dst_offset,
                               refs->remaining,
                               0,
                               0,
                               chimera_nfs4_copy_complete,
                               req);
    } else {
        chimera_nfs4_copy_rw_step(refs);
    }
} /* chimera_nfs4_copy */
