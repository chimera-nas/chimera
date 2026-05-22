// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"

struct nfs4_copy_state_refs {
    void   *src_state;
    uint8_t src_type;
    void   *dst_state;
    uint8_t dst_type;
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

    req->nfs_state_ref = refs;

    chimera_vfs_copy_range(thread->vfs_thread, &req->cred,
                           src_handle,
                           args->ca_src_offset,
                           dst_handle,
                           args->ca_dst_offset,
                           args->ca_count ? args->ca_count : UINT64_MAX,
                           0,
                           0,
                           chimera_nfs4_copy_complete,
                           req);
} /* chimera_nfs4_copy */
