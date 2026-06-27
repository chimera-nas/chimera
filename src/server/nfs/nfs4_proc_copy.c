// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "nfs4_stateid.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_procs.h"

#define CHIMERA_NFS4_COPY_IO_SIZE (128 * 1024)
#define CHIMERA_NFS4_COPY_IOV_MAX 256

struct nfs4_copy_state_refs {
    void                           *src_state;
    uint8_t                         src_type;
    void                           *dst_state;
    uint8_t                         dst_type;
    /* Handles opened by COPY itself for a special stateid, which names no
     * state-table entry; released with the refs. */
    struct chimera_vfs_open_handle *src_own;
    struct chimera_vfs_open_handle *dst_own;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;
    uint8_t                         src_special;
    uint8_t                         dst_special;
    uint64_t                        req_count; /* ca_count as sent; 0 means "to EOF" */
    struct nfs_request             *req;
    uint64_t                        src_offset;
    uint64_t                        dst_offset;
    uint64_t                        remaining;
    uint64_t                        copied;
    uint32_t                        rw_count;
    uint32_t                        rw_eof;
    int                             rw_niov;
    struct evpl_iovec               rw_iov[CHIMERA_NFS4_COPY_IOV_MAX];
};

/* Returns NULL for any state type that carries no open handle -- a delegation
 * or layout stateid must not be reinterpreted as one of the two that do. */
static struct chimera_vfs_open_handle *
chimera_nfs4_copy_state_handle(
    void   *state,
    uint8_t state_type)
{
    if (!state) {
        return NULL;
    }
    switch (state_type) {
        case NFS4_SLOT_TYPE_OPEN:
            return ((struct nfs_open_state *) state)->handle;
        case NFS4_SLOT_TYPE_LOCK:
            return ((struct nfs_lock_state *) state)->handle;
        default:
            return NULL;
    } /* switch */
} /* chimera_nfs4_copy_state_handle */

/* The client that owns `state`, for attributing the copy's internal I/O.
 * Mirrors chimera_nfs4_copy_state_handle: only the two state types that carry
 * an open handle carry an owning client. */
static uint64_t
chimera_nfs4_copy_state_client(
    void   *state,
    uint8_t state_type)
{
    switch (state_type) {
        case NFS4_SLOT_TYPE_OPEN:
            return ((struct nfs_open_state *) state)->owner->client->client_id;
        case NFS4_SLOT_TYPE_LOCK:
            return ((struct nfs_lock_state *) state)->open_state->owner->
                   client->client_id;
        default:
            return 0;
    } /* switch */
} /* chimera_nfs4_copy_state_client */

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
    if (refs->src_own) {
        chimera_vfs_release(req->thread->vfs_thread, refs->src_own);
    }
    if (refs->dst_own) {
        chimera_vfs_release(req->thread->vfs_thread, refs->dst_own);
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

    /* As the read above: the destination's own holder must not be blocked by
     * its own share reservation. */
    struct chimera_claim_actor io_owner = {
        .owner          = {
            .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
            .client_key = chimera_nfs4_copy_state_client(refs->dst_state,
                                                         refs->dst_type),
            .owner_lo = dst_handle->fh_hash,
            .owner_hi = 0,
        },
    };

    chimera_vfs_write_owned(req->thread->vfs_thread, &req->cred, NULL,
                            dst_handle,
                            refs->dst_offset,
                            count,
                            1,
                            0,
                            0,
                            refs->rw_iov,
                            refs->rw_niov,
                            &io_owner,
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

    /* Attribute the read to the client that holds the source stateid, as
     * READ does.  Left unowned it is admitted as the per-file implicit claim,
     * which carries no client identity -- so a copy whose source the same
     * client has open with a deny share is refused by that client's own share
     * reservation (NFS4ERR_ACCESS). */
    struct chimera_claim_actor io_owner = {
        .owner          = {
            .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
            .client_key = chimera_nfs4_copy_state_client(refs->src_state,
                                                         refs->src_type),
            .owner_lo = src_handle->fh_hash,
            .owner_hi = 0,
        },
    };

    chimera_vfs_read_owned(req->thread->vfs_thread, &req->cred, NULL,
                           src_handle,
                           refs->src_offset,
                           (uint32_t) chunk,
                           refs->rw_iov,
                           refs->rw_niov,
                           0,
                           &io_owner,
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

static void
chimera_nfs4_copy_fail(
    struct nfs4_copy_state_refs *refs,
    nfsstat4                     status);

static void
chimera_nfs4_copy_start(
    struct nfs4_copy_state_refs *refs);

/* The source range must lie inside the source file: RFC 7862 §15.2.3 makes a
 * COPY whose ca_src_offset + ca_count runs past the end of the source
 * NFS4ERR_INVAL rather than a short copy.  ca_count == 0 means "to EOF", so
 * only an offset past the end is out of range there.  The size is not known
 * until the handle is open, hence the extra round trip. */
static void
chimera_nfs4_copy_srcattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs4_copy_state_refs *refs = private_data;
    struct nfs_request          *req  = refs->req;
    nfsstat4                     status;
    uint64_t                     size;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_copy_fail(refs,
                               chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    size = (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? attr->va_size : 0;

    if (refs->req_count == 0) {
        if (refs->src_offset > size) {
            chimera_nfs4_copy_fail(refs, NFS4ERR_INVAL);
            return;
        }
        refs->remaining = size - refs->src_offset;
    } else {
        if (refs->src_offset > size ||
            refs->req_count > size - refs->src_offset) {
            chimera_nfs4_copy_fail(refs, NFS4ERR_INVAL);
            return;
        }
        refs->remaining = refs->req_count;
    }

    /* Share reservations are checked here, after the range: an out-of-range
     * COPY is an argument error the server can answer without consulting any
     * state, so it reports NFS4ERR_INVAL rather than masking it behind a
     * conflict.  A special stateid names no open, so the deny modes held by
     * any owner of any client still have to be honored explicitly -- exactly
     * as READ, SEEK and DEALLOCATE do -- or the anonymous stateid would be a
     * way around them. */
    if (refs->src_special) {
        status = nfs4_clients_check_io_denied(
            &req->thread->shared->nfs4_shared_clients,
            req->saved_fh, req->saved_fhlen, OPEN4_SHARE_ACCESS_READ);
        if (status != NFS4_OK) {
            chimera_nfs4_copy_fail(refs, status);
            return;
        }
    }

    if (refs->dst_special) {
        status = nfs4_clients_check_io_denied(
            &req->thread->shared->nfs4_shared_clients,
            req->fh, req->fhlen, OPEN4_SHARE_ACCESS_WRITE);
        if (status != NFS4_OK) {
            chimera_nfs4_copy_fail(refs, status);
            return;
        }
    }

    chimera_nfs4_copy_start(refs);
} /* chimera_nfs4_copy_srcattr_complete */

/* Start the transfer once both endpoints have a usable handle. */
static void
chimera_nfs4_copy_begin(struct nfs4_copy_state_refs *refs)
{
    struct nfs_request             *req  = refs->req;
    struct COPY4args               *args =
        &req->args_compound->argarray[req->index].opcopy;
    struct COPY4res                *res =
        &req->res_compound.resarray[req->index].opcopy;
    struct chimera_vfs_open_handle *src_handle;
    struct chimera_vfs_open_handle *dst_handle;

    src_handle = refs->src_own
        ? refs->src_own
        : chimera_nfs4_copy_state_handle(refs->src_state, refs->src_type);
    dst_handle = refs->dst_own
        ? refs->dst_own
        : chimera_nfs4_copy_state_handle(refs->dst_state, refs->dst_type);

    if (!src_handle || !dst_handle) {
        chimera_nfs4_copy_release_refs(req, refs);
        res->cr_status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, res->cr_status);
        return;
    }

    refs->src_offset = args->ca_src_offset;
    refs->dst_offset = args->ca_dst_offset;
    refs->req_count  = args->ca_count;
    refs->src_handle = src_handle;
    refs->dst_handle = dst_handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, NULL,
                        src_handle,
                        CHIMERA_VFS_ATTR_SIZE,
                        chimera_nfs4_copy_srcattr_complete,
                        refs);
} /* chimera_nfs4_copy_begin */

static void
chimera_nfs4_copy_start(struct nfs4_copy_state_refs *refs)
{
    struct nfs_request             *req        = refs->req;
    struct chimera_vfs_open_handle *src_handle = refs->src_handle;
    struct chimera_vfs_open_handle *dst_handle = refs->dst_handle;

    req->nfs_state_ref = refs;

    if (src_handle->vfs_module == dst_handle->vfs_module &&
        (dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE)) {
        chimera_vfs_copy_range(req->thread->vfs_thread, &req->cred,
                               src_handle,
                               refs->src_offset,
                               dst_handle,
                               refs->dst_offset,
                               refs->remaining,
                               0,
                               0,
                               0,
                               chimera_nfs4_copy_complete,
                               req);
    } else {
        chimera_nfs4_copy_rw_step(refs);
    }
} /* chimera_nfs4_copy_start */

static void
chimera_nfs4_copy_fail(
    struct nfs4_copy_state_refs *refs,
    nfsstat4                     status)
{
    struct nfs_request *req = refs->req;
    struct COPY4res    *res = &req->res_compound.resarray[req->index].opcopy;

    chimera_nfs4_copy_release_refs(req, refs);
    res->cr_status = status;
    chimera_nfs4_compound_complete(req, res->cr_status);
} /* chimera_nfs4_copy_fail */

static void
chimera_nfs4_copy_dst_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs4_copy_state_refs *refs = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_copy_fail(refs,
                               chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    refs->dst_own = handle;
    chimera_nfs4_copy_begin(refs);
} /* chimera_nfs4_copy_dst_open_complete */

static void
chimera_nfs4_copy_open_dst(struct nfs4_copy_state_refs *refs)
{
    struct nfs_request *req = refs->req;

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_copy_dst_open_complete,
                        refs);
} /* chimera_nfs4_copy_open_dst */

static void
chimera_nfs4_copy_src_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs4_copy_state_refs *refs = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_copy_fail(refs,
                               chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    refs->src_own = handle;

    if (refs->dst_special) {
        chimera_nfs4_copy_open_dst(refs);
    } else {
        chimera_nfs4_copy_begin(refs);
    }
} /* chimera_nfs4_copy_src_open_complete */

void
chimera_nfs4_copy(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct COPY4args            *args  = &argop->opcopy;
    struct COPY4res             *res   = &resop->opcopy;
    struct nfs_state_table      *table = &thread->shared->nfs4_state_table;
    struct nfs4_copy_state_refs *refs;
    nfsstat4                     status;

    if (req->saved_fhlen == 0 || req->fhlen == 0) {
        res->cr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->cr_status);
        return;
    }

    /* RFC 7862 §15.2.3: COPY moves data between two files.  Naming one file
     * as both source and destination is NFS4ERR_INVAL whatever the ranges --
     * the overlap qualifier belongs to CLONE (§15.13.3), not here.  The Linux
     * client knows it: nfs4_copy_file_range() refuses when the two inodes
     * match rather than send a COPY the server has to reject. */
    if (req->saved_fhlen == req->fhlen &&
        memcmp(req->saved_fh, req->fh, req->fhlen) == 0) {
        res->cr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->cr_status);
        return;
    }

    chimera_nfs4_resolve_current_stateid(req, &args->ca_src_stateid);
    chimera_nfs4_resolve_current_stateid(req, &args->ca_dst_stateid);

    refs = calloc(1, sizeof(*refs));
    chimera_nfs_abort_if(refs == NULL, "copy state refs OOM");

    refs->req         = req;
    refs->src_special = nfs4_stateid_is_special(&args->ca_src_stateid);
    refs->dst_special = nfs4_stateid_is_special(&args->ca_dst_stateid);

    /* RFC 7862 §15.2.3 asks only that ca_src_stateid be READ-valid and
     * ca_dst_stateid WRITE-valid, and RFC 8881 §8.2.3 makes the anonymous
     * and READ-bypass stateids exactly that.  Neither names a state-table
     * entry, so open the filehandle the compound supplied -- the saved FH
     * for the source, the current FH for the destination -- the same way
     * READ, SEEK and DEALLOCATE do, rather than rejecting the operation.
     *
     * The share-reservation checks those ops perform apply here too: a
     * special-stateid COPY reads the source and writes the destination, so
     * a conflicting deny held by any owner of any client makes it
     * NFS4ERR_LOCKED. */
    if (!refs->src_special) {
        status = nfs_state_table_acquire(table, &args->ca_src_stateid, 0,
                                         &refs->src_state, &refs->src_type);
        if (status != NFS4_OK) {
            chimera_nfs4_copy_fail(refs, status);
            return;
        }
    }

    if (!refs->dst_special) {
        status = nfs_state_table_acquire(table, &args->ca_dst_stateid, 0,
                                         &refs->dst_state, &refs->dst_type);
        if (status != NFS4_OK) {
            chimera_nfs4_copy_fail(refs, status);
            return;
        }

        /* The destination stateid must name a write-capable open of the
        * current filehandle (the saved filehandle is the source -- RFC 7862
        * §15.2).  Without this the write would target whatever handle the
        * stateid carries, rather than the object the compound named. */
        status = nfs_state_check_write_for_fh(refs->dst_state, refs->dst_type,
                                              req->fh, req->fhlen);
        if (status != NFS4_OK) {
            chimera_nfs4_copy_fail(refs, status);
            return;
        }
    }

    if (refs->src_special) {
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                            req->saved_fh,
                            req->saved_fhlen,
                            CHIMERA_VFS_OPEN_INFERRED |
                            CHIMERA_VFS_OPEN_READ_ONLY,
                            chimera_nfs4_copy_src_open_complete,
                            refs);
    } else if (refs->dst_special) {
        chimera_nfs4_copy_open_dst(refs);
    } else {
        chimera_nfs4_copy_begin(refs);
    }
} /* chimera_nfs4_copy */
