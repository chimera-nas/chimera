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

/*
 * WRITE_SAME (RFC 7862 15.13) writes an Application Data Block pattern -- a
 * small pattern replicated across a run of fixed-size blocks -- so the client
 * initializes a large region with a tiny request.  Chimera projects the
 * expansion into the backend (chimera_vfs_write_same); backends without
 * CAP_WRITE_SAME surface NFS4ERR_NOTSUPP.
 *
 * Phase 1 does not stamp per-block numbers: an ADB requesting block-number
 * stamping (adb_reloff_blocknum != NFS4_UINT64_MAX) is rejected with
 * NFS4ERR_UNION_NOTSUPP (the unsupported arm of the operation).
 */

static void
chimera_nfs4_write_same_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  count,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request    *req = private_data;
    struct WRITE_SAME4res *res = &req->res_compound.resarray[req->index].opwrite_same;

    if (error_code == CHIMERA_VFS_OK) {
        res->wsr_status                = NFS4_OK;
        res->resok4.num_wr_callback_id = 0;
        res->resok4.wr_callback_id     = NULL;
        res->resok4.wr_count           = count;
        res->resok4.wr_committed       = sync;
        memcpy(res->resok4.wr_writeverf,
               &req->thread->shared->nfs_verifier,
               sizeof(res->resok4.wr_writeverf));
    } else {
        res->wsr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    } else if (req->handle) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs4_compound_complete(req, res->wsr_status);
} /* chimera_nfs4_write_same_complete */

static void
chimera_nfs4_write_same_issue(
    struct nfs_request             *req,
    struct chimera_vfs_open_handle *handle)
{
    struct WRITE_SAME4args *args = &req->args_compound->argarray[req->index].opwrite_same;

    chimera_vfs_write_same(req->thread->vfs_thread, &req->cred,
                           handle,
                           args->wsa_adb.adb_offset,
                           (uint32_t) args->wsa_adb.adb_block_size,
                           args->wsa_adb.adb_block_count,
                           args->wsa_adb.adb_pattern.data,
                           args->wsa_adb.adb_pattern.len,
                           (uint32_t) args->wsa_adb.adb_reloff_pattern,
                           args->wsa_stable,
                           0, 0,
                           chimera_nfs4_write_same_complete,
                           req);
} /* chimera_nfs4_write_same_issue */

static void
chimera_nfs4_write_same_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request    *req = private_data;
    struct WRITE_SAME4res *res = &req->res_compound.resarray[req->index].opwrite_same;

    if (error_code != CHIMERA_VFS_OK) {
        res->wsr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    req->handle = handle;
    chimera_nfs4_write_same_issue(req, handle);
} /* chimera_nfs4_write_same_open_callback */

void
chimera_nfs4_write_same(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct WRITE_SAME4args         *args  = &argop->opwrite_same;
    struct WRITE_SAME4res          *res   = &resop->opwrite_same;
    struct app_data_block4         *adb   = &args->wsa_adb;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    struct nfs_open_state          *open_state;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* Reject per-block-number stamping (Phase 1): the union arm is unsupported. */
    if (adb->adb_reloff_blocknum != NFS4_UINT64_MAX) {
        res->wsr_status = NFS4ERR_UNION_NOTSUPP;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    /* Validate the ADB geometry.  block_size is length4 (64-bit) but a sane
     * block fits in 32 bits; oversize or zero is invalid. */
    if (adb->adb_block_size == 0 || adb->adb_block_size > UINT32_MAX ||
        adb->adb_reloff_pattern > adb->adb_block_size ||
        adb->adb_reloff_pattern + adb->adb_pattern.len > adb->adb_block_size) {
        res->wsr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    /* Guard against block_size * block_count overflowing 64 bits. */
    if (adb->adb_block_count != 0 &&
        adb->adb_block_size > NFS4_UINT64_MAX / adb->adb_block_count) {
        res->wsr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    /* NFS4.1 current-stateid substitution (RFC 8881 16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->wsa_stateid);

    /* Special/anonymous stateids carry no state-table entry: open the current
     * FH on the fly.  The backend enforces the regular-file requirement. */
    if (nfs4_stateid_is_special(&args->wsa_stateid)) {
        if (req->fhlen == 0) {
            res->wsr_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->wsr_status);
            return;
        }

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->fh, req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED,
                            chimera_nfs4_write_same_open_callback,
                            req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->wsa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->wsr_status = status;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    status = nfs_state_check_client(
        state_void, state_type,
        req->session ? req->session->client_unified : NULL);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->wsr_status = status;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    /* A write-delegation stateid carries no open handle; open the FH on the fly. */
    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        if (req->fhlen == 0) {
            res->wsr_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->wsr_status);
            return;
        }
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            req->fh, req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED,
                            chimera_nfs4_write_same_open_callback,
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

    if ((open_state->share_access & OPEN4_SHARE_ACCESS_WRITE) == 0) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->wsr_status = NFS4ERR_OPENMODE;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    status = nfs_open_state_check_io_denied(open_state,
                                            OPEN4_SHARE_ACCESS_WRITE);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->wsr_status = status;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    if (!nfs_open_state_check_principal(open_state,
                                        req->principal_flavor,
                                        req->principal_machinename,
                                        req->principal_machinename_len)) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->wsr_status = NFS4ERR_ACCESS;
        chimera_nfs4_compound_complete(req, res->wsr_status);
        return;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    chimera_nfs4_write_same_issue(req, state_handle);
} /* chimera_nfs4_write_same */
