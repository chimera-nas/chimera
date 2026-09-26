// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "vfs/vfs_compound.h"

/*
 * The special-stateid run: PUTFH, OPEN_CURRENT(meta), GETATTR(MODE),
 * OPEN_CURRENT(data), DEALLOCATE.  A stateid DEALLOCATE is one op against the
 * handle the state already holds, so the two shapes differ only in what
 * precedes the punch, which is the last op in both.
 */
#define NFS4_DEALLOCATE_OP_TYPE 2

/*
 * The current filehandle of a special-stateid DEALLOCATE is not guaranteed to
 * be a regular file: nothing has OPENed it, so no earlier op rejected the type.
 * RFC 7862 §11.9: DEALLOCATE operates on a regular file, so a directory cfh is
 * NFS4ERR_ISDIR rather than a backend-level success.
 *
 * The VFS error the gate sets is only a stop signal; the completion re-derives
 * the NFSv4 status from the mode this op reported.
 */
static void
chimera_nfs4_deallocate_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *top;

    if (index != NFS4_DEALLOCATE_OP_TYPE || *status != CHIMERA_VFS_OK) {
        return;
    }

    top = chimera_vfs_compound_op(compound, index);

    if ((top->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(top->attr.va_mode)) {
        *status = CHIMERA_VFS_EINVAL;
    }
} /* chimera_nfs4_deallocate_gate */

static void
chimera_nfs4_deallocate_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct DEALLOCATE4res                *res = &req->res_compound.resarray[req->index].opdeallocate;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                error_code;
    uint32_t                              nops;

    error_code = chimera_vfs_compound_status(compound);
    nops       = chimera_vfs_compound_num_ops(compound);

    if (error_code == CHIMERA_VFS_OK) {
        res->dr_status = NFS4_OK;
    } else if (nops > 1 + NFS4_DEALLOCATE_OP_TYPE &&
               (op = chimera_vfs_compound_op(compound,
                                             NFS4_DEALLOCATE_OP_TYPE)) &&
               op->status != CHIMERA_VFS_OK &&
               (op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
               !S_ISREG(op->attr.va_mode)) {
        res->dr_status = chimera_nfs4_sparse_nonreg_status(op->attr.va_mode);
    } else {
        res->dr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_compound_free(compound);

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    }

    chimera_nfs4_compound_complete(req, res->dr_status);
} /* chimera_nfs4_deallocate_complete */

void
chimera_nfs4_deallocate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct DEALLOCATE4args         *args  = &argop->opdeallocate;
    struct DEALLOCATE4res          *res   = &resop->opdeallocate;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    struct chimera_vfs_compound    *compound;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->da_stateid);

    /* Every path below acts on the current filehandle: the special-stateid
     * path opens it directly, and the state-table path must name it. */
    if (req->fhlen == 0) {
        res->dr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->dr_status);
        return;
    }

    /* RFC 7862 §15.2 punches [da_offset, da_offset + da_length); a range whose
     * end does not fit in a uint64 names no such interval, so reject it rather
     * than letting the wrapped end reach a backend. */
    if (args->da_length &&
        args->da_offset > UINT64_MAX - args->da_length) {
        res->dr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->dr_status);
        return;
    }

    /*
     * RFC 8881 §8.2.3 requires DEALLOCATE to honor the special stateids.  These
     * carry no state-table entry, so open the current FH on the fly instead of
     * consulting the state table.
     */
    if (nfs4_stateid_is_special(&args->da_stateid)) {
        /* A special-stateid write op must still honor share-reservation
         * deny-WRITE modes held by any owner of any client (RFC 8881
         * §9.7): DEALLOCATE modifies file data, so a conflicting
         * deny-WRITE open makes it NFS4ERR_LOCKED, exactly as WRITE. */
        nfsstat4 dstatus = nfs4_clients_check_io_denied(
            &thread->shared->nfs4_shared_clients,
            req->fh, req->fhlen, OPEN4_SHARE_ACCESS_WRITE);

        if (dstatus != NFS4_OK) {
            res->dr_status = dstatus;
            chimera_nfs4_compound_complete(req, res->dr_status);
            return;
        }
        compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

        chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH |
                                              CHIMERA_VFS_OPEN_NOFOLLOW, 0);
        chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MODE);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED, 0);
        chimera_vfs_compound_add_allocate(compound, NULL,
                                          args->da_offset, args->da_length,
                                          CHIMERA_VFS_ALLOCATE_DEALLOCATE,
                                          0, 0);

        chimera_vfs_compound_set_gate(compound, chimera_nfs4_deallocate_gate,
                                      req);

        chimera_vfs_compound_submit(compound,
                                    chimera_nfs4_deallocate_complete, req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->da_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->dr_status = status;
        chimera_nfs4_compound_complete(req, res->dr_status);
        return;
    }

    status = nfs_state_check_write_for_fh(state_void, state_type,
                                          req->fh, req->fhlen);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->dr_status = status;
        chimera_nfs4_compound_complete(req, res->dr_status);
        return;
    }

    state_handle = nfs_state_io_handle(state_void, state_type, OPEN4_SHARE_ACCESS_WRITE);

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    /* The stateid names the object, so the run is the punch alone against the
     * handle the state holds -- lent, and released with the state below.  No
     * type check: the OPEN that minted the stateid established the type. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_allocate(compound, state_handle,
                                      args->da_offset, args->da_length,
                                      CHIMERA_VFS_ALLOCATE_DEALLOCATE, 0, 0);

    chimera_vfs_compound_submit(compound, chimera_nfs4_deallocate_complete,
                                req);
} /* chimera_nfs4_deallocate */
