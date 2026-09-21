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
 * OPEN_CURRENT(data), SEEK.  A stateid SEEK is one op against the handle the
 * state already holds, so the two shapes differ only in what precedes the SEEK,
 * which is the last op in both.
 */
#define NFS4_SEEK_OP_TYPE 2

/*
 * The current filehandle of a special-stateid SEEK is not guaranteed to be a
 * regular file: nothing has OPENed it, so no earlier op rejected the type.
 * RFC 7862 §15.11.3 lists NFS4ERR_ISDIR for SEEK on a directory; without this
 * the offset check below runs first and reports NFS4ERR_NXIO instead.
 *
 * The VFS error the gate sets is only a stop signal; the completion re-derives
 * the NFSv4 status from the mode this op reported.
 */
static void
chimera_nfs4_seek_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *top;

    if (index != NFS4_SEEK_OP_TYPE || *status != CHIMERA_VFS_OK) {
        return;
    }

    top = chimera_vfs_compound_op(compound, index);

    if ((top->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(top->attr.va_mode)) {
        *status = CHIMERA_VFS_EINVAL;
    }
} /* chimera_nfs4_seek_gate */

static void
chimera_nfs4_seek_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct SEEK4res                      *res = &req->res_compound.resarray[req->index].opseek;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                error_code;
    uint32_t                              nops;

    error_code = chimera_vfs_compound_status(compound);
    nops       = chimera_vfs_compound_num_ops(compound);

    if (error_code == CHIMERA_VFS_OK) {
        op                    = chimera_vfs_compound_op(compound, nops - 1);
        res->sa_status        = NFS4_OK;
        res->resok4.sr_eof    = op->seek_eof;
        res->resok4.sr_offset = op->seek_offset;
    } else if (nops > 1 + NFS4_SEEK_OP_TYPE &&
               (op = chimera_vfs_compound_op(compound, NFS4_SEEK_OP_TYPE)) &&
               op->status != CHIMERA_VFS_OK &&
               (op->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
               !S_ISREG(op->attr.va_mode)) {
        res->sa_status = chimera_nfs4_sparse_nonreg_status(op->attr.va_mode);
    } else {
        res->sa_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_compound_free(compound);

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    }

    chimera_nfs4_compound_complete(req, res->sa_status);
} /* chimera_nfs4_seek_complete */

void
chimera_nfs4_seek(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SEEK4args               *args  = &argop->opseek;
    struct SEEK4res                *res   = &resop->opseek;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    struct chimera_vfs_compound    *compound;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* RFC 7862 §15.11.3: sa_what selects the content type to seek for and is
     * only defined for NFS4_CONTENT_DATA / NFS4_CONTENT_HOLE.  Reject anything
     * else with NFS4ERR_INVAL rather than passing it through to the backend. */
    if (args->sa_what != NFS4_CONTENT_DATA &&
        args->sa_what != NFS4_CONTENT_HOLE) {
        res->sa_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->sa_stateid);

    /*
     * RFC 8881 §8.2.3 requires SEEK to honor the special stateids.  These carry
     * no state-table entry, so open the current FH on the fly instead of
     * consulting the state table.
     */
    if (nfs4_stateid_is_special(&args->sa_stateid)) {
        if (req->fhlen == 0) {
            res->sa_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->sa_status);
            return;
        }

        /* A special-stateid read op must still honor share-reservation
         * deny-READ modes held by any owner of any client (RFC 8881
         * §9.7): SEEK reports the file's data/hole structure, so a
         * conflicting deny-READ open makes it NFS4ERR_LOCKED, exactly as
         * READ (and as DEALLOCATE does for deny-WRITE).  Without this the
         * offset check below runs first and reports NFS4ERR_NXIO for a
         * file the caller was never entitled to inspect. */
        nfsstat4 dstatus = nfs4_clients_check_io_denied(
            &thread->shared->nfs4_shared_clients,
            req->fh, req->fhlen, OPEN4_SHARE_ACCESS_READ);

        if (dstatus != NFS4_OK) {
            res->sa_status = dstatus;
            chimera_nfs4_compound_complete(req, res->sa_status);
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
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_READ_ONLY, 0);
        chimera_vfs_compound_add_seek(compound, NULL, args->sa_offset,
                                      args->sa_what);

        chimera_vfs_compound_set_gate(compound, chimera_nfs4_seek_gate, req);

        chimera_vfs_compound_submit(compound, chimera_nfs4_seek_complete, req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->sa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->sa_status = status;
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    state_handle = nfs_state_io_handle(state_void, state_type, OPEN4_SHARE_ACCESS_READ);

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    /* The stateid names the object, so the run is the SEEK alone against the
     * handle the state holds -- lent, and released with the state below.  No
     * type check: the OPEN that minted the stateid established the type. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_seek(compound, state_handle, args->sa_offset,
                                  args->sa_what);

    chimera_vfs_compound_submit(compound, chimera_nfs4_seek_complete, req);
} /* chimera_nfs4_seek */
