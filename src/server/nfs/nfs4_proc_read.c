// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "server/server.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_compound.h"
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

/*
 * How many iovecs one READ's answer may arrive in; the VFS-compound path
 * reserves the same number.
 */
#define NFS4_READ_MAX_IOV 256

/*
 * PUTFH, OPEN_CURRENT, READ for a stateid that carries no handle, and the READ
 * alone against the handle an open or lock stateid holds.  The READ is the last
 * op either way, and its data iovecs are TAKEN from the run: they are the
 * reply's payload and have to outlive the sequence that produced them.
 */
static void
chimera_nfs4_read_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct READ4res                      *res = &req->res_compound.resarray[req->index].opread;
    const struct chimera_vfs_compound_op *rop;
    struct evpl_iovec                    *iov  = NULL;
    int                                   niov = 0;
    enum chimera_vfs_error                error_code;
    uint32_t                              idx;

    error_code = chimera_vfs_compound_status(compound);

    idx = chimera_vfs_compound_num_ops(compound) - 1;
    rop = chimera_vfs_compound_op(compound, idx);

    if (error_code == CHIMERA_VFS_OK) {
        chimera_vfs_compound_take_iov(compound, idx, &iov, &niov);

        res->status             = NFS4_OK;
        res->resok4.eof         = rop->eof_read;
        res->resok4.data.length = rop->read_len;
        res->resok4.data.iov    = iov;
        res->resok4.data.niov   = niov;
    } else {
        /* Nothing to release: a READ that did not run took no references, and
         * one that ran and was then failed by a later op has its data freed
         * with the sequence. */
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_compound_free(compound);

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    }

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_read_complete */

/*
 * The shape for a stateid with no handle of its own -- an anonymous or special
 * one, a pNFS data-server read, or a delegation stateid.  The object is the
 * current file handle and the sequence opens it, with the same REGULAR_ONLY
 * data open the per-op path used, so a non-regular object is refused by the
 * open rather than by the backend's read.
 *
 * `io_owner` says whose I/O it is when a delegation authorizes it: without it
 * the claim layer arbitrates this client's read against this client's own
 * delegation, denies it, and recalls the delegation the read is being done
 * under.  owner_lo is chimera_vfs_hash of the FILE HANDLE and nothing else, so
 * it is computable here, before anything is open.
 */
static void
chimera_nfs4_read_by_fh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct chimera_claim_actor *io_owner)
{
    struct READ4args            *args = &req->args_compound->argarray[req->index].opread;
    struct chimera_vfs_compound *compound;
    struct evpl_iovec           *iov;

    iov = xdr_dbuf_alloc_space(sizeof(*iov) * NFS4_READ_MAX_IOV,
                               req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_READ_ONLY |
                                          CHIMERA_VFS_OPEN_NOFOLLOW |
                                          CHIMERA_VFS_OPEN_REGULAR_ONLY, 0);
    chimera_vfs_compound_add_read(compound, NULL,
                                  args->offset, args->count,
                                  iov, NFS4_READ_MAX_IOV,
                                  0, io_owner, NULL, 0);

    chimera_vfs_compound_submit(compound, chimera_nfs4_read_complete, req);
} /* chimera_nfs4_read_by_fh */

void
chimera_nfs4_read(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READ4args               *args  = &argop->opread;
    struct READ4res                *res   = &resop->opread;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    struct nfs_open_state          *open_state;
    struct nfs_lock_state          *lock_state;
    uint32_t                        current_seqid;
    nfsstat4                        status;

    req->nfs_state_ref       = NULL;
    req->handle              = NULL;
    req->io_owner_from_deleg = false;

    /* RFC 7530 §9.6.2: READ is refused with NFS4ERR_GRACE while the
     * server-reboot grace window is open, because a byte-range lock that
     * would conflict with it may not have been reclaimed yet. */
    status = nfs_recovery_io_check(&thread->shared->nfs4_recovery);
    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    /*
     * NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2).
     */
    chimera_nfs4_resolve_current_stateid(req, &args->stateid);

    /*
     * RFC 7530 9.1.4.3 / RFC 8881 8.2.3 require READ to honor special
     * stateids.  Open the current FH on the fly
     * instead of consulting the state table.
     */
    /* A pNFS data server serves READ by file handle without consulting its
     * (empty) state table; the MDS authorizes the I/O via the layout. */
    if (nfs4_stateid_is_special(&args->stateid) ||
        chimera_server_config_get_nfs_data_server(thread->shared->config)) {
        if (req->fhlen == 0) {
            res->status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        /* RFC 7530 §9.1.4.3 / RFC 8881 §9.7: special-stateid READ against
         * a deny-READ share reservation held by ANY owner of ANY client is
         * NFS4ERR_LOCKED.  Honoring the all-ones READ-bypass stateid is a
         * MAY (RFC 8881 §8.2.3) chimera does not exercise: the unified VFS
         * lease machinery enforces deny modes below this layer anyway, so
         * the bypass is treated as anonymous and gets the RFC errno here. */
        if (!chimera_server_config_get_nfs_data_server(
                thread->shared->config)) {
            status = nfs4_clients_check_io_denied(
                &thread->shared->nfs4_shared_clients,
                req->fh,
                req->fhlen,
                OPEN4_SHARE_ACCESS_READ);
            if (status != NFS4_OK) {
                res->status = status;
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
        }

        chimera_nfs4_read_by_fh(thread, req, NULL);
        return;
    }

    status = nfs_state_table_acquire(table, &args->stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = nfs_state_check_client(
        state_void, state_type,
        req->session ? req->session->client_unified : NULL);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* A delegation stateid authorizes the READ but carries no open handle
     * (only open/lock states do).  Drop the ref and open the current FH on
     * the fly, as for the anonymous stateid. */
    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        if (req->fhlen == 0) {
            res->status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
        /* This read is authorized by the client's own delegation: carry the
         * holder's lease owner on the on-the-fly read so it does not recall the
         * client's own delegation. */
        req->io_owner_from_deleg = (req->session &&
                                    req->session->client_unified);

        if (req->io_owner_from_deleg) {
            struct chimera_claim_actor io_owner = {
                .owner          = {
                    .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
                    .client_key = req->session->client_unified->client_id,
                    .owner_lo   = chimera_vfs_hash(req->fh,               req->fhlen),
                    .owner_hi   = 0,
                },
            };

            chimera_nfs4_read_by_fh(thread, req, &io_owner);
        } else {
            chimera_nfs4_read_by_fh(thread, req, NULL);
        }
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        open_state    = state_void;
        current_seqid = open_state->seqid;
    } else {
        lock_state    = state_void;
        open_state    = lock_state->open_state;
        current_seqid = lock_state->seqid;
    }
    state_handle = nfs_state_io_handle(state_void, state_type, OPEN4_SHARE_ACCESS_READ);

    if (req->minorversion == 0) {
        status = nfs4_stateid_check_seqid(current_seqid, args->stateid.seqid);
        if (status != NFS4_OK) {
            nfs_state_table_release(table, state_void, state_type,
                                    thread->vfs_thread);
            res->status = status;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
    }

    /* RFC 7530 §9.1.4 / RFC 8881 §9.1.2: I/O through an open (or lock) stateid
     * is limited to the associated open's granted access mode; a READ needs
     * OPEN4_SHARE_ACCESS_READ (WRITE enforces the symmetric check). */
    if ((open_state->share_access & OPEN4_SHARE_ACCESS_READ) == 0) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->status = NFS4ERR_OPENMODE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = nfs_open_state_check_io_denied(open_state,
                                            OPEN4_SHARE_ACCESS_READ);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (!nfs_open_state_check_principal(open_state,
                                        req->principal_flavor,
                                        req->principal_machinename,
                                        req->principal_machinename_len)) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->status = NFS4ERR_ACCESS;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    struct chimera_claim_actor   io_owner = {
        .owner          = {
            .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
            .client_key = open_state->owner->client->client_id,
            .owner_lo   = state_handle->fh_hash,
            .owner_hi   = 0,
        },
    };
    struct evpl_iovec           *iov = xdr_dbuf_alloc_space(sizeof(*iov) *
                                                            NFS4_READ_MAX_IOV,
                                                            req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    /* The stateid names the object, so the run is the READ alone against the
     * handle the state holds -- lent, and released with the state above. */
    struct chimera_vfs_compound *compound =
        chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_read(compound, state_handle,
                                  args->offset, args->count,
                                  iov, NFS4_READ_MAX_IOV,
                                  0, &io_owner, NULL, 0);

    chimera_vfs_compound_submit(compound, chimera_nfs4_read_complete, req);
} /* chimera_nfs4_read */
