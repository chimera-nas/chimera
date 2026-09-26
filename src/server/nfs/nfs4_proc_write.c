// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "server/server.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_compound.h"
#include "evpl/evpl.h"
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */

static inline int
chimera_nfs4_write_stateid_is_special(const struct stateid4 *sid)
{
    return nfs4_stateid_is_special(sid);
} /* chimera_nfs4_write_stateid_is_special */

/*
 * Marshal the reply and put back everything the request still holds.  Reached
 * from the sequence completion, and directly for a zero-length WRITE, which has
 * nothing to ask a backend.
 */
static void
chimera_nfs4_write_finish(
    struct nfs_request    *req,
    enum chimera_vfs_error error_code,
    uint32_t               length,
    uint32_t               sync)
{
    struct WRITE4args *args = req->args_write4;
    struct WRITE4res  *res  = &req->res_compound.resarray[req->index].opwrite;

    /* Release write iovecs here on the server thread, not in VFS backend.
     * The iovecs were allocated on this thread and must be released here
     * to avoid cross-thread access to non-atomic refcounts.
     */
    evpl_iovecs_release(req->thread->evpl, args->data.iov, args->data.niov);

    if (error_code == CHIMERA_VFS_OK) {
        res->status       = NFS4_OK;
        res->resok4.count = length;
        /* Achieved durability (UNSTABLE/DATA_SYNC/FILE_SYNC = 0/1/2 match the
         * stable_how4 enum); the backend may make data durable but defer
         * metadata, reporting DATA_SYNC4. */
        res->resok4.committed = sync;

        memcpy(res->resok4.writeverf,
               &req->thread->shared->nfs_verifier,
               sizeof(res->resok4.writeverf));
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    }

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_write_finish */

/*
 * PUTFH, OPEN_CURRENT, WRITE for a stateid that carries no handle, and the
 * WRITE alone against the handle an open or lock stateid holds.  The payload
 * iovecs are BORROWED by the run, as they were by the per-op call, and are
 * released above once it is over.
 */
static void
chimera_nfs4_write_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *wop;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    wop = chimera_vfs_compound_op(compound,
                                  chimera_vfs_compound_num_ops(compound) - 1);

    uint32_t                              length = wop->written;
    uint32_t                              sync   = wop->committed;

    chimera_vfs_compound_free(compound);

    chimera_nfs4_write_finish(req, error_code, length, sync);
} /* chimera_nfs4_write_complete */

/*
 * The shape for a stateid with no handle of its own -- an anonymous or special
 * one, a pNFS data-server write, or a (write) delegation stateid.  The object
 * is the current file handle and the sequence opens it, with the same
 * REGULAR_ONLY data open the per-op path used.
 *
 * `io_owner` says whose I/O it is when a delegation authorizes it; without it
 * the claim layer recalls the very delegation the write is being done under.
 * owner_lo is chimera_vfs_hash of the FILE HANDLE and nothing else, so it is
 * computable here, before anything is open.
 */
static void
chimera_nfs4_write_by_fh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct chimera_claim_actor *io_owner)
{
    struct WRITE4args           *args = req->args_write4;
    struct chimera_vfs_compound *compound;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_NOFOLLOW |
                                          CHIMERA_VFS_OPEN_REGULAR_ONLY, 0);
    chimera_vfs_compound_add_write(compound, NULL,
                                   args->offset, args->data.length,
                                   args->stable,
                                   args->data.iov, args->data.niov,
                                   0, 0, io_owner);

    chimera_vfs_compound_submit(compound, chimera_nfs4_write_complete, req);
} /* chimera_nfs4_write_by_fh */

void
chimera_nfs4_write(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct WRITE4args              *args  = &argop->opwrite;
    struct WRITE4res               *res   = &resop->opwrite;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    struct nfs_open_state          *open_state;
    nfsstat4                        status;

    req->nfs_state_ref       = NULL;
    req->handle              = NULL;
    req->io_owner_from_deleg = false;

    /* RFC 7530 §9.6.2: WRITE is refused with NFS4ERR_GRACE while the
     * server-reboot grace window is open, because a byte-range lock that
     * would conflict with it may not have been reclaimed yet.  Checked
     * before the read chunk is taken below, so the RPC layer still owns the
     * payload iovecs on this path. */
    status = nfs_recovery_io_check(&thread->shared->nfs4_recovery);
    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->stateid);

    /*
     * Transfer ownership of write iovecs from the RPC2 message to prevent
     * msg_free from double-releasing (args->data.iov points to msg->read_chunk.iov
     * via XDR zerocopy).  The iovecs will be released in the write completion
     * callback on this server thread, not in the VFS backend (which may run on
     * a different delegation thread).
     */
    evpl_rpc2_encoding_take_read_chunk(req->encoding, NULL, NULL);

    /*
     * RFC 7530 9.1.4.3 / RFC 8881 8.2.3 require WRITE to honor special
     * stateids.  Open the current FH on the fly instead of consulting the
     * state table.
     *
     * A pNFS data server does not hold open state for the files it stores --
     * the metadata server authorizes the client's I/O via the layout -- so it
     * likewise serves WRITE by file handle without a state-table lookup.
     */
    if (chimera_nfs4_write_stateid_is_special(&args->stateid) ||
        chimera_server_config_get_nfs_data_server(thread->shared->config)) {
        if (req->fhlen == 0) {
            res->status = NFS4ERR_NOFILEHANDLE;
            evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        /* RFC 7530 §9.1.4.3 / RFC 8881 §9.7: a special-stateid WRITE
         * against a deny-WRITE share reservation held by ANY owner of ANY
         * client is NFS4ERR_LOCKED (the all-ones stateid bypasses only
         * READ deny checking). */
        if (!chimera_server_config_get_nfs_data_server(
                thread->shared->config)) {
            status = nfs4_clients_check_io_denied(
                &thread->shared->nfs4_shared_clients,
                req->fh,
                req->fhlen,
                OPEN4_SHARE_ACCESS_WRITE);
            if (status != NFS4_OK) {
                res->status = status;
                evpl_iovecs_release(thread->evpl, args->data.iov,
                                    args->data.niov);
                chimera_nfs4_compound_complete(req, res->status);
                return;
            }
        }

        req->args_write4 = args;

        chimera_nfs4_write_by_fh(thread, req, NULL);
        return;
    }

    status = nfs_state_table_acquire(table, &args->stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->status = status;
        evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
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
        evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* A (write) delegation stateid authorizes the WRITE but carries no open
     * handle; drop the ref and open the current FH on the fly. */
    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        if (req->fhlen == 0) {
            res->status = NFS4ERR_NOFILEHANDLE;
            evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
        req->args_write4 = args;
        /* This write is authorized by the client's own (write) delegation:
         * carry the holder's lease owner on the on-the-fly write so it does
         * not recall the client's own delegation. */
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

            chimera_nfs4_write_by_fh(thread, req, &io_owner);
        } else {
            chimera_nfs4_write_by_fh(thread, req, NULL);
        }
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        open_state = state_void;
    } else {
        open_state = ((struct nfs_lock_state *) state_void)->open_state;
    }
    state_handle = nfs_state_io_handle(state_void, state_type, OPEN4_SHARE_ACCESS_WRITE);

    if (req->minorversion == 0) {
        uint32_t current_seqid = (state_type == NFS4_SLOT_TYPE_OPEN) ?
            open_state->seqid :
            ((struct nfs_lock_state *) state_void)->seqid;

        status = nfs4_stateid_check_seqid(current_seqid,
                                          args->stateid.seqid);
        if (status != NFS4_OK) {
            nfs_state_table_release(table, state_void, state_type,
                                    thread->vfs_thread);
            res->status = status;
            evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
    }

    if ((open_state->share_access & OPEN4_SHARE_ACCESS_WRITE) == 0) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->status = NFS4ERR_OPENMODE;
        evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = nfs_open_state_check_io_denied(open_state,
                                            OPEN4_SHARE_ACCESS_WRITE);
    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        res->status = status;
        evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
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
        evpl_iovecs_release(thread->evpl, args->data.iov, args->data.niov);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;
    req->args_write4    = args;

    if (args->data.length == 0) {
        chimera_nfs4_write_finish(req, CHIMERA_VFS_OK, 0, FILE_SYNC4);
        return;
    }

    struct chimera_claim_actor   io_owner = {
        .owner          = {
            .proto      = CHIMERA_CLAIM_PROTO_NFSV4,
            .client_key = open_state->owner->client->client_id,
            .owner_lo   = state_handle->fh_hash,
            .owner_hi   = 0,
        },
    };

    /* The stateid names the object, so the run is the WRITE alone against the
     * handle the state holds -- lent, and released with the state above. */
    struct chimera_vfs_compound *compound =
        chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_write(compound, state_handle,
                                   args->offset, args->data.length,
                                   args->stable,
                                   args->data.iov, args->data.niov,
                                   0, 0, &io_owner);

    chimera_vfs_compound_submit(compound, chimera_nfs4_write_complete, req);
} /* chimera_nfs4_write */
