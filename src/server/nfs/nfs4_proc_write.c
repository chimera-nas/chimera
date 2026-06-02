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
#include "evpl/evpl.h"
#include <sys/stat.h>

static inline int
chimera_nfs4_write_stateid_is_special(const struct stateid4 *sid)
{
    return nfs4_stateid_is_special(sid);
} /* chimera_nfs4_write_stateid_is_special */

static void
chimera_nfs4_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct WRITE4args  *args = req->args_write4;
    struct WRITE4res   *res  = &req->res_compound.resarray[req->index].opwrite;

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
    } else if (req->handle) {
        /* Anonymous stateid: release the on-the-fly handle we opened. */
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_write_complete */

static void
chimera_nfs4_write_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct WRITE4args  *args = &req->args_compound->argarray[req->index].opwrite;
    struct WRITE4res   *res  = &req->res_compound.resarray[req->index].opwrite;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        evpl_iovecs_release(req->thread->evpl, args->data.iov, args->data.niov);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle      = handle;
    req->args_write4 = args;

    chimera_vfs_write(req->thread->vfs_thread, &req->cred, NULL,
                      handle,
                      args->offset,
                      args->data.length,
                      args->stable,               /* 3-level requested stability */
                      0,
                      0,
                      args->data.iov,
                      args->data.niov,
                      chimera_nfs4_write_complete,
                      req);
} /* chimera_nfs4_write_open_callback */

static void
chimera_nfs4_write_typecheck_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct WRITE4args  *args = &req->args_compound->argarray[req->index].opwrite;
    struct WRITE4res   *res  = &req->res_compound.resarray[req->index].opwrite;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        evpl_iovecs_release(req->thread->evpl, args->data.iov, args->data.niov);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISREG(attr->va_mode)) {
        res->status = S_ISDIR(attr->va_mode) ? NFS4ERR_ISDIR : NFS4ERR_INVAL;
        evpl_iovecs_release(req->thread->evpl, args->data.iov, args->data.niov);
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    req->handle = NULL;

    if (args->data.length == 0) {
        req->args_write4 = args;
        chimera_nfs4_write_complete(CHIMERA_VFS_OK, 0, FILE_SYNC4, NULL, NULL, req);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_write_open_callback,
                        req);
} /* chimera_nfs4_write_typecheck_complete */

static void
chimera_nfs4_write_typecheck_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct WRITE4args  *args = &req->args_compound->argarray[req->index].opwrite;
    struct WRITE4res   *res  = &req->res_compound.resarray[req->index].opwrite;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        evpl_iovecs_release(req->thread->evpl, args->data.iov, args->data.niov);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        CHIMERA_VFS_ATTR_MODE,
                        chimera_nfs4_write_typecheck_complete,
                        req);
} /* chimera_nfs4_write_typecheck_open_callback */

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

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

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

        if (req->session && req->session->client_unified) {
            status = nfs_client_check_io_denied(req->session->client_unified,
                                                NULL,
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

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                            CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_write_typecheck_open_callback,
                            req);
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
        chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                            CHIMERA_VFS_OPEN_NOFOLLOW,
                            chimera_nfs4_write_typecheck_open_callback,
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
        chimera_nfs4_write_complete(CHIMERA_VFS_OK, 0, FILE_SYNC4, NULL, NULL, req);
        return;
    }

    struct chimera_vfs_lease_owner io_owner = {
        .protocol   = CHIMERA_VFS_LEASE_PROTO_NFSV4,
        .client_key = open_state->owner->client->client_id,
        .owner_lo   = state_handle->fh_hash,
        .owner_hi   = 0,
    };

    chimera_vfs_write_owned(thread->vfs_thread, &req->cred, NULL,
                            state_handle,
                            args->offset,
                            args->data.length,
                            args->stable,         /* 3-level requested stability */
                            0,
                            0,
                            args->data.iov,
                            args->data.niov,
                            &io_owner,
                            chimera_nfs4_write_complete,
                            req);
} /* chimera_nfs4_write */
