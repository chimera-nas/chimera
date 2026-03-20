// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_lockt_complete(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct nfs_request             *req    = private_data;
    struct LOCKT4res               *res    = &req->res_compound.resarray[req->index].oplockt;
    struct chimera_vfs_open_handle *handle = req->handle;

    chimera_vfs_release(req->thread->vfs_thread, handle);
    req->handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* F_GETLK always returns OK; conflict is signalled via conflict_type.
     * LOCKT NFS4ERR_DENIED is a successful query result - compound stays NFS4_OK. */
    if (conflict_type == CHIMERA_VFS_LOCK_UNLOCK) {
        res->status = NFS4_OK;
    } else {
        res->status          = NFS4ERR_DENIED;
        res->denied.offset   = conflict_offset;
        res->denied.length   = conflict_length;
        res->denied.locktype = (conflict_type == CHIMERA_VFS_LOCK_READ)
            ? READ_LT : WRITE_LT;
        /* The VFS layer does not expose the conflicting lock's NFS owner;
         * return a zeroed owner rather than the requester's clientid. */
        res->denied.owner.clientid   = 0;
        res->denied.owner.owner.len  = 0;
        res->denied.owner.owner.data = NULL;
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_lockt_complete */

static void
chimera_nfs4_lockt_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct LOCKT4args  *args = &req->args_compound->argarray[req->index].oplockt;
    struct LOCKT4res   *res  = &req->res_compound.resarray[req->index].oplockt;
    uint32_t            lock_type;
    uint64_t            vfs_length;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 Section 16.11.4: same length rules as LOCK */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        res->status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;

    if (args->locktype == READ_LT || args->locktype == READW_LT) {
        lock_type = CHIMERA_VFS_LOCK_READ;
    } else {
        lock_type = CHIMERA_VFS_LOCK_WRITE;
    }

    /* NFS uses UINT64_MAX to mean "to end of file"; POSIX fcntl uses 0. */
    vfs_length = args->length == UINT64_MAX ? 0 : args->length;

    chimera_vfs_lock(req->thread->vfs_thread, &req->cred,
                     handle,
                     SEEK_SET,
                     args->offset,
                     vfs_length,
                     lock_type,
                     CHIMERA_VFS_LOCK_TEST,
                     chimera_nfs4_lockt_complete,
                     req);
} /* chimera_nfs4_lockt_open_complete */

void
chimera_nfs4_lockt(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCKT4res *res = &resop->oplockt;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* LOCKT operates on CURRENT_FH - open it temporarily to get a handle */
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_lockt_open_complete,
                        req);
} /* chimera_nfs4_lockt */
