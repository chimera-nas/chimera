// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"

static void
chimera_nfs4_lockt_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request            *req       = private_data;
    struct LOCKT4args             *args      = &req->args_compound->argarray[req->index].oplockt;
    struct LOCKT4res              *res       = &req->res_compound.resarray[req->index].oplockt;
    struct chimera_vfs_state      *vfs_state = req->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file_state;
    struct chimera_vfs_lease       probe;
    struct chimera_vfs_lease      *conflict = NULL;
    enum chimera_vfs_lease_result  result;
    uint64_t                       vfs_length;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.11.4: same length rules as LOCK */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        res->status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    vfs_length = args->length == UINT64_MAX ? 0 : args->length;

    file_state = chimera_vfs_state_get(vfs_state,
                                       handle->fh, handle->fh_len,
                                       handle->fh_hash, false);

    if (!file_state) {
        /* No state on this file means no lock could conflict. */
        chimera_vfs_release(req->thread->vfs_thread, handle);
        res->status = NFS4_OK;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    memset(&probe, 0, sizeof(probe));
    probe.kind         = CHIMERA_VFS_LEASE_RANGE;
    probe.mode.granted = (args->locktype == READ_LT || args->locktype == READW_LT)
                         ? CHIMERA_VFS_LEASE_MODE_R : CHIMERA_VFS_LEASE_MODE_W;
    probe.offset           = args->offset;
    probe.length           = vfs_length;
    probe.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_NFSV4;
    probe.owner.client_key = args->owner.clientid;
    probe.owner.owner_lo   = XXH3_64bits(args->owner.owner.data,
                                         args->owner.owner.len);
    probe.owner.owner_hi = 0;

    result = chimera_vfs_lease_test(file_state, &probe, &conflict);

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        res->status = NFS4_OK;
    } else {
        res->status          = NFS4ERR_DENIED;
        res->denied.offset   = conflict ? conflict->offset : 0;
        res->denied.length   = (conflict && conflict->length) ? conflict->length : UINT64_MAX;
        res->denied.locktype = (conflict && (conflict->mode.granted & CHIMERA_VFS_LEASE_MODE_W))
                               ? WRITE_LT : READ_LT;
        res->denied.owner.clientid   = 0;
        res->denied.owner.owner.len  = 0;
        res->denied.owner.owner.data = NULL;
    }

    chimera_vfs_state_put(vfs_state, file_state);
    chimera_vfs_release(req->thread->vfs_thread, handle);

    /* LOCKT NFS4ERR_DENIED is a successful query result. */
    chimera_nfs4_compound_complete(req, res->status);
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

    /* LOCKT operates on CURRENT_FH - open it temporarily to validate. */
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_lockt_open_complete,
                        req);
} /* chimera_nfs4_lockt */
