// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>
#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"

static void
chimera_nfs4_lockt_probe(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req       = private_data;
    struct LOCKT4args                *args      = &req->args_compound->argarray[req->index].oplockt;
    struct LOCKT4res                 *res       = &req->res_compound.resarray[req->index].oplockt;
    struct chimera_vfs_open_handle   *handle    = req->handle;
    struct chimera_vfs_state         *vfs_state = req->thread->vfs->vfs_state;
    struct chimera_vfs_file_state    *file_state;
    struct chimera_vfs_claim          probe;
    struct chimera_claim_owner        owner;
    struct chimera_vfs_claim_conflict conflict;
    enum chimera_vfs_claim_result     result;
    uint64_t                          vfs_length;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.11.4: byte-range locking is defined only for regular
     * files.  A directory target is NFS4ERR_ISDIR; any other non-regular
     * type is NFS4ERR_INVAL. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISREG(attr->va_mode)) {
        chimera_vfs_release(req->thread->vfs_thread, handle);
        res->status = chimera_nfs4_data_nonreg_status(attr->va_mode);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* NFSv4 and the claim core both use UINT64_MAX as the "to EOF"
     * sentinel, so the wire length passes through unchanged. */
    vfs_length = args->length;

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

    memset(&owner, 0, sizeof(owner));
    owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    owner.client_key = args->owner.clientid;
    owner.owner_lo   = XXH3_64bits(args->owner.owner.data,
                                   args->owner.owner.len);
    owner.owner_hi = 0;

    chimera_vfs_claim_init_range(&probe,
                                 !(args->locktype == READ_LT ||
                                   args->locktype == READW_LT),
                                 /*smb=*/ false,
                                 args->offset, vfs_length,
                                 &owner);

    memset(&conflict, 0, sizeof(conflict));
    conflict.length = UINT64_MAX;

    result = chimera_vfs_claim_test(file_state, &probe, &conflict);

    if (result == CHIMERA_CLAIM_GRANTED) {
        res->status = NFS4_OK;
    } else {
        res->status        = NFS4ERR_DENIED;
        res->denied.offset = conflict.offset;
        /* conflict.length already uses UINT64_MAX for a to-EOF holder, so it
         * maps directly to the NFSv4 denied length. */
        res->denied.length = conflict.length;
        /* WRITE_LT iff the holder writes: a write delegation (CW) must report
         * WRITE_LT though it holds no LW. */
        res->denied.locktype = (conflict.used & (CHIMERA_CLAIM_W |
                                                 CHIMERA_CLAIM_CW |
                                                 CHIMERA_CLAIM_LW))
                               ? WRITE_LT : READ_LT;
        nfs4_fill_denied_owner(&req->thread->shared->nfs4_shared_clients,
                               &conflict, &res->denied.owner,
                               req->encoding->dbuf);
    }

    chimera_vfs_state_put(vfs_state, file_state);
    chimera_vfs_release(req->thread->vfs_thread, handle);

    /* LOCKT NFS4ERR_DENIED is a successful query result. */
    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_lockt_probe */

static void
chimera_nfs4_lockt_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct LOCKT4args  *args = &req->args_compound->argarray[req->index].oplockt;
    struct LOCKT4res   *res  = &req->res_compound.resarray[req->index].oplockt;

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

    /* Fetch the target's type before probing -- LOCKT is only valid on a
     * regular file (see chimera_nfs4_lockt_probe). */
    req->handle = handle;
    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred, req->compound, handle,
                        CHIMERA_VFS_ATTR_MASK_STAT,
                        chimera_nfs4_lockt_probe, req);
} /* chimera_nfs4_lockt_open_complete */

void
chimera_nfs4_lockt(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCKT4args *args = &argop->oplockt;
    struct LOCKT4res  *res  = &resop->oplockt;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §9.1.4: the lock-owner names a clientid; reject one the server
     * has no record of.  4.1+ identifies the client via the session instead. */
    if (req->minorversion == 0) {
        struct nfs4_session *s = nfs4_session_find_by_clientid(
            &thread->shared->nfs4_shared_clients, args->owner.clientid);
        if (s) {
            /* RFC 7530 §9.5: LOCKT is a clientid-bearing operation and renews
             * all of the client's leases. */
            nfs_client_touch(s->client_unified);
            nfs4_session_put(s);
        } else {
            res->status = NFS4ERR_STALE_CLIENTID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
    }

    /* LOCKT operates on CURRENT_FH - open it temporarily to validate. */
    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, req->compound,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED |
                        CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_NOFOLLOW,
                        chimera_nfs4_lockt_open_complete,
                        req);
} /* chimera_nfs4_lockt */
