// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"

/*
 * PUTFH, OPEN_CURRENT(meta), GETATTR, GETHANDLE.
 *
 * The probe itself is not a VFS operation and stays where it was: it reads the
 * claim core's per-file state directly and inserts nothing.  What it needs from
 * the run is the object's type and an open handle to key the file state on --
 * which is why the last op is a GETHANDLE, transferring the handle the run
 * opened so the probe can outlive the sequence.
 */
#define NFS4_LOCKT_OP_GETATTR   2
#define NFS4_LOCKT_OP_GETHANDLE 3

static void
chimera_nfs4_lockt_probe(
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
    owner.proto = CHIMERA_CLAIM_PROTO_NFSV4;
    /* RFC 8881 §2.4: in 4.1+ the client is identified by the session, not by
     * the clientid embedded in lock_owner4 -- clients routinely leave that
     * field zero or stale.  LOCK registers its claims under the server-assigned
     * client id (nfs4_proc_lock.c), so keying the probe on the wire field would
     * make the caller's own locks look foreign and defeat the SHOULD-exclude-
     * self rule of RFC 7530 §16.11.5. */
    if (req->minorversion > 0 && req->session && req->session->client_unified) {
        owner.client_key = req->session->client_unified->client_id;
    } else {
        owner.client_key = args->owner.clientid;
    }
    owner.owner_lo = XXH3_64bits(args->owner.owner.data,
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
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req  = private_data;
    struct LOCKT4args                    *args = &req->args_compound->argarray[req->index].oplockt;
    struct LOCKT4res                     *res  = &req->res_compound.resarray[req->index].oplockt;
    const struct chimera_vfs_compound_op *gop;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    gop  = chimera_vfs_compound_op(compound, NFS4_LOCKT_OP_GETATTR);
    attr = gop->attr;

    /* The handle becomes the request's: the probe below keys the claim core's
     * file state on it and runs after the sequence is over.  The ACL the stat
     * may carry is the compound's and is not read by any of this. */
    req->handle = chimera_vfs_compound_take_handle(compound,
                                                   NFS4_LOCKT_OP_GETHANDLE);

    chimera_vfs_compound_free(compound);

    if (!req->handle) {
        res->status = NFS4ERR_SERVERFAULT;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.11.4: same length rules as LOCK */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
        res->status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_lockt_probe(&attr, req);
} /* chimera_nfs4_lockt_open_complete */

void
chimera_nfs4_lockt(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCKT4args           *args = &argop->oplockt;
    struct LOCKT4res            *res  = &resop->oplockt;
    struct chimera_vfs_compound *compound;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §9.6.2: inside the server-reboot grace window the byte-range
     * locks held before the restart have not all been reclaimed, so LOCKT
     * would report a range as available that another client is about to
     * reclaim.  Refuse it with NFS4ERR_GRACE rather than answer from
     * half-rebuilt state. */
    {
        nfsstat4 g_status = nfs_recovery_io_check(
            &thread->shared->nfs4_recovery);

        if (g_status != NFS4_OK) {
            res->status = g_status;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }
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

    /* LOCKT operates on CURRENT_FH - open it temporarily to validate.  A PATH
     * open, because nothing is read through the handle and a data open of a
     * FIFO blocks. */
    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_NOFOLLOW, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_MASK_STAT);
    chimera_vfs_compound_add_gethandle(compound);

    chimera_vfs_compound_submit(compound, chimera_nfs4_lockt_open_complete,
                                req);
} /* chimera_nfs4_lockt */
