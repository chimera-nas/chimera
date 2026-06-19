// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"
#include "vfs/vfs.h"

static void
chimera_nfs4_allocate_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request    = private_data;
    int                         is_dealloc =
        request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE;
    nfsstat4                    op_status;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(res->status);
        request->complete(request);
        return;
    }

    /* SEQUENCE + PUTFH + (DE)ALLOCATE */
    if (res->num_resarray < 3 ||
        res->resarray[0].opsequence.sr_status != NFS4_OK ||
        res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    op_status = is_dealloc ?
        res->resarray[2].opdeallocate.dr_status :
        res->resarray[2].opallocate.ar_status;

    if (op_status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(op_status);
        request->complete(request);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_allocate_callback */

void
chimera_nfs4_allocate(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread =
        chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_open_state          *open_state;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;
    int                                      is_dealloc =
        request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    server  = server_thread->server;
    session = server->nfs4_session;

    if (!session) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    open_state = (struct chimera_nfs4_open_state *) request->allocate.handle->vfs_private;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + ALLOCATE/DEALLOCATE.  ALLOCATE and
     * DEALLOCATE are NFSv4.2 operations, so this compound advertises
     * minorversion 2 (the server gates ops per-compound minorversion; the
     * session itself is not pinned to a minor version). */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 2;
    args.argarray     = argarray;
    args.num_argarray = 3;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Op 2: ALLOCATE or DEALLOCATE -- both carry { stateid, offset, length }. */
    if (is_dealloc) {
        argarray[2].argop = OP_DEALLOCATE;
        if (open_state) {
            argarray[2].opdeallocate.da_stateid = open_state->stateid;
        } else {
            memset(&argarray[2].opdeallocate.da_stateid, 0,
                   sizeof(argarray[2].opdeallocate.da_stateid));
        }
        argarray[2].opdeallocate.da_offset = request->allocate.offset;
        argarray[2].opdeallocate.da_length = request->allocate.length;
    } else {
        argarray[2].argop = OP_ALLOCATE;
        if (open_state) {
            argarray[2].opallocate.aa_stateid = open_state->stateid;
        } else {
            memset(&argarray[2].opallocate.aa_stateid, 0,
                   sizeof(argarray[2].opallocate.aa_stateid));
        }
        argarray[2].opallocate.aa_offset = request->allocate.offset;
        argarray[2].opallocate.aa_length = request->allocate.length;
    }

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    chimera_nfs4_compound_call(
        thread,
        shared,
        server_thread,
        request,
        &args,
        &rpc2_cred,
        0, 0, NULL, 0, 0,
        chimera_nfs4_allocate_callback,
        request,
        chimera_nfs4_dispatch, private_data);
} /* chimera_nfs4_allocate */
