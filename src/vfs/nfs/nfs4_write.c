// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include "nfs_internal.h"
#include "nfs4_open_state.h"
#include "nfs4_pnfs.h"
#include "vfs/sdk/vfs_error.h"

struct chimera_nfs4_write_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
    struct chimera_nfs4_open_state   *open_state;
};

static void
chimera_nfs4_write_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs4_write_ctx *ctx     = request->plugin_data;
    struct nfs_resop4             *write_res;

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

    /* Check SEQUENCE result */
    if (res->num_resarray < 1 || res->resarray[0].opsequence.sr_status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result */
    if (res->num_resarray < 2 || res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check WRITE result */
    if (res->num_resarray < 3) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    write_res = &res->resarray[2];
    if (write_res->opwrite.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(write_res->opwrite.status);
        request->complete(request);
        return;
    }

    /* Mark file as dirty if the write was not fully committed to stable storage */
    if (write_res->opwrite.resok4.committed != FILE_SYNC4 && ctx->open_state) {
        chimera_nfs4_open_state_mark_dirty(ctx->open_state);
    }

    request->write.r_sync   = write_res->opwrite.resok4.committed;
    request->write.r_length = write_res->opwrite.resok4.count;
    request->status         = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_write_callback */

void
chimera_nfs4_write(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_write_ctx           *ctx;
    struct chimera_nfs4_open_state          *open_state;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    struct evpl_iovec                       *ds_iov;
    uint8_t                                 *fh;
    int                                      fhlen;
    int                                      parked;
    int                                      i;

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

    open_state = (struct chimera_nfs4_open_state *) request->write.handle->vfs_private;

    /* pNFS overlay: if a flex-files RW layout covers this write, drive it
    * straight to the data server (or acquire the layout first).  Returns 1 if
    * it took the request; 0 means fall through to the MDS write below. */
    if (open_state &&
        chimera_nfs4_pnfs_write(thread, shared, request, private_data, server_thread, open_state)) {
        return;
    }

    ctx             = request->plugin_data;
    ctx->thread     = thread;
    ctx->server     = server;
    ctx->open_state = open_state;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + WRITE */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Op 2: WRITE */
    argarray[2].argop = OP_WRITE;

    /* Use the stateid from the open state, or anonymous stateid if not available */
    if (open_state) {
        argarray[2].opwrite.stateid = open_state->stateid;
    } else {
        /* Anonymous stateid */
        memset(&argarray[2].opwrite.stateid, 0, sizeof(argarray[2].opwrite.stateid));
        argarray[2].opwrite.stateid.seqid = 0;
    }

    argarray[2].opwrite.offset = request->write.offset;
    argarray[2].opwrite.stable = request->write.sync;  /* 3-level UNSTABLE/DATA_SYNC/FILE_SYNC */

    /* The WRITE4 marshaller MOVES (consumes + frees) the payload iovecs into
     * the outgoing RPC message, but our payload is BORROWED from whoever
     * dispatched this VFS write -- when that is chimera's own NFS server layer,
     * chimera_nfs4_write_complete releases the very same iovecs once we
     * complete.  Handing the marshaller the originals lets it free them out
     * from under that release: heap-use-after-free in evpl_iovecs_release.
     * (The pNFS DS write path has always cloned for this reason; this path is
     * only reached with server-owned iovecs when chimera fronts a remote NFS
     * server, which is why it went unnoticed.)
     *
     * So hand over CLONES -- each takes its own buffer reference, dropped when
     * the RPC message is released -- and leave the borrowed originals intact.
     * If the compound PARKS instead of sending, the marshaller never ran and
     * the clones are still ours: drop them, because the replay rebuilds these
     * args (and re-clones) from scratch. */
    if (getenv("CHIMERA_PNFS_IO_TRACE")) {
        fprintf(stderr, "PNFSIO: nfs4_write niov=%d iov=%p len=%u off=%llu\n",
                request->write.niov, (void *) request->write.iov,
                request->write.length,
                (unsigned long long) request->write.offset);
        for (i = 0; i < request->write.niov; i++) {
            fprintf(stderr, "PNFSIO:   iov[%d] data=%p len=%u\n", i,
                    request->write.iov[i].data, request->write.iov[i].length);
        }
    }

    ds_iov = malloc((size_t) request->write.niov * sizeof(*ds_iov));
    for (i = 0; i < request->write.niov; i++) {
        evpl_iovec_clone(&ds_iov[i], &request->write.iov[i]);
    }
    argarray[2].opwrite.data.iov    = ds_iov;
    argarray[2].opwrite.data.niov   = request->write.niov;
    argarray[2].opwrite.data.length = request->write.length;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    parked = chimera_nfs4_compound_call(
        thread,
        shared,
        server_thread,
        request,
        &args,
        &rpc2_cred,
        1, 0, NULL, 0, 0,
        chimera_nfs4_write_callback,
        request,
        chimera_nfs4_dispatch, private_data);

    if (getenv("CHIMERA_VFS_WRITE_TRACE")) {
        fprintf(stderr, "WRITETRACE: nfs4_write parked=%d niov=%d clone0=%p src0=%p\n",
                parked, request->write.niov,
                request->write.niov > 0 ? ds_iov[0].data : NULL,
                request->write.niov > 0 ? request->write.iov[0].data : NULL);
    }

    if (parked) {
        evpl_iovecs_release(thread->evpl, ds_iov, request->write.niov);
    }
    free(ds_iov);
} /* chimera_nfs4_write */
