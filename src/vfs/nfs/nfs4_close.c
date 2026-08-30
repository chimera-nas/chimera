// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"
#include "nfs4_pnfs.h"

/* Carries the state across the compound so the reply can free it.  The thread
 * and shared handles the retry path needs arrive as its arguments, so they are
 * deliberately not kept here. */
struct chimera_nfs4_close_ctx {
    struct chimera_nfs4_open_state *open_state;
    struct stateid4                 stateid;    /* extracted by close_send */
};

/*
 * Finish the close: drop the local state and report success.
 *
 * The status of the CLOSE compound deliberately does not reach the caller.  By
 * the time we get here the handle is gone from the open cache and the caller
 * has no way to retry, so the file is closed locally whatever the server said;
 * returning an error would only fail an application close() (or an umount
 * drain) for state the server has already discarded, which is the usual reason
 * a CLOSE fails.  Failures are logged instead -- see chimera_nfs4_close_callback,
 * which is where the leak, if it is one, becomes visible.
 */
static void
chimera_nfs4_close_done(struct chimera_vfs_request *request)
{
    struct chimera_nfs4_close_ctx *ctx = request->plugin_data;

    chimera_nfs4_open_state_free(ctx->open_state);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_close_done */

static void
chimera_nfs4_close_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        chimera_nfsclient_error("CLOSE failed with transport error %d; "
                                "open stateid leaked until lease expiry", status);
    } else if (res->status != NFS4_OK) {
        chimera_nfsclient_error("CLOSE failed with NFS status %d", res->status);
    } else if (res->num_resarray < 3) {
        chimera_nfsclient_error("CLOSE reply truncated: %u ops", res->num_resarray);
    } else if (res->resarray[2].opclose.status != NFS4_OK) {
        chimera_nfsclient_error("CLOSE rejected with NFS status %d",
                                res->resarray[2].opclose.status);
    }

    chimera_nfs4_close_done(request);
} /* chimera_nfs4_close_callback */

static void chimera_nfs4_close_transmit(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request);

/*
 * Replay hook for the slot layer: re-send this CLOSE once a session slot frees.
 * Deliberately not chimera_nfs4_close_send -- that already consumed this
 * handle's reference on the shared open file (chimera_nfs4_open_file_put) and
 * captured the stateid into the ctx; running it again would find the file
 * entry gone, mistake this CLOSE for "not the last handle", and complete
 * without ever sending it, leaking the open state on the server.  (And not
 * chimera_nfs4_dispatch either, which would repeat any pNFS
 * LAYOUTCOMMIT/LAYOUTRETURN that already ran.)  Only the transmit repeats.
 */
static void
chimera_nfs4_close_retry(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    chimera_nfs4_close_transmit(thread, shared, request);
} /* chimera_nfs4_close_retry */

/*
 * Build and send the CLOSE compound for the stateid already captured in the
 * ctx by chimera_nfs4_close_send.  Safe to run more than once: the slot layer
 * replays a parked request through chimera_nfs4_close_retry, which lands here
 * rather than in close_send, whose bookkeeping must run exactly once.
 */
static void
chimera_nfs4_close_transmit(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs4_close_ctx           *ctx = request->plugin_data;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);

    /* No route to the server: the state is unreachable, not releasable.  This
     * is the one case worth shouting about, because it means the session went
     * away while an open was still held -- the ordering bug this CLOSE exists
     * to prevent. */
    if (!server_thread || !server_thread->server->nfs4_session) {
        chimera_nfsclient_error("CLOSE: no session for open stateid; "
                                "leaked until lease expiry");
        chimera_nfs4_close_done(request);
        return;
    }

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + CLOSE */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    argarray[0].argop = OP_SEQUENCE;   /* slot fields filled by compound_call */

    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* seqid is ignored in minor version 1 (RFC 8881 §18.2.3); OPEN sends 0 too. */
    argarray[2].argop                = OP_CLOSE;
    argarray[2].opclose.seqid        = 0;
    argarray[2].opclose.open_stateid = ctx->stateid;

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
        chimera_nfs4_close_callback,
        request,
        chimera_nfs4_close_retry, NULL);
} /* chimera_nfs4_close_transmit */

void
chimera_nfs4_close_send(
    struct chimera_nfs_thread      *thread,
    struct chimera_nfs_shared      *shared,
    struct chimera_vfs_request     *request,
    struct chimera_nfs4_open_state *open_state)
{
    struct chimera_nfs4_close_ctx *ctx;

    /* plugin_data may still hold the pNFS close context we were chained from;
    * open_state arrives as a parameter precisely so it survives the reuse. */
    ctx             = request->plugin_data;
    ctx->open_state = open_state;

    /* This handle is done with the file.  The open on the server is not this
     * handle's to end, though: every handle on the file shares it, so the CLOSE
     * goes only when the last one lets go.  This consumes the handle's
     * reference, so it must run exactly once per close -- the slot layer's
     * park replay re-enters chimera_nfs4_close_transmit, never here. */
    if (!chimera_nfs4_open_file_put(shared->servers[open_state->server_index],
                                    request->fh, request->fh_len,
                                    &ctx->stateid)) {
        chimera_nfs4_close_done(request);
        return;
    }

    /* Nothing was opened on the server, so there is no stateid to release. */
    if (!chimera_nfs4_stateid_is_open(&ctx->stateid)) {
        chimera_nfs4_close_done(request);
        return;
    }

    chimera_nfs4_close_transmit(thread, shared, request);
} /* chimera_nfs4_close_send */

void
chimera_nfs4_close(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs4_open_state *open_state;

    open_state = (struct chimera_nfs4_open_state *) request->close.vfs_private;

    /* Handle case where no open state was tracked (e.g., inferred opens) */
    if (!open_state) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Drop this handle's layout from the recall registry before it is freed.
    * Idempotent: a no-op unless the layout reached VALID (or was fenced). */
    chimera_nfs4_pnfs_layout_unregister(shared, &open_state->layout);

    /* If this handle holds a pNFS layout, report the new size to the MDS
     * (LAYOUTCOMMIT) and return the layout (LAYOUTRETURN) before the CLOSE.
     * This is per handle, as the layout is: the writes it drove to the data
     * server are invisible to the MDS until its own LAYOUTCOMMIT lands.  When
     * it takes the request it chains into chimera_nfs4_close_send once the
     * layout is back. */
    if (chimera_nfs4_pnfs_close(thread, shared, request, open_state)) {
        return;
    }

    chimera_nfs4_close_send(thread, shared, request, open_state);
} /* chimera_nfs4_close */
