// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "evpl/evpl_rpc2.h"
#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_stateid.h"
#include "nfs4_layout_table.h"
#include "nfs_internal.h"
#include "nfs4_cb.h"

#define LAYOUT4_FLEX_FILES  0x4  /* RFC 8435; not in the generated XDR */
#define NFS4_CB_MAX_HOLDERS 32   /* recall fan-out per file */

/* Per-recall reply context: identifies the holder so the reply handler can
* advance the backchannel slot and (on a decline) drop the stale layout. */
struct nfs4_cb_recall_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs_client                *client;     /* the holder */
    struct nfs4_session              *cb_session;  /* ref held for the call */
    uint8_t                           fh[NFS4_FHSIZE];
    uint16_t                          fhlen;
};

/* The backchannel credential.  The Linux client accepts AUTH_NONE only for
 * CB_NULL; a real CB_COMPOUND must carry AUTH_SYS or it is rejected with
 * AUTH_ERROR. */
static const struct evpl_rpc2_cred nfs4_cb_cred = {
    .flavor  = EVPL_RPC2_AUTH_SYS,
    .authsys = {
        .uid             = 0,
        .gid             = 0,
        .num_gids        = 0,
        .gids            = NULL,
        .machinename     = "chimera",
        .machinename_len = 7,
    },
};

static void
nfs4_cb_recall_reply(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct CB_COMPOUND4res      *reply,
    int                          status,
    void                        *private_data)
{
    struct nfs4_cb_recall_ctx *ctx       = private_data;
    int                        returning = 0;

    if (status == 0 && reply) {
        /* CB_SEQUENCE (op 0) ran, so the backchannel slot seqid is consumed. */
        ctx->cb_session->nfs4_session_cb_seqid++;
        returning = (reply->status == NFS4_OK);
        chimera_nfs_info("CB_LAYOUTRECALL reply: cb_status=%d (%s)",
                         reply->status,
                         returning ? "client returning" : "no return expected");
    } else {
        chimera_nfs_error("CB_LAYOUTRECALL transport failed: rpc status=%d", status);
    }

    /* NFS4_OK means the client will return the layout via LAYOUTRETURN (which
     * deregisters it and resumes the waiters).  Anything else -- NOMATCHING, an
     * error, or a dead connection -- means no return is coming, so drop the
     * stale layout now; deregistration resumes the waiters once the last holder
     * is gone. */
    if (!returning) {
        struct nfs_layout_state *layout =
            nfs_layout_state_find(ctx->client, ctx->fh, ctx->fhlen);

        if (layout) {
            nfs_layout_state_destroy(layout,
                                     &ctx->thread->shared->nfs4_state_table,
                                     ctx->thread->vfs_thread);
        }
    }

    nfs4_session_put(ctx->cb_session);
    free(ctx);
} /* nfs4_cb_recall_reply */

/*
 * Send CB_COMPOUND{CB_SEQUENCE, CB_LAYOUTRECALL} for `holder` to the client that
 * holds it.  If the holder has no usable backchannel it cannot be asked to
 * return the layout, so it is revoked locally (which deregisters it and lets
 * the recall make progress).  `holder` is pinned by the caller.
 */
static void
nfs4_cb_recall_holder(
    struct chimera_server_nfs_thread *thread,
    struct nfs_layout_state          *holder)
{
    struct nfs_client         *client = holder->client;
    struct nfs4_session       *cb;
    struct CB_COMPOUND4args    args;
    struct nfs_cb_argop4       argop[2];
    struct nfs4_cb_recall_ctx *ctx;
    struct stateid4            recall_stateid;

    /* Pin the client's backchannel session for the duration of the call. */
    pthread_mutex_lock(&client->lock);
    cb = client->cb_session;
    if (cb) {
        nfs4_session_get(cb);
    }
    pthread_mutex_unlock(&client->lock);

    if (!cb || !cb->nfs4_session_cb_conn) {
        chimera_nfs_error("CB: holder has no backchannel; revoking layout");
        if (cb) {
            nfs4_session_put(cb);
        }
        nfs_layout_state_destroy(holder, &thread->shared->nfs4_state_table,
                                 thread->vfs_thread);
        return;
    }

    nfs4_stateid_encode(&recall_stateid, holder->seqid, NFS4_STATEID_TYPE_LAYOUT,
                        holder->shard, holder->slot_idx, holder->generation,
                        (uint32_t) client->client_id);

    memset(&args, 0, sizeof(args));
    memset(argop, 0, sizeof(argop));

    args.minorversion = 1;
    args.num_argarray = 2;
    args.argarray     = argop;

    argop[0].argop = OP_CB_SEQUENCE;
    memcpy(argop[0].opcbsequence.csa_sessionid, cb->nfs4_session_id,
           NFS4_SESSIONID_SIZE);
    argop[0].opcbsequence.csa_sequenceid     = cb->nfs4_session_cb_seqid;
    argop[0].opcbsequence.csa_slotid         = 0;
    argop[0].opcbsequence.csa_highest_slotid = 0;
    argop[0].opcbsequence.csa_cachethis      = 0;

    argop[1].argop                                                = OP_CB_LAYOUTRECALL;
    argop[1].opcblayoutrecall.clora_type                          = LAYOUT4_FLEX_FILES;
    argop[1].opcblayoutrecall.clora_iomode                        = LAYOUTIOMODE4_ANY;
    argop[1].opcblayoutrecall.clora_changed                       = 0;
    argop[1].opcblayoutrecall.clora_recall.lor_recalltype         = LAYOUTRECALL4_FILE;
    argop[1].opcblayoutrecall.clora_recall.lor_layout.lor_fh.len  = holder->fh_len;
    argop[1].opcblayoutrecall.clora_recall.lor_layout.lor_fh.data = (void *) holder->fh;
    argop[1].opcblayoutrecall.clora_recall.lor_layout.lor_offset  = 0;
    argop[1].opcblayoutrecall.clora_recall.lor_layout.lor_length  = UINT64_MAX;
    argop[1].opcblayoutrecall.clora_recall.lor_layout.lor_stateid = recall_stateid;

    ctx             = calloc(1, sizeof(*ctx));
    ctx->thread     = thread;
    ctx->client     = client;
    ctx->cb_session = cb;          /* reply handler drops this ref */
    memcpy(ctx->fh, holder->fh, holder->fh_len);
    ctx->fhlen = holder->fh_len;

    chimera_nfs_info("CB: sending CB_LAYOUTRECALL fhlen=%u seqid=%u",
                     holder->fh_len, cb->nfs4_session_cb_seqid);

    thread->shared->nfs_v4_cb.send_call_CB_COMPOUND(
        &cb->nfs4_session_cb_prog, thread->evpl, cb->nfs4_session_cb_conn,
        &nfs4_cb_cred, &args, 0, 0, 0, nfs4_cb_recall_reply, ctx);
} /* nfs4_cb_recall_holder */

void
chimera_nfs4_cb_recall_and_wait(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint32_t                          fhlen,
    void (                           *resume )(void *arg),
    void                             *resume_arg)
{
    struct nfs_layout_recall_waiter *waiter;
    struct nfs_layout_state         *holders[NFS4_CB_MAX_HOLDERS];
    int                              n, i;

    waiter         = calloc(1, sizeof(*waiter));
    waiter->resume = resume;
    waiter->arg    = resume_arg;

    n = nfs_layout_table_recall_prepare(&thread->shared->nfs4_layout_table,
                                        fh, (uint16_t) fhlen, waiter,
                                        holders, NFS4_CB_MAX_HOLDERS);

    if (n == 0) {
        /* No layouts held for this file: nothing to recall, proceed now. */
        free(waiter);
        resume(resume_arg);
        return;
    }

    chimera_nfs_info("pNFS: recalling layout from %d holder(s) before conflicting op", n);

    /* Recall every holder.  Each one's return (or decline) deregisters it; the
     * last deregistration resumes the deferred operation. */
    for (i = 0; i < n; i++) {
        nfs4_cb_recall_holder(thread, holders[i]);
        nfs_layout_state_put(holders[i]);
    }
} /* chimera_nfs4_cb_recall_and_wait */
