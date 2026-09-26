// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <assert.h>
#include "../nfs4_slot.c"

static int completions, retries, completion_status;

static void
complete(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    completions++;
    completion_status = status;
} /* complete */

static void
retry(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    if (private_data) {
        (*(int *) private_data)++;
    } else {
        retries++;
    }
} /* retry */

int
main(void)
{
    struct evpl                            *evpl          = evpl_create(NULL);
    struct chimera_nfs4_client_session      session       = { 0 };
    struct chimera_nfs_client_server_thread server_thread = { 0 };
    struct chimera_nfs4_slot_table         *st            = &server_thread.slots;
    struct chimera_nfs4_compound_ctx       *ctx;
    uint32_t                                local_free[4] = { 0 };
    uint32_t                                seqids[4]     = { 7 };
    struct nfs_resop4                       ops[4]        = { 0 };
    struct COMPOUND4res                     res           = { 0 };

    int                                     parked_woken = 0;

    assert(evpl);
    session.slot_seqid                                   = seqids;
    st->session                                          = &session;
    st->initialized                                      = 1;
    st->local_free                                       = local_free;
    st->cached_target                                    = 4;
    ops[0].resop                                         = OP_SEQUENCE;
    ops[0].opsequence.sr_status                          = NFS4_OK;
    ops[0].opsequence.sr_resok4.sr_target_highest_slotid = 3;
    ops[1].resop                                         = OP_PUTFH;
    ops[2].resop                                         = OP_WRITE;
    ops[2].opwrite.status                                = NFS4ERR_DELAY;
    res.status                                           = NFS4ERR_DELAY;
    res.resarray                                         = ops;
    res.num_resarray                                     = 3;

    ctx                = chimera_nfs4_ctx_alloc(st);
    ctx->server_thread = &server_thread;
    ctx->seqid         = 7;
    ctx->slot_id       = 0;
    ctx->real_cb       = complete;
    ctx->retry_fn      = retry;
    chimera_nfs4_inflight_add(st, ctx);
    chimera_nfs4_compound_call_cb(evpl, NULL, &res, 0, ctx);
    assert(ctx->delayed && st->inflight == ctx);
    assert(st->local_free_top == 0 && seqids[0] == 8);
    assert(completions == 0 && retries == 0);

    /* Drive the armed timer deterministically: its handler returns exactly
     * one slot and reconstructs once, without publishing the rejected reply. */
    chimera_nfs4_park(st, NULL, NULL, NULL, retry, &parked_woken);
    evpl_remove_timer(evpl, &ctx->delay_timer);
    chimera_nfs4_delay_retry(evpl, &ctx->delay_timer);
    assert(retries == 1 && completions == 0 && parked_woken == 1);
    assert(st->inflight == NULL && st->local_free_top == 1);

    /* A successful WRITE followed by delayed GETATTR cannot be replayed. */
    ctx = chimera_nfs4_ctx_alloc(st);
    st->local_free_top--;
    ctx->seqid              = 8;
    ops[2].opwrite.status   = NFS4_OK;
    ops[3].resop            = OP_GETATTR;
    ops[3].opgetattr.status = NFS4ERR_DELAY;
    res.num_resarray        = 4;
    chimera_nfs4_inflight_add(st, ctx);
    chimera_nfs4_compound_call_cb(evpl, NULL, &res, 0, ctx);
    assert(completions == 1 && completion_status == 0 && retries == 1);
    assert(st->inflight == NULL && st->local_free_top == 1);

    /* Connection reset while parked cancels the timer, completes once with
     * transport error, and returns its held slot without a late retry. */
    ctx = chimera_nfs4_ctx_alloc(st);
    st->local_free_top--;
    ctx->seqid            = 9;
    res.num_resarray      = 3;
    ops[2].opwrite.status = NFS4ERR_DELAY;
    chimera_nfs4_inflight_add(st, ctx);
    chimera_nfs4_compound_call_cb(evpl, NULL, &res, 0, ctx);
    assert(ctx->delayed);
    chimera_nfs4_slot_table_reset(evpl, st);
    assert(completions == 2 && completion_status == 1 && retries == 1);
    assert(!ctx->delayed && st->inflight == NULL && st->local_free_top == 1);
    free(st->parked_freelist);
    free(st->ctx_freelist);
    evpl_destroy(evpl);
    return 0;
} /* main */
