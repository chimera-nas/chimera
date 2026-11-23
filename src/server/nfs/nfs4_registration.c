// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "clientid_allocator.h"
#include "nfs4_registration.h"
#include "nfs4_cb.h"
#include "nfs4_state.h"
#include "nfs_common.h"
#include "nfs_internal.h"

struct nfs4_reg_ctx {
    struct chimera_server_nfs_shared *shared;
    struct chimera_server_nfs_thread *thread;   /* the original request's */
    enum nfs4_reg_kind                kind;
    uint64_t                          token;
    int                               owner_len;
    uint8_t                           owner[NFS4_OPAQUE_LIMIT];
};

struct nfs4_reg_reply {
    struct nfs4_reg_waiter  w;
    enum nfs4_reg_kind kind;
    struct nfs4_reg_outcome out;
    bool                    primary;
};

static void
nfs4_reg_reply_one(
    const struct nfs4_reg_waiter  *w,
    enum nfs4_reg_kind             kind,
    const struct nfs4_reg_outcome *out,
    bool                           primary)
{
    if (kind == NFS4_REG_SETCLIENTID) {
        chimera_nfs4_setclientid_reply(w->thread, w->req, w->argop, w->resop,
                                       &out->scid);
    } else {
        chimera_nfs4_exchange_id_reply(w->thread, w->req, w->argop, w->resop,
                                       &out->eid, primary);
    }
} /* nfs4_reg_reply_one */

static void
nfs4_reg_reply_bounce(void *arg)
{
    struct nfs4_reg_reply *r = arg;

    nfs4_reg_reply_one(&r->w, r->kind, &r->out, r->primary);
    free(r);
} /* nfs4_reg_reply_bounce */

/* Superseded records' state hierarchies are torn down outside the table lock,
 * once, on the thread finalizing the registration. */
static void
nfs4_reg_teardown(
    struct chimera_server_nfs_thread *thread,
    enum nfs4_reg_kind                kind,
    struct nfs4_reg_outcome          *out)
{
    struct nfs_state_table *st = &thread->shared->nfs4_state_table;

    if (kind == NFS4_REG_SETCLIENTID) {
        if (out->scid.destroy_unified) {
            nfs_client_destroy(out->scid.destroy_unified, st, thread->vfs_thread, false);
            out->scid.destroy_unified = NULL;
        }
        return;
    }
    if (out->eid.destroy_unified) {
        nfs_client_destroy(out->eid.destroy_unified, st, thread->vfs_thread, false);
        out->eid.destroy_unified = NULL;
    }
    if (out->eid.destroy_unified2) {
        nfs_client_destroy(out->eid.destroy_unified2, st, thread->vfs_thread, false);
        out->eid.destroy_unified2 = NULL;
    }
} /* nfs4_reg_teardown */

/* Finalize, drop the reservation, then reply to the original and to every
 * coalesced waiter on its own thread. */
static void
nfs4_reg_complete(
    struct nfs4_reg_ctx     *ctx,
    struct nfs4_reg_outcome *out)
{
    struct nfs4_reg_waiter waiters[1 + NFS4_REG_MAX_WAITERS];
    int                    n, i;

    nfs4_reg_teardown(ctx->thread, ctx->kind, out);

    n = nfs4_client_reg_finish(&ctx->shared->nfs4_shared_clients, ctx->owner,
                               ctx->owner_len, ctx->token, waiters,
                               1 + NFS4_REG_MAX_WAITERS);

    for (i = 0; i < n; i++) {
        if (waiters[i].thread == ctx->thread) {
            nfs4_reg_reply_one(&waiters[i], ctx->kind, out, i == 0);
        } else {
            struct nfs4_reg_reply *r = calloc(1, sizeof(*r));

            chimera_nfs_abort_if(r == NULL, "registration reply: out of memory");
            r->w       = waiters[i];
            r->kind    = ctx->kind;
            r->out     = *out;
            r->primary = (i == 0);
            nfs4_thread_run(waiters[i].thread, nfs4_reg_reply_bounce, r);
        }
    }

    free(ctx);
} /* nfs4_reg_complete */

static void
nfs4_reg_submit(
    struct nfs4_reg_ctx *ctx);

static void
nfs4_reg_alloc_done(
    void    *arg,
    int      status,
    uint64_t clientid)
{
    struct nfs4_reg_ctx    *ctx = arg;
    struct nfs4_reg_outcome out;

    switch (nfs4_client_reg_commit(&ctx->shared->nfs4_shared_clients, ctx->owner,
                                   ctx->owner_len, ctx->token, status, clientid,
                                   &out)) {
        case NFS4_REG_RETRY:
            nfs4_reg_submit(ctx);
            return;
        case NFS4_REG_STALE:
            chimera_nfs_error("NFS4 registration: allocator answered a reservation "
                              "that no longer exists");
            free(ctx);
            return;
        case NFS4_REG_COMMITTED:
        default:
            nfs4_reg_complete(ctx, &out);
            return;
    } /* switch */
} /* nfs4_reg_alloc_done */

static void
nfs4_reg_submit(struct nfs4_reg_ctx *ctx)
{
    struct nfs4_reg_outcome out;
    int                     rc;

    rc = ctx->shared->clientid_alloc ?
        clientid_allocator_allocate_async(ctx->shared->clientid_alloc,
                                          ctx->thread->evpl,
                                          nfs4_reg_alloc_done, ctx) : EIO;
    if (rc == 0) {
        return;
    }

    /* Rejected before acceptance: no callback will come. */
    if (nfs4_client_reg_commit(&ctx->shared->nfs4_shared_clients, ctx->owner,
                               ctx->owner_len, ctx->token, rc, 0, &out) ==
        NFS4_REG_STALE) {
        free(ctx);
        return;
    }
    nfs4_reg_complete(ctx, &out);
} /* nfs4_reg_submit */

void
nfs4_registration_run(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop,
    const struct nfs4_reg_args       *args)
{
    struct nfs4_reg_waiter  self = { thread, req, argop, resop };
    struct nfs4_reg_outcome out;
    struct nfs4_reg_ctx    *ctx;
    uint64_t                token = 0;

    /* Allocated up front so that, once the owner is reserved, nothing can
     * fail before the allocator owns the request. */
    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        memset(&out, 0, sizeof(out));
        out.scid.status = NFS4ERR_DELAY;
        out.eid.status  = NFS4ERR_DELAY;
        nfs4_reg_reply_one(&self, args->kind, &out, true);
        return;
    }

    switch (nfs4_client_reg_begin(&thread->shared->nfs4_shared_clients, args, &self,
                                  &out, &token)) {
        case NFS4_REG_WAITING:
            free(ctx);
            return;
        case NFS4_REG_DONE:
            free(ctx);
            nfs4_reg_teardown(thread, args->kind, &out);
            nfs4_reg_reply_one(&self, args->kind, &out, true);
            return;
        case NFS4_REG_ALLOCATE:
        default:
            break;
    } /* switch */

    ctx->shared    = thread->shared;
    ctx->thread    = thread;
    ctx->kind      = args->kind;
    ctx->token     = token;
    ctx->owner_len = args->owner_len;
    memcpy(ctx->owner, args->owner, args->owner_len);

    nfs4_reg_submit(ctx);
} /* nfs4_registration_run */
