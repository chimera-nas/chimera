// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "vfs/vfs_compound.h"

/* One RPC owns one compound and its private attempt state. Nothing in an
 * operation callback sends a reply or releases the RPC's payload. */
struct nfs3_compound {
    struct nfs_request            *req;
    struct chimera_vfs_compound   *compound;
    int                            result;
    nfsstat3                       protocol_status;
    uint32_t                       arena_mark;
    unsigned int                   retries;
    struct chimera_vfs_compound_op empty;
};

static inline void
nfs3_compound_reset(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound *ctx = private_data;

    ctx->protocol_status = NFS3_OK;
} // nfs3_compound_reset

static inline struct nfs3_compound *
nfs3_compound_alloc(
    struct nfs_request *req,
    unsigned int        open_flags)
{
    struct nfs3_compound *ctx = calloc(1, sizeof(*ctx));

    chimera_nfs_abort_if(!ctx, "NFS3 compound context allocation failed");
    ctx->req      = req;
    ctx->result   = -1;
    ctx->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread, &req->cred);
    chimera_vfs_compound_add_putfh(ctx->compound, req->fh, req->fhlen);
    if (open_flags) {
        chimera_vfs_compound_add_open_current(ctx->compound, open_flags, 0);
    }
    chimera_vfs_compound_set_attempt_reset(ctx->compound, nfs3_compound_reset, ctx);
    return ctx;
} // nfs3_compound_alloc

static inline bool
nfs3_compound_retry(struct nfs3_compound *ctx)
{
    /* A finish rejection means the transaction adapter has aborted the
     * attempt. Ordinary operation EAGAIN must not replay a successful prefix. */
    if (chimera_vfs_compound_finish_status(ctx->compound) == CHIMERA_VFS_EAGAIN &&
        ctx->retries++ < 8) {
        return chimera_vfs_compound_retry(ctx->compound);
    }
    return false;
} // nfs3_compound_retry

static inline nfsstat3
nfs3_compound_status(struct nfs3_compound *ctx)
{
    if (chimera_vfs_compound_finish_status(ctx->compound) == CHIMERA_VFS_OK &&
        ctx->protocol_status != NFS3_OK) {
        return ctx->protocol_status;
    }
    return chimera_vfs_error_to_nfsstat3(chimera_vfs_compound_status(ctx->compound));
} // nfs3_compound_status

static inline const struct chimera_vfs_compound_op *
nfs3_compound_result(struct nfs3_compound *ctx)
{
    /* Rejected finish results must not leak tentative attributes or data. */
    if (ctx->result < 0 || chimera_vfs_compound_finish_status(ctx->compound) != CHIMERA_VFS_OK) {
        return &ctx->empty;
    }
    return chimera_vfs_compound_op(ctx->compound, ctx->result);
} // nfs3_compound_result

static inline void
nfs3_compound_free(struct nfs3_compound *ctx)
{
    chimera_vfs_compound_free(ctx->compound);
    free(ctx);
} // nfs3_compound_free
