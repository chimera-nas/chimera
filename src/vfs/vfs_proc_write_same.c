// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

/*
 * WRITE_SAME (RFC 7862) expands a small Application Data Block pattern into a
 * run of identical blocks.  A backend that advertises CHIMERA_VFS_CAP_WRITE_SAME
 * does this natively (and can be smartest -- e.g. diskfs routes an all-zero
 * pattern to a hole-punch, O(metadata)).  A backend WITHOUT the cap still gets
 * the operation's main benefit -- the pattern is not sent over the wire, the
 * server expands it -- via this generic fallback: build ONE block-size template
 * buffer and issue ordinary writes whose iovec array is N refcounted clones of
 * that single buffer.  No data is duplicated in memory (the clones share the
 * one buffer) and nothing crosses the wire; the only cost not saved is handing
 * the full-size write to the backend.
 *
 * (A further refinement, left to the native backends which already do it, is to
 * map an all-zero pattern to allocate/punch for a metadata-only result.)
 */

/* Bound a single fallback write: at most this many cloned blocks, and at most
 * this many bytes, per hop. */
#define CHIMERA_VFS_WS_FALLBACK_IOV   128
#define CHIMERA_VFS_WS_FALLBACK_BYTES (256 * 1024)

struct chimera_vfs_write_same_fallback {
    struct chimera_vfs_thread        *thread;
    struct chimera_vfs_cred           cred;
    struct chimera_vfs_open_handle   *handle;
    uint64_t                          offset;       /* next write offset */
    uint64_t                          remaining;    /* bytes left (block multiple) */
    uint64_t                          written;
    uint32_t                          block_size;
    uint32_t                          sync;
    uint32_t                          r_sync;
    uint64_t                          post_attr_mask;
    struct chimera_vfs_attrs          r_pre_attr;
    struct chimera_vfs_attrs          r_post_attr;
    /* One block-size template (zero-filled + pattern at reloff); each write
     * borrows N refcounted clones of it. */
    struct evpl_iovec                 tmpl;
    struct evpl_iovec                 chunk_iov[CHIMERA_VFS_WS_FALLBACK_IOV];
    int                               chunk_niov;
    chimera_vfs_write_same_callback_t callback;
    void                             *private_data;
};

static void
chimera_vfs_write_same_fallback_step(
    struct chimera_vfs_write_same_fallback *ctx);

static void
chimera_vfs_write_same_fallback_finish(
    struct chimera_vfs_write_same_fallback *ctx,
    enum chimera_vfs_error                  error_code)
{
    chimera_vfs_write_same_callback_t callback     = ctx->callback;
    void                             *private_data = ctx->private_data;
    uint64_t                          written      = ctx->written;
    uint32_t                          r_sync       = ctx->r_sync;
    struct chimera_vfs_attrs          pre          = ctx->r_pre_attr;
    struct chimera_vfs_attrs          post         = ctx->r_post_attr;

    evpl_iovec_release(ctx->thread->evpl, &ctx->tmpl);
    free(ctx);

    callback(error_code, written, r_sync, &pre, &post, private_data);
} /* chimera_vfs_write_same_fallback_finish */

static void
chimera_vfs_write_same_fallback_write_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_write_same_fallback *ctx = private_data;

    /* Drop the per-write clones; the template's own ref persists for the next
     * hop (and is released in finish). */
    if (ctx->chunk_niov) {
        evpl_iovecs_release(ctx->thread->evpl, ctx->chunk_iov, ctx->chunk_niov);
        ctx->chunk_niov = 0;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_write_same_fallback_finish(ctx, error_code);
        return;
    }

    if (ctx->written == 0 && pre_attr) {
        ctx->r_pre_attr = *pre_attr;
    }
    if (post_attr) {
        ctx->r_post_attr = *post_attr;
    }
    ctx->r_sync     = sync;
    ctx->written   += length;
    ctx->offset    += length;
    ctx->remaining -= length;

    chimera_vfs_write_same_fallback_step(ctx);
} /* chimera_vfs_write_same_fallback_write_cb */

static void
chimera_vfs_write_same_fallback_step(struct chimera_vfs_write_same_fallback *ctx)
{
    uint64_t blocks, k;
    uint32_t chunk;

    if (ctx->remaining == 0) {
        chimera_vfs_write_same_fallback_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    /* Clone as many whole blocks as fit the per-hop iovec and byte budgets. */
    blocks = ctx->remaining / ctx->block_size;
    k      = blocks;
    if (k > CHIMERA_VFS_WS_FALLBACK_IOV) {
        k = CHIMERA_VFS_WS_FALLBACK_IOV;
    }
    if (k * ctx->block_size > CHIMERA_VFS_WS_FALLBACK_BYTES) {
        k = CHIMERA_VFS_WS_FALLBACK_BYTES / ctx->block_size;
    }
    if (k == 0) {
        k = 1;          /* a single block larger than the byte budget */
    }

    for (uint64_t i = 0; i < k; i++) {
        evpl_iovec_clone(&ctx->chunk_iov[i], &ctx->tmpl);
    }
    ctx->chunk_niov = (int) k;
    chunk           = (uint32_t) (k * ctx->block_size);

    chimera_vfs_write(
        ctx->thread,
        &ctx->cred,
        ctx->handle,
        ctx->offset,
        chunk,
        ctx->sync,
        ctx->written == 0 ? CHIMERA_VFS_ATTR_MASK_STAT : 0,
        ctx->post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE,
        ctx->chunk_iov,
        ctx->chunk_niov,
        chimera_vfs_write_same_fallback_write_cb,
        ctx);
} /* chimera_vfs_write_same_fallback_step */

static void
chimera_vfs_write_same_fallback(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          block_size,
    uint64_t                          block_count,
    const void                       *pattern,
    uint32_t                          pattern_len,
    uint32_t                          reloff_pattern,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_write_same_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_write_same_fallback *ctx;
    int                                     n;

    if (block_size == 0 || block_count == 0) {
        callback(CHIMERA_VFS_OK, 0, sync, NULL, NULL, private_data);
        return;
    }

    ctx = calloc(1, sizeof(*ctx));
    if (unlikely(!ctx)) {
        callback(CHIMERA_VFS_EIO, 0, 0, NULL, NULL, private_data);
        return;
    }

    /* One contiguous template block: zero-filled with the pattern at reloff. */
    n = evpl_iovec_alloc(thread->evpl, block_size, 4096, 1, 0, &ctx->tmpl);
    if (unlikely(n != 1)) {
        if (n > 0) {
            evpl_iovec_release(thread->evpl, &ctx->tmpl);
        }
        free(ctx);
        callback(CHIMERA_VFS_EIO, 0, 0, NULL, NULL, private_data);
        return;
    }
    ctx->tmpl.length = block_size;
    memset(ctx->tmpl.data, 0, block_size);
    if (pattern_len) {
        memcpy((uint8_t *) ctx->tmpl.data + reloff_pattern, pattern, pattern_len);
    }

    ctx->thread         = thread;
    ctx->cred           = *cred;
    ctx->handle         = handle;
    ctx->offset         = offset;
    ctx->remaining      = (uint64_t) block_size * block_count;
    ctx->written        = 0;
    ctx->block_size     = block_size;
    ctx->sync           = sync;
    ctx->r_sync         = sync;
    ctx->post_attr_mask = post_attr_mask;
    ctx->callback       = callback;
    ctx->private_data   = private_data;

    ctx->r_pre_attr.va_req_mask  = pre_attr_mask;
    ctx->r_pre_attr.va_set_mask  = 0;
    ctx->r_post_attr.va_req_mask = post_attr_mask;
    ctx->r_post_attr.va_set_mask = 0;

    chimera_vfs_write_same_fallback_step(ctx);
} /* chimera_vfs_write_same_fallback */

static void
chimera_vfs_write_same_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_write_same_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread,
                                      request->thread->vfs->vfs_attr_cache,
                                      request->write_same.handle->fh_hash,
                                      request->write_same.handle->fh,
                                      request->write_same.handle->fh_len,
                                      &request->write_same.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->write_same.r_count,
             request->write_same.r_sync,
             &request->write_same.r_pre_attr,
             &request->write_same.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_write_same_complete */

SYMBOL_EXPORT void
chimera_vfs_write_same(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          block_size,
    uint64_t                          block_count,
    const void                       *pattern,
    uint32_t                          pattern_len,
    uint32_t                          reloff_pattern,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_write_same_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    /* No native WRITE_SAME: expand the pattern in the VFS core and write it via
     * the backend's ordinary write path.  The NFS proxy is excluded -- its write
     * MOVES (consumes) the supplied iovecs onto the upstream RPC rather than
     * borrowing them, which the clone-and-release fallback cannot drive safely
     * (same reasoning as the copy_range fallback) -- so it keeps surfacing
     * ENOTSUP and lets the client send a plain WRITE. */
    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_WRITE_SAME)) {
        if (handle->vfs_module->fh_magic == CHIMERA_VFS_FH_MAGIC_NFS) {
            callback(CHIMERA_VFS_ENOTSUP, 0, 0, NULL, NULL, private_data);
            return;
        }
        chimera_vfs_write_same_fallback(thread, cred, handle, offset, block_size,
                                        block_count, pattern, pattern_len,
                                        reloff_pattern, sync, pre_attr_mask,
                                        post_attr_mask, callback, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, NULL, private_data);
        return;
    }

    request->opcode                             = CHIMERA_VFS_OP_WRITE_SAME;
    request->complete                           = chimera_vfs_write_same_complete;
    request->write_same.handle                  = handle;
    request->write_same.offset                  = offset;
    request->write_same.block_size              = block_size;
    request->write_same.block_count             = block_count;
    request->write_same.pattern                 = pattern;
    request->write_same.pattern_len             = pattern_len;
    request->write_same.reloff_pattern          = reloff_pattern;
    request->write_same.sync                    = sync;
    request->write_same.r_count                 = 0;
    request->write_same.r_sync                  = sync;
    request->write_same.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->write_same.r_pre_attr.va_set_mask  = 0;
    request->write_same.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->write_same.r_post_attr.va_set_mask = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_write_same */
