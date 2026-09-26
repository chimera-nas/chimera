// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_claim.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

/*
 * Generic server-side copy fallback.
 *
 * A backend that does not implement CHIMERA_VFS_OP_COPY_RANGE (no
 * CHIMERA_VFS_CAP_COPY_RANGE) is still given working server-side copy semantics
 * by streaming the byte range through the backend's ordinary async read and
 * write ops: read a bounded slice from the source handle, write it to the
 * destination handle, repeat until the range is exhausted or the source hits
 * EOF.  Different modules use the same fallback through their ordinary I/O
 * interfaces, including NFS proxies whose WRITE sends clone borrowed payloads.
 */

/* Bound a single read/write hop.  At the 4 KiB minimum block size a 256 KiB hop
 * is 64 descriptors; the iov arrays carry headroom above that. */
#define CHIMERA_VFS_COPY_FALLBACK_CHUNK (256 * 1024)
#define CHIMERA_VFS_COPY_FALLBACK_IOV   128

struct chimera_vfs_copy_fallback {
    struct chimera_vfs_thread        *thread;
    struct chimera_vfs_cred           cred;
    struct chimera_vfs_open_handle   *src_handle;
    struct chimera_vfs_open_handle   *dst_handle;
    uint64_t                          src_offset;
    uint64_t                          dst_offset;
    uint64_t                          remaining;
    uint64_t                          copied;
    struct chimera_claim_actor        src_owner, dst_owner;
    struct chimera_vfs_io_view        src_view, dst_view;
    int                               stepping, step_pending, finished;
    enum chimera_vfs_error            finish_status;
    /* The last read came up short or reported EOF: the source has no more
     * bytes to give as of this copy, so the in-flight write is the final hop.
     * Without this the loop keeps reading -- and a same-file copy whose
     * destination writes extend the source then feeds on its own output,
     * copying the zeros it just materialized until the requested length is
     * exhausted. */
    int                               src_exhausted;
    uint32_t                          read_len;     /* current hop's ask */
    uint64_t                          post_attr_mask;
    struct chimera_vfs_attrs          r_pre_attr;
    struct chimera_vfs_attrs          r_post_attr;
    /* read_iov: the scatter array handed to read for backends that read into
     * caller memory (a CAP_READ_PROVIDES_BUFFERS backend ignores it and returns
     * its own refs instead).  hold_iov: a single contiguous buffer we allocate
     * and own, into which the read result is gathered before the write, so the
     * write's landing data has a lifetime independent of the backend's read
     * memory; released in the write callback. */
    struct evpl_iovec                 read_iov[CHIMERA_VFS_COPY_FALLBACK_IOV];
    struct evpl_iovec                 hold_iov[CHIMERA_VFS_COPY_FALLBACK_IOV];
    int                               hold_niov;
    chimera_vfs_copy_range_callback_t callback;
    void                             *private_data;
};

static void
chimera_vfs_copy_fallback_step(
    struct chimera_vfs_copy_fallback *ctx);

static int
chimera_vfs_copy_fallback_attr(
    struct chimera_vfs_attrs       *dst,
    const struct chimera_vfs_attrs *src)
{
    struct chimera_acl *acl = NULL;

    if ((src->va_set_mask & CHIMERA_VFS_ATTR_ACL) && src->va_acl) {
        size_t size = chimera_acl_size(src->va_acl->num_aces);
        acl = malloc(size);
        if (!acl) {
            return -1;
        }
        memcpy(acl, src->va_acl, size);
    }
    free(dst->va_acl);
    *dst        = *src;
    dst->va_acl = acl;
    return 0;
} /* chimera_vfs_copy_fallback_attr */

static void
chimera_vfs_copy_fallback_finish(
    struct chimera_vfs_copy_fallback *ctx,
    enum chimera_vfs_error            error_code)
{
    if (ctx->stepping) {
        ctx->finished      = 1;
        ctx->finish_status = error_code;
        return;
    }
    chimera_vfs_copy_range_callback_t callback     = ctx->callback;
    void                             *private_data = ctx->private_data;
    uint64_t                          copied       = ctx->copied;
    struct chimera_vfs_attrs          pre          = ctx->r_pre_attr;
    struct chimera_vfs_attrs          post         = ctx->r_post_attr;

    free(ctx);

    callback(error_code, copied, &pre, &post, private_data);
    free(pre.va_acl);
    free(post.va_acl);
} /* chimera_vfs_copy_fallback_finish */

static void
chimera_vfs_copy_fallback_write_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_copy_fallback *ctx = private_data;

    /* Drop the reference we held across the write. */
    if (ctx->hold_niov) {
        evpl_iovecs_release(ctx->thread->evpl, ctx->hold_iov, ctx->hold_niov);
        ctx->hold_niov = 0;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_copy_fallback_finish(ctx, error_code);
        return;
    }

    if (length != ctx->read_len) {
        chimera_vfs_copy_fallback_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    /* Capture destination pre-attrs from the first write, post-attrs from the
     * most recent one, mirroring a native copy_range's reporting. */
    if (ctx->copied == 0 && pre_attr) {
        if (chimera_vfs_copy_fallback_attr(&ctx->r_pre_attr, pre_attr)) {
            chimera_vfs_copy_fallback_finish(ctx, CHIMERA_VFS_ENOSPC);
            return;
        }
    }
    if (post_attr) {
        if (chimera_vfs_copy_fallback_attr(&ctx->r_post_attr, post_attr)) {
            chimera_vfs_copy_fallback_finish(ctx, CHIMERA_VFS_ENOSPC);
            return;
        }
    }

    ctx->copied     += length;
    ctx->src_offset += length;
    ctx->dst_offset += length;
    ctx->remaining  -= length;

    if (ctx->src_exhausted) {
        chimera_vfs_copy_fallback_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    chimera_vfs_copy_fallback_step(ctx);
} /* chimera_vfs_copy_fallback_write_cb */

static void
chimera_vfs_copy_fallback_read_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_copy_fallback *ctx = private_data;
    int                               n;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_copy_fallback_finish(ctx, error_code);
        return;
    }

    if (count == 0) {
        /* Source exhausted (hole at/after EOF): nothing more to copy. */
        if (niov) {
            evpl_iovecs_release(ctx->thread->evpl, iov, niov);
        }
        chimera_vfs_copy_fallback_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    /* A short or EOF-flagged read is the source's EOF as of this copy: write
     * what it gave and stop there (copy_file_range(2) semantics).  Reading on
     * regardless is how a same-file copy came to copy its own freshly written
     * zeros. */
    if (eof || (uint64_t) count < ctx->read_len) {
        ctx->src_exhausted = 1;
    }

    /* Copy the read result into a single freshly allocated buffer we fully own.
     * The read result may be the backend's own memory (CAP_READ_PROVIDES_BUFFERS,
     * e.g. an NFS-proxy reply) whose lifetime ends with the read and cannot
     * survive the async write, so an owned copy is the only safe carrier.  count
     * is bounded by CHIMERA_VFS_COPY_FALLBACK_CHUNK, so a contiguous allocation
     * is always a single iovec. */
    n = evpl_iovec_alloc(ctx->thread->evpl, count, 0, 1, 0, ctx->hold_iov);

    if (unlikely(n != 1)) {
        if (n > 0) {
            evpl_iovecs_release(ctx->thread->evpl, ctx->hold_iov, n);
        }
        evpl_iovecs_release(ctx->thread->evpl, iov, niov);
        chimera_vfs_copy_fallback_finish(ctx, CHIMERA_VFS_EIO);
        return;
    }

    ctx->read_len           = count;
    ctx->hold_niov          = 1;
    ctx->hold_iov[0].length = count;

    /* Gather the (possibly scattered) read data into the owned buffer. */
    {
        uint8_t *dst       = ctx->hold_iov[0].data;
        uint32_t remaining = count;
        for (int si = 0; si < niov && remaining; si++) {
            uint32_t take = iov[si].length;
            if (take > remaining) {
                take = remaining;
            }
            memcpy(dst, iov[si].data, take);
            dst       += take;
            remaining -= take;
        }
    }

    /* Release the backend's read buffers now that the data is copied out. */
    evpl_iovecs_release(ctx->thread->evpl, iov, niov);

    chimera_vfs_write_view(
        ctx->thread,
        &ctx->cred,
        ctx->dst_handle,
        ctx->dst_offset,
        count,
        1, /* stable: copy completes durably as a native copy_range would */
        ctx->copied == 0 ? ctx->r_pre_attr.va_req_mask : 0,
        ctx->post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE,
        ctx->hold_iov,
        ctx->hold_niov,
        &ctx->dst_view,
        chimera_vfs_copy_fallback_write_cb,
        ctx);
} /* chimera_vfs_copy_fallback_read_cb */

static void
chimera_vfs_copy_fallback_step_once(struct chimera_vfs_copy_fallback *ctx)
{
    uint32_t chunk;

    if (ctx->remaining == 0) {
        chimera_vfs_copy_fallback_finish(ctx, CHIMERA_VFS_OK);
        return;
    }

    chunk = ctx->remaining > CHIMERA_VFS_COPY_FALLBACK_CHUNK
            ? CHIMERA_VFS_COPY_FALLBACK_CHUNK
            : (uint32_t) ctx->remaining;

    ctx->hold_niov = 0;
    ctx->read_len  = chunk;

    chimera_vfs_read_view(
        ctx->thread,
        &ctx->cred,
        ctx->src_handle,
        ctx->src_offset,
        chunk,
        ctx->read_iov,
        CHIMERA_VFS_COPY_FALLBACK_IOV,
        0,
        &ctx->src_view,
        chimera_vfs_copy_fallback_read_cb,
        ctx);
} /* chimera_vfs_copy_fallback_step_once */

/* A synchronous backend may complete both hops inline.  Defer continuation
 * until their callbacks unwind instead of consuming stack per copied chunk.
 * Completion is also deferred while driving, so no callback frees ctx while
 * this loop still owns it. Async completions resume through the same entry. */
static void
chimera_vfs_copy_fallback_step(struct chimera_vfs_copy_fallback *ctx)
{
    ctx->step_pending = 1;
    if (ctx->stepping) {
        return;
    }
    ctx->stepping = 1;
    while (ctx->step_pending && !ctx->finished) {
        ctx->step_pending = 0;
        chimera_vfs_copy_fallback_step_once(ctx);
    }
    ctx->stepping = 0;
    if (ctx->finished) {
        chimera_vfs_copy_fallback_finish(ctx, ctx->finish_status);
    }
} /* chimera_vfs_copy_fallback_step */

static void
chimera_vfs_copy_range_fallback(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_vfs_io_view *src_view,
    const struct chimera_vfs_io_view *dst_view,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_copy_fallback *ctx;

    ctx = calloc(1, sizeof(*ctx));

    if (unlikely(!ctx)) {
        callback(CHIMERA_VFS_EIO, 0, NULL, NULL, private_data);
        return;
    }

    chimera_vfs_io_view_copy(&ctx->src_view, &ctx->src_owner, src_view);
    chimera_vfs_io_view_copy(&ctx->dst_view, &ctx->dst_owner, dst_view);
    ctx->thread         = thread;
    ctx->cred           = *cred;
    ctx->src_handle     = src_handle;
    ctx->dst_handle     = dst_handle;
    ctx->src_offset     = src_offset;
    ctx->dst_offset     = dst_offset;
    ctx->remaining      = length;
    ctx->copied         = 0;
    ctx->post_attr_mask = post_attr_mask;
    ctx->callback       = callback;
    ctx->private_data   = private_data;

    ctx->r_pre_attr.va_req_mask  = pre_attr_mask;
    ctx->r_pre_attr.va_set_mask  = 0;
    ctx->r_post_attr.va_req_mask = post_attr_mask;
    ctx->r_post_attr.va_set_mask = 0;

    chimera_vfs_copy_fallback_step(ctx);
} /* chimera_vfs_copy_range_fallback */

static void
chimera_vfs_copy_range_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_copy_range_callback_t callback = request->proto_callback;

    chimera_vfs_io_claim_release(request);

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread, request->thread->vfs->vfs_attr_cache,
                                      request->copy_range.dst_handle->fh_hash,
                                      request->copy_range.dst_handle->fh,
                                      request->copy_range.dst_handle->fh_len,
                                      &request->copy_range.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->copy_range.r_length,
             &request->copy_range.r_pre_attr,
             &request->copy_range.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_copy_range_complete */

SYMBOL_EXPORT void
chimera_vfs_copy_range_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_claim_actor *src_owner,
    const struct chimera_claim_actor *dst_owner,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_io_view src_view = { .owner = src_owner };
    struct chimera_vfs_io_view dst_view = { .owner = dst_owner };

    chimera_vfs_copy_range_view(thread, cred, src_handle, src_offset,
                                dst_handle, dst_offset, length, flags,
                                pre_attr_mask, post_attr_mask,
                                &src_view, &dst_view, callback, private_data);
} /* chimera_vfs_copy_range_owned */

SYMBOL_EXPORT void
chimera_vfs_copy_range_view(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_vfs_io_view *src_view,
    const struct chimera_vfs_io_view *dst_view,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    /* A storage module without native server-side copy uses bounded read/write
     * streaming. This includes NFS proxies: their RPC writes clone the borrowed
     * input references, and the fallback retains its own gathered read buffer
     * until the write callback releases it. */
    /* Anonymous or scoped views traverse ordinary read/write gates. A native
     * owned copy uses the same WRITE invalidation gate at its destination;
     * the supplied source actor already represents its read-side claim. */
    if (chimera_vfs_pnfs_io_possible(thread, dst_handle) ||
        chimera_vfs_pnfs_io_possible(thread, src_handle) ||
        !src_view || !src_view->owner || !dst_view || !dst_view->owner ||
        src_view->num_excluded || dst_view->num_excluded ||
        src_handle->vfs_module != dst_handle->vfs_module ||
        !(dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE)) {
        chimera_vfs_copy_range_fallback(thread, cred, src_handle, src_offset,
                                        dst_handle, dst_offset, length,
                                        pre_attr_mask, post_attr_mask,
                                        src_view, dst_view, callback, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, dst_handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, NULL, NULL, private_data);
        return;
    }

    request->opcode                             = CHIMERA_VFS_OP_COPY_RANGE;
    request->complete                           = chimera_vfs_copy_range_complete;
    request->copy_range.src_handle              = src_handle;
    request->copy_range.dst_handle              = dst_handle;
    request->copy_range.src_offset              = src_offset;
    request->copy_range.dst_offset              = dst_offset;
    request->copy_range.length                  = length;
    request->copy_range.r_length                = 0;
    request->copy_range.flags                   = flags;
    request->copy_range.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->copy_range.r_pre_attr.va_set_mask  = 0;
    request->copy_range.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->copy_range.r_post_attr.va_set_mask = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    request->io_handle = dst_handle;
    chimera_vfs_io_view_copy(&request->io_view, &request->io_owner, dst_view);
    request->io_owner_valid = request->io_view.owner != NULL;
    chimera_vfs_io_claim_acquire(request, request->io_view.owner, chimera_vfs_dispatch);
} /* chimera_vfs_copy_range */

SYMBOL_EXPORT void
chimera_vfs_copy_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data)
{
    chimera_vfs_copy_range_owned(thread, cred, src_handle, src_offset,
                                 dst_handle, dst_offset, length, flags, pre_attr_mask, post_attr_mask,
                                 NULL, NULL, callback, private_data);
} /* chimera_vfs_copy_range */
