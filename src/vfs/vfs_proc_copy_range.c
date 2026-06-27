// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
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
 * EOF.  Both handles are served by the same module (the protocol layer only
 * issues same-module copies), so this never crosses backends.
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

static void
chimera_vfs_copy_fallback_finish(
    struct chimera_vfs_copy_fallback *ctx,
    enum chimera_vfs_error            error_code)
{
    chimera_vfs_copy_range_callback_t callback     = ctx->callback;
    void                             *private_data = ctx->private_data;
    uint64_t                          copied       = ctx->copied;
    struct chimera_vfs_attrs          pre          = ctx->r_pre_attr;
    struct chimera_vfs_attrs          post         = ctx->r_post_attr;

    free(ctx);

    callback(error_code, copied, &pre, &post, private_data);
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

    /* Capture destination pre-attrs from the first write, post-attrs from the
     * most recent one, mirroring a native copy_range's reporting. */
    if (ctx->copied == 0 && pre_attr) {
        ctx->r_pre_attr = *pre_attr;
    }
    if (post_attr) {
        ctx->r_post_attr = *post_attr;
    }

    ctx->copied     += length;
    ctx->src_offset += length;
    ctx->dst_offset += length;
    ctx->remaining  -= length;

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

    chimera_vfs_write(
        ctx->thread,
        &ctx->cred, NULL,
        ctx->dst_handle,
        ctx->dst_offset,
        count,
        1, /* stable: copy completes durably as a native copy_range would */
        ctx->copied == 0 ? CHIMERA_VFS_ATTR_MASK_STAT : 0,
        ctx->post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE,
        ctx->hold_iov,
        ctx->hold_niov,
        chimera_vfs_copy_fallback_write_cb,
        ctx);
} /* chimera_vfs_copy_fallback_read_cb */

static void
chimera_vfs_copy_fallback_step(struct chimera_vfs_copy_fallback *ctx)
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

    chimera_vfs_read(
        ctx->thread,
        &ctx->cred, NULL,
        ctx->src_handle,
        ctx->src_offset,
        chunk,
        ctx->read_iov,
        CHIMERA_VFS_COPY_FALLBACK_IOV,
        0,
        chimera_vfs_copy_fallback_read_cb,
        ctx);
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
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_copy_fallback *ctx;

    ctx = calloc(1, sizeof(*ctx));

    if (unlikely(!ctx)) {
        callback(CHIMERA_VFS_EIO, 0, NULL, NULL, private_data);
        return;
    }

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
chimera_vfs_copy_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    /* A storage module without native server-side copy still gets working copy
     * semantics via a generic read/write streaming fallback.
     *
     * The fallback assumes the standard write contract: the backend borrows the
     * supplied iovecs and the caller releases them after the write completes.
     * The NFS proxy module breaks that contract — it zero-copy-marshals (moves)
     * the write iovecs onto the upstream RPC, consuming them — so the fallback
     * cannot drive it safely.  A proxy doing server-side copy by round-tripping
     * every byte to its upstream would also defeat the point, so the proxy keeps
     * surfacing ENOTSUP (its prior behaviour) and lets the client copy. */
    if (!(dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE)) {
        if (dst_handle->vfs_module->fh_magic == CHIMERA_VFS_FH_MAGIC_NFS ||
            src_handle->vfs_module->fh_magic == CHIMERA_VFS_FH_MAGIC_NFS) {
            callback(CHIMERA_VFS_ENOTSUP, 0, NULL, NULL, private_data);
            return;
        }
        chimera_vfs_copy_range_fallback(thread, cred, src_handle, src_offset,
                                        dst_handle, dst_offset, length,
                                        pre_attr_mask, post_attr_mask,
                                        callback, private_data);
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
    request->copy_range.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->copy_range.r_pre_attr.va_set_mask  = 0;
    request->copy_range.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->copy_range.r_post_attr.va_set_mask = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_copy_range */
