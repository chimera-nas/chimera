// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFS3 has no ALLOCATE/DEALLOCATE on the wire, but both are emulatable with
 * the calls it does have, which keeps posix_fallocate() and hole punching
 * usable over the proxy instead of failing ENOTSUP:
 *
 *  - ALLOCATE (posix_fallocate: ensure [offset, offset+length) is backed) is
 *    a size extension when the range reaches past EOF and a no-op otherwise.
 *    GETATTR for the current size, then SETATTR size = offset+length if that
 *    grows the file.  Blocks are not really reserved -- no NFS3 client can do
 *    that -- but the visible POSIX contract (size, readable zeros) holds.
 *
 *  - DEALLOCATE (FALLOC_FL_PUNCH_HOLE | KEEP_SIZE) is zero-writes over the
 *    punched range, clamped to EOF (KEEP_SIZE never extends).  A real hole
 *    and written zeros are indistinguishable through the proxy: NFS3 has no
 *    SEEK_HOLE on the wire, so holes are unobservable client-side anyway.
 *
 * I/O goes out with the opening credential when the handle has one, exactly
 * like read/write (see nfs3_open_state.h).
 */

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

#define CHIMERA_NFS3_ALLOCATE_ZERO_CHUNK (64 * 1024)

struct chimera_nfs3_allocate_ctx {
    struct chimera_nfs_thread *thread;
    struct chimera_nfs_shared *shared;
    uint64_t                   cur;        /* next zero-write offset      */
    uint64_t                   end;        /* zeroing end (EOF-clamped)   */
    struct evpl_iovec          zero;       /* zeroed chunk; consumed by send */
};

static void chimera_nfs3_allocate_send_write(
    struct chimera_vfs_request *request);

static const struct chimera_vfs_cred *
chimera_nfs3_allocate_cred(struct chimera_vfs_request *request)
{
    struct chimera_nfs3_open_state *state =
        (struct chimera_nfs3_open_state *) request->allocate.handle->vfs_private;

    return (state && state->open_cred_valid) ? &state->open_cred :
           request->cred;
} /* chimera_nfs3_allocate_cred */

static void
chimera_nfs3_allocate_done(
    struct chimera_vfs_request *request,
    enum chimera_vfs_error      status)
{
    request->status = status;
    request->complete(request);
} /* chimera_nfs3_allocate_done */

static void
chimera_nfs3_allocate_write_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct WRITE3res            *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request       *request = private_data;
    struct chimera_nfs3_allocate_ctx *ctx     = request->plugin_data;

    if (unlikely(status)) {
        chimera_nfs3_allocate_done(request, CHIMERA_VFS_EFAULT);
        return;
    }

    if (res->status != NFS3_OK) {
        chimera_nfs3_allocate_done(request,
                                   nfs3_client_status_to_chimera_vfs_error(
                                       res->status));
        return;
    }

    ctx->cur += res->resok.count;

    if (res->resok.count == 0 || ctx->cur >= ctx->end) {
        chimera_nfs3_allocate_done(request, CHIMERA_VFS_OK);
        return;
    }

    chimera_nfs3_allocate_send_write(request);
} /* chimera_nfs3_allocate_write_callback */

static void
chimera_nfs3_allocate_send_write(struct chimera_vfs_request *request)
{
    struct chimera_nfs3_allocate_ctx        *ctx = request->plugin_data;
    struct chimera_nfs_client_server_thread *server_thread;
    struct WRITE3args                        args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint64_t                                 len;
    uint8_t                                 *fh;
    int                                      fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(ctx->thread,
                                                         request->fh,
                                                         request->fh_len);
    if (!server_thread) {
        chimera_nfs3_allocate_done(request, CHIMERA_VFS_ESTALE);
        return;
    }

    len = ctx->end - ctx->cur;
    if (len > CHIMERA_NFS3_ALLOCATE_ZERO_CHUNK) {
        len = CHIMERA_NFS3_ALLOCATE_ZERO_CHUNK;
    }

    /* One exactly-sized zero iovec per WRITE.  The XDR marshaller MOVES the
     * args iovecs into the outgoing message (marshall_WRITE3args ->
     * evpl_iovec_move), so send_call consumes this reference -- the caller
     * must not release it, and a trimmed stack copy of a longer shared
     * buffer would be illegal (the iovec canary trips). */
    if (evpl_iovec_alloc(ctx->thread->evpl, (unsigned int) len, 4096, 1, 0,
                         &ctx->zero) < 1) {
        chimera_nfs3_allocate_done(request, CHIMERA_VFS_EFAULT);
        return;
    }
    memset(ctx->zero.data, 0, len);

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.file.data.data = fh;
    args.file.data.len  = fhlen;
    args.offset         = ctx->cur;
    args.count          = (uint32_t) len;
    args.stable         = FILE_SYNC;
    args.data.iov       = &ctx->zero;
    args.data.niov      = 1;
    args.data.length    = (uint32_t) len;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, chimera_nfs3_allocate_cred(request),
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    ctx->shared->nfs_v3.send_call_NFSPROC3_WRITE(&ctx->shared->nfs_v3.rpc2,
                                                 ctx->thread->evpl,
                                                 server_thread->nfs_conn,
                                                 &rpc2_cred, &args, 1, 0,
                                                 NULL, 0, 0,
                                                 chimera_nfs3_allocate_write_callback,
                                                 request);
} /* chimera_nfs3_allocate_send_write */

static void
chimera_nfs3_allocate_setattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct SETATTR3res          *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        chimera_nfs3_allocate_done(request, CHIMERA_VFS_EFAULT);
        return;
    }

    if (res->status != NFS3_OK) {
        chimera_nfs3_get_wcc_data(&request->allocate.r_pre_attr,
                                  &request->allocate.r_post_attr,
                                  &res->resfail.obj_wcc);
        chimera_nfs3_allocate_done(request,
                                   nfs3_client_status_to_chimera_vfs_error(
                                       res->status));
        return;
    }

    chimera_nfs3_get_wcc_data(&request->allocate.r_pre_attr,
                              &request->allocate.r_post_attr,
                              &res->resok.obj_wcc);

    chimera_nfs3_allocate_done(request, CHIMERA_VFS_OK);
} /* chimera_nfs3_allocate_setattr_callback */

static void
chimera_nfs3_allocate_getattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct GETATTR3res          *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request              *request = private_data;
    struct chimera_nfs3_allocate_ctx        *ctx     = request->plugin_data;
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_vfs_attrs                 attr;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint64_t                                 size, target;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (unlikely(status)) {
        chimera_nfs3_allocate_done(request, CHIMERA_VFS_EFAULT);
        return;
    }

    if (res->status != NFS3_OK) {
        chimera_nfs3_allocate_done(request,
                                   nfs3_client_status_to_chimera_vfs_error(
                                       res->status));
        return;
    }

    attr.va_set_mask = 0;
    attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_STAT;
    chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes, &attr);
    size   = attr.va_size;
    target = request->allocate.offset + request->allocate.length;

    if (!(request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE)) {
        /* Allocation is a size extension or nothing. */
        if (target <= size) {
            chimera_nfs3_allocate_done(request, CHIMERA_VFS_OK);
            return;
        }

        struct chimera_vfs_attrs set_attr;
        struct SETATTR3args      args;

        set_attr.va_req_mask = 0;
        set_attr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        set_attr.va_size     = target;

        server_thread = chimera_nfs_thread_get_server_thread(ctx->thread,
                                                             request->fh,
                                                             request->fh_len);
        if (!server_thread) {
            chimera_nfs3_allocate_done(request, CHIMERA_VFS_ESTALE);
            return;
        }

        chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

        args.object.data.data = fh;
        args.object.data.len  = fhlen;
        args.guard.check      = 0;
        chimera_nfs_va_to_sattr3(&args.new_attributes, &set_attr);

        chimera_nfs_init_rpc2_cred(&rpc2_cred,
                                   chimera_nfs3_allocate_cred(request),
                                   request->thread->vfs->machine_name,
                                   request->thread->vfs->machine_name_len);

        ctx->shared->nfs_v3.send_call_NFSPROC3_SETATTR(
            &ctx->shared->nfs_v3.rpc2, ctx->thread->evpl,
            server_thread->nfs_conn, &rpc2_cred, &args, 0, 0, NULL, 0, 0,
            chimera_nfs3_allocate_setattr_callback, request);
        return;
    }

    /* Punch: zero [offset, offset+length) clamped to EOF (KEEP_SIZE). */
    ctx->cur = request->allocate.offset;
    ctx->end = target < size ? target : size;

    if (ctx->cur >= ctx->end) {
        chimera_nfs3_allocate_done(request, CHIMERA_VFS_OK);
        return;
    }

    chimera_nfs3_allocate_send_write(request);
} /* chimera_nfs3_allocate_getattr_callback */

void
chimera_nfs3_allocate(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread =
        chimera_nfs_thread_get_server_thread(thread, request->fh,
                                             request->fh_len);
    struct chimera_nfs3_allocate_ctx        *ctx;
    struct GETATTR3args                      args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    ctx         = request->plugin_data;
    ctx->thread = thread;
    ctx->shared = shared;

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.object.data.data = fh;
    args.object.data.len  = fhlen;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, chimera_nfs3_allocate_cred(request),
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v3.send_call_NFSPROC3_GETATTR(&shared->nfs_v3.rpc2,
                                              thread->evpl,
                                              server_thread->nfs_conn,
                                              &rpc2_cred, &args, 0, 0, NULL,
                                              0, 0,
                                              chimera_nfs3_allocate_getattr_callback,
                                              request);
} /* chimera_nfs3_allocate */
