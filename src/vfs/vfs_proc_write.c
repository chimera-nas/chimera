// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include "vfs/vfs_procs.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_pnfs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "sdk/vfs_access.h"
#include "sdk/vfs_acl.h"
#include "common/macros.h"

static void
chimera_vfs_write_finish(struct chimera_vfs_request *request);

static void
chimera_vfs_write_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_pnfs_sync_mds(request, &request->write.r_post_attr,
                              request->write.offset + request->write.r_length,
                              chimera_vfs_write_finish);
} /* chimera_vfs_write_complete */

static void
chimera_vfs_write_finish(struct chimera_vfs_request *request)
{
    chimera_vfs_write_callback_t callback = request->proto_callback;

    /* Report the MDS inode's attributes, not the backing file's. */
    if (request->io_pnfs_backing && request->status == CHIMERA_VFS_OK) {
        uint64_t want = request->write.r_post_attr.va_req_mask;

        request->write.r_post_attr             = request->io_pnfs_sync_attr;
        request->write.r_post_attr.va_req_mask = want;
    }

    /* Release the implicit-claim pin taken before dispatch (no-op when none
     * was taken). */
    chimera_vfs_io_claim_release(request);

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread, request->thread->vfs->vfs_attr_cache,
                                      request->io_handle ?
                                      request->io_handle->fh_hash :
                                      request->write.handle->fh_hash,
                                      request->io_handle ?
                                      request->io_handle->fh :
                                      request->write.handle->fh,
                                      request->io_handle ?
                                      request->io_handle->fh_len :
                                      request->write.handle->fh_len,
                                      &request->write.r_post_attr);
    }

    chimera_vfs_complete(request);

    /* Drop the pNFS backing-file reference the redirect took (no-op when the
     * write was not redirected). */
    if (request->io_pnfs_backing) {
        chimera_vfs_release(request->thread, request->io_pnfs_backing);
        request->io_pnfs_backing = NULL;
    }

    callback(request->status,
             request->write.r_length,
             request->write.r_sync,
             &request->write.r_pre_attr,
             &request->write.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_write_finish */

static void
chimera_vfs_write_dispatch(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    struct chimera_vfs_open_handle   *io_handle,
    int                               redirected,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_write_callback_t      callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    /* Allocated against io_handle -- the DS backing file when this file is
     * DS-resident -- so the backend writes where the bytes actually live. */
    request = chimera_vfs_request_alloc_by_handle(thread, cred, io_handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        if (redirected) {
            chimera_vfs_release(thread, io_handle);
        }
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, NULL, private_data);
        return;
    }

    request->opcode       = CHIMERA_VFS_OP_WRITE;
    request->complete     = chimera_vfs_write_complete;
    request->write.handle = io_handle;
    request->io_pnfs_backing = redirected ? io_handle : NULL;
    /* Anchor the implicit claim on the cached handle (chimera_vfs_io_claim_acquire).
     * The CALLER-named handle even when redirected: leases and oplocks are held
     * against the MDS file, not the backing file its bytes live in. */
    request->io_handle                     = handle;
    request->write.offset                  = offset;
    request->write.length                  = count;
    request->write.sync                    = sync;
    request->write.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->write.r_pre_attr.va_set_mask  = 0;
    request->write.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->write.r_post_attr.va_set_mask = 0;
    request->write.iov                     = iov;
    request->write.niov                    = niov;
    request->proto_callback                = callback;
    request->proto_private_data            = private_data;

    /* Mediate the write through the claim layer (acquire/hold the implicit
     * claim for a leaseless actor, or break other holders' read caches for a
     * lease-holding client), then dispatch.  The caller's actor is copied
     * onto the request so the claim layer sees a stable address. */
    if (io_owner) {
        request->io_owner       = *io_owner;
        request->io_owner_valid = 1;
    }

    chimera_vfs_io_claim_acquire(request,
                                 io_owner ? &request->io_owner : NULL,
                                 chimera_vfs_dispatch);
} /* chimera_vfs_write_dispatch */

/* Carries the write arguments across the pNFS backing-handle resolve.  See the
 * read counterpart; the fast path keeps a non-pNFS write allocation-free. */
struct chimera_vfs_write_resolve_ctx {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        offset;
    uint32_t                        count;
    uint32_t                        sync;
    uint64_t                        pre_attr_mask;
    uint64_t                        post_attr_mask;
    struct evpl_iovec              *iov;
    int                             niov;
    bool                            has_io_owner;
    struct chimera_claim_actor      io_owner;
    chimera_vfs_write_callback_t    callback;
    void                           *private_data;
};

_Static_assert(sizeof(struct chimera_vfs_write_resolve_ctx) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "write resolve context outgrew the request gate scratch area");

static void
chimera_vfs_write_resolve_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *io_handle,
    int                             redirected,
    void                           *private_data)
{
    struct chimera_vfs_write_resolve_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        ctx->callback(error_code, 0, 0, NULL, NULL, ctx->private_data);
        chimera_vfs_gate_scratch_free(ctx->thread, ctx);
        return;
    }

    chimera_vfs_write_dispatch(ctx->thread, ctx->cred, ctx->handle,
                               io_handle, redirected,
                               ctx->offset, ctx->count, ctx->sync,
                               ctx->pre_attr_mask, ctx->post_attr_mask,
                               ctx->iov, ctx->niov,
                               ctx->has_io_owner ? &ctx->io_owner : NULL,
                               ctx->callback, ctx->private_data);
    chimera_vfs_gate_scratch_free(ctx->thread, ctx);
} /* chimera_vfs_write_resolve_complete */

static void
chimera_vfs_write_resolve(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_write_callback_t      callback,
    void                             *private_data)
{
    struct chimera_vfs_write_resolve_ctx *ctx;

    if (getenv("CHIMERA_VFS_WRITE_TRACE") && count > 0 && niov > 0 &&
        iov[0].data == NULL) {
        fprintf(stderr, "WRITETRACE: count=%u niov=%d but iov[0] is NULL/empty (len=%u)\n",
                count, niov, iov[0].length);
    }

    if (!chimera_vfs_pnfs_io_possible(thread, handle)) {
        chimera_vfs_write_dispatch(thread, cred, handle, handle, 0,
                                   offset, count, sync, pre_attr_mask,
                                   post_attr_mask, iov, niov, io_owner,
                                   callback, private_data);
        return;
    }

    ctx = chimera_vfs_gate_scratch_alloc(thread);

    ctx->thread         = thread;
    ctx->cred           = cred;
    ctx->handle         = handle;
    ctx->offset         = offset;
    ctx->count          = count;
    ctx->sync           = sync;
    ctx->pre_attr_mask  = pre_attr_mask;
    ctx->post_attr_mask = post_attr_mask;
    ctx->iov            = iov;
    ctx->niov           = niov;
    if (io_owner) {
        ctx->has_io_owner = true;
        ctx->io_owner     = *io_owner;
    } else {
        ctx->has_io_owner = false;
    }
    ctx->callback     = callback;
    ctx->private_data = private_data;

    chimera_vfs_pnfs_resolve_io(thread, cred, handle, 1,
                                chimera_vfs_write_resolve_complete, ctx);
} /* chimera_vfs_write_resolve */

/* Continuation for the first gated write on a handle (see read counterpart).
 * io_owner is copied by value (not by pointer) because the callback fires
 * after an async getattr has returned to the event loop -- the SMB caller's
 * stack frame that owned the original io_owner is gone by then.  Both the
 * has_io_owner flag and the copy let the dispatch tail re-emit a const-
 * pointer to a stable address. */
struct chimera_vfs_write_gate {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        offset;
    uint32_t                        count;
    uint32_t                        sync;
    uint64_t                        pre_attr_mask;
    uint64_t                        post_attr_mask;
    struct evpl_iovec              *iov;
    int                             niov;
    bool                            has_io_owner;
    struct chimera_claim_actor      io_owner;
    chimera_vfs_write_callback_t    callback;
    void                           *private_data;
};

_Static_assert(sizeof(struct chimera_vfs_write_gate) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "write gate context outgrew the request gate scratch area");

static void
chimera_vfs_write_gate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_write_gate *gate = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        gate->callback(error_code, 0, 0, NULL, NULL, gate->private_data);
        chimera_vfs_gate_scratch_free(gate->thread, gate);
        return;
    }

    uint32_t                       granted = chimera_vfs_access_check(attr, gate->cred,
                                                                      CHIMERA_ACE_MASK_ALL);

    /* Owner override, as Linux nfsd applies to READ/WRITE (NFSD_MAY_OWNER_
     * OVERRIDE): a POSIX caller who OWNS the file may move its data through
     * an already-open descriptor regardless of the current mode bits --
     * POSIX binds I/O rights at open(2), and stateless per-op re-checking
     * would otherwise break every open-then-chmod sequence.  Only for
     * credentials the protocol server stamped as stateless remote callers
     * (the NFS server; see CHIMERA_VFS_CRED_FLAG_OWNER_OVERRIDE): a local
     * caller's opens are visible, so strict POSIX evaluation stands, and an
     * SMB caller's rights come from the NT security descriptor, where
     * ownership does not imply data access. */
    if ((gate->cred->flags & CHIMERA_VFS_CRED_FLAG_OWNER_OVERRIDE) &&
        gate->cred->flavor == CHIMERA_VFS_AUTH_UNIX &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
        gate->cred->uid == attr->va_uid) {
        granted |= CHIMERA_ACE_READ_DATA |
            CHIMERA_ACE_WRITE_DATA;
    }

    /* The stamp on a handle records ONE credential's effective access -- the
     * cache shards handles per (fh, access_mode, cred_hash), so for a
     * cache-acquired handle every consumer shares that credential.  A
     * stateful protocol server's handle (the NFSv4 open-state handle) is the
     * exception: one handle serves every principal of the open-owner, so a
     * different caller must evaluate for itself and must NOT overwrite the
     * stamp -- a non-owner's narrow evaluation would otherwise revoke the
     * opener's own bound rights. */
    if (!gate->handle->granted_bound &&
        gate->handle->cred_hash == chimera_vfs_cred_hash(gate->cred)) {
        gate->handle->granted_access = granted;
        gate->handle->granted_valid  = 1;
    }

    if (!(granted & CHIMERA_ACE_WRITE_DATA)) {
        gate->callback(CHIMERA_VFS_EACCES, 0, 0, NULL, NULL, gate->private_data);
        chimera_vfs_gate_scratch_free(gate->thread, gate);
        return;
    }

    chimera_vfs_write_resolve(gate->thread, gate->cred, gate->handle,
                              gate->offset, gate->count, gate->sync,
                              gate->pre_attr_mask, gate->post_attr_mask,
                              gate->iov, gate->niov,
                              gate->has_io_owner ? &gate->io_owner : NULL,
                               gate->callback, gate->private_data);
    chimera_vfs_gate_scratch_free(gate->thread, gate);
} /* chimera_vfs_write_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_write_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_write_callback_t      callback,
    void                             *private_data)
{
    struct chimera_vfs_write_gate *gate;

    if (getenv("CHIMERA_VFS_WRITE_TRACE") && count > 0 && niov > 0 &&
        iov[0].data == NULL) {
        Dl_info dli;
        void   *ra = __builtin_return_address(0);

        if (dladdr(ra, &dli) && dli.dli_sname) {
            fprintf(stderr, "WRITETRACE: NULL payload count=%u caller=%s (%s)\n",
                    count, dli.dli_sname,
                    dli.dli_fname ? dli.dli_fname : "?");
        } else {
            fprintf(stderr, "WRITETRACE: NULL payload count=%u ra=%p unresolved\n",
                    count, ra);
        }
    }

    /* gate_needed_dac, not gate_needed: a DELEGATES_DAC passthrough backend
     * resolves the object with open_by_handle_at (privileged) and caches the
     * resulting fd, so the kernel never checks WRITE under the caller's fsuid --
     * the "kernel enforces the leaf" assumption does not hold for cached handle
     * opens.  The engine must enforce write access itself against the real
     * on-disk attrs, exactly as it already does for the path prefix. */
    if (chimera_vfs_gate_needed_dac(handle->vfs_module->capabilities, cred)) {
        if (handle->granted_valid && handle->granted_bound) {
            /* Fast path only for an OPEN-bound grant (NFSv4/SMB bind write
             * rights at OPEN).  An unbound grant computed for a stateless
             * (NFSv3) caller is never revalidated on a mode/ACL change, so an
             * unbound handle re-gates against current attrs on every write --
             * see the matching note in vfs_proc_read.c. */
            if (!(handle->granted_access & CHIMERA_ACE_WRITE_DATA)) {
                callback(CHIMERA_VFS_EACCES, 0, 0, NULL, NULL, private_data);
                return;
            }
        } else {
            gate = chimera_vfs_gate_scratch_alloc(thread);

            gate->thread         = thread;
            gate->cred           = cred;
            gate->handle         = handle;
            gate->offset         = offset;
            gate->count          = count;
            gate->sync           = sync;
            gate->pre_attr_mask  = pre_attr_mask;
            gate->post_attr_mask = post_attr_mask;
            gate->iov            = iov;
            gate->niov           = niov;
            if (io_owner) {
                /* Deep copy: the caller's io_owner is a stack variable that
                 * will be gone by the time the async getattr callback fires. */
                gate->has_io_owner = true;
                gate->io_owner     = *io_owner;
            } else {
                gate->has_io_owner = false;
            }
            gate->callback     = callback;
            gate->private_data = private_data;

            chimera_vfs_getattr(thread, cred, handle,
                                CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                                chimera_vfs_write_gate_complete, gate);
            return;
        }
    }

    chimera_vfs_write_resolve(thread, cred, handle, offset, count, sync,
                              pre_attr_mask, post_attr_mask, iov, niov,
                              io_owner, callback, private_data);
} /* chimera_vfs_write_owned */

SYMBOL_EXPORT void
chimera_vfs_write(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    uint32_t                        sync,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    struct evpl_iovec              *iov,
    int                             niov,
    chimera_vfs_write_callback_t    callback,
    void                           *private_data)
{
    chimera_vfs_write_owned(thread, cred, handle, offset, count, sync,
                            pre_attr_mask, post_attr_mask, iov, niov,
                            NULL, callback, private_data);
} /* chimera_vfs_write */
