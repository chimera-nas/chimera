// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>   /* S_ISBLK / S_ISCHR for the device gate */
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_notify.h"
#include "sdk/vfs_access.h"
#include "sdk/vfs_acl.h"
#include "common/misc.h"
#include "common/macros.h"
static void
chimera_vfs_mknod_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread   = request->thread;
    struct chimera_vfs_name_cache  *cache    = thread->vfs->vfs_name_cache;
    chimera_vfs_mknod_at_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        /* A new node is a directory content change, observable by change
         * watchers and directory-lease holders like any other create. */
        chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                request->mknod_at.handle->fh,
                                request->mknod_at.handle->fh_len,
                                CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                request->mknod_at.name,
                                request->mknod_at.name_len,
                                NULL, 0);

        chimera_vfs_name_cache_insert(thread, cache,
                                      request->mknod_at.handle->fh_hash,
                                      request->mknod_at.handle->fh,
                                      request->mknod_at.handle->fh_len,
                                      request->mknod_at.name_hash,
                                      request->mknod_at.name,
                                      request->mknod_at.name_len,
                                      request->mknod_at.r_attr.va_fh,
                                      request->mknod_at.r_attr.va_fh_len);

        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      request->mknod_at.handle->fh_hash,
                                      request->mknod_at.handle->fh,
                                      request->mknod_at.handle->fh_len,
                                      &request->mknod_at.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      chimera_vfs_hash(request->mknod_at.r_attr.va_fh, request->mknod_at.r_attr.
                                                       va_fh_len)
                                      ,
                                      request->mknod_at.r_attr.va_fh,
                                      request->mknod_at.r_attr.va_fh_len,
                                      &request->mknod_at.r_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->mknod_at.set_attr,
             &request->mknod_at.r_attr,
             &request->mknod_at.r_dir_pre_attr,
             &request->mknod_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_mknod_at_complete */

/*
 * A device mknod by an unprivileged caller is refused -- but only once the
 * name is known to be free.  Linux builds the new dentry first
 * (filename_create, which is where EEXIST comes from) and reaches the
 * CAP_MKNOD test only afterwards, so an unprivileged mknod over a name that
 * already exists reports EEXIST, not EPERM.  The fast path above answers
 * EEXIST straight from the caches; when they miss there is nothing to answer
 * from, so resolve the name before deciding.  This costs a lookup only on a
 * call that was going to fail either way.
 */
struct chimera_vfs_mknod_at_priv {
    struct chimera_vfs_thread      *thread;
    chimera_vfs_mknod_at_callback_t callback;
    void                           *private_data;
};

_Static_assert(sizeof(struct chimera_vfs_mknod_at_priv) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "mknod_at privilege context outgrew the request gate scratch area");

static void
chimera_vfs_mknod_at_priv_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_mknod_at_priv *ctx = private_data;

    ctx->callback(error_code == CHIMERA_VFS_OK ?
                  CHIMERA_VFS_EEXIST : CHIMERA_VFS_EPERM,
                  NULL, NULL, NULL, NULL, ctx->private_data);
    chimera_vfs_gate_scratch_free(ctx->thread, ctx);
} /* chimera_vfs_mknod_at_priv_complete */

static void
chimera_vfs_mknod_at_dispatch(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mknod_at_callback_t callback,
    void                           *private_data)
{
    struct chimera_vfs_request    *request;
    uint64_t                       name_hash;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_attrs       cached_attr;
    struct chimera_vfs_attrs       cached_dir_attr;
    int                            rc;

    name_hash = chimera_vfs_hash(name, namelen);

    if (!(attr_mask & ~(CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE)) &&
        !(pre_attr_mask & ~(CHIMERA_VFS_ATTR_MASK_CACHEABLE)) &&
        !(post_attr_mask & ~(CHIMERA_VFS_ATTR_MASK_CACHEABLE))) {

        cached_attr.va_req_mask = 0;
        cached_attr.va_set_mask = 0;

        rc = chimera_vfs_name_cache_lookup(
            name_cache,
            handle->fh_hash,
            handle->fh,
            handle->fh_len,
            name_hash,
            name,
            namelen,
            cached_attr.va_fh,
            &cached_attr.va_fh_len);

        if (rc == 0 && cached_attr.va_fh_len > 0) {

            rc = chimera_vfs_attr_cache_lookup(
                attr_cache,
                handle->fh_hash,
                handle->fh,
                handle->fh_len,
                &cached_dir_attr);

            if (rc == 0) {

                rc = chimera_vfs_attr_cache_lookup(
                    attr_cache,
                    chimera_vfs_hash(cached_attr.va_fh, cached_attr.va_fh_len),
                    cached_attr.va_fh,
                    cached_attr.va_fh_len,
                    &cached_attr);

                if (rc == 0) {
                    callback(CHIMERA_VFS_EEXIST,
                             &cached_attr,
                             &cached_attr,
                             &cached_dir_attr,
                             &cached_dir_attr,
                             private_data);
                    return;
                }
            }
        }
    }

    /* POSIX: only a privileged process may create a block or character
     * special file; an unprivileged device mknod is EPERM.  FIFOs and
     * UNIX-domain sockets need no privilege (mknod(2)).  Checked after the
     * create gate (EACCES) and the target-exists check (EEXIST), matching
     * Linux's may_create -> CAP_MKNOD order -- which is why the denial goes
     * through a lookup when the caches could not settle EEXIST above. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        (S_ISBLK(attr->va_mode) || S_ISCHR(attr->va_mode)) &&
        cred->uid != 0) {
        struct chimera_vfs_mknod_at_priv *ctx;

        ctx               = chimera_vfs_gate_scratch_alloc(thread);
        ctx->thread       = thread;
        ctx->callback     = callback;
        ctx->private_data = private_data;

        chimera_vfs_lookup(thread, cred, NULL, handle->fh, handle->fh_len,
                           name, namelen, CHIMERA_VFS_ATTR_FH, 0,
                           chimera_vfs_mknod_at_priv_complete, ctx);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, NULL, private_data);
        return;
    }

    request->transaction = txn;

    request->opcode                               = CHIMERA_VFS_OP_MKNOD_AT;
    request->complete                             = chimera_vfs_mknod_at_complete;
    request->mknod_at.handle                      = handle;
    request->mknod_at.name                        = name;
    request->mknod_at.name_len                    = namelen;
    request->mknod_at.name_hash                   = name_hash;
    request->mknod_at.set_attr                    = attr;
    request->mknod_at.r_attr.va_req_mask          = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mknod_at.r_attr.va_set_mask          = 0;
    request->mknod_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mknod_at.r_dir_pre_attr.va_set_mask  = 0;
    request->mknod_at.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mknod_at.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                       = callback;
    request->proto_private_data                   = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mknod_at_dispatch */

/*
 * Enforcement pre-step context: creating a node (special file / regular file)
 * requires ADD_FILE on the parent directory.
 */
struct chimera_vfs_mknod_at_gate {
    struct chimera_vfs_gate_ctx     gate_ctx;
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_transaction *txn;
    struct chimera_vfs_open_handle *handle;
    const char                     *name;
    int                             namelen;
    struct chimera_vfs_attrs       *attr;
    uint64_t                        attr_mask;
    uint64_t                        pre_attr_mask;
    uint64_t                        post_attr_mask;
    chimera_vfs_mknod_at_callback_t callback;
    void                           *private_data;
};

_Static_assert(sizeof(struct chimera_vfs_mknod_at_gate) <= CHIMERA_VFS_GATE_SCRATCH_SIZE,
               "mknod_at gate context outgrew the request gate scratch area");

static void
chimera_vfs_mknod_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_mknod_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        gate->callback(status, NULL, NULL, NULL, NULL, gate->private_data);
        chimera_vfs_gate_scratch_free(gate->thread, gate);
        return;
    }

    chimera_vfs_mknod_at_dispatch(gate->thread, gate->cred, gate->txn, gate->handle,
                                  gate->name, gate->namelen, gate->attr,
                                  gate->attr_mask, gate->pre_attr_mask,
                                  gate->post_attr_mask, gate->callback,
                                  gate->private_data);
    chimera_vfs_gate_scratch_free(gate->thread, gate);
} /* chimera_vfs_mknod_at_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_mknod_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mknod_at_callback_t callback,
    void                           *private_data)
{
    struct chimera_vfs_mknod_at_gate *gate;

    if (namelen >= CHIMERA_VFS_NAME_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    /* The engine's create gate normally runs only where the engine is the
     * DAC authority.  One remote-DAC exception: a DEVICE mknod by an
     * unprivileged caller is answered EPERM by the engine itself (the
     * dispatch path below) -- but POSIX/Linux order the parent-write EACCES
     * first (may_create before CAP_MKNOD), and the remote server can only
     * deliver that EACCES for an operation we would actually send.  Run the
     * engine's own create gate for exactly this case so the denials come
     * out in the right order. */
    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred) ||
        (chimera_vfs_open_gate_needed(handle->vfs_module->capabilities,
                                      cred) &&
         (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
         (S_ISBLK(attr->va_mode) || S_ISCHR(attr->va_mode)))) {
        gate                 = chimera_vfs_gate_scratch_alloc(thread);
        gate->thread         = thread;
        gate->cred           = cred;
        gate->txn            = txn;
        gate->handle         = handle;
        gate->name           = name;
        gate->namelen        = namelen;
        gate->attr           = attr;
        gate->attr_mask      = attr_mask;
        gate->pre_attr_mask  = pre_attr_mask;
        gate->post_attr_mask = post_attr_mask;
        gate->callback       = callback;
        gate->private_data   = private_data;

        /* _always: on a remote-DAC proxy gate_fh would defer to a backend
         * that will never see this op (the engine denies it below); the
         * engine must evaluate the parent access itself. */
        chimera_vfs_gate_fh_always(&gate->gate_ctx, thread, cred,
                                   handle->fh, handle->fh_len,
                                   CHIMERA_ACE_WRITE_DATA |
                                   CHIMERA_ACE_EXECUTE,
                                   chimera_vfs_mknod_at_gate_complete, gate);
        return;
    }

    chimera_vfs_mknod_at_dispatch(thread, cred, txn, handle, name, namelen, attr,
                                  attr_mask, pre_attr_mask, post_attr_mask,
                                  callback, private_data);
} /* chimera_vfs_mknod_at */
