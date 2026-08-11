// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "vfs_access.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

/* Whether the engine applies POSIX open semantics (type checks and, for
 * non-exempt credentials, the access gate) to this open at completion --
 * see chimera_vfs_open_at_hdl_callback.  SMB (AUTH_ATTR) evaluates its own
 * access model and legitimately opens directories with write-ish desired
 * access, so it is exempt; so are internal INFERRED opens (NFS3 create and
 * the path-setattr plumbing), which keep their protocol-specific
 * per-operation semantics. */
static inline int
chimera_vfs_open_at_checked(
    const struct chimera_vfs_cred *cred,
    unsigned int                   flags)
{
    return !(flags & CHIMERA_VFS_OPEN_INFERRED) &&
           cred->flavor != CHIMERA_VFS_AUTH_ATTR;
} /* chimera_vfs_open_at_checked */

static void
chimera_vfs_open_at_hdl_callback(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    struct chimera_vfs_name_cache *cache    = thread->vfs->vfs_name_cache;
    chimera_vfs_open_at_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        /* A path-only open returns an opaque per-open token, not a stable child
         * fh; caching name->token would hand out a dead token, so skip it. */
        if (!chimera_vfs_module_is_path_only(request->module)) {
            chimera_vfs_name_cache_insert(thread, cache,
                                          request->open_at.handle->fh_hash,
                                          request->open_at.handle->fh,
                                          request->open_at.handle->fh_len,
                                          request->open_at.name_hash,
                                          request->open_at.name,
                                          request->open_at.namelen,
                                          request->open_at.r_attr.va_fh,
                                          request->open_at.r_attr.va_fh_len);
        }

        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      request->open_at.handle->fh_hash,
                                      request->open_at.handle->fh,
                                      request->open_at.handle->fh_len,
                                      &request->open_at.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      chimera_vfs_hash(request->open_at.r_attr.va_fh, request->open_at.r_attr.
                                                       va_fh_len),
                                      request->open_at.r_attr.va_fh,
                                      request->open_at.r_attr.va_fh_len,
                                      &request->open_at.r_attr);
    }

    if (handle) {
        handle->r_created = request->open_at.r_created;
    }

    /* POSIX open semantics, evaluated against the just-returned attrs
     * (mirroring the checks the plain-open wrapper applies on its lookup
     * path, which this create/openat path previously skipped entirely):
     *
     *  - O_NOFOLLOW and the object is a symlink -> ELOOP (an O_PATH-style
     *    open of the link itself is allowed);
     *  - a directory opened with write intent -> EISDIR;
     *  - a FIFO/socket/device opened with data access -> ENXIO (no
     *    blocking-open or device semantics behind a NAS client);
     *  - for non-exempt credentials, authorize the requested access:
     *    an existing file the caller may not access this way fails
     *    EACCES, while a freshly created file grants the requested
     *    access unconditionally (no permission check applies to a file
     *    that did not exist).  The effective grant is stamped on the
     *    handle so subsequent I/O is immune to later mode changes. */
    if (request->status == CHIMERA_VFS_OK && handle &&
        chimera_vfs_open_at_checked(request->cred, request->open_at.flags) &&
        (request->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE)) {

        unsigned int           f      = request->open_at.flags;
        uint32_t               mode   = request->open_at.r_attr.va_mode;
        enum chimera_vfs_error status = CHIMERA_VFS_OK;

        if (S_ISLNK(mode) && !(f & CHIMERA_VFS_OPEN_NOFOLLOW) &&
            !(f & CHIMERA_VFS_OPEN_PATH)) {
            /* A symlink the open will FOLLOW (chimera_vfs_open's create
             * leg restarts resolution on the link target): neither type
             * checks nor the access gate apply to the link itself -- a
             * symlink's permission bits are never consulted, and the
             * restarted open authorizes the real target. */
        } else if (S_ISLNK(mode) && (f & CHIMERA_VFS_OPEN_NOFOLLOW) &&
                   !(f & CHIMERA_VFS_OPEN_PATH)) {
            status = CHIMERA_VFS_ELOOP;
        } else if (S_ISDIR(mode) &&
                   (f & (CHIMERA_VFS_OPEN_WRITE_ONLY |
                         CHIMERA_VFS_OPEN_TRUNCATE))) {
            status = CHIMERA_VFS_EISDIR;
        } else if (!S_ISREG(mode) && !S_ISDIR(mode) && !S_ISLNK(mode) &&
                   (f & (CHIMERA_VFS_OPEN_READ_ONLY |
                         CHIMERA_VFS_OPEN_WRITE_ONLY))) {
            status = CHIMERA_VFS_ENXIO;
        } else if (chimera_vfs_gate_needed(request->module->capabilities,
                                           request->cred)) {
            uint32_t required = chimera_vfs_open_required_access(f);
            uint32_t granted  =
                chimera_vfs_access_check(&request->open_at.r_attr,
                                         request->cred,
                                         CHIMERA_ACE_MASK_ALL);

            if (request->open_at.r_created) {
                granted |= required;
            }

            if ((granted & required) != required) {
                status = CHIMERA_VFS_EACCES;
            } else {
                chimera_vfs_handle_stamp_access(handle, granted);
            }
        }

        if (status != CHIMERA_VFS_OK) {
            chimera_vfs_release(thread, handle);
            handle          = NULL;
            request->status = status;
        }
    }

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             request->open_at.set_attr,
             &request->open_at.r_attr,
             &request->open_at.r_dir_pre_attr,
             &request->open_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_at_hdl_callback */

static void
chimera_vfs_open_finish(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread = request->thread;
    uint64_t                        fh_hash;
    struct vfs_open_cache          *cache;
    struct chimera_vfs_open_handle *handle;

    if (request->open_at.flags & CHIMERA_VFS_OPEN_PATH) {
        cache = thread->vfs->vfs_open_path_cache;
    } else {
        cache = thread->vfs->vfs_open_file_cache;
    }

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_abort_if(!(request->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "open_at: no fh returned from vfs module");

        fh_hash = chimera_vfs_hash(request->open_at.r_attr.va_fh,
                                   request->open_at.r_attr.va_fh_len);

        if ((request->module->capabilities & CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED) ||
            !(request->open_at.flags &  CHIMERA_VFS_OPEN_INFERRED)) {
            chimera_vfs_open_cache_insert(
                thread,
                cache,
                request->module,
                request,
                request->open_at.r_attr.va_fh,
                request->open_at.r_attr.va_fh_len,
                fh_hash,
                request->open_at.r_vfs_private,
                request->open_at.flags,
                chimera_vfs_open_at_hdl_callback);
        } else {

            /* This is an inferred open from the likes of NFS3 create
             * where caller does not need to hold a reference count
             * and our module does not need open handles, so
             * we can synthesize a handle and return it immediately.
             */

            handle = chimera_vfs_synth_handle_alloc(thread);

            memcpy(handle->fh, request->open_at.r_attr.va_fh, request->open_at.r_attr.va_fh_len);
            handle->vfs_module  = request->module;
            handle->fh_len      = request->open_at.r_attr.va_fh_len;
            handle->fh_hash     = fh_hash;
            handle->vfs_private = 0;

            chimera_vfs_open_at_hdl_callback(request, handle);

        }

    } else {
        chimera_vfs_open_at_hdl_callback(request, NULL);
    }
} /* chimera_vfs_open_finish */

/* Continuation after the non-atomic handle-state record has been persisted to
 * the default KV (best-effort: a failure leaves the file open without its
 * durable record, which only the in-memory backends ever hit). */
static void
chimera_vfs_open_hs_put_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_error("open_at: failed to persist handle-state to default KV: %d",
                          error_code);
    }

    chimera_vfs_open_finish(request);
} /* chimera_vfs_open_hs_put_complete */

static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_handle_state *hs = request->open_at.handle_state;

    /* Backends that persist handle-state atomically (CAP_ATOMIC_HANDLE_STATE)
     * have already stored it as part of the open.  For backends without native
     * KV, the VFS core persists the record to the default KV instead, keyed by
     * the new file's fh so close/recovery find it on the same backend.  This is
     * a separate, non-atomic put; it is only reached by in-memory backends
     * (memfs) and passthrough, where cross-crash atomicity is moot. */
    if (request->status == CHIMERA_VFS_OK && hs &&
        !(request->module->capabilities & CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE) &&
        request->thread->vfs->kv_module &&
        (request->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH)) {

        chimera_vfs_put_key_at(request->thread, request->cred,
                               request->open_at.r_attr.va_fh,
                               request->open_at.r_attr.va_fh_len,
                               hs->key, hs->key_len,
                               hs->value, hs->value_len,
                               chimera_vfs_open_hs_put_complete, request);
        return;
    }

    chimera_vfs_open_finish(request);
} /* chimera_vfs_open_complete */

SYMBOL_EXPORT void
chimera_vfs_open_at_hs(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    unsigned int                     flags,
    struct chimera_vfs_attrs        *set_attr,
    uint64_t                         attr_mask,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_at_callback_t   callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    chimera_vfs_abort_if(!set_attr, "no setattr provided");

    /* On a creating open the trailing component is a new name; reject one longer
     * than {NAME_MAX} with ENAMETOOLONG.  FS_PATH_OP backends receive the whole
     * path as `name` and let the kernel enforce this. */
    if ((flags & CHIMERA_VFS_OPEN_CREATE) &&
        !(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_FS_PATH_OP) &&
        namelen >= CHIMERA_VFS_NAME_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    request->opcode                     = CHIMERA_VFS_OP_OPEN_AT;
    request->complete                   = chimera_vfs_open_complete;
    request->open_at.handle             = handle;
    request->open_at.name               = name;
    request->open_at.namelen            = namelen;
    request->open_at.name_hash          = chimera_vfs_hash(name, namelen);
    request->open_at.flags              = flags;
    request->open_at.set_attr           = set_attr;
    request->open_at.handle_state       = handle_state;
    request->open_at.r_created          = 0;
    request->open_at.r_attr.va_req_mask = attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;

    /* The completion checks need the mode (and, for the access gate,
     * uid/gid plus a native ACL if any); make sure the backend returns
     * them. */
    if (chimera_vfs_open_at_checked(cred, flags)) {
        request->open_at.r_attr.va_req_mask |=
            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL;
    }
    request->open_at.r_attr.va_set_mask          = 0;
    request->open_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->open_at.r_dir_pre_attr.va_set_mask  = 0;
    request->open_at.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->open_at.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                      = callback;
    request->proto_private_data                  = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_open_at_hs */

SYMBOL_EXPORT void
chimera_vfs_open_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_open_at_callback_t  callback,
    void                           *private_data)
{
    chimera_vfs_open_at_hs(thread, cred, handle, name, namelen, flags, set_attr,
                           attr_mask, pre_attr_mask, post_attr_mask, NULL,
                           callback, private_data);
} /* chimera_vfs_open_at */
