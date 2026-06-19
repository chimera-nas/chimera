// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "common/macros.h"

static void
chimera_vfs_open_stream_hdl_callback(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_open_stream_callback_t callback = request->proto_callback;

    if (handle) {
        handle->r_created = request->open_stream.r_created;
        /* Tag the handle so the attr cache is bypassed for it: a stream shares
         * the base inode's metadata, which changes out-of-band relative to the
         * stream fh (smb2.streams.attributes2). */
        handle->flags |= CHIMERA_VFS_OPEN_HANDLE_STREAM;
    }

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             &request->open_stream.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_stream_hdl_callback */

static void
chimera_vfs_open_stream_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;
    uint64_t                   fh_hash;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_abort_if(!(request->open_stream.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "open_stream: no fh returned from vfs module");

        fh_hash = chimera_vfs_hash(request->open_stream.r_attr.va_fh,
                                   request->open_stream.r_attr.va_fh_len);

        chimera_vfs_open_cache_insert(
            thread,
            thread->vfs->vfs_open_file_cache,
            request->module,
            request,
            request->open_stream.r_attr.va_fh,
            request->open_stream.r_attr.va_fh_len,
            fh_hash,
            request->open_stream.r_vfs_private,
            request->open_stream.flags,
            chimera_vfs_open_stream_hdl_callback);
    } else {
        chimera_vfs_open_stream_hdl_callback(request, NULL);
    }
} /* chimera_vfs_open_stream_complete */

SYMBOL_EXPORT void
chimera_vfs_open_stream(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *handle,
    const char                        *name,
    uint32_t                           namelen,
    uint32_t                           flags,
    struct chimera_vfs_attrs          *set_attr,
    uint64_t                           attr_mask,
    chimera_vfs_open_stream_callback_t callback,
    void                              *private_data)
{
    struct chimera_vfs_request *request;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_NAMED_STREAMS)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                         = CHIMERA_VFS_OP_OPEN_STREAM;
    request->complete                       = chimera_vfs_open_stream_complete;
    request->open_stream.handle             = handle;
    request->open_stream.name               = name;
    request->open_stream.namelen            = namelen;
    request->open_stream.flags              = flags;
    request->open_stream.set_attr           = set_attr;
    request->open_stream.r_vfs_private      = 0;
    request->open_stream.r_created          = 0;
    request->open_stream.r_attr.va_req_mask = attr_mask | CHIMERA_VFS_ATTR_FH |
        CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->open_stream.r_attr.va_set_mask = 0;
    request->proto_callback                 = callback;
    request->proto_private_data             = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_open_stream */
