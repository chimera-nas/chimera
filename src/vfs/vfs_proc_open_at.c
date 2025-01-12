#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"

static void
chimera_vfs_open_at_hdl_callback(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_open_at_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             &request->open_at.r_attr,
             &request->open_at.r_dir_pre_attr,
             &request->open_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_at_hdl_callback */

static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
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

        fh_hash = XXH3_64bits(request->open_at.r_attr.va_fh,
                              request->open_at.r_attr.va_fh_len);

        if (request->module->file_open_required || !(request->open_at.flags & CHIMERA_VFS_OPEN_INFERRED)) {
            chimera_vfs_open_cache_acquire(
                thread,
                cache,
                request->module,
                request,
                request->open_at.r_attr.va_fh,
                request->open_at.r_attr.va_fh_len,
                fh_hash,
                request->open_at.r_vfs_private,
                0,
                chimera_vfs_open_at_hdl_callback);
        } else {

            /* This is an inferred open from the likes of NFS3 create
             * where caller does not need to hold a reference count
             * and our module does not need open handles, so
             * we can synthesize a handle and return it immediately.
             */

            handle = chimera_vfs_synth_handle_alloc(thread);

            memcpy(handle->fh, request->open_at.r_attr.va_fh, request->open_at.r_attr.va_fh_len);
            handle->fh_len      = request->open_at.r_attr.va_fh_len;
            handle->fh_hash     = fh_hash;
            handle->vfs_private =  0xdeadbeefUL;

            chimera_vfs_open_at_hdl_callback(request, handle);

        }

    } else {
        chimera_vfs_open_at_hdl_callback(request, NULL);
    }
} /* chimera_vfs_open_complete */

void
chimera_vfs_open_at(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    unsigned int                    mode,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_open_at_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                              = CHIMERA_VFS_OP_OPEN_AT;
    request->complete                            = chimera_vfs_open_complete;
    request->open_at.handle                      = handle;
    request->open_at.name                        = name;
    request->open_at.namelen                     = namelen;
    request->open_at.flags                       = flags;
    request->open_at.mode                        = mode;
    request->open_at.r_attr.va_req_mask          = attr_mask;
    request->open_at.r_attr.va_set_mask          = 0;
    request->open_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->open_at.r_dir_pre_attr.va_set_mask  = 0;
    request->open_at.r_dir_post_attr.va_req_mask = post_attr_mask;
    request->open_at.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                      = callback;
    request->proto_private_data                  = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_open */
