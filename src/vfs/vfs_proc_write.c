#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
static void
chimera_vfs_write_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_write_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->write.r_length,
             request->write.r_sync,
             &request->write.r_pre_attr,
             &request->write.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_write_complete */

void
chimera_vfs_write(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    uint32_t                        sync,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    const struct evpl_iovec        *iov,
    int                             niov,
    chimera_vfs_write_callback_t    callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                        = CHIMERA_VFS_OP_WRITE;
    request->complete                      = chimera_vfs_write_complete;
    request->write.handle                  = handle;
    request->write.offset                  = offset;
    request->write.length                  = count;
    request->write.sync                    = sync;
    request->write.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->write.r_pre_attr.va_set_mask  = 0;
    request->write.r_post_attr.va_req_mask = post_attr_mask;
    request->write.r_post_attr.va_set_mask = 0;
    request->write.iov                     = iov;
    request->write.niov                    = niov;
    request->proto_callback                = callback;
    request->proto_private_data            = private_data;

    chimera_vfs_dispatch(request);


} /* chimera_vfs_write */
