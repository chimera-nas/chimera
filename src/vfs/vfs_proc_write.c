#include "vfs/vfs_procs.h"
#include "vfs_internal.h"

static void
chimera_vfs_write_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_write_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->write.r_length,
             request->write.r_sync,
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
    const struct evpl_iovec        *iov,
    int                             niov,
    chimera_vfs_write_callback_t    callback,
    void                           *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = handle->vfs_module;

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_WRITE;
    request->complete           = chimera_vfs_write_complete;
    request->write.handle       = handle;
    request->write.offset       = offset;
    request->write.length       = count;
    request->write.sync         = sync;
    request->write.iov          = iov;
    request->write.niov         = niov;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);


} /* chimera_vfs_write */
