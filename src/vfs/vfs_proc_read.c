
#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "core/evpl.h"

static void
chimera_vfs_read_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_read_callback_t callback = request->proto_callback;

    chimera_vfs_debug("read complete: %d, %d, %d",
                      request->status,
                      request->read.result_length,
                      request->read.result_eof);

    callback(request->status,
             request->read.result_length,
             request->read.result_eof,
             request->read.iov,
             request->read.niov,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_read_complete */

void
chimera_vfs_read(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = handle->vfs_module;

    request = chimera_vfs_request_alloc(thread);

    request->read.niov = evpl_iovec_alloc(
        thread->evpl, count, 4096, 2, request->read.iov);

    request->opcode             = CHIMERA_VFS_OP_READ;
    request->complete           = chimera_vfs_read_complete;
    request->read.handle        = handle;
    request->read.offset        = offset;
    request->read.length        = count;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);


} /* chimera_vfs_write */