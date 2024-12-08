
#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "core/evpl.h"

static void
chimera_vfs_read_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_read_callback_t callback = request->proto_callback;
    int                         i;

    chimera_vfs_complete(request);

    callback(request->status,
             request->read.r_length,
             request->read.r_eof,
             request->read.r_iov,
             request->read.r_niov,
             &request->read.r_attr,
             request->proto_private_data);

    /* XXX
     * Serialization will have taken a new reference so we need
     * to drop ours, maybe arrange for it to steal ours instead?
     */

    for (i = 0; i < request->read.r_niov; i++) {
        evpl_iovec_release(&request->read.r_iov[i]);
    }

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_read_complete */

void
chimera_vfs_read(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    uint64_t                        attrmask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = handle->vfs_module;

    request = chimera_vfs_request_alloc(thread);

    request->read.r_attr.va_mask = 0;

    request->read.r_attr.va_mask = 0;

    request->opcode             = CHIMERA_VFS_OP_READ;
    request->complete           = chimera_vfs_read_complete;
    request->read.handle        = handle;
    request->read.offset        = offset;
    request->read.length        = count;
    request->read.attrmask      = attrmask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);


} /* chimera_vfs_write */