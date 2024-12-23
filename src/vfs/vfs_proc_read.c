
#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "core/evpl.h"

static void
chimera_vfs_read_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_read_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->read.r_length,
             request->read.r_eof,
             request->read.iov,
             request->read.r_niov,
             &request->read.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_read_complete */

void
chimera_vfs_read(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *iov,
    int                             niov,
    uint64_t                        attrmask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, handle->fh, handle->fh_len);

    request->read.r_attr.va_mask = 0;

    request->opcode             = CHIMERA_VFS_OP_READ;
    request->complete           = chimera_vfs_read_complete;
    request->read.handle        = handle;
    request->read.offset        = offset;
    request->read.length        = count;
    request->read.iov           = iov;
    request->read.niov          = niov;
    request->read.attrmask      = attrmask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_write */