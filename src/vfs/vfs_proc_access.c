#include "vfs/vfs_procs.h"
#include "vfs_internal.h"

static void
chimera_vfs_access_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_access_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->access.r_access,
             &request->access.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_access_complete */

void
chimera_vfs_access(
    struct chimera_vfs_thread    *vfs,
    const void                   *fh,
    int                           fhlen,
    uint32_t                      access,
    uint64_t                      attrmask,
    chimera_vfs_access_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_thread  *thread = vfs;
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->access.r_attr.va_mask = 0;

    request->opcode             = CHIMERA_VFS_OP_ACCESS;
    request->complete           = chimera_vfs_access_complete;
    request->access.access      = access;
    request->access.attrmask    = attrmask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_access */
