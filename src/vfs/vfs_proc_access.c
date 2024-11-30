#include "vfs/vfs_procs.h"
#include "vfs_internal.h"

static void
chimera_vfs_access_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_access_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_access_complete */

void
chimera_vfs_access(
    struct chimera_vfs_thread    *vfs,
    const void                   *fh,
    int                           fhlen,
    uint32_t                      access,
    chimera_vfs_access_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_thread  *thread = vfs;
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_ACCESS;
    request->complete           = chimera_vfs_access_complete;
    request->access.fh          = fh;
    request->access.fh_len      = fhlen;
    request->access.access      = access;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_access */