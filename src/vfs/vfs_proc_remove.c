#include "vfs/vfs_procs.h"
#include "vfs_internal.h"

static void
chimera_vfs_remove_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_remove_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_remove_complete */

void
chimera_vfs_remove(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    chimera_vfs_remove_callback_t   callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode             = CHIMERA_VFS_OP_REMOVE;
    request->complete           = chimera_vfs_remove_complete;
    request->remove.handle      = handle;
    request->remove.name        = name;
    request->remove.namelen     = namelen;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_remove */
