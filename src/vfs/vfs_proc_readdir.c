#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "common/macros.h"
static void
chimera_vfs_readdir_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_readdir_complete_t complete = request->proto_callback;

    chimera_vfs_complete(request);

    complete(request->status,
             request->readdir.handle,
             request->readdir.r_cookie,
             request->readdir.r_eof,
             &request->readdir.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_readdir_complete */

SYMBOL_EXPORT void
chimera_vfs_readdir(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    uint64_t                        cookie,
    chimera_vfs_readdir_callback_t  callback,
    chimera_vfs_readdir_complete_t  complete,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                         = CHIMERA_VFS_OP_READDIR;
    request->complete                       = chimera_vfs_readdir_complete;
    request->readdir.handle                 = handle;
    request->readdir.attr_mask              = attr_mask;
    request->readdir.cookie                 = cookie;
    request->readdir.callback               = callback;
    request->readdir.r_dir_attr.va_req_mask = dir_attr_mask;
    request->readdir.r_dir_attr.va_set_mask = 0;
    request->proto_callback                 = complete;
    request->proto_private_data             = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_readdir */
