#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "common/macros.h"
static void
chimera_vfs_getattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_getattr_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->getattr.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_getattr_complete */

SYMBOL_EXPORT void
chimera_vfs_getattr(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        req_attr_mask,
    chimera_vfs_getattr_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                     = CHIMERA_VFS_OP_GETATTR;
    request->complete                   = chimera_vfs_getattr_complete;
    request->getattr.handle             = handle;
    request->getattr.r_attr.va_req_mask = req_attr_mask;
    request->getattr.r_attr.va_set_mask = 0;
    request->proto_callback             = callback;
    request->proto_private_data         = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_getattr */
