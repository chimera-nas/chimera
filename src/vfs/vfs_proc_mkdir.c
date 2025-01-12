#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_mkdir_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_mkdir_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->mkdir.r_attr,
             &request->mkdir.r_dir_pre_attr,
             &request->mkdir.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_mkdir_complete */

void
chimera_vfs_mkdir(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_callback_t    callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                            = CHIMERA_VFS_OP_MKDIR;
    request->complete                          = chimera_vfs_mkdir_complete;
    request->mkdir.handle                      = handle;
    request->mkdir.name                        = name;
    request->mkdir.name_len                    = namelen;
    request->mkdir.set_attr                    = attr;
    request->mkdir.set_attr->va_set_mask       = 0;
    request->mkdir.r_attr.va_req_mask          = attr_mask;
    request->mkdir.r_attr.va_set_mask          = 0;
    request->mkdir.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->mkdir.r_dir_pre_attr.va_set_mask  = 0;
    request->mkdir.r_dir_post_attr.va_req_mask = post_attr_mask;
    request->mkdir.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                    = callback;
    request->proto_private_data                = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mkdir */
