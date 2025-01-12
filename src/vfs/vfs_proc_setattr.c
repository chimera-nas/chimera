#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_setattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_setattr_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->setattr.r_pre_attr,
             &request->setattr.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_setattr_complete */

void
chimera_vfs_setattr(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_setattr_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->opcode                          = CHIMERA_VFS_OP_SETATTR;
    request->complete                        = chimera_vfs_setattr_complete;
    request->setattr.set_attr                = set_attr;
    request->setattr.set_attr->va_set_mask   = 0;
    request->setattr.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->setattr.r_pre_attr.va_set_mask  = 0;
    request->setattr.r_post_attr.va_req_mask = post_attr_mask;
    request->setattr.r_post_attr.va_set_mask = 0;
    request->proto_callback                  = callback;
    request->proto_private_data              = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_setattr */