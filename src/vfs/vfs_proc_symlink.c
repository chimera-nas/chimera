
#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_symlink_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_symlink_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->symlink.r_attr,
             &request->symlink.r_dir_pre_attr,
             &request->symlink.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_symlink_complete */

void
chimera_vfs_symlink(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    const char                    *name,
    int                            namelen,
    const char                    *target,
    int                            targetlen,
    uint64_t                       attr_mask,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_symlink_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->opcode                              = CHIMERA_VFS_OP_SYMLINK;
    request->complete                            = chimera_vfs_symlink_complete;
    request->symlink.name                        = name;
    request->symlink.namelen                     = namelen;
    request->symlink.target                      = target;
    request->symlink.targetlen                   = targetlen;
    request->symlink.r_attr.va_req_mask          = attr_mask;
    request->symlink.r_attr.va_set_mask          = 0;
    request->symlink.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->symlink.r_dir_pre_attr.va_set_mask  = 0;
    request->symlink.r_dir_post_attr.va_req_mask = post_attr_mask;
    request->symlink.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                      = callback;
    request->proto_private_data                  = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_symlink */
