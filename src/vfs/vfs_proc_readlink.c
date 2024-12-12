#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_readlink_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread   = request->thread;
    chimera_vfs_readlink_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->readlink.r_target_length,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_readlink_complete */

void
chimera_vfs_readlink(
    struct chimera_vfs_thread      *thread,
    const void                     *fh,
    int                             fhlen,
    void                           *target,
    uint32_t                        target_maxlength,
    chimera_vfs_readlink_callback_t callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->opcode                    = CHIMERA_VFS_OP_READLINK;
    request->complete                  = chimera_vfs_readlink_complete;
    request->readlink.r_target         = target;
    request->readlink.target_maxlength = target_maxlength;
    request->proto_callback            = callback;
    request->proto_private_data        = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_readlink */