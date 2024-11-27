#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_getattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_getattr_callback_t callback = request->proto_callback;

    callback(request->status,
             &request->getattr.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_getattr_complete */

void
chimera_vfs_getattr(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       req_attr_mask,
    chimera_vfs_getattr_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_GETATTR;
    request->complete           = chimera_vfs_getattr_complete;
    request->getattr.fh         = fh;
    request->getattr.fh_len     = fhlen;
    request->getattr.attr_mask  = req_attr_mask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_getattr */
