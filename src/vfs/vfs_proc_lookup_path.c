#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_lookup_path_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_lookup_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->lookup_path.r_attr,
             &request->lookup_path.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_complete */

void
chimera_vfs_lookup_path(
    struct chimera_vfs_thread         *thread,
    const void                        *fh,
    int                                fhlen,
    const char                        *path,
    int                                pathlen,
    uint64_t                           attr_mask,
    uint64_t                           dir_attr_mask,
    chimera_vfs_lookup_path_callback_t callback,
    void                              *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->opcode                             = CHIMERA_VFS_OP_LOOKUP_PATH;
    request->complete                           = chimera_vfs_lookup_path_complete;
    request->lookup_path.path                   = path;
    request->lookup_path.pathlen                = pathlen;
    request->lookup_path.r_attr.va_req_mask     = attr_mask;
    request->lookup_path.r_attr.va_set_mask     = 0;
    request->lookup_path.r_dir_attr.va_req_mask = dir_attr_mask;
    request->lookup_path.r_dir_attr.va_set_mask = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_lookup_path */
