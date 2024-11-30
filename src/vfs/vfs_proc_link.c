#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_link_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_link_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_link_complete */

void
chimera_vfs_link(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    const void                 *dir_fh,
    int                         dir_fhlen,
    const char                 *name,
    int                         namelen,
    chimera_vfs_link_callback_t callback,
    void                       *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_LINK;
    request->complete           = chimera_vfs_link_complete;
    request->link.fh            = fh;
    request->link.fh_len        = fhlen;
    request->link.dir_fh        = dir_fh;
    request->link.dir_fhlen     = dir_fhlen;
    request->link.name          = name;
    request->link.namelen       = namelen;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_link */
