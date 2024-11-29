#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_open_callback_t callback = request->proto_callback;

    callback(request->status,
             &request->open.handle,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_complete */

void
chimera_vfs_open(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    unsigned int                flags,
    chimera_vfs_open_callback_t callback,
    void                       *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode                 = CHIMERA_VFS_OP_OPEN;
    request->complete               = chimera_vfs_open_complete;
    request->open.fh                = fh;
    request->open.fh_len            = fhlen;
    request->open.flags             = flags;
    request->open.handle.vfs_module = module;
    request->proto_callback         = callback;
    request->proto_private_data     = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_open */
