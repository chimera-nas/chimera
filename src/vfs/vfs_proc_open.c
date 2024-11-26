#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_open_callback_t callback = request->proto_callback;

    callback(request->status,
             request->open_at.fh,
             request->open_at.fh_len,
             &request->open_at.handle,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_complete */

void
chimera_vfs_open_at(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    const char                 *name,
    int                         namelen,
    unsigned int                flags,
    unsigned int                mode,
    chimera_vfs_open_callback_t callback,
    void                       *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode                    = CHIMERA_VFS_OP_OPEN_AT;
    request->complete                  = chimera_vfs_open_complete;
    request->open_at.parent_fh         = fh;
    request->open_at.parent_fh_len     = fhlen;
    request->open_at.name              = name;
    request->open_at.namelen           = namelen;
    request->open_at.flags             = flags;
    request->open_at.mode              = mode;
    request->open_at.handle.vfs_module = module;
    request->proto_callback            = callback;
    request->proto_private_data        = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_open */
