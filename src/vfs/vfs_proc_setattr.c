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
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_setattr_complete */

void
chimera_vfs_setattr(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    struct chimera_vfs_attrs      *attr,
    chimera_vfs_setattr_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_SETATTR;
    request->complete           = chimera_vfs_setattr_complete;
    request->setattr.fh         = fh;
    request->setattr.fh_len     = fhlen;
    request->setattr.attr       = *attr;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_setattr */