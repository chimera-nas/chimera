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
             &request->mkdir.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_mkdir_complete */

void
chimera_vfs_mkdir(
    struct chimera_vfs_thread   *thread,
    const void                  *fh,
    int                          fhlen,
    const char                  *name,
    int                          namelen,
    unsigned int                 mode,
    uint64_t                     attrmask,
    chimera_vfs_mkdir_callback_t callback,
    void                        *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->mkdir.r_attr.va_mask     = 0;
    request->mkdir.r_dir_attr.va_mask = 0;

    request->opcode             = CHIMERA_VFS_OP_MKDIR;
    request->complete           = chimera_vfs_mkdir_complete;
    request->mkdir.fh           = fh;
    request->mkdir.fh_len       = fhlen;
    request->mkdir.name         = name;
    request->mkdir.name_len     = namelen;
    request->mkdir.mode         = mode;
    request->mkdir.attrmask     = attrmask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_mkdir */
