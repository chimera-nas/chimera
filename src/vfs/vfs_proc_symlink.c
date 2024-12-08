
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
             &request->symlink.r_dir_attr,
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
    uint64_t                       attrmask,
    chimera_vfs_symlink_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->symlink.r_attr.va_mask     = 0;
    request->symlink.r_dir_attr.va_mask = 0;

    request->opcode             = CHIMERA_VFS_OP_SYMLINK;
    request->complete           = chimera_vfs_symlink_complete;
    request->symlink.fh         = fh;
    request->symlink.fh_len     = fhlen;
    request->symlink.name       = name;
    request->symlink.namelen    = namelen;
    request->symlink.target     = target;
    request->symlink.targetlen  = targetlen;
    request->symlink.attrmask   = attrmask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_symlink */
