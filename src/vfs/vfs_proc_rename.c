#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_rename_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_rename_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_rename_complete */

void
chimera_vfs_rename(
    struct chimera_vfs_thread    *thread,
    const void                   *fh,
    int                           fhlen,
    const char                   *name,
    int                           namelen,
    const void                   *new_fh,
    int                           new_fhlen,
    const char                   *new_name,
    int                           new_namelen,
    chimera_vfs_rename_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->opcode             = CHIMERA_VFS_OP_RENAME;
    request->complete           = chimera_vfs_rename_complete;
    request->rename.name        = name;
    request->rename.namelen     = namelen;
    request->rename.new_fh      = new_fh;
    request->rename.new_fhlen   = new_fhlen;
    request->rename.new_name    = new_name;
    request->rename.new_namelen = new_namelen;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_rename */