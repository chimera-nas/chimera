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
             request->lookup_path.r_fh,
             request->lookup_path.r_fh_len,
             &request->lookup_path.r_attr,
             &request->lookup_path.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_complete */

void
chimera_vfs_lookup_path(
    struct chimera_vfs_thread         *thread,
    const char                        *path,
    int                                pathlen,
    uint64_t                           attrmask,
    chimera_vfs_lookup_path_callback_t callback,
    void                              *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    const char                 *slash;

    while (*path == '/') {
        path++;
        pathlen--;
    }

    module = thread->vfs->modules[CHIMERA_VFS_FH_MAGIC_ROOT];

    slash = strchr(path, '/');

    if (slash) {
        chimera_vfs_error("handle slash case");
        abort();
    } else {
        request = chimera_vfs_request_alloc(thread);

        request->lookup_path.r_attr.va_mask     = 0;
        request->lookup_path.r_dir_attr.va_mask = 0;

        request->opcode               = CHIMERA_VFS_OP_LOOKUP_PATH;
        request->complete             = chimera_vfs_lookup_path_complete;
        request->lookup_path.path     = path;
        request->lookup_path.pathlen  = pathlen;
        request->lookup_path.attrmask = attrmask;
        request->proto_callback       = callback;
        request->proto_private_data   = private_data;

        chimera_vfs_dispatch(thread, module, request);
    }
} /* chimera_vfs_lookup_path */
