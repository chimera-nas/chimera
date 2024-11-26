#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"

static void
chimera_vfs_lookup_path_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_lookup_callback_t callback = request->proto_callback;

    callback(request->status,
             request->lookup.r_fh,
             request->lookup.r_fh_len,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_complete */

void
chimera_vfs_lookup_path(
    struct chimera_vfs_thread         *thread,
    const char                        *path,
    int                                pathlen,
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
        request                       = chimera_vfs_request_alloc(thread);
        request->opcode               = CHIMERA_VFS_OP_LOOKUP;
        request->complete             = chimera_vfs_lookup_path_complete;
        request->lookup.fh            = &module->fh_magic;
        request->lookup.fh_len        = 1;
        request->lookup.component     = path;
        request->lookup.component_len = pathlen;
        request->proto_callback       = callback;
        request->proto_private_data   = private_data;

        chimera_vfs_dispatch(thread, module, request);
    }
} /* chimera_vfs_lookup_path */
