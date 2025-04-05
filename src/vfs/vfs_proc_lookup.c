#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/format.h"
#include "common/misc.h"
#include "common/macros.h"
static void
chimera_vfs_lookup_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_lookup_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->lookup.r_attr,
             &request->lookup.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_complete */

SYMBOL_EXPORT void
chimera_vfs_lookup(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    uint32_t                        namelen,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    chimera_vfs_lookup_callback_t   callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                        = CHIMERA_VFS_OP_LOOKUP;
    request->complete                      = chimera_vfs_lookup_complete;
    request->lookup.handle                 = handle;
    request->lookup.component              = name;
    request->lookup.component_len          = namelen;
    request->lookup.r_attr.va_req_mask     = attr_mask;
    request->lookup.r_attr.va_set_mask     = 0;
    request->lookup.r_dir_attr.va_req_mask = dir_attr_mask;
    request->lookup.r_dir_attr.va_set_mask = 0;
    request->proto_callback                = callback;
    request->proto_private_data            = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_lookup */
