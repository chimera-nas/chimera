#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"

static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread   = request->thread;
    chimera_vfs_open_at_callback_t  callback = request->proto_callback;
    struct chimera_vfs_open_handle *handle;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_abort_if(!(request->open_at.r_attr.va_mask & CHIMERA_VFS_ATTR_FH),
                             "open_at: no fh returned from vfs module");

        handle = chimera_vfs_open_cache_insert(
            thread,
            thread->vfs->vfs_open_cache,
            request->module,
            request->open_at.r_attr.va_fh,
            request->open_at.r_attr.va_fh_len,
            request->open_at.r_vfs_private);
    } else {
        handle = NULL;
    }

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             &request->open_at.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_complete */

void
chimera_vfs_open_at(
    struct chimera_vfs_thread     *thread,
    const void                    *fh,
    int                            fhlen,
    const char                    *name,
    int                            namelen,
    unsigned int                   flags,
    unsigned int                   mode,
    uint64_t                       attrmask,
    chimera_vfs_open_at_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, fh, fhlen);

    request->open_at.r_attr.va_mask = 0;

    request->opcode             = CHIMERA_VFS_OP_OPEN_AT;
    request->complete           = chimera_vfs_open_complete;
    request->open_at.name       = name;
    request->open_at.namelen    = namelen;
    request->open_at.flags      = flags;
    request->open_at.mode       = mode;
    request->open_at.attrmask   = attrmask;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_open */
