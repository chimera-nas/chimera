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
    struct chimera_vfs_module      *module;
    struct chimera_vfs_open_handle *handle;
    int                             is_new;

    module = chimera_vfs_get_module(thread,
                                    request->open_at.parent_fh,
                                    request->open_at.parent_fh_len);

    is_new = chimera_vfs_open_cache_acquire(
        &handle,
        thread->vfs->vfs_open_cache,
        module,
        request->open_at.fh,
        request->open_at.fh_len);

    if (!is_new) {
        if (handle->pending) {
            chimera_vfs_abort("handle open race");
        }
    }

    chimera_vfs_open_cache_ready(
        thread->vfs->vfs_open_cache,
        handle,
        request->open_at.r_vfs_private);

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
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc(thread);

    request->open_at.r_attr.va_mask = 0;

    request->opcode                = CHIMERA_VFS_OP_OPEN_AT;
    request->complete              = chimera_vfs_open_complete;
    request->open_at.parent_fh     = fh;
    request->open_at.parent_fh_len = fhlen;
    request->open_at.name          = name;
    request->open_at.namelen       = namelen;
    request->open_at.flags         = flags;
    request->open_at.mode          = mode;
    request->open_at.attrmask      = attrmask;
    request->proto_callback        = callback;
    request->proto_private_data    = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_open */
