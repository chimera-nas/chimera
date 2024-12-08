#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread   = request->thread;
    chimera_vfs_open_callback_t     callback = request->proto_callback;
    struct chimera_vfs_open_handle *handle   = request->open.handle;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_open_cache_ready(
            thread->vfs->vfs_open_cache,
            handle,
            request->open.r_vfs_private);
    } else {
        chimera_vfs_open_cache_release(
            thread->vfs->vfs_open_cache,
            handle);
        handle = NULL;
    }

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_complete */

void
chimera_vfs_open(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    unsigned int                flags,
    chimera_vfs_open_callback_t callback,
    void                       *private_data)
{
    struct chimera_vfs             *vfs = thread->vfs;
    struct chimera_vfs_module      *module;
    struct chimera_vfs_request     *request;
    struct chimera_vfs_open_handle *handle;
    int                             is_new;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    is_new = chimera_vfs_open_cache_acquire(
        &handle,
        vfs->vfs_open_cache,
        module,
        fh,
        fhlen);

    if (!is_new) {
        if (handle->pending) {
            chimera_vfs_abort("handle open race");
        } else {
            /* XXX O_EXCL */
            callback(CHIMERA_VFS_OK, handle, private_data);
            return;
        }
    }

    request = chimera_vfs_request_alloc(thread);

    request->opcode             = CHIMERA_VFS_OP_OPEN;
    request->complete           = chimera_vfs_open_complete;
    request->open.fh            = fh;
    request->open.fh_len        = fhlen;
    request->open.flags         = flags;
    request->open.handle        = handle;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(thread, module, request);
} /* chimera_vfs_open */
