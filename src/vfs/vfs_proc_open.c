#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"

static void
chimera_vfs_open_hdl_callback(
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_request *request  = private_data;
    chimera_vfs_open_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);

} /* chimera_vfs_open_hdl_callback */

static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;
    struct chimera_vfs_module *module;

    if (request->status == CHIMERA_VFS_OK) {

        module = chimera_vfs_get_module(thread,
                                        request->fh,
                                        request->fh_len);

        chimera_vfs_open_cache_insert(
            thread,
            thread->vfs->vfs_open_cache,
            module,
            request->fh,
            request->fh_len,
            request->fh_hash,
            request->open.r_vfs_private,
            chimera_vfs_open_hdl_callback,
            request);
    } else {
        chimera_vfs_open_hdl_callback(NULL, request);
    }

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
    uint64_t                        fh_hash;

    fh_hash = XXH3_64bits(fh, fhlen);

    module = chimera_vfs_get_module(thread, fh, fhlen);

    handle = chimera_vfs_open_cache_lookup(
        vfs->vfs_open_cache,
        module,
        fh,
        fhlen,
        fh_hash);

    if (handle) {
        callback(CHIMERA_VFS_OK, handle, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_hash(thread, fh, fhlen, fh_hash);

    request->opcode             = CHIMERA_VFS_OP_OPEN;
    request->complete           = chimera_vfs_open_complete;
    request->open.flags         = flags;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_open */
