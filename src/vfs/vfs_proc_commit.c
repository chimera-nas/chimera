#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
static void
chimera_vfs_commit_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_commit_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_commit_complete */

void
chimera_vfs_commit(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    chimera_vfs_commit_callback_t   callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, handle->fh, handle->fh_len);

    request->opcode             = CHIMERA_VFS_OP_COMMIT;
    request->complete           = chimera_vfs_commit_complete;
    request->commit.handle      = handle;
    request->commit.offset      = offset;
    request->commit.length      = count;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);


} /* chimera_vfs_write */