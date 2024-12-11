#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"

static void
chimera_vfs_close_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_complete(request);
    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_close_complete */

void
chimera_vfs_close(
    struct chimera_vfs_thread *thread,
    struct chimera_vfs_module *module,
    const void                *fh,
    uint32_t                   fhlen,
    uint64_t                   vfs_private)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread);

    request->opcode   = CHIMERA_VFS_OP_CLOSE;
    request->complete = chimera_vfs_close_complete;
    memcpy(request->close.fh, fh, fhlen);
    request->close.fh_len      = fhlen;
    request->close.vfs_private = vfs_private;

    chimera_vfs_dispatch(thread, module, request);

} /* chimera_vfs_close */
