#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"

void
chimera_vfs_release(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs *vfs = thread->vfs;

    chimera_vfs_open_cache_release(vfs->vfs_open_cache, handle);
} /* chimera_vfs_close */
