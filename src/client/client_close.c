#include "client_internal.h"
#include "vfs/vfs_release.h"

SYMBOL_EXPORT void
chimera_client_close(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *oh)
{
    chimera_vfs_release(thread->vfs_thread, oh);

} /* chimera_client_close */