#pragma once

#include "vfs_open_cache.h"

static inline void
chimera_vfs_release(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    if (handle->cache_id == CHIMERA_VFS_OPEN_ID_SYNTHETIC) {
        chimera_vfs_synth_handle_free(thread, handle);
    } else if (handle->cache_id == CHIMERA_VFS_OPEN_ID_PATH) {
        chimera_vfs_open_cache_release(thread->vfs->vfs_open_path_cache, handle);
    } else if (handle->cache_id == CHIMERA_VFS_OPEN_ID_FILE) {
        chimera_vfs_open_cache_release(thread->vfs->vfs_open_file_cache, handle);
    } else {
        chimera_vfs_abort("invalid cache id");
    }
} /* chimera_vfs_release_handle */