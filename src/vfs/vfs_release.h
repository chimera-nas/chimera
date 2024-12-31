#pragma once

#include "vfs_open_cache.h"

static inline void
chimera_vfs_populate_handle(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        vfs_private_data)
{
    struct vfs_open_cache *cache;

    if (handle->cache_id == CHIMERA_VFS_OPEN_ID_PATH) {
        cache = thread->vfs->vfs_open_path_cache;
    } else if (handle->cache_id == CHIMERA_VFS_OPEN_ID_FILE) {
        cache = thread->vfs->vfs_open_file_cache;
    } else {
        return;
    }

    chimera_vfs_open_cache_populate(thread, cache, handle, vfs_private_data);

} /* chimera_vfs_populate_handle */

static inline void
chimera_vfs_release(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    if (handle->cache_id == CHIMERA_VFS_OPEN_ID_SYNTHETIC) {
        chimera_vfs_synth_handle_free(thread, handle);
    } else if (handle->cache_id == CHIMERA_VFS_OPEN_ID_PATH) {
        chimera_vfs_open_cache_release(thread, thread->vfs->vfs_open_path_cache, handle);
    } else if (handle->cache_id == CHIMERA_VFS_OPEN_ID_FILE) {
        chimera_vfs_open_cache_release(thread, thread->vfs->vfs_open_file_cache, handle);
    } else {
        chimera_vfs_abort("invalid cache id");
    }
} /* chimera_vfs_release_handle */