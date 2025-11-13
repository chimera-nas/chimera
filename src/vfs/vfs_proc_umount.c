
#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_release.h"
#include "common/macros.h"


static void
chimera_vfs_umount_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_umount_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(thread, CHIMERA_VFS_OK, request->proto_private_data);

    chimera_vfs_request_free(thread, request);

    free(request->umount.mount->path);
    free(request->umount.mount);

} /* chimera_vfs_umount */


SYMBOL_EXPORT void
chimera_vfs_umount(
    struct chimera_vfs_thread    *thread,
    const char                   *mount_path,
    chimera_vfs_umount_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs         *vfs = thread->vfs;
    struct chimera_vfs_mount   *mount;
    struct chimera_vfs_request *request;
    const char                 *path = mount_path;

    while (*path == '/') {
        path++;
    }

    pthread_rwlock_rdlock(&vfs->mounts_lock);

    DL_FOREACH(vfs->mounts, mount)
    {
        if (strcmp(mount->path, path) == 0) {
            break;
        }
    }

    if (mount) {
        DL_DELETE(vfs->mounts, mount);
    }

    pthread_rwlock_unlock(&vfs->mounts_lock);

    if (!mount) {
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, &mount->fh, mount->fhlen);

    request->opcode               = CHIMERA_VFS_OP_UMOUNT;
    request->complete             = chimera_vfs_umount_complete;
    request->umount.mount         = mount;
    request->umount.mount_private = mount->mount_private;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_umount */