#include "nfs_internal.h"
#include "vfs/vfs_internal.h"

void
nfs3_umount(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct nfs_client_mount  *mount  = NULL;
    struct nfs_client_server *server = NULL;

    pthread_mutex_lock(&shared->lock);

    DL_FOREACH(shared->mounts, mount)
    {
        if (mount->fhlen == request->umount.mount->fhlen && memcmp(mount->fh, request->umount.mount->fh, mount->fhlen)
            == 0) {
            break;
        }
    }

    if (!mount) {
        pthread_mutex_unlock(&shared->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    DL_DELETE(shared->mounts, mount);

    server = mount->server;

    free(mount);

    server->refcnt--;

    if (server->refcnt == 0) {
        /* Server is no longer in use, so we can remove it */
        shared->servers[server->index] = NULL;
        free(server);
    }

    pthread_mutex_unlock(&shared->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* nfs3_mount */