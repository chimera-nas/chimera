#include <stdio.h>
#include <stdlib.h>
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw-nfs.h>
#include <nfsc/libnfs-raw-nfs4.h>
#include "server/server.h"

int main(int argc, char **argv)
{
    struct chimera_server *server;
    struct nfs_context *nfs;
    struct nfsdir *dir;
    struct nfsdirent *entry;

    server = chimera_server_init(NULL);

    nfs = nfs_init_context();
    if (!nfs)
    {
        fprintf(stderr, "Failed to initialize NFS context\n");
        return EXIT_FAILURE;
    }

    // Set NFS version to 3
    nfs_set_version(nfs, NFS_V4);
    nfs_set_timeout(nfs, 1000);
    // Mount the NFS export
    if (nfs_mount(nfs, "127.0.0.1", "/share") < 0)
    {
        fprintf(stderr, "Failed to mount NFS share: %s\n", nfs_get_error(nfs));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }

    printf("Mounted NFS share\n");

    if (nfs_opendir(nfs, "/", &dir) < 0)
    {
        fprintf(stderr, "Failed to open root directory: %s\n", nfs_get_error(nfs));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }
    printf("Reading entries in the root directory:\n");

    // Read directory entries
    while ((entry = nfs_readdir(nfs, dir)) != NULL)
    {
        printf(" - %s\n", entry->name);
    }

    // Check if the loop ended due to an error
    if (nfs_get_error(nfs))
    {
        fprintf(stderr, "Error reading directory: %s\n", nfs_get_error(nfs));
    }

    // Close directory and unmount the share
    nfs_closedir(nfs, dir);
    nfs_umount(nfs);
    nfs_destroy_context(nfs);

    chimera_server_destroy(server);

    return EXIT_SUCCESS;
}