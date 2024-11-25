#include <stdio.h>
#include <stdlib.h>
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw-nfs.h>
#include <nfsc/libnfs-raw-nfs4.h>
#include "server/server.h"

int
main(
    int    argc,
    char **argv)
{
    struct chimera_server *server;
    struct nfs_context    *nfs;
    struct nfsdir         *dir;
    struct nfsdirent      *entry;

    server = chimera_server_init(NULL);

    chimera_server_create_share(server, "linux", "share", "/build");

    nfs = nfs_init_context();
    if (!nfs) {
        fprintf(stderr, "Failed to initialize NFS context\n");
        return EXIT_FAILURE;
    }

    nfs_set_version(nfs, NFS_V3);

    if (nfs_mount(nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n", nfs_get_error(nfs));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }

    printf("Mounted NFS share\n");

    if (nfs_opendir(nfs, "/", &dir) < 0) {
        fprintf(stderr, "Failed to open root directory: %s\n", nfs_get_error(nfs
                                                                             ));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }

    printf("Reading entries in the root directory:\n");

    while ((entry = nfs_readdir(nfs, dir)) != NULL) {
        printf(" - %s\n", entry->name);
    }

    nfs_closedir(nfs, dir);
    nfs_umount(nfs);
    nfs_destroy_context(nfs);

    chimera_server_destroy(server);

    return EXIT_SUCCESS;
} /* main */