#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
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
    int                    rc;

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

    printf("Removing a directory in the share\n");

    (void) unlink("/build/testdir");
    mkdir("/build/testdir", 0755);

    rc = nfs_rmdir(nfs, "/testdir");

    if (rc < 0) {
        fprintf(stderr, "Failed to unlink file: %s\n", nfs_get_error(nfs));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }

    nfs_umount(nfs);

    nfs_destroy_context(nfs);

    chimera_server_destroy(server);

    return EXIT_SUCCESS;
} /* main */