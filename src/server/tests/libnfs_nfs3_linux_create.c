#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw-nfs.h>
#include <nfsc/libnfs-raw-nfs4.h>
#include "server/server.h"
#include "test_common.h"
int
main(
    int    argc,
    char **argv)
{
    struct chimera_server *server;
    struct nfs_context    *nfs;
    struct nfsfh          *fh;
    int                    rc;

    server = test_server_init(argv, argc);

    chimera_server_create_share(server, "linux", "share", "/build");

    nfs = nfs_init_context();
    if (!nfs) {
        fprintf(stderr, "Failed to initialize NFS context\n");
        return EXIT_FAILURE;
    }

    nfs_set_version(nfs, NFS_V3);

    (void) unlink("/build/testfile");

    fprintf(stderr, "Mounting NFS share\n");

    if (nfs_mount(nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n", nfs_get_error(nfs));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }

    printf("Creating a file in the share\n");

    rc = nfs_create(nfs, "/testfile", O_CREAT | O_WRONLY, 0, &fh);

    if (rc < 0) {
        fprintf(stderr, "Failed to create file: %s\n", nfs_get_error(nfs));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }

    printf("Writing to the file\n");
    nfs_write(nfs, fh, 13, "Hello, world!");

    printf("Closing the file\n");
    nfs_close(nfs, fh);

    printf("Unmounting the share\n");

    nfs_umount(nfs);

    nfs_destroy_context(nfs);

    chimera_server_destroy(server);

    return EXIT_SUCCESS;
} /* main */