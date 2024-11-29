#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
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
    char                   buffer[80];

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

    printf("Creating a symlink in the share\n");

    (void) unlink("/build/testsymlink");

    rc = nfs_symlink(nfs, "/testtarget", "/testsymlink");

    if (rc < 0) {
        fprintf(stderr, "Failed to create symlink: %s\n", nfs_get_error(nfs));
        nfs_destroy_context(nfs);
        return EXIT_FAILURE;
    }

    rc = nfs_readlink(nfs, "/testsymlink", buffer, sizeof(buffer));

    if (memcmp(buffer, "/testtarget", rc) != 0) {
        fprintf(stderr, "Failed to read symlink: %s\n", nfs_get_error(nfs));
        return EXIT_FAILURE;
    }

    nfs_umount(nfs);

    nfs_destroy_context(nfs);

    chimera_server_destroy(server);

    return EXIT_SUCCESS;
} /* main */