// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "libnfs_test_common.h"

#define WRITE_LEN (16  * 1024 * 1024)

int
main(
    int    argc,
    char **argv)
{
    struct test_env env;
    struct nfsfh   *fh;
    int             rc;
    char           *buffer;

    buffer = calloc(1, WRITE_LEN);

    memset(buffer, 'a', WRITE_LEN);

    libnfs_test_init(&env, argv, argc);

    fprintf(stderr, "Mounting NFS share\n");

    if (nfs_mount(env.nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    printf("Creating a file in the share\n");

    rc = nfs_create(env.nfs, "/testfile", O_CREAT | O_WRONLY, 0, &fh);

    if (rc < 0) {
        fprintf(stderr, "Failed to create file: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    printf("Writing to the file\n");
    nfs_write(env.nfs, fh, WRITE_LEN, buffer);

    printf("Closing the file\n");
    nfs_close(env.nfs, fh);

    printf("Unmounting the share\n");
    nfs_umount(env.nfs);

    libnfs_test_success(&env);

    free(buffer);
    return EXIT_SUCCESS;
} /* main */