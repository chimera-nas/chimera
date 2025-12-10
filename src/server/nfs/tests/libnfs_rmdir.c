// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "libnfs_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct test_env env;
    int             rc;

    libnfs_test_init(&env, argv, argc);

    if (nfs_mount(env.nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    rc = nfs_mkdir(env.nfs, "/testdir");

    if (rc < 0) {
        fprintf(stderr, "Failed to create directory: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    rc = nfs_rmdir(env.nfs, "/testdir");

    if (rc < 0) {
        fprintf(stderr, "Failed to unlink file: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    nfs_umount(env.nfs);

    libnfs_test_success(&env);

} /* main */
