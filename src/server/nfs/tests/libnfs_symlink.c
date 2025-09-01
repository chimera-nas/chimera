// SPDX-FileCopyrightText: 2025 Ben Jarvis
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
    char            buffer[80];

    libnfs_test_init(&env, argv, argc);

    if (nfs_mount(env.nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    rc = nfs_symlink(env.nfs, "/testtarget", "/testsymlink");

    if (rc < 0) {
        fprintf(stderr, "Failed to create symlink: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    rc = nfs_readlink(env.nfs, "/testsymlink", buffer, sizeof(buffer));

    if (memcmp(buffer, "/testtarget", rc) != 0) {
        fprintf(stderr, "Failed to read symlink: %s\n", nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    nfs_umount(env.nfs);

    libnfs_test_success(&env);

} /* main */