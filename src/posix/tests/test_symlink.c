// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    char                  target[256];
    ssize_t               len;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/testfile", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = chimera_posix_symlink("/test/testfile", "/test/symlink");

    if (rc != 0) {
        fprintf(stderr, "Failed to create symlink: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Created symlink successfully\n");

    len = chimera_posix_readlink("/test/symlink", target, sizeof(target) - 1);

    if (len < 0) {
        fprintf(stderr, "Failed to readlink: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    target[len] = '\0';

    if (len != 14 || strcmp(target, "/test/testfile") != 0) {
        fprintf(stderr, "Readlink returned wrong target: '%s' (expected '/test/testfile')\n", target);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Readlink successful: '%s'\n", target);

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
