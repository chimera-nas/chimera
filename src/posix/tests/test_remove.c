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
    struct stat           st;

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

    rc = chimera_posix_stat("/test/testfile", &st);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Created test file successfully\n");

    rc = chimera_posix_unlink("/test/testfile");

    if (rc != 0) {
        fprintf(stderr, "Failed to remove file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Removed file successfully\n");

    rc = chimera_posix_stat("/test/testfile", &st);

    if (rc == 0) {
        fprintf(stderr, "File still exists after remove\n");
        posix_test_fail(&env);
    }

    if (errno != ENOENT) {
        fprintf(stderr, "Expected ENOENT, got: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified file no longer exists\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
