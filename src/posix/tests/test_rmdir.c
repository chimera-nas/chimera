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
    int                   rc;
    struct stat           st;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Create a directory
    rc = chimera_posix_mkdir("/test/testdir", 0755);
    if (rc != 0) {
        fprintf(stderr, "Failed to create directory: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Created directory /test/testdir\n");

    // Verify it exists
    rc = chimera_posix_stat("/test/testdir", &st);
    if (rc != 0) {
        fprintf(stderr, "Failed to stat directory: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Expected directory, got something else\n");
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified directory exists\n");

    // Remove the directory with rmdir
    rc = chimera_posix_rmdir("/test/testdir");
    if (rc != 0) {
        fprintf(stderr, "Failed to rmdir: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Removed directory with rmdir\n");

    // Verify it's gone
    rc = chimera_posix_stat("/test/testdir", &st);
    if (rc == 0) {
        fprintf(stderr, "Directory still exists after rmdir\n");
        posix_test_fail(&env);
    }

    if (errno != ENOENT) {
        fprintf(stderr, "Expected ENOENT, got: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified directory no longer exists\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
