// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests fopen and fclose

#include <string.h>
#include "posix_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    CHIMERA_FILE         *fp;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fopen/fclose...\n");

    // Test opening a new file for writing
    fp = chimera_posix_fopen("/test/testfile.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_fclose(fp) != 0) {
        fprintf(stderr, "fclose failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Test opening the file for reading
    fp = chimera_posix_fopen("/test/testfile.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_fclose(fp) != 0) {
        fprintf(stderr, "fclose failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Test opening a non-existent file for reading
    fp = chimera_posix_fopen("/test/nonexistent.txt", "r");
    if (fp != NULL) {
        fprintf(stderr, "fopen should have failed for non-existent file\n");
        posix_test_fail(&env);
    }

    fprintf(stderr, "fopen/fclose tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
