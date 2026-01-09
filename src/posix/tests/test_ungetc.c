// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests ungetc

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
    int                   c;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing ungetc...\n");

    // Create test file
    fp = chimera_posix_fopen("/test/ungetc_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_fwrite("ABC", 1, 3, fp);
    chimera_posix_fclose(fp);

    // Test ungetc
    fp = chimera_posix_fopen("/test/ungetc_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Read 'A'
    c = chimera_posix_fgetc(fp);
    if (c != 'A') {
        fprintf(stderr, "fgetc: expected 'A', got '%c'\n", c);
        posix_test_fail(&env);
    }

    // Push 'X' back
    if (chimera_posix_ungetc('X', fp) == EOF) {
        fprintf(stderr, "ungetc failed\n");
        posix_test_fail(&env);
    }

    // Read again - should get 'X'
    c = chimera_posix_fgetc(fp);
    if (c != 'X') {
        fprintf(stderr, "fgetc after ungetc: expected 'X', got '%c'\n", c);
        posix_test_fail(&env);
    }

    // Next should be 'B'
    c = chimera_posix_fgetc(fp);
    if (c != 'B') {
        fprintf(stderr, "fgetc: expected 'B', got '%c'\n", c);
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "ungetc tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
