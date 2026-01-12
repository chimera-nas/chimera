// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests fgets and fputs

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
    char                  buf[256];

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fgets/fputs...\n");

    // Test fputs
    fp = chimera_posix_fopen("/test/fputs_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_fputs("Line 1\n", fp);
    if (rc == EOF) {
        fprintf(stderr, "fputs failed\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_fputs("Line 2\n", fp);
    if (rc == EOF) {
        fprintf(stderr, "fputs failed\n");
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    // Test fgets
    fp = chimera_posix_fopen("/test/fputs_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_fgets(buf, sizeof(buf), fp) == NULL) {
        fprintf(stderr, "fgets failed\n");
        posix_test_fail(&env);
    }
    if (strcmp(buf, "Line 1\n") != 0) {
        fprintf(stderr, "fgets: expected 'Line 1\\n', got '%s'\n", buf);
        posix_test_fail(&env);
    }

    if (chimera_posix_fgets(buf, sizeof(buf), fp) == NULL) {
        fprintf(stderr, "fgets failed\n");
        posix_test_fail(&env);
    }
    if (strcmp(buf, "Line 2\n") != 0) {
        fprintf(stderr, "fgets: expected 'Line 2\\n', got '%s'\n", buf);
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fgets/fputs tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
