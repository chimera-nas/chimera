// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests fgetc and fputc

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

    fprintf(stderr, "Testing fgetc/fputc...\n");

    // Test fputc
    fp = chimera_posix_fopen("/test/fputc_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_fputc('H', fp) == EOF) {
        fprintf(stderr, "fputc failed\n");
        posix_test_fail(&env);
    }
    if (chimera_posix_fputc('i', fp) == EOF) {
        fprintf(stderr, "fputc failed\n");
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    // Test fgetc
    fp = chimera_posix_fopen("/test/fputc_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    c = chimera_posix_fgetc(fp);
    if (c != 'H') {
        fprintf(stderr, "fgetc: expected 'H', got '%c' (%d)\n", c, c);
        posix_test_fail(&env);
    }

    c = chimera_posix_fgetc(fp);
    if (c != 'i') {
        fprintf(stderr, "fgetc: expected 'i', got '%c' (%d)\n", c, c);
        posix_test_fail(&env);
    }

    c = chimera_posix_fgetc(fp);
    if (c != EOF) {
        fprintf(stderr, "fgetc: expected EOF, got '%c' (%d)\n", c, c);
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fgetc/fputc tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
