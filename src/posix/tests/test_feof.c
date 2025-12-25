// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests feof, ferror, and clearerr

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
    char                  buf[16];

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing feof/ferror...\n");

    // Create a small test file
    fp = chimera_posix_fopen("/test/eof_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_fwrite("AB", 1, 2, fp);
    chimera_posix_fclose(fp);

    // Read and test feof
    fp = chimera_posix_fopen("/test/eof_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Initially feof should return 0
    if (chimera_posix_feof(fp) != 0) {
        fprintf(stderr, "feof should be 0 initially\n");
        posix_test_fail(&env);
    }

    // Read all data
    chimera_posix_fread(buf, 1, 10, fp);

    // After reading past end, feof should return non-zero
    if (chimera_posix_feof(fp) == 0) {
        fprintf(stderr, "feof should be non-zero after reading past end\n");
        posix_test_fail(&env);
    }

    // Test clearerr
    chimera_posix_clearerr(fp);
    if (chimera_posix_feof(fp) != 0) {
        fprintf(stderr, "feof should be 0 after clearerr\n");
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "feof/ferror tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
