// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests rewind

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
    const char           *test_data = "Hello";
    char                  buf[16];
    long                  pos;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing rewind...\n");

    fp = chimera_posix_fopen("/test/rewind_test.txt", "w+");
    if (!fp) {
        fprintf(stderr, "fopen failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_fwrite(test_data, 1, strlen(test_data), fp);
    pos = chimera_posix_ftell(fp);
    if (pos != 5) {
        fprintf(stderr, "ftell after write: expected 5, got %ld\n", pos);
        posix_test_fail(&env);
    }

    chimera_posix_rewind(fp);
    pos = chimera_posix_ftell(fp);
    if (pos != 0) {
        fprintf(stderr, "ftell after rewind: expected 0, got %ld\n", pos);
        posix_test_fail(&env);
    }

    // Read and verify
    memset(buf, 0, sizeof(buf));
    chimera_posix_fread(buf, 1, 5, fp);
    if (memcmp(buf, test_data, 5) != 0) {
        fprintf(stderr, "Data mismatch after rewind\n");
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "rewind tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
