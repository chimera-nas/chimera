// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests fgetpos and fsetpos

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
    const char           *test_data = "ABCDEFGHIJ";
    chimera_fpos_t        pos;
    char                  buf[16];

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fgetpos/fsetpos...\n");

    // Create test file
    fp = chimera_posix_fopen("/test/fpos_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_fwrite(test_data, 1, strlen(test_data), fp);
    chimera_posix_fclose(fp);

    // Test fgetpos/fsetpos
    fp = chimera_posix_fopen("/test/fpos_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Read 3 chars and save position
    chimera_posix_fread(buf, 1, 3, fp);
    if (chimera_posix_fgetpos(fp, &pos) != 0) {
        fprintf(stderr, "fgetpos failed\n");
        posix_test_fail(&env);
    }

    // Read more
    chimera_posix_fread(buf, 1, 4, fp);

    // Restore position
    if (chimera_posix_fsetpos(fp, &pos) != 0) {
        fprintf(stderr, "fsetpos failed\n");
        posix_test_fail(&env);
    }

    // Read from saved position
    memset(buf, 0, sizeof(buf));
    chimera_posix_fread(buf, 1, 3, fp);
    if (memcmp(buf, "DEF", 3) != 0) {
        fprintf(stderr, "Data mismatch after fsetpos: got '%s'\n", buf);
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fgetpos/fsetpos tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
