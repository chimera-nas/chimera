// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests fread and fwrite

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
    const char           *test_data = "Hello, World! This is a test.";
    char                  buf[256];
    size_t                len = strlen(test_data);
    size_t                written, nread;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fread/fwrite...\n");

    // Write data to file
    fp = chimera_posix_fopen("/test/fwrite_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    written = chimera_posix_fwrite(test_data, 1, len, fp);
    if (written != len) {
        fprintf(stderr, "fwrite failed: wrote %zu, expected %zu\n", written, len);
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    // Read data back
    fp = chimera_posix_fopen("/test/fwrite_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    memset(buf, 0, sizeof(buf));
    nread = chimera_posix_fread(buf, 1, len, fp);
    if (nread != len) {
        fprintf(stderr, "fread failed: read %zu, expected %zu\n", nread, len);
        posix_test_fail(&env);
    }

    if (memcmp(buf, test_data, len) != 0) {
        fprintf(stderr, "fread data mismatch\n");
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fread/fwrite tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
