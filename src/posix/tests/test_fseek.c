// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests fseek and ftell

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
    const char           *test_data = "0123456789ABCDEF";
    char                  buf[16];
    size_t                len = strlen(test_data);
    long                  pos;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fseek/ftell...\n");

    // Create test file
    fp = chimera_posix_fopen("/test/fseek_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen for write failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_fwrite(test_data, 1, len, fp);
    chimera_posix_fclose(fp);

    // Open for reading
    fp = chimera_posix_fopen("/test/fseek_test.txt", "r");
    if (!fp) {
        fprintf(stderr, "fopen for read failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Test ftell at beginning
    pos = chimera_posix_ftell(fp);
    if (pos != 0) {
        fprintf(stderr, "ftell at start: expected 0, got %ld\n", pos);
        posix_test_fail(&env);
    }

    // Read 5 bytes
    chimera_posix_fread(buf, 1, 5, fp);
    pos = chimera_posix_ftell(fp);
    if (pos != 5) {
        fprintf(stderr, "ftell after read: expected 5, got %ld\n", pos);
        posix_test_fail(&env);
    }

    // Seek to position 10
    if (chimera_posix_fseek(fp, 10, SEEK_SET) != 0) {
        fprintf(stderr, "fseek SEEK_SET failed\n");
        posix_test_fail(&env);
    }
    pos = chimera_posix_ftell(fp);
    if (pos != 10) {
        fprintf(stderr, "ftell after SEEK_SET: expected 10, got %ld\n", pos);
        posix_test_fail(&env);
    }

    // Seek backwards by 3
    if (chimera_posix_fseek(fp, -3, SEEK_CUR) != 0) {
        fprintf(stderr, "fseek SEEK_CUR failed\n");
        posix_test_fail(&env);
    }
    pos = chimera_posix_ftell(fp);
    if (pos != 7) {
        fprintf(stderr, "ftell after SEEK_CUR: expected 7, got %ld\n", pos);
        posix_test_fail(&env);
    }

    // Seek to end
    if (chimera_posix_fseek(fp, 0, SEEK_END) != 0) {
        fprintf(stderr, "fseek SEEK_END failed\n");
        posix_test_fail(&env);
    }
    pos = chimera_posix_ftell(fp);
    if (pos != (long) len) {
        fprintf(stderr, "ftell after SEEK_END: expected %zu, got %ld\n", len, pos);
        posix_test_fail(&env);
    }

    chimera_posix_fclose(fp);

    fprintf(stderr, "fseek/ftell tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
