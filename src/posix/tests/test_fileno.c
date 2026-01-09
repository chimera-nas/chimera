// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests fileno

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
    int                   fd;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fileno...\n");

    fp = chimera_posix_fopen("/test/fileno_test.txt", "w");
    if (!fp) {
        fprintf(stderr, "fopen failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_fileno(fp);
    if (fd < 0) {
        fprintf(stderr, "fileno failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "fileno returned fd=%d\n", fd);

    chimera_posix_fclose(fp);

    fprintf(stderr, "fileno tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
