// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    struct stat           st;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fchown...\n");

    // Create test file
    fd = chimera_posix_open("/test/fchown_test", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Change owner using fchown
    rc = chimera_posix_fchown(fd, 1000, 1000);

    if (rc != 0) {
        fprintf(stderr, "fchown failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Verify the owner changed using fstat
    rc = chimera_posix_fstat(fd, &st);

    if (rc != 0) {
        fprintf(stderr, "fstat failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    if (st.st_uid != 1000 || st.st_gid != 1000) {
        fprintf(stderr, "fchown: expected uid=1000 gid=1000, got uid=%u gid=%u\n",
                st.st_uid, st.st_gid);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    fprintf(stderr, "fchown test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
