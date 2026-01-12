// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif /* ifndef AT_FDCWD */

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

    fprintf(stderr, "Testing fchownat...\n");

    // Create test file
    fd = chimera_posix_open("/test/fchownat_test", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Test fchownat with AT_FDCWD
    rc = chimera_posix_fchownat(AT_FDCWD, "/test/fchownat_test", 1000, 1000, 0);

    if (rc != 0) {
        fprintf(stderr, "fchownat with AT_FDCWD failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify the owner changed
    rc = chimera_posix_stat("/test/fchownat_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st.st_uid != 1000 || st.st_gid != 1000) {
        fprintf(stderr, "fchownat: expected uid=1000 gid=1000, got uid=%u gid=%u\n",
                st.st_uid, st.st_gid);
        posix_test_fail(&env);
    }

    fprintf(stderr, "fchownat test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
