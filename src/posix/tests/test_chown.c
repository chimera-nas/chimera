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

    fprintf(stderr, "Testing chown...\n");

    // Create test file
    fd = chimera_posix_open("/test/chown_test", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Change owner to uid=1000, gid=1000
    rc = chimera_posix_chown("/test/chown_test", 1000, 1000);

    if (rc != 0) {
        fprintf(stderr, "chown failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify the owner changed
    rc = chimera_posix_stat("/test/chown_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st.st_uid != 1000 || st.st_gid != 1000) {
        fprintf(stderr, "chown: expected uid=1000 gid=1000, got uid=%u gid=%u\n",
                st.st_uid, st.st_gid);
        posix_test_fail(&env);
    }

    // Test changing only uid (-1 for gid means don't change)
    rc = chimera_posix_chown("/test/chown_test", 2000, -1);

    if (rc != 0) {
        fprintf(stderr, "chown (uid only) failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/chown_test", &st);

    if (rc != 0) {
        fprintf(stderr, "stat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (st.st_uid != 2000 || st.st_gid != 1000) {
        fprintf(stderr, "chown: expected uid=2000 gid=1000, got uid=%u gid=%u\n",
                st.st_uid, st.st_gid);
        posix_test_fail(&env);
    }

    fprintf(stderr, "chown test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
