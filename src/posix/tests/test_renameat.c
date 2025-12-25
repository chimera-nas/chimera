// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   fd;
    struct stat           st;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing renameat...\n");

    // Create a file
    fd = chimera_posix_openat(AT_FDCWD, "/test/rename_src.txt", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create source file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    // Rename it
    rc = chimera_posix_renameat(AT_FDCWD, "/test/rename_src.txt",
                                 AT_FDCWD, "/test/rename_dst.txt");
    if (rc != 0) {
        fprintf(stderr, "renameat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify source is gone and dest exists
    rc = chimera_posix_fstatat(AT_FDCWD, "/test/rename_src.txt", &st, 0);
    if (rc == 0) {
        fprintf(stderr, "Source file still exists after rename\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_fstatat(AT_FDCWD, "/test/rename_dst.txt", &st, 0);
    if (rc != 0) {
        fprintf(stderr, "Dest file doesn't exist after rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Cleanup
    chimera_posix_unlinkat(AT_FDCWD, "/test/rename_dst.txt", 0);

    fprintf(stderr, "renameat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
