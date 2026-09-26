// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD     -100
#endif /* ifndef AT_FDCWD */

#ifndef AT_REMOVEDIR
#define AT_REMOVEDIR 0x200
#endif /* ifndef AT_REMOVEDIR */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    chimera_posix_stat_t  st;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing mkdirat...\n");

    // Test with AT_FDCWD
    rc = chimera_posix_mkdirat(AT_FDCWD, "/test/mkdirat_test", 0755);
    if (rc != 0) {
        fprintf(stderr, "mkdirat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify it exists
    rc = chimera_posix_fstatat(AT_FDCWD, "/test/mkdirat_test", &st, 0);
    if (rc != 0) {
        fprintf(stderr, "fstatat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Expected directory\n");
        posix_test_fail(&env);
    }

    /* A real directory descriptor, opened with O_DIRECTORY this time: a
     * single-component name, then one below a directory just made. */
    {
        int dfd = chimera_posix_open("/test/mkdirat_test", O_RDONLY | O_DIRECTORY);

        if (dfd < 0) {
            fprintf(stderr, "open mkdirat_test failed: %s\n", strerror(errno));
            posix_test_fail(&env);
        }

        rc = chimera_posix_mkdirat(dfd, "a", 0755);
        if (rc != 0) {
            fprintf(stderr, "mkdirat(dfd, a) failed: %s\n", strerror(errno));
            posix_test_fail(&env);
        }

        rc = chimera_posix_mkdirat(dfd, "a/b", 0755);
        if (rc != 0) {
            fprintf(stderr, "mkdirat(dfd, a/b) failed: %s\n", strerror(errno));
            posix_test_fail(&env);
        }

        rc = chimera_posix_fstatat(dfd, "a/b", &st, 0);
        if (rc != 0 || !S_ISDIR(st.st_mode)) {
            fprintf(stderr, "fstatat(dfd, a/b) failed: %s\n", strerror(errno));
            posix_test_fail(&env);
        }

        rc = chimera_posix_mkdirat(dfd, "a/b", 0755);
        if (rc == 0 || errno != EEXIST) {
            fprintf(stderr, "mkdirat(dfd, a/b) again expected EEXIST, got rc=%d errno=%s\n",
                    rc, strerror(errno));
            posix_test_fail(&env);
        }

        rc = chimera_posix_mkdirat(dfd, "missing/c", 0755);
        if (rc == 0 || errno != ENOENT) {
            fprintf(stderr, "mkdirat(dfd, missing/c) expected ENOENT, got rc=%d errno=%s\n",
                    rc, strerror(errno));
            posix_test_fail(&env);
        }

        rc = chimera_posix_unlinkat(dfd, "a/b", AT_REMOVEDIR);
        if (rc != 0) {
            fprintf(stderr, "unlinkat(dfd, a/b, AT_REMOVEDIR) failed: %s\n", strerror(errno));
            posix_test_fail(&env);
        }

        rc = chimera_posix_unlinkat(dfd, "a", AT_REMOVEDIR);
        if (rc != 0) {
            fprintf(stderr, "unlinkat(dfd, a, AT_REMOVEDIR) failed: %s\n", strerror(errno));
            posix_test_fail(&env);
        }

        chimera_posix_close(dfd);
    }

    // Cleanup with unlinkat + AT_REMOVEDIR
    rc = chimera_posix_unlinkat(AT_FDCWD, "/test/mkdirat_test", AT_REMOVEDIR);
    if (rc != 0) {
        fprintf(stderr, "Failed to remove directory: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "mkdirat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
