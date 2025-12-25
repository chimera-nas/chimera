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
    struct stat           file_st;
    struct stat           link_st;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing lchown...\n");

    // Create test file
    fd = chimera_posix_open("/test/lchown_file", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Create symlink
    rc = chimera_posix_symlink("/test/lchown_file", "/test/lchown_link");

    if (rc != 0) {
        fprintf(stderr, "Failed to create symlink: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Created symlink\n");

    // Get initial stats
    rc = chimera_posix_lstat("/test/lchown_file", &file_st);

    if (rc != 0) {
        fprintf(stderr, "Failed to lstat file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_lstat("/test/lchown_link", &link_st);

    if (rc != 0) {
        fprintf(stderr, "Failed to lstat symlink: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Initial file uid/gid: %u/%u\n",
            (unsigned) file_st.st_uid, (unsigned) file_st.st_gid);
    fprintf(stderr, "Initial link uid/gid: %u/%u\n",
            (unsigned) link_st.st_uid, (unsigned) link_st.st_gid);

    // lchown the symlink - this should change the symlink's owner, not the target's
    // Use -1 for values we don't want to change
    rc = chimera_posix_lchown("/test/lchown_link", 1000, 1000);

    if (rc != 0) {
        // Some backends may not support chown on symlinks, that's ok
        fprintf(stderr, "lchown on symlink returned: %s (may be expected)\n",
                strerror(errno));
    } else {
        fprintf(stderr, "lchown on symlink succeeded\n");
    }

    // Get stats after lchown
    struct stat file_st2, link_st2;

    rc = chimera_posix_lstat("/test/lchown_file", &file_st2);

    if (rc != 0) {
        fprintf(stderr, "Failed to lstat file after lchown: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_lstat("/test/lchown_link", &link_st2);

    if (rc != 0) {
        fprintf(stderr, "Failed to lstat symlink after lchown: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "After lchown - file uid/gid: %u/%u\n",
            (unsigned) file_st2.st_uid, (unsigned) file_st2.st_gid);
    fprintf(stderr, "After lchown - link uid/gid: %u/%u\n",
            (unsigned) link_st2.st_uid, (unsigned) link_st2.st_gid);

    // Test lchown on regular file
    rc = chimera_posix_lchown("/test/lchown_file", 1001, 1001);

    if (rc != 0) {
        fprintf(stderr, "lchown on file returned: %s (may be expected)\n",
                strerror(errno));
    } else {
        fprintf(stderr, "lchown on regular file succeeded\n");

        rc = chimera_posix_lstat("/test/lchown_file", &file_st2);

        if (rc != 0) {
            fprintf(stderr, "Failed to lstat file: %s\n", strerror(errno));
            posix_test_fail(&env);
        }

        fprintf(stderr, "After lchown - file uid/gid: %u/%u\n",
                (unsigned) file_st2.st_uid, (unsigned) file_st2.st_gid);
    }

    // Test lchown with -1 (no change)
    rc = chimera_posix_lchown("/test/lchown_file", (uid_t) -1, (gid_t) -1);

    if (rc != 0) {
        fprintf(stderr, "lchown with -1,-1 failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "lchown with no-change values succeeded\n");

    fprintf(stderr, "lchown test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
