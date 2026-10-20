// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
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
    chimera_posix_stat_t  st;
    chimera_posix_stat_t  pre;
    chimera_posix_stat_t  post;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/testfile", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = chimera_posix_rename("/test/testfile", "/test/renamedfile");

    if (rc != 0) {
        fprintf(stderr, "Failed to rename file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Renamed file successfully\n");

    rc = chimera_posix_stat("/test/testfile", &st);

    if (rc == 0) {
        fprintf(stderr, "Old file name still exists after rename\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/renamedfile", &st);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat renamed file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified rename: old name gone, new name exists\n");

    fd = chimera_posix_open("/test/renamedfile", O_RDONLY, 0);

    if (fd < 0) {
        fprintf(stderr, "Failed to open renamed file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Opened renamed file successfully\n");

    chimera_posix_close(fd);

    /* Rename over one link of a multi-linked inode: the surviving links lose
    * a link, which is a status change, so the inode's ctime must advance. */

    fd = chimera_posix_open("/test/hl_a", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create hardlink source: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = chimera_posix_link("/test/hl_a", "/test/hl_b");

    if (rc != 0) {
        fprintf(stderr, "Failed to link hl_a to hl_b: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_link("/test/hl_a", "/test/hl_c");

    if (rc != 0) {
        fprintf(stderr, "Failed to link hl_a to hl_c: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/victim_src", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create victim_src: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = chimera_posix_stat("/test/hl_b", &pre);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat hl_b before rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (pre.st_nlink != 3) {
        fprintf(stderr, "Expected 3 links on hl_b, got %ju\n", (uintmax_t) pre.st_nlink);
        posix_test_fail(&env);
    }

    /* Keep the rename out of the pre-stat's clock tick so a stale ctime
     * cannot compare equal by coincidence. */
    usleep(20000);

    rc = chimera_posix_rename("/test/victim_src", "/test/hl_a");

    if (rc != 0) {
        fprintf(stderr, "Failed to rename over hl_a: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/hl_b", &post);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat hl_b after rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (post.st_nlink != 2) {
        fprintf(stderr, "Expected 2 links on hl_b after rename, got %ju\n",
                (uintmax_t) post.st_nlink);
        posix_test_fail(&env);
    }

    if (CHIMERA_STAT_CTIM(post).tv_sec < CHIMERA_STAT_CTIM(pre).tv_sec ||
        (CHIMERA_STAT_CTIM(post).tv_sec == CHIMERA_STAT_CTIM(pre).tv_sec &&
         CHIMERA_STAT_CTIM(post).tv_nsec <= CHIMERA_STAT_CTIM(pre).tv_nsec)) {
        fprintf(stderr,
                "rename over a hard link left the surviving inode's ctime unchanged\n");
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified ctime advance on rename over a hard link\n");

    /* Rename over a last link: the replaced inode must be torn down, not kept
     * alive by the ctime bump, and the name must carry the source file. */

    fd = chimera_posix_open("/test/solo", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create solo: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    fd = chimera_posix_open("/test/solo_src", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create solo_src: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = chimera_posix_stat("/test/solo_src", &pre);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat solo_src before rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_rename("/test/solo_src", "/test/solo");

    if (rc != 0) {
        fprintf(stderr, "Failed to rename over solo: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/solo_src", &post);

    if (rc == 0) {
        fprintf(stderr, "solo_src still exists after rename over solo\n");
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/solo", &post);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat solo after rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (post.st_ino != pre.st_ino) {
        fprintf(stderr, "solo does not carry the source inode after rename\n");
        posix_test_fail(&env);
    }

    if (post.st_nlink != 1) {
        fprintf(stderr, "Expected 1 link on solo after rename, got %ju\n",
                (uintmax_t) post.st_nlink);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified rename over a last link\n");

    /* Rename of one link of an inode onto another link of the same inode is
     * a POSIX no-op: both names stay and nothing on the inode changes. */

    usleep(20000);

    rc = chimera_posix_stat("/test/hl_b", &pre);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat hl_b before no-op rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_rename("/test/hl_b", "/test/hl_c");

    if (rc != 0) {
        fprintf(stderr, "Failed no-op rename of hl_b to hl_c: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/hl_b", &post);

    if (rc != 0) {
        fprintf(stderr, "hl_b gone after no-op rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/hl_c", &post);

    if (rc != 0) {
        fprintf(stderr, "hl_c gone after no-op rename: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (CHIMERA_STAT_CTIM(post).tv_sec != CHIMERA_STAT_CTIM(pre).tv_sec ||
        CHIMERA_STAT_CTIM(post).tv_nsec != CHIMERA_STAT_CTIM(pre).tv_nsec) {
        fprintf(stderr, "no-op rename of a hardlink pair moved the inode's ctime\n");
        posix_test_fail(&env);
    }

    if (post.st_nlink != 2) {
        fprintf(stderr, "Expected 2 links on hl_c after no-op rename, got %ju\n",
                (uintmax_t) post.st_nlink);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Verified no-op rename of a hardlink pair\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
