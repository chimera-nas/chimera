// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test ftruncate extending a file
// Based on cthon/special/truncate.c from Connectathon 2004
//
// Verifies that ftruncate can both shrink and extend a file

#include "cthon_common.h"

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    int                   fd;
    struct stat           statb;

    cthon_Myname = "cthon_special_truncate";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hb:")) != -1) {
        switch (opt) {
            case 'b': break;
            default: break;
        }
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: ftruncate extend test\n", cthon_Myname);

    snprintf(str, sizeof(str), "%s/testfile", cthon_getcwd());

    fd = chimera_posix_open(str, O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        cthon_error("can't create %s", str);
        posix_test_fail(&env);
    }

    // Test 1: truncate to 0 (should work even on empty file)
    if (chimera_posix_ftruncate(fd, 0) < 0) {
        cthon_error("ftruncate to 0 failed");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    if (chimera_posix_stat(str, &statb) < 0) {
        cthon_error("stat after ftruncate(0) failed");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    if (statb.st_size != 0L) {
        fprintf(stderr, "\ttestfile not zero length after ftruncate(0), size=%ld\n",
                (long)statb.st_size);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    fprintf(stdout, "\tftruncate to 0 succeeded\n");

    // Test 2: extend file via ftruncate
    if (chimera_posix_ftruncate(fd, 10) < 0) {
        cthon_error("ftruncate to 10 failed");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    if (chimera_posix_stat(str, &statb) < 0) {
        cthon_error("stat after ftruncate(10) failed");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    if (statb.st_size != 10L) {
        fprintf(stderr, "\ttestfile length not set correctly by ftruncate(10), size=%ld\n",
                (long)statb.st_size);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    fprintf(stdout, "\tftruncate extend to 10 bytes succeeded\n");

    chimera_posix_close(fd);
    chimera_posix_unlink(str);

    fprintf(stdout, "\ttruncate test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
