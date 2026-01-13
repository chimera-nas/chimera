// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test file rewind after truncate
// Based on cthon/special/rewind.c from Connectathon 2004
//
// Tests that file position is handled correctly after truncate

#include "cthon_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    char                  buffer[8192];
    int                   size = 8192;
    int                   fd;
    int                   i;
    off_t                 off;

    cthon_Myname = "cthon_special_rewind";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hb:")) != -1) {
        switch (opt) {
            case 'b': break;
            default: break;
        } /* switch */
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: rewind after truncate test\n", cthon_Myname);

    snprintf(str, sizeof(str), "%s/test.file", cthon_getcwd());

    fd = chimera_posix_open(str, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        cthon_error("can't create %s", str);
        posix_test_fail(&env);
    }

    // Write 3 blocks
    memset(buffer, 'X', size);
    for (i = 0; i < 3; i++) {
        if (chimera_posix_write(fd, buffer, size) != size) {
            cthon_error("write failed");
            chimera_posix_close(fd);
            posix_test_fail(&env);
        }
    }

    // Rewind to beginning
    off = chimera_posix_lseek(fd, (off_t) 0, SEEK_SET);
    if (off != 0) {
        fprintf(stderr, "\tfile offset=%ld after rewind, expected 0\n", (long) off);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    fprintf(stdout, "\trewind succeeded (offset=0)\n");

    // Truncate to 0
    if (chimera_posix_ftruncate(fd, 0) < 0) {
        cthon_error("ftruncate failed");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    fprintf(stdout, "\ttruncate to 0 succeeded\n");

    // Write 1 byte
    buffer[0] = 'Y';
    if (chimera_posix_write(fd, buffer, 1) != 1) {
        cthon_error("write of 1 byte failed");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Seek to end and verify position
    off = chimera_posix_lseek(fd, 0, SEEK_END);
    if (off != 1) {
        fprintf(stderr, "\tfile offset=%ld after write, expected 1\n", (long) off);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }
    fprintf(stdout, "\tfile position after truncate and write is correct (offset=1)\n");

    chimera_posix_close(fd);
    chimera_posix_unlink(str);

    fprintf(stdout, "\trewind test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
