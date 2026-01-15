// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Touch n files
// Based on cthon/special/touchn.c from Connectathon
//
// Creates n files in the test directory

#include "cthon_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    int                   n;
    int                   fd;
    char                  buf[MAXPATHLEN];

    cthon_Myname = "cthon_special_touchn";

    posix_test_init(&env, argv, argc);

    optind = 1;
    n      = 10; // default
    while ((opt = getopt(argc, argv, "hb:n:")) != -1) {
        switch (opt) {
            case 'b': break;
            case 'n':
                n = atoi(optarg);
                break;
            default: break;
        } /* switch */
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: create %d files test\n", cthon_Myname, n);

    for (; n > 0; n--) {
        snprintf(buf, sizeof(buf), "%s/name%d", cthon_getcwd(), n);
        fd = chimera_posix_open(buf, O_CREAT | O_WRONLY, 0666);
        if (fd < 0) {
            cthon_error("can't create %s: %s", buf, strerror(errno));
            posix_test_fail(&env);
        }
        chimera_posix_close(fd);
    }

    fprintf(stdout, "\ttouchN test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
