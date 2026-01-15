// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test exclusive create (O_EXCL)
// Based on cthon/special/excltest.c from Connectathon
//
// Verifies that O_CREAT | O_EXCL properly fails with EEXIST on second create

#include "cthon_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  testfile[MAXPATHLEN];
    int                   count;
    int                   res, i;

    cthon_Myname = "cthon_special_excltest";

    posix_test_init(&env, argv, argc);

    optind = 1;
    count  = 2;
    while ((opt = getopt(argc, argv, "hb:c:")) != -1) {
        switch (opt) {
            case 'b': break;
            case 'c':
                count = atoi(optarg);
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

    fprintf(stdout, "%s: exclusive create test\n", cthon_Myname);

    snprintf(testfile, sizeof(testfile), "%s/exctest.file", cthon_getcwd());

    // Clean up any leftover file
    chimera_posix_unlink(testfile);

    for (i = 0; i < count; i++) {
        res = chimera_posix_open(testfile, O_CREAT | O_EXCL, 0777);
        if (i == 0) {
            // First create should succeed
            if (res < 0) {
                cthon_error("exclusive create failed on first try: %s", strerror(errno));
                posix_test_fail(&env);
            }
            fprintf(stdout, "\tfirst exclusive create succeeded (fd=%d)\n", res);
            chimera_posix_close(res);
        } else {
            // Subsequent creates should fail with EEXIST
            if (res >= 0) {
                fprintf(stderr, "\texclusive create succeeded when it should have failed\n");
                chimera_posix_close(res);
                posix_test_fail(&env);
            } else if (errno != EEXIST) {
                cthon_error("exclusive create failed with wrong errno: %s (expected EEXIST)",
                            strerror(errno));
                posix_test_fail(&env);
            }
            fprintf(stdout, "\texclusive create %d correctly failed with EEXIST\n", i + 1);
        }
    }

    // Clean up
    chimera_posix_unlink(testfile);

    fprintf(stdout, "\texclusive create test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
} /* main */
